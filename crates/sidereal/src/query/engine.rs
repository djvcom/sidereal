//! DataFusion query engine setup.
//!
//! Configures a DataFusion SessionContext with ListingTables for traces,
//! metrics, and logs with automatic Hive-style partition pruning.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use object_store::ObjectStore;
use url::Url;

use super::cache::BlockCache;
use crate::attributes::AttributeRegistry;
use crate::schema::{
    logs::logs_storage_schema, metrics::number_metrics_storage_schema,
    traces::traces_storage_schema, PARTITION_COLUMNS,
};
use crate::storage::Signal;
use crate::TelemetryError;

/// Memory pool strategy for query execution.
#[derive(Debug, Clone, Copy, Default)]
pub enum MemoryPoolStrategy {
    /// No memory limit (default).
    #[default]
    Unlimited,
    /// Greedy allocation up to a limit, then fail.
    Greedy {
        /// Maximum memory in bytes.
        max_bytes: usize,
    },
    /// Fair allocation with spilling support.
    FairSpill {
        /// Maximum memory in bytes.
        max_bytes: usize,
    },
}

/// Default query timeout (30 seconds).
pub const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Default size of the in-memory object cache (256 MiB).
pub const DEFAULT_QUERY_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Default size of the cache of parsed file footers (256 MiB).
///
/// Every query needs the footer of every file it considers. Parsing one
/// costs a fraction of a millisecond, which adds up over hundreds of
/// files, so parsed footers are kept between queries. DataFusion's own
/// default of 50 MiB holds too few: on the benchmark harness's 725-file
/// dataset it was overflowing, and queries spanning all files took nearly
/// twice as long as with room for every footer.
pub const DEFAULT_METADATA_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Default DataFusion batch size for query execution.
///
/// This value balances memory usage against processing efficiency for typical
/// telemetry workloads. Smaller values reduce memory pressure but increase
/// per-batch overhead.
pub const DEFAULT_QUERY_BATCH_SIZE: usize = 8192;

/// How many times a query is re-planned after a data file turns out to be
/// missing from storage.
const MISSING_FILE_RETRIES: usize = 3;

/// Whether `error` was caused by an object that no longer exists.
///
/// Storage errors reach us wrapped in several layers (DataFusion, Arrow,
/// Parquet), so the whole chain is searched.
fn is_missing_object(error: &(dyn std::error::Error + 'static)) -> bool {
    use arrow::error::ArrowError;
    use datafusion::error::DataFusionError;
    use parquet::errors::ParquetError;

    if let Some(store_error) = error.downcast_ref::<object_store::Error>() {
        return matches!(store_error, object_store::Error::NotFound { .. });
    }
    let inner: Option<&(dyn std::error::Error + 'static)> =
        if let Some(df_error) = error.downcast_ref::<DataFusionError>() {
            match df_error {
                DataFusionError::ObjectStore(e) => Some(e.as_ref()),
                DataFusionError::ArrowError(e, _) => Some(e.as_ref()),
                DataFusionError::ParquetError(e) => Some(e.as_ref()),
                DataFusionError::External(e) => Some(e.as_ref()),
                DataFusionError::Context(_, e) => Some(e.as_ref()),
                DataFusionError::Shared(e) => Some(e.as_ref()),
                _ => None,
            }
        } else if let Some(ArrowError::ExternalError(e)) = error.downcast_ref::<ArrowError>() {
            Some(e.as_ref())
        } else if let Some(ParquetError::External(e)) = error.downcast_ref::<ParquetError>() {
            Some(e.as_ref())
        } else {
            None
        };
    inner
        .or_else(|| error.source())
        .is_some_and(is_missing_object)
}

/// Query engine wrapping a DataFusion SessionContext.
pub struct QueryEngine {
    ctx: SessionContext,
    base_url: Url,
    query_timeout: Option<Duration>,
    /// Source of the attribute columns each table has, if any.
    attributes: Option<Arc<AttributeRegistry>>,
    /// The registry version the registered tables were built from.
    tables_version: AtomicU64,
    /// Held for reading while a query is planned and for writing while
    /// tables are swapped, so no query is planned against a missing table.
    tables: tokio::sync::RwLock<()>,
}

/// Builder for configuring a QueryEngine.
#[must_use = "builders do nothing until .build() is called"]
pub struct QueryEngineBuilder {
    store: Arc<dyn ObjectStore>,
    base_url: String,
    memory_pool: MemoryPoolStrategy,
    query_timeout: Option<Duration>,
    cache_bytes: usize,
    metadata_cache_bytes: usize,
    attributes: Option<Arc<AttributeRegistry>>,
}

impl QueryEngineBuilder {
    /// Create a new builder with the required store and base URL.
    ///
    /// By default, uses the default query timeout (30 seconds).
    pub fn new(store: Arc<dyn ObjectStore>, base_url: impl Into<String>) -> Self {
        Self {
            store,
            base_url: base_url.into(),
            memory_pool: MemoryPoolStrategy::default(),
            query_timeout: Some(DEFAULT_QUERY_TIMEOUT),
            cache_bytes: DEFAULT_QUERY_CACHE_BYTES,
            metadata_cache_bytes: DEFAULT_METADATA_CACHE_BYTES,
            attributes: None,
        }
    }

    /// Set how much memory parsed file footers may occupy between
    /// queries, in bytes.
    pub const fn with_metadata_cache_bytes(mut self, bytes: usize) -> Self {
        self.metadata_cache_bytes = bytes;
        self
    }

    /// Give each table the attribute columns recorded in `registry`, and
    /// keep them current as new attributes are flushed.
    ///
    /// Without a registry the tables have only their fixed columns, and
    /// attributes are reachable only through `cbor_extract`.
    pub fn with_attribute_registry(mut self, registry: Arc<AttributeRegistry>) -> Self {
        self.attributes = Some(registry);
        self
    }

    /// Set how much object data is cached in memory, in bytes.
    ///
    /// Reads from remote object storage are served from a [`BlockCache`]
    /// of this size, so repeated queries over the same files do not pay
    /// for the network again. Zero disables the cache. Local filesystem
    /// storage is never cached here, since the operating system already
    /// caches it.
    pub const fn with_cache_bytes(mut self, cache_bytes: usize) -> Self {
        self.cache_bytes = cache_bytes;
        self
    }

    /// Set a query timeout.
    ///
    /// Queries that exceed this duration will return a `QueryTimeout` error.
    /// Use `None` to disable the timeout.
    pub const fn with_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.query_timeout = timeout;
        self
    }

    /// Disable the query timeout.
    #[allow(clippy::missing_const_for_fn)]
    pub fn without_timeout(self) -> Self {
        self.with_timeout(None)
    }

    /// Set the memory pool strategy.
    ///
    /// Use `MemoryPoolStrategy::Greedy` or `MemoryPoolStrategy::FairSpill` to limit
    /// memory usage during query execution. This is useful in production to prevent
    /// a single query from exhausting system memory.
    pub const fn with_memory_pool(mut self, strategy: MemoryPoolStrategy) -> Self {
        self.memory_pool = strategy;
        self
    }

    /// Set a greedy memory limit (convenience method).
    #[allow(clippy::missing_const_for_fn)]
    pub fn with_memory_limit(self, max_bytes: usize) -> Self {
        self.with_memory_pool(MemoryPoolStrategy::Greedy { max_bytes })
    }

    /// Build the query engine.
    pub async fn build(self) -> Result<QueryEngine, TelemetryError> {
        let url = Url::parse(&self.base_url).map_err(TelemetryError::UrlParse)?;

        // Build runtime environment with optional memory pool. Parsed file
        // footers are cached between queries.
        let runtime = RuntimeEnvBuilder::new().with_metadata_cache_limit(self.metadata_cache_bytes);
        let runtime = match self.memory_pool {
            MemoryPoolStrategy::Unlimited => runtime,
            MemoryPoolStrategy::Greedy { max_bytes } => {
                runtime.with_memory_pool(Arc::new(GreedyMemoryPool::new(max_bytes)))
            }
            MemoryPoolStrategy::FairSpill { max_bytes } => {
                runtime.with_memory_pool(Arc::new(FairSpillPool::new(max_bytes)))
            }
        }
        .build_arc()?;

        // Configure session for telemetry query workload
        let mut config = SessionConfig::new()
            // Enable statistics collection for better query planning
            .with_collect_statistics(true)
            // Enable repartitioning for file scans to improve parallelism
            .with_repartition_file_scans(true)
            // Enable repartitioning for aggregations (common in metrics queries)
            .with_repartition_aggregations(true)
            // Enable repartitioning for joins (useful for trace correlation queries)
            .with_repartition_joins(true)
            // Use a reasonable batch size for telemetry data (balance memory vs performance)
            .with_batch_size(DEFAULT_QUERY_BATCH_SIZE)
            // Expose information_schema so clients can discover tables and columns
            .with_information_schema(true);

        // Evaluate filters while decoding Parquet rather than afterwards, so
        // a selective query (one trace out of millions of spans) only
        // materialises the wide attribute columns for rows that match.
        // Reordering runs the cheapest, most selective predicates first.
        let parquet_options = &mut config.options_mut().execution.parquet;
        parquet_options.pushdown_filters = true;
        parquet_options.reorder_filters = true;

        let state = SessionStateBuilder::new_with_default_features()
            .with_config(config)
            .with_runtime_env(runtime)
            .build();
        let mut ctx = SessionContext::from(state);

        // Register custom UDFs for error tracking
        for udf in super::udfs::all_udfs() {
            ctx.register_udf(udf);
        }
        for udf in super::udfs::attr::attribute_udfs(self.attributes.as_ref()) {
            ctx.register_udf(udf);
        }
        datafusion::execution::FunctionRegistry::register_function_rewrite(
            &mut ctx,
            Arc::new(super::udfs::attr::AttributeRewrite),
        )?;

        // Register object store with DataFusion
        if url.scheme() == "file" {
            let root_url = Url::parse("file:///").map_err(TelemetryError::UrlParse)?;
            let local_store: Arc<dyn ObjectStore> =
                Arc::new(object_store::local::LocalFileSystem::new());
            ctx.register_object_store(&root_url, local_store);
        } else if self.cache_bytes > 0 {
            let cached = BlockCache::new(self.store, self.cache_bytes);
            ctx.register_object_store(&url, Arc::new(cached));
        } else {
            ctx.register_object_store(&url, self.store);
        }

        let engine = QueryEngine {
            ctx,
            base_url: url,
            query_timeout: self.query_timeout,
            attributes: self.attributes,
            tables_version: AtomicU64::new(0),
            tables: tokio::sync::RwLock::new(()),
        };
        engine.register_tables().await?;

        Ok(engine)
    }
}

impl QueryEngine {
    /// Create a new query engine with the given object store.
    ///
    /// The `base_url` should be the root URL for telemetry data (e.g., `file:///data/telemetry`
    /// or `s3://bucket/telemetry`).
    ///
    /// Note: For local filesystem storage, this creates a new `LocalFileSystem` at root
    /// rather than using the provided store. This is because DataFusion's URL-based store
    /// lookup requires the store to be registered at the URL scheme root for local paths.
    pub async fn new(store: Arc<dyn ObjectStore>, base_url: &str) -> Result<Self, TelemetryError> {
        QueryEngineBuilder::new(store, base_url).build().await
    }

    /// Register the three signal tables, replacing any already registered,
    /// with whatever attribute columns the registry knows of right now.
    async fn register_tables(&self) -> Result<(), TelemetryError> {
        let version = self.attributes.as_ref().map_or(0, |r| r.version());
        for (signal, base) in [
            (Signal::Traces, traces_storage_schema()),
            (Signal::Metrics, number_metrics_storage_schema()),
            (Signal::Logs, logs_storage_schema()),
        ] {
            let schema = self
                .attributes
                .as_ref()
                .map_or_else(|| Arc::clone(&base), |r| r.schema(signal, &base));
            self.ctx
                .deregister_table(TableReference::bare(signal.as_str()))?;
            self.register_signal_table(signal, schema).await?;
        }
        self.tables_version.store(version, Ordering::Release);
        Ok(())
    }

    /// Plan `sql` against tables that include every attribute column
    /// flushed so far.
    async fn plan(&self, sql: &str) -> Result<DataFrame, TelemetryError> {
        if let Some(registry) = &self.attributes {
            if registry.version() != self.tables_version.load(Ordering::Acquire) {
                let _swap = self.tables.write().await;
                if registry.version() != self.tables_version.load(Ordering::Acquire) {
                    self.register_tables().await?;
                }
            }
        }
        let _planning = self.tables.read().await;
        self.ctx.sql(sql).await.map_err(Into::into)
    }

    /// Register a ListingTable for a telemetry signal.
    #[allow(clippy::unused_async)]
    async fn register_signal_table(
        &self,
        signal: Signal,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Result<(), TelemetryError> {
        let table_name = signal.as_str();
        let table_path = format!("{}/{}/", self.base_url, table_name);

        // Configure listing options with Hive-style partition columns.
        // DataFusion extracts partition values from directory structure:
        //   traces/date=2024-01-15/hour=14/file.parquet
        // And adds them as virtual columns to query results.
        let partition_cols: Vec<(String, arrow::datatypes::DataType)> = PARTITION_COLUMNS
            .iter()
            .map(|(name, dtype)| ((*name).to_owned(), dtype.clone()))
            .collect();

        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_collect_stat(true) // Enable file-level statistics for optimisation
            .with_table_partition_cols(partition_cols); // Enable partition pruning

        // Create table URL
        let table_url = ListingTableUrl::parse(&table_path)
            .map_err(|e| TelemetryError::Config(format!("invalid table path: {e}")))?;

        // Create listing table config with the full schema (includes partition columns).
        // The partition columns are populated by DataFusion from the directory structure,
        // not from the Parquet files.
        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);

        // Create and register the table
        let table = ListingTable::try_new(config)?;

        self.ctx
            .register_table(TableReference::bare(table_name), Arc::new(table))?;

        tracing::debug!(signal = %signal, path = %table_path, "Registered table with partition pruning");

        Ok(())
    }

    /// Execute a SQL query and collect all results.
    ///
    /// DataFusion automatically handles partition pruning from WHERE clauses
    /// on the `date` and `hour` columns.
    ///
    /// The query is subject to the configured timeout (default 30 seconds).
    #[tracing::instrument(skip(self), fields(sql_len = sql.len()))]
    pub async fn query(
        &self,
        sql: &str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, TelemetryError> {
        let df = self.plan(sql).await?;

        self.collect(df).await
    }

    /// Execute a SQL query with an enforced maximum row limit.
    ///
    /// The limit is applied at the DataFusion level via `DataFrame::limit`,
    /// ensuring the query planner can optimise accordingly. This is preferred
    /// over string-based LIMIT clause detection.
    #[tracing::instrument(skip(self), fields(sql_len = sql.len(), max_rows))]
    pub async fn query_limited(
        &self,
        sql: &str,
        max_rows: usize,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, TelemetryError> {
        let df = self.plan(sql).await?;
        let df = df.limit(0, Some(max_rows))?;

        self.collect(df).await
    }

    /// Collect a query's results, subject to the configured timeout.
    ///
    /// A data file can disappear between being listed and being read, for
    /// example when a bucket lifecycle rule expires it. That is not a
    /// failure of the query: the file's data is gone by the operator's own
    /// policy. The query is planned again, which no longer sees the missing
    /// file, and answers from the data that remains.
    async fn collect(
        &self,
        df: DataFrame,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, TelemetryError> {
        let attempts = async {
            let mut retries = 0;
            loop {
                match df.clone().collect().await {
                    Err(error) if retries < MISSING_FILE_RETRIES && is_missing_object(&error) => {
                        retries += 1;
                        tracing::info!(
                            %error,
                            retries,
                            "Data file vanished during query, planning again without it"
                        );
                    }
                    result => return result,
                }
            }
        };

        if let Some(timeout) = self.query_timeout {
            match tokio::time::timeout(timeout, attempts).await {
                Ok(result) => result.map_err(Into::into),
                Err(_) => Err(TelemetryError::QueryTimeout { duration: timeout }),
            }
        } else {
            attempts.await.map_err(Into::into)
        }
    }

    /// Execute a SQL query and return a streaming result.
    ///
    /// Use this for large result sets to avoid loading everything into memory.
    #[tracing::instrument(skip(self), fields(sql_len = sql.len()))]
    pub async fn query_stream(
        &self,
        sql: &str,
    ) -> Result<SendableRecordBatchStream, TelemetryError> {
        let df = self.plan(sql).await?;
        df.execute_stream().await.map_err(Into::into)
    }

    /// Execute a DataFrame query directly.
    ///
    /// Useful when building queries programmatically with the query builders.
    /// The query is subject to the configured timeout (default 30 seconds).
    #[tracing::instrument(skip_all)]
    pub async fn execute(
        &self,
        df: DataFrame,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, TelemetryError> {
        self.collect(df).await
    }

    /// A DataFrame over exactly the given data files, read with `schema`.
    ///
    /// For lookups that already know which files hold their rows: no
    /// other file is listed, opened or pruned. A file that has since
    /// disappeared contributes no rows.
    ///
    /// # Errors
    ///
    /// Returns an error if the files cannot be registered for reading.
    pub async fn read_files(
        &self,
        files: &[object_store::path::Path],
        schema: &arrow::datatypes::Schema,
    ) -> Result<DataFrame, TelemetryError> {
        let base = self.base_url.as_str().trim_end_matches('/');
        let urls: Vec<String> = files.iter().map(|path| format!("{base}/{path}")).collect();
        let options = ParquetReadOptions::default().schema(schema);
        self.ctx
            .read_parquet(urls, options)
            .await
            .map_err(Into::into)
    }

    /// Get the underlying SessionContext for advanced use cases.
    pub const fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get a DataFrame for a table.
    pub async fn table(&self, name: &str) -> Result<DataFrame, TelemetryError> {
        self.ctx.table(name).await.map_err(Into::into)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use arrow::array::{FixedSizeBinaryBuilder, StringBuilder, UInt64Builder, UInt8Array};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use parquet::arrow::AsyncArrowWriter;
    use parquet::file::properties::WriterProperties;

    async fn setup_test_engine() -> (QueryEngine, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        let engine = QueryEngine::new(store.clone(), "memory://").await.unwrap();
        (engine, store)
    }

    /// Test with LocalFileSystem to ensure real filesystem works.
    ///
    /// This test is marked as ignored by default because it performs real filesystem I/O.
    /// Run with `cargo test -- --ignored` to include it.
    #[tokio::test]
    #[ignore = "slow: performs real filesystem I/O"]
    async fn query_traces_table_local_filesystem() {
        use object_store::local::LocalFileSystem;
        use tempfile::tempdir;

        // Create temp directory with Hive-style partitioning
        let temp_dir = tempdir().unwrap();
        let base_path = temp_dir.path();
        let traces_path = base_path.join("traces/date=2024-01-01/hour=12");
        std::fs::create_dir_all(&traces_path).unwrap();

        // Write test parquet file
        let batch = sample_traces_batch();
        let file_path = traces_path.join("test.parquet");

        let props = WriterProperties::builder().build();
        let file = std::fs::File::create(&file_path).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        assert!(file_path.exists(), "Parquet file should exist on disk");

        // Create QueryEngine with local filesystem
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
        let base_url = format!("file://{}", base_path.display());
        let engine = QueryEngine::new(store, &base_url).await.unwrap();

        // Query should find the data
        let results = engine
            .query("SELECT name FROM traces ORDER BY name")
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 2, "Should find 2 rows from the parquet file");

        // Verify data
        let names = results[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "span-1");
        assert_eq!(names.value(1), "span-2");
    }

    async fn write_test_parquet(store: &Arc<InMemory>, path: &str, batch: RecordBatch) {
        let props = WriterProperties::builder().build();
        let mut buffer = Vec::new();

        {
            let mut writer =
                AsyncArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
            writer.write(&batch).await.unwrap();
            writer.close().await.unwrap();
        }

        store.put(&Path::from(path), buffer.into()).await.unwrap();
    }

    fn sample_traces_batch() -> RecordBatch {
        let schema = traces_storage_schema();

        // Build all non-nullable columns
        let mut trace_id = FixedSizeBinaryBuilder::new(16);
        trace_id.append_value([0xAA; 16]).unwrap();
        trace_id.append_value([0xBB; 16]).unwrap();

        let mut span_id = FixedSizeBinaryBuilder::new(8);
        span_id.append_value([0x11; 8]).unwrap();
        span_id.append_value([0x22; 8]).unwrap();

        let mut name = StringBuilder::new();
        name.append_value("span-1");
        name.append_value("span-2");

        let mut service_name = StringBuilder::new();
        service_name.append_value("test-service");
        service_name.append_value("test-service");

        let mut start_time = UInt64Builder::new();
        start_time.append_value(1_000_000_000);
        start_time.append_value(2_000_000_000);

        let mut end_time = UInt64Builder::new();
        end_time.append_value(1_500_000_000);
        end_time.append_value(2_500_000_000);

        let mut duration = UInt64Builder::new();
        duration.append_value(500_000_000);
        duration.append_value(500_000_000);

        // Build all columns (setting most to null/defaults)
        let num_rows = 2;
        let columns: Vec<Arc<dyn arrow::array::Array>> = schema
            .fields()
            .iter()
            .map(|field| -> Arc<dyn arrow::array::Array> {
                match field.name().as_str() {
                    "trace_id" => Arc::new(trace_id.finish()),
                    "span_id" => Arc::new(span_id.finish()),
                    "name" => Arc::new(name.finish()),
                    "service.name" => Arc::new(service_name.finish()),
                    "start_time_unix_nano" => Arc::new(start_time.finish()),
                    "end_time_unix_nano" => Arc::new(end_time.finish()),
                    "duration_ns" => Arc::new(duration.finish()),
                    "kind" => Arc::new(UInt8Array::from(vec![1u8, 2u8])), // SERVER, CLIENT
                    "status_code" => Arc::new(UInt8Array::from(vec![0u8, 0u8])), // UNSET
                    "flags" => Arc::new(arrow::array::UInt32Array::from(vec![0u32, 0u32])), // No flags
                    "resource_dropped_attributes_count"
                    | "scope_dropped_attributes_count"
                    | "span_dropped_attributes_count"
                    | "span_dropped_events_count"
                    | "span_dropped_links_count" => {
                        Arc::new(arrow::array::UInt32Array::from(vec![0u32; num_rows]))
                    }
                    _ if field.is_nullable() => {
                        arrow::array::new_null_array(field.data_type(), num_rows)
                    }
                    _ => panic!("Unexpected non-nullable field: {}", field.name()),
                }
            })
            .collect();

        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[tokio::test]
    async fn engine_registers_tables() {
        let (engine, _store) = setup_test_engine().await;

        // Should be able to reference all tables
        let tables = ["traces", "metrics", "logs"];
        for table in tables {
            let result = engine.ctx.table_exist(TableReference::bare(table));
            assert!(result.unwrap_or(false), "Table '{table}' should exist");
        }
    }

    #[tokio::test]
    async fn information_schema_exposes_table_columns() {
        let (engine, _store) = setup_test_engine().await;

        let results = engine
            .query(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_name = 'traces' AND column_name = 'trace_id'",
            )
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 1, "traces.trace_id should be discoverable");
    }

    #[tokio::test]
    async fn query_traces_table() {
        let (engine, store) = setup_test_engine().await;

        // Write test data
        let batch = sample_traces_batch();
        write_test_parquet(&store, "traces/date=2024-01-01/hour=12/test.parquet", batch).await;

        // Query the data
        let results = engine
            .query("SELECT name, \"service.name\" FROM traces ORDER BY name")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2);

        // Verify data
        let names = results[0]
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "span-1");
        assert_eq!(names.value(1), "span-2");
    }

    #[tokio::test]
    async fn query_with_filter() {
        let (engine, store) = setup_test_engine().await;

        let batch = sample_traces_batch();
        write_test_parquet(&store, "traces/date=2024-01-01/hour=12/test.parquet", batch).await;

        // Query with filter
        let results = engine
            .query("SELECT name FROM traces WHERE name = 'span-1'")
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn query_stream_returns_batches() {
        let (engine, store) = setup_test_engine().await;

        let batch = sample_traces_batch();
        write_test_parquet(&store, "traces/date=2024-01-01/hour=12/test.parquet", batch).await;

        // Query as stream
        let mut stream = engine.query_stream("SELECT * FROM traces").await.unwrap();

        let mut total_rows = 0;
        while let Some(batch) = stream.next().await {
            total_rows += batch.unwrap().num_rows();
        }
        assert_eq!(total_rows, 2);
    }

    #[tokio::test]
    async fn empty_table_returns_empty_results() {
        let (engine, _store) = setup_test_engine().await;

        // Query empty table - should return empty results, not error
        let results = engine.query("SELECT * FROM traces").await.unwrap();
        assert!(results.is_empty() || results[0].num_rows() == 0);
    }

    #[tokio::test]
    async fn builder_creates_engine_with_defaults() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = QueryEngineBuilder::new(store, "memory://")
            .build()
            .await
            .unwrap();

        // Should be able to query tables
        let result = engine.ctx.table_exist(TableReference::bare("traces"));
        assert!(result.unwrap_or(false));
    }

    #[tokio::test]
    async fn builder_with_memory_limit() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = QueryEngineBuilder::new(store, "memory://")
            .with_memory_limit(100 * 1024 * 1024) // 100 MB
            .build()
            .await
            .unwrap();

        // Should be able to query tables with memory limit set
        let result = engine.ctx.table_exist(TableReference::bare("traces"));
        assert!(result.unwrap_or(false));
    }

    #[tokio::test]
    async fn builder_with_fair_spill_pool() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let engine = QueryEngineBuilder::new(store, "memory://")
            .with_memory_pool(MemoryPoolStrategy::FairSpill {
                max_bytes: 50 * 1024 * 1024,
            })
            .build()
            .await
            .unwrap();

        // Should be able to query tables with fair spill pool
        let result = engine.ctx.table_exist(TableReference::bare("traces"));
        assert!(result.unwrap_or(false));
    }
}
