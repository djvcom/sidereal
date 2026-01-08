//! In-memory buffer with background flush to object storage.
//!
//! The Ingester buffers Arrow RecordBatches in memory and periodically flushes
//! them to Parquet files in object storage when size or time thresholds are met.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use tokio::sync::{oneshot, RwLock};
use tokio::task::JoinHandle;

use crate::config::{BufferConfig, ParquetConfig};
use crate::storage::Signal;
use crate::TelemetryError;

/// Calculate exponential backoff delay for retry attempts.
///
/// The delay doubles with each attempt (2^attempt * initial_delay), capped at max_delay.
#[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
fn calculate_backoff_delay(attempt: u32, initial_delay: Duration, max_delay: Duration) -> Duration {
    let multiplier = 2u64.saturating_pow(attempt);
    let delay = initial_delay.saturating_mul(multiplier as u32);
    delay.min(max_delay)
}

/// Handle for controlling a background flush task.
pub struct FlushHandle {
    shutdown_tx: Option<oneshot::Sender<()>>,
    join_handle: JoinHandle<()>,
}

impl FlushHandle {
    /// Signal the background task to stop and wait for it to complete.
    ///
    /// This will trigger one final flush before shutting down.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        let _ = self.join_handle.await;
    }

    /// Abort the background task immediately without flushing.
    pub fn abort(self) {
        self.join_handle.abort();
    }
}

/// Ingester buffers telemetry data and flushes to object storage.
pub struct Ingester {
    signal: Signal,
    schema: SchemaRef,
    buffer: RwLock<Vec<RecordBatch>>,
    buffer_rows: AtomicUsize,
    buffer_bytes: AtomicUsize,
    store: Arc<dyn ObjectStore>,
    buffer_config: BufferConfig,
    #[allow(dead_code)]
    parquet_config: ParquetConfig,
}

impl Ingester {
    /// Create a new ingester for a telemetry signal.
    pub fn new(
        signal: Signal,
        schema: SchemaRef,
        store: Arc<dyn ObjectStore>,
        buffer_config: BufferConfig,
        parquet_config: ParquetConfig,
    ) -> Self {
        Self {
            signal,
            schema,
            buffer: RwLock::new(Vec::new()),
            buffer_rows: AtomicUsize::new(0),
            buffer_bytes: AtomicUsize::new(0),
            store,
            buffer_config,
            parquet_config,
        }
    }

    /// Ingest a record batch into the buffer.
    ///
    /// Returns an error if:
    /// - The batch exceeds `max_records_per_request` (RequestTooLarge)
    /// - The buffer is full (BufferOverflow - backpressure)
    pub async fn ingest(&self, batch: RecordBatch) -> Result<(), TelemetryError> {
        let batch_rows = batch.num_rows();
        let batch_bytes = batch
            .columns()
            .iter()
            .map(|c| c.get_buffer_memory_size())
            .sum::<usize>();

        self.check_request_size(batch_rows)?;
        self.check_buffer_capacity(batch_bytes)?;

        let new_rows = self
            .append_batch_with_consistent_counters(batch, batch_rows, batch_bytes)
            .await;

        if new_rows >= self.buffer_config.max_batch_size {
            self.flush().await?;
        }

        Ok(())
    }

    const fn check_request_size(&self, batch_rows: usize) -> Result<(), TelemetryError> {
        if batch_rows > self.buffer_config.max_records_per_request {
            return Err(TelemetryError::RequestTooLarge {
                records: batch_rows,
                limit: self.buffer_config.max_records_per_request,
            });
        }
        Ok(())
    }

    fn check_buffer_capacity(&self, batch_bytes: usize) -> Result<(), TelemetryError> {
        let current_bytes = self.buffer_bytes.load(Ordering::SeqCst);
        if current_bytes + batch_bytes > self.buffer_config.max_buffer_bytes {
            return Err(TelemetryError::BufferOverflow {
                message: format!(
                    "buffer full: {} bytes, max {} bytes",
                    current_bytes + batch_bytes,
                    self.buffer_config.max_buffer_bytes
                ),
            });
        }
        Ok(())
    }

    /// Append a batch to the buffer while keeping counters consistent with buffer contents.
    ///
    /// Counters are updated while holding the write lock to ensure `buffered_rows()`
    /// and `buffered_bytes()` always reflect the actual buffer state.
    async fn append_batch_with_consistent_counters(
        &self,
        batch: RecordBatch,
        batch_rows: usize,
        batch_bytes: usize,
    ) -> usize {
        let mut buffer = self.buffer.write().await;
        buffer.push(batch);
        self.buffer_bytes.fetch_add(batch_bytes, Ordering::SeqCst);
        self.buffer_rows.fetch_add(batch_rows, Ordering::SeqCst) + batch_rows
    }

    /// Flush the buffer to Parquet in object storage.
    ///
    /// This method holds the buffer lock for the entire duration of the flush
    /// to prevent TOCTOU races. Failed flushes are retried with exponential
    /// backoff. If all retries are exhausted, data is restored to the buffer.
    pub async fn flush(&self) -> Result<(), TelemetryError> {
        use parquet::arrow::AsyncArrowWriter;
        use parquet::basic::{Compression, ZstdLevel};
        use parquet::file::properties::WriterProperties;

        let mut buffer = self.buffer.write().await;

        if buffer.is_empty() {
            return Ok(());
        }

        let batches = std::mem::take(&mut *buffer);
        let saved_rows = self.buffer_rows.swap(0, Ordering::SeqCst);
        let saved_bytes = self.buffer_bytes.swap(0, Ordering::SeqCst);

        let partition_time = self.extract_partition_timestamp(&batches);
        let path = crate::storage::partition_path(self.signal, partition_time, None);

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_size(self.parquet_config.row_group_size)
            .build();

        let max_retries = self.buffer_config.flush_max_retries;
        let initial_delay = Duration::from_millis(self.buffer_config.flush_initial_delay_ms);
        let max_delay = Duration::from_millis(self.buffer_config.flush_max_delay_ms);

        let mut last_error: Option<TelemetryError> = None;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let delay = calculate_backoff_delay(attempt - 1, initial_delay, max_delay);
                tracing::warn!(
                    signal = %self.signal,
                    attempt = attempt,
                    max_retries = max_retries,
                    delay_ms = delay.as_millis(),
                    "Retrying flush after failure"
                );
                tokio::time::sleep(delay).await;
            }

            let mut parquet_buffer = Vec::new();
            let write_result: Result<(), TelemetryError> = async {
                let mut writer = AsyncArrowWriter::try_new(
                    &mut parquet_buffer,
                    self.schema.clone(),
                    Some(props.clone()),
                )?;

                for batch in &batches {
                    writer.write(batch).await?;
                }

                writer.close().await?;
                Ok(())
            }
            .await;

            if let Err(e) = write_result {
                tracing::error!(
                    signal = %self.signal,
                    attempt = attempt,
                    error = %e,
                    "Parquet write failed"
                );
                last_error = Some(e);
                continue;
            }

            let upload_result: Result<_, TelemetryError> = self
                .store
                .put(&path, parquet_buffer.into())
                .await
                .map_err(Into::into);

            match upload_result {
                Ok(_) => {
                    tracing::info!(
                        signal = %self.signal,
                        path = %path,
                        attempts = attempt + 1,
                        "Flushed buffer to Parquet"
                    );
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        signal = %self.signal,
                        attempt = attempt,
                        error = %e,
                        "Object store upload failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        tracing::error!(
            signal = %self.signal,
            attempts = max_retries + 1,
            "All flush retries exhausted, restoring buffer"
        );
        *buffer = batches;
        self.buffer_rows.store(saved_rows, Ordering::SeqCst);
        self.buffer_bytes.store(saved_bytes, Ordering::SeqCst);

        Err(
            last_error.unwrap_or_else(|| TelemetryError::BufferOverflow {
                message: "flush failed with unknown error after all retries".to_owned(),
            }),
        )
    }

    /// Get the current number of buffered rows.
    pub fn buffered_rows(&self) -> usize {
        self.buffer_rows.load(Ordering::SeqCst)
    }

    /// Get the current buffer size in bytes.
    pub fn buffered_bytes(&self) -> usize {
        self.buffer_bytes.load(Ordering::SeqCst)
    }

    /// Get the flush interval in seconds.
    pub const fn flush_interval_secs(&self) -> u64 {
        self.buffer_config.flush_interval_secs
    }

    /// Get the signal type this ingester handles.
    pub const fn signal(&self) -> Signal {
        self.signal
    }

    /// Extract the earliest timestamp from the batches for partitioning.
    ///
    /// Uses the data's own timestamp (not ingestion time) to ensure correct
    /// partitioning for out-of-order data. Falls back to current time if no
    /// timestamp can be extracted.
    fn extract_partition_timestamp(
        &self,
        batches: &[RecordBatch],
    ) -> chrono::DateTime<chrono::Utc> {
        use arrow::array::{Array, UInt64Array};
        use chrono::{TimeZone, Utc};

        let timestamp_column = match self.signal {
            Signal::Traces => "start_time_unix_nano",
            Signal::Metrics | Signal::Logs => "time_unix_nano",
        };

        let min_nanos = batches
            .iter()
            .filter_map(|batch| batch.column_by_name(timestamp_column))
            .filter_map(|col| col.as_any().downcast_ref::<UInt64Array>())
            .flat_map(|arr| {
                (0..arr.len()).filter_map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
            })
            .min();

        match min_nanos {
            Some(nanos) => {
                #[allow(
                    clippy::cast_possible_wrap,
                    clippy::cast_possible_truncation,
                    clippy::as_conversions
                )]
                let secs = (nanos / 1_000_000_000) as i64;
                #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
                let nsecs = (nanos % 1_000_000_000) as u32;
                Utc.timestamp_opt(secs, nsecs)
                    .single()
                    .unwrap_or_else(Utc::now)
            }
            None => Utc::now(),
        }
    }
}

/// Start a background flush task for an ingester.
///
/// The task will periodically flush the buffer based on `flush_interval_secs`.
/// Returns a handle that can be used to gracefully shut down the task.
///
/// # Example
///
/// ```no_run
/// use std::sync::Arc;
/// use sidereal_telemetry::buffer::{Ingester, start_background_flush};
/// use sidereal_telemetry::config::{BufferConfig, ParquetConfig};
/// use sidereal_telemetry::schema::traces::traces_storage_schema;
/// use sidereal_telemetry::Signal;
/// use object_store::memory::InMemory;
///
/// # #[tokio::main]
/// # async fn main() {
/// let store = Arc::new(InMemory::new());
/// let ingester = Arc::new(Ingester::new(
///     Signal::Traces,
///     traces_storage_schema(),  // Use storage schema (without partition columns)
///     store,
///     BufferConfig::default(),
///     ParquetConfig::default(),
/// ));
/// let handle = start_background_flush(ingester.clone());
///
/// // ... application runs ...
///
/// // On shutdown:
/// handle.shutdown().await;
/// # }
/// ```
pub fn start_background_flush(ingester: Arc<Ingester>) -> FlushHandle {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let interval = Duration::from_secs(ingester.flush_interval_secs());
    let signal = ingester.signal();

    let join_handle = tokio::spawn(async move {
        tracing::info!(
            signal = %signal,
            interval_secs = interval.as_secs(),
            "Starting background flush task"
        );

        let mut interval_timer =
            tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);

        loop {
            tokio::select! {
                _ = interval_timer.tick() => {
                    if ingester.buffered_rows() > 0 {
                        if let Err(e) = ingester.flush().await {
                            tracing::error!(
                                signal = %signal,
                                error = %e,
                                "Background flush failed"
                            );
                        }
                    }
                }
                _ = &mut shutdown_rx => {
                    tracing::info!(
                        signal = %signal,
                        "Shutdown signal received, performing final flush"
                    );
                    if ingester.buffered_rows() > 0 {
                        if let Err(e) = ingester.flush().await {
                            tracing::error!(
                                signal = %signal,
                                error = %e,
                                "Final flush failed"
                            );
                        }
                    }
                    break;
                }
            }
        }

        tracing::info!(signal = %signal, "Background flush task stopped");
    });

    FlushHandle {
        shutdown_tx: Some(shutdown_tx),
        join_handle,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use object_store::memory::InMemory;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn test_batch(schema: &SchemaRef, rows: usize) -> RecordBatch {
        let ids: Vec<u64> = (0..rows as u64).collect();
        let names: Vec<String> = (0..rows).map(|i| format!("name-{}", i)).collect();

        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    fn test_buffer_config(max_batch_size: usize, max_buffer_bytes: usize) -> BufferConfig {
        BufferConfig {
            max_batch_size,
            flush_interval_secs: 30,
            max_buffer_bytes,
            max_records_per_request: 100_000,
            flush_max_retries: 3,
            flush_initial_delay_ms: 10,
            flush_max_delay_ms: 100,
        }
    }

    #[test]
    fn backoff_delay_calculation() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_millis(10_000);

        assert_eq!(
            calculate_backoff_delay(0, initial, max),
            Duration::from_millis(100)
        );
        assert_eq!(
            calculate_backoff_delay(1, initial, max),
            Duration::from_millis(200)
        );
        assert_eq!(
            calculate_backoff_delay(2, initial, max),
            Duration::from_millis(400)
        );
        assert_eq!(
            calculate_backoff_delay(3, initial, max),
            Duration::from_millis(800)
        );
        assert_eq!(
            calculate_backoff_delay(10, initial, max),
            Duration::from_millis(10_000)
        );
        assert_eq!(
            calculate_backoff_delay(20, initial, max),
            Duration::from_millis(10_000)
        );
    }

    #[tokio::test]
    async fn ingest_and_flush() {
        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let config = test_buffer_config(100, 10 * 1024 * 1024);
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Ingester::new(
            Signal::Traces,
            schema.clone(),
            store.clone(),
            config,
            parquet_config,
        );

        // Ingest a batch
        let batch = test_batch(&schema, 10);
        ingester.ingest(batch).await.unwrap();

        assert_eq!(ingester.buffered_rows(), 10);

        // Flush manually
        ingester.flush().await.unwrap();

        assert_eq!(ingester.buffered_rows(), 0);

        // Verify a file was written
        let files: Vec<_> = store.list(None).collect::<Vec<_>>().await;
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn auto_flush_on_threshold() {
        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let config = test_buffer_config(20, 10 * 1024 * 1024);
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Ingester::new(
            Signal::Metrics,
            schema.clone(),
            store.clone(),
            config,
            parquet_config,
        );

        // Ingest batches until threshold
        for _ in 0..3 {
            let batch = test_batch(&schema, 10);
            ingester.ingest(batch).await.unwrap();
        }

        // Should have auto-flushed after 20 rows
        // After flush, 10 more rows were added, so we should have 10 rows buffered
        // Actually, the flush happens when we hit 20, so after 30 rows:
        // - First 10: buffered_rows = 10
        // - Second 10: buffered_rows = 20, triggers flush, then reset to 0
        // - Third 10: buffered_rows = 10
        assert_eq!(ingester.buffered_rows(), 10);

        // Verify a file was written
        let files: Vec<_> = store.list(None).collect::<Vec<_>>().await;
        assert_eq!(files.len(), 1);
    }

    #[tokio::test]
    async fn backpressure_on_full_buffer() {
        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let config = test_buffer_config(1000, 250);
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Ingester::new(Signal::Logs, schema.clone(), store, config, parquet_config);

        // First batch should succeed
        let batch = test_batch(&schema, 10);
        ingester.ingest(batch).await.unwrap();

        // Second batch should fail with backpressure
        let batch = test_batch(&schema, 10);
        let result = ingester.ingest(batch).await;
        assert!(matches!(result, Err(TelemetryError::BufferOverflow { .. })));
    }

    #[tokio::test(start_paused = true)]
    async fn background_flush_on_interval() {
        use object_store::ObjectStore;

        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let mut config = test_buffer_config(1000, 10 * 1024 * 1024);
        config.flush_interval_secs = 1;
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Arc::new(Ingester::new(
            Signal::Traces,
            schema.clone(),
            store.clone(),
            config,
            parquet_config,
        ));

        // Start background flush task
        let handle = start_background_flush(ingester.clone());

        // Ingest some data
        let batch = test_batch(&schema, 10);
        ingester.ingest(batch).await.unwrap();
        assert_eq!(ingester.buffered_rows(), 10);

        // No files yet (interval hasn't elapsed)
        let files: Vec<_> = store.list(None).collect().await;
        assert_eq!(files.len(), 0);

        // Advance time past the flush interval and let the task run
        // Use sleep which advances time and yields to other tasks
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Buffer should be flushed now
        assert_eq!(ingester.buffered_rows(), 0);

        // File should exist
        let files: Vec<_> = store.list(None).collect().await;
        assert_eq!(files.len(), 1);

        // Shut down gracefully
        handle.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn background_flush_final_flush_on_shutdown() {
        use object_store::ObjectStore;

        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let mut config = test_buffer_config(1000, 10 * 1024 * 1024);
        config.flush_interval_secs = 60;
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Arc::new(Ingester::new(
            Signal::Metrics,
            schema.clone(),
            store.clone(),
            config,
            parquet_config,
        ));

        let handle = start_background_flush(ingester.clone());

        // Ingest some data
        let batch = test_batch(&schema, 10);
        ingester.ingest(batch).await.unwrap();
        assert_eq!(ingester.buffered_rows(), 10);

        // No files yet
        let files: Vec<_> = store.list(None).collect().await;
        assert_eq!(files.len(), 0);

        // Shutdown should trigger final flush
        handle.shutdown().await;

        // Buffer should be empty after shutdown
        assert_eq!(ingester.buffered_rows(), 0);

        // File should exist from final flush
        let files: Vec<_> = store.list(None).collect().await;
        assert_eq!(files.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn background_flush_no_flush_when_empty() {
        use object_store::ObjectStore;

        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let mut config = test_buffer_config(1000, 10 * 1024 * 1024);
        config.flush_interval_secs = 1;
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Arc::new(Ingester::new(
            Signal::Logs,
            schema,
            store.clone(),
            config,
            parquet_config,
        ));

        let handle = start_background_flush(ingester.clone());

        // Don't ingest anything - buffer is empty
        assert_eq!(ingester.buffered_rows(), 0);

        // Advance time past multiple intervals
        tokio::time::sleep(Duration::from_secs(5)).await;

        // No files should be created (nothing to flush)
        let files: Vec<_> = store.list(None).collect().await;
        assert_eq!(files.len(), 0);

        // Shutdown (also shouldn't create files)
        handle.shutdown().await;

        let files: Vec<_> = store.list(None).collect().await;
        assert_eq!(files.len(), 0);
    }

    /// Test concurrent flushes don't lose data (TOCTOU fix verification)
    #[tokio::test]
    async fn concurrent_flush_no_data_loss() {
        use object_store::ObjectStore;

        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let mut config = test_buffer_config(10000, 100 * 1024 * 1024);
        config.flush_interval_secs = 3600;
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Arc::new(Ingester::new(
            Signal::Traces,
            schema.clone(),
            store.clone(),
            config,
            parquet_config,
        ));

        // Ingest data
        let batch = test_batch(&schema, 100);
        ingester.ingest(batch).await.unwrap();
        assert_eq!(ingester.buffered_rows(), 100);

        // Spawn multiple concurrent flushes
        let mut handles = vec![];
        for _ in 0..10 {
            let ing = ingester.clone();
            handles.push(tokio::spawn(async move { ing.flush().await }));
        }

        // Wait for all flushes to complete
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Buffer should be empty
        assert_eq!(ingester.buffered_rows(), 0);

        // Only one file should exist (first flush writes, others find empty buffer)
        let files: Vec<_> = store.list(None).collect().await;
        assert_eq!(files.len(), 1);
    }

    /// Test concurrent ingest and flush operations
    #[tokio::test]
    async fn concurrent_ingest_and_flush() {
        use object_store::ObjectStore;

        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let mut config = test_buffer_config(10000, 100 * 1024 * 1024);
        config.flush_interval_secs = 3600;
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Arc::new(Ingester::new(
            Signal::Metrics,
            schema.clone(),
            store.clone(),
            config,
            parquet_config,
        ));

        // Spawn concurrent ingest tasks
        let mut handles = vec![];
        for _ in 0..5 {
            let ing = ingester.clone();
            let sch = schema.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..10 {
                    let batch = test_batch(&sch, 10);
                    ing.ingest(batch).await.unwrap();
                }
            }));
        }

        // Also spawn flush tasks
        for _ in 0..3 {
            let ing = ingester.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..5 {
                    ing.flush().await.unwrap();
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // Final flush to capture any remaining data
        ingester.flush().await.unwrap();

        // All data should be persisted: 5 tasks * 10 batches * 10 rows = 500 rows
        let expected_rows = 5 * 10 * 10;

        // Count rows in all parquet files
        let files: Vec<_> = store.list(None).collect().await;
        let mut total_rows = 0;
        for file in files {
            let meta = file.unwrap();
            let data = store
                .get(&meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let reader =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(data)
                    .unwrap()
                    .build()
                    .unwrap();
            for batch in reader {
                total_rows += batch.unwrap().num_rows();
            }
        }

        assert_eq!(
            total_rows, expected_rows,
            "data loss detected in concurrent operations"
        );
    }

    #[tokio::test]
    async fn request_too_large_rejected() {
        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let mut config = test_buffer_config(1000, 10 * 1024 * 1024);
        config.max_records_per_request = 50;
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Ingester::new(
            Signal::Traces,
            schema.clone(),
            store,
            config,
            parquet_config,
        );

        // Batch within limit should succeed
        let batch = test_batch(&schema, 40);
        assert!(ingester.ingest(batch).await.is_ok());

        // Batch exceeding limit should be rejected
        let batch = test_batch(&schema, 60);
        let result = ingester.ingest(batch).await;
        match result {
            Err(TelemetryError::RequestTooLarge { records, limit }) => {
                assert_eq!(records, 60);
                assert_eq!(limit, 50);
            }
            other => panic!("expected RequestTooLarge, got {:?}", other),
        }
    }

    /// Test multiple concurrent ingests race safely
    #[tokio::test]
    async fn concurrent_ingests_no_race() {
        let schema = test_schema();
        let store = Arc::new(InMemory::new());
        let mut config = test_buffer_config(100000, 100 * 1024 * 1024);
        config.flush_interval_secs = 3600;
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let ingester = Arc::new(Ingester::new(
            Signal::Logs,
            schema.clone(),
            store,
            config,
            parquet_config,
        ));

        // Spawn many concurrent ingest tasks
        let mut handles = vec![];
        let tasks = 20;
        let batches_per_task = 50;
        let rows_per_batch = 10;

        for _ in 0..tasks {
            let ing = ingester.clone();
            let sch = schema.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..batches_per_task {
                    let batch = test_batch(&sch, rows_per_batch);
                    ing.ingest(batch).await.unwrap();
                }
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // All rows should be buffered
        let expected = tasks * batches_per_task * rows_per_batch;
        assert_eq!(
            ingester.buffered_rows(),
            expected,
            "row count mismatch in concurrent ingests"
        );
    }
}
