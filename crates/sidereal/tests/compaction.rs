//! End-to-end tests for compaction.
//!
//! Compaction rewrites and deletes stored telemetry, so what matters most
//! is what a query sees: the same rows before, during and after, whatever
//! goes wrong and whenever. Each failure test stops the protocol at a
//! different step and checks both the live process and a restarted one.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sidereal::buffer::Ingester;
use sidereal::catalogue::CatalogueStore;
use sidereal::compaction::Compactor;
use sidereal::config::{BufferConfig, CompactionConfig, ParquetConfig};
use sidereal::ingest::convert_traces_to_arrow;
use sidereal::query::{QueryEngine, QueryEngineBuilder};
use sidereal::schema::traces::traces_storage_schema;
use sidereal::storage::Signal;

/// 2025-07-01T20:00:00Z; every span falls within this hour.
const HOUR_START: u64 = 1_751_400_000_000_000_000;

/// A bucket whose deletes can be made to fail, to stop compaction after
/// its commit point.
#[derive(Debug)]
struct Bucket {
    inner: InMemory,
    fail_deletes: AtomicBool,
}

impl Bucket {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: InMemory::new(),
            fail_deletes: AtomicBool::new(false),
        })
    }
}

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Bucket")
    }
}

#[async_trait]
impl ObjectStore for Bucket {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }
    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        if self.fail_deletes.load(Ordering::SeqCst) {
            return Err(object_store::Error::Generic {
                store: "Bucket",
                source: "deletes are failing".into(),
            });
        }
        self.inner.delete(location).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }
    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_owned())),
        }),
    }
}

/// `count` spans for `service`, with IDs derived from `seed` so every
/// span in a test is distinct.
fn request(service: &str, seed: u64, count: u64) -> ExportTraceServiceRequest {
    let spans = (0..count)
        .map(|i| {
            let n = seed * 1_000_000 + i;
            let mut trace_id = [0u8; 16];
            trace_id[8..].copy_from_slice(&n.to_be_bytes());
            Span {
                trace_id: trace_id.to_vec(),
                span_id: (n + 1).to_be_bytes().to_vec(),
                name: format!("op-{seed}"),
                kind: 2,
                start_time_unix_nano: HOUR_START + n * 1_000,
                end_time_unix_nano: HOUR_START + n * 1_000 + 500,
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_attr("service.name", service)],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

struct Fixture {
    bucket: Arc<Bucket>,
    catalogue: Arc<CatalogueStore>,
    ingester: Ingester,
    engine: QueryEngine,
}

async fn engine_over(catalogue: &Arc<CatalogueStore>) -> QueryEngine {
    // No block cache, so every read reflects what is in the bucket.
    QueryEngineBuilder::new(catalogue.clone(), "memory://")
        .with_cache_bytes(0)
        .build()
        .await
        .expect("engine")
}

async fn fixture() -> Fixture {
    let bucket = Bucket::new();
    let catalogue = Arc::new(CatalogueStore::load(bucket.clone()).await.expect("load"));
    let ingester = Ingester::new(
        Signal::Traces,
        traces_storage_schema(),
        catalogue.clone(),
        BufferConfig {
            max_records_per_request: 1_000_000,
            ..Default::default()
        },
        ParquetConfig::default(),
    );
    let engine = engine_over(&catalogue).await;
    Fixture {
        bucket,
        catalogue,
        ingester,
        engine,
    }
}

impl Fixture {
    /// Ingest and flush, producing one new file.
    async fn flush(&self, service: &str, seed: u64, count: u64) {
        let batch = convert_traces_to_arrow(&request(service, seed, count), None)
            .expect("convert")
            .batch;
        self.ingester.ingest(batch).await.expect("ingest");
        self.ingester.flush().await.expect("flush");
    }

    fn compactor(&self, config: CompactionConfig) -> Compactor {
        Compactor::new(self.catalogue.clone(), config, ParquetConfig::default())
    }

    /// Names of the objects in the bucket itself, bypassing the catalogue.
    async fn bucket_files(&self) -> Vec<String> {
        let metas: Vec<ObjectMeta> = self.bucket.list(None).try_collect().await.expect("list");
        metas
            .into_iter()
            .filter_map(|meta| meta.location.filename().map(str::to_owned))
            .collect()
    }

    /// A fresh process over the same bucket.
    async fn restart(&self) -> (Arc<CatalogueStore>, QueryEngine) {
        let catalogue = Arc::new(
            CatalogueStore::load(self.bucket.clone())
                .await
                .expect("load"),
        );
        let engine = engine_over(&catalogue).await;
        (catalogue, engine)
    }
}

/// Settled immediately, so one pass produces the final form.
fn settle_now() -> CompactionConfig {
    CompactionConfig {
        settle_secs: 0,
        ..Default::default()
    }
}

async fn span_count(engine: &QueryEngine) -> i64 {
    let batches = engine
        .query("SELECT COUNT(*) FROM traces")
        .await
        .expect("count query");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer count")
        .value(0)
}

fn count_matching(names: &[String], suffix: &str) -> usize {
    names.iter().filter(|name| name.ends_with(suffix)).count()
}

#[tokio::test]
async fn many_flushed_files_become_one_sorted_file_with_the_same_rows() {
    let f = fixture().await;
    let services = ["payments", "checkout", "auth", "cart"];
    for seed in 0..12u64 {
        f.flush(services[(seed % 4) as usize], seed, 5).await;
    }
    assert_eq!(f.bucket_files().await.len(), 12);
    assert_eq!(span_count(&f.engine).await, 60);

    let report = f.compactor(settle_now()).run_once().await.expect("compact");
    assert_eq!(report.jobs, 1);
    assert_eq!(report.files_in, 12);
    assert_eq!(report.files_out, 1);
    assert_eq!(report.rows, 60);
    assert_eq!(report.failed, 0);

    // The bucket holds exactly the merged file: inputs and marker are gone.
    let names = f.bucket_files().await;
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].ends_with(".l2.parquet"), "{names:?}");

    // Queries see the same rows, in the layout's order within the file.
    assert_eq!(span_count(&f.engine).await, 60);
    let batches = f
        .engine
        .query("SELECT \"service.name\" FROM traces")
        .await
        .expect("query");
    let services_read: Vec<String> = batches
        .iter()
        .flat_map(|batch: &RecordBatch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string column")
                .clone();
            (0..column.len())
                .map(|i| column.value(i).to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    let mut sorted = services_read.clone();
    sorted.sort();
    assert_eq!(services_read, sorted, "rows are sorted by service");

    // A trace that lived in one of the merged files is still found.
    let found = f
        .engine
        .query("SELECT name FROM traces WHERE trace_id = X'000000000000000000000000006ACFC2'")
        .await
        .expect("lookup");
    let rows: usize = found.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "seed 7, span 2");

    // A second pass has nothing left to do.
    let again = f.compactor(settle_now()).run_once().await.expect("compact");
    assert_eq!(again.jobs, 0);

    // And a restart sees the same thing from the bucket alone.
    let (_, restarted) = f.restart().await;
    assert_eq!(span_count(&restarted).await, 60);
}

#[tokio::test]
async fn an_active_partition_merges_flushed_files_in_groups() {
    let f = fixture().await;
    let config = CompactionConfig {
        settle_secs: 86_400,
        minor_files: 4,
        ..Default::default()
    };
    let compactor = f.compactor(config);

    for seed in 0..3u64 {
        f.flush("payments", seed, 2).await;
    }
    assert_eq!(compactor.run_once().await.expect("compact").jobs, 0);

    f.flush("payments", 3, 2).await;
    let report = compactor.run_once().await.expect("compact");
    assert_eq!((report.jobs, report.files_in, report.files_out), (1, 4, 1));

    // New flushes accumulate beside the merged file, which is not rewritten.
    f.flush("payments", 4, 2).await;
    assert_eq!(compactor.run_once().await.expect("compact").jobs, 0);
    let names = f.bucket_files().await;
    assert_eq!(count_matching(&names, ".l1.parquet"), 1, "{names:?}");
    assert_eq!(names.len(), 2, "{names:?}");
    assert_eq!(span_count(&f.engine).await, 10);
}

#[tokio::test]
async fn a_large_partition_is_split_into_several_files() {
    let f = fixture().await;
    for seed in 0..3u64 {
        f.flush("payments", seed, 10_000).await;
    }
    let config = CompactionConfig {
        settle_secs: 0,
        target_file_bytes: 1,
        ..Default::default()
    };

    let report = f.compactor(config).run_once().await.expect("compact");
    assert_eq!(report.rows, 30_000);
    assert!(report.files_out >= 2, "{report:?}");
    assert_eq!(f.bucket_files().await.len(), report.files_out);
    assert_eq!(span_count(&f.engine).await, 30_000);
}

#[tokio::test]
async fn a_crash_while_writing_outputs_leaves_the_inputs_authoritative() {
    let f = fixture().await;
    for seed in 0..3u64 {
        f.flush("payments", seed, 4).await;
    }

    // Stage the state a crash mid-write leaves behind: a marker in state
    // `writing` and a complete-looking output holding rows that are also
    // in the inputs.
    let inputs: Vec<ObjectMeta> = f.bucket.list(None).try_collect().await.expect("list");
    let partition = "traces/date=2025-07-01/hour=20";
    let orphan = Path::from(format!("{partition}/01CRASHED-000.l2.parquet"));
    f.bucket
        .copy(&inputs[0].location, &orphan)
        .await
        .expect("copy");
    let marker = serde_json::json!({
        "id": "01CRASHED",
        "state": "writing",
        "inputs": inputs.iter().map(|m| m.location.to_string()).collect::<Vec<_>>(),
        "outputs": [],
    });
    f.bucket
        .put(
            &Path::from(format!("{partition}/01CRASHED.compaction.json")),
            marker.to_string().into_bytes().into(),
        )
        .await
        .expect("put marker");

    // A restarted process does not count the orphan's rows twice...
    let (catalogue, engine) = f.restart().await;
    assert_eq!(span_count(&engine).await, 12);

    // ...and recovery removes the orphan and the marker, keeping the inputs.
    let compactor = Compactor::new(
        catalogue,
        CompactionConfig {
            settle_secs: 86_400,
            ..Default::default()
        },
        ParquetConfig::default(),
    );
    assert_eq!(compactor.recover().await.expect("recover"), 1);
    let names = f.bucket_files().await;
    assert_eq!(names.len(), 3, "{names:?}");
    assert_eq!(count_matching(&names, ".l2.parquet"), 0);
    assert_eq!(count_matching(&names, ".compaction.json"), 0);
    assert_eq!(span_count(&engine).await, 12);
}

#[tokio::test]
async fn a_crash_after_the_commit_leaves_the_outputs_authoritative() {
    let f = fixture().await;
    for seed in 0..4u64 {
        f.flush("payments", seed, 3).await;
    }

    // Deletes fail, so compaction commits but cannot remove its inputs.
    f.bucket.fail_deletes.store(true, Ordering::SeqCst);
    let report = f.compactor(settle_now()).run_once().await.expect("pass");
    assert_eq!(report.failed, 1);

    let names = f.bucket_files().await;
    assert_eq!(count_matching(&names, ".l2.parquet"), 1, "{names:?}");
    assert_eq!(count_matching(&names, ".compaction.json"), 1, "{names:?}");
    assert_eq!(names.len(), 6, "inputs are still in the bucket: {names:?}");

    // The live process already serves the outputs, each row once.
    assert_eq!(span_count(&f.engine).await, 12);

    // So does a restarted one, which has only the bucket to go on.
    let (catalogue, engine) = f.restart().await;
    assert_eq!(span_count(&engine).await, 12);

    // Once storage recovers, recovery finishes the job.
    f.bucket.fail_deletes.store(false, Ordering::SeqCst);
    let compactor = Compactor::new(catalogue, settle_now(), ParquetConfig::default());
    assert_eq!(compactor.recover().await.expect("recover"), 1);
    let names = f.bucket_files().await;
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].ends_with(".l2.parquet"));
    assert_eq!(span_count(&engine).await, 12);
}

#[tokio::test]
async fn an_input_expired_by_the_bucket_is_simply_absent_from_the_merge() {
    let f = fixture().await;
    for seed in 0..3u64 {
        f.flush("payments", seed, 4).await;
    }

    // A lifecycle rule removes one input behind the catalogue's back, so
    // the merge is planned over three files of which two exist.
    let inputs: Vec<ObjectMeta> = f.bucket.list(None).try_collect().await.expect("list");
    f.bucket
        .inner
        .delete(&inputs[1].location)
        .await
        .expect("delete");

    let report = f.compactor(settle_now()).run_once().await.expect("pass");
    assert_eq!((report.jobs, report.failed), (1, 0));
    assert_eq!(report.rows, 8, "only the surviving rows");

    // The survivors are merged, nothing half-made is left behind, and the
    // expired file is no longer in the catalogue.
    let names = f.bucket_files().await;
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].ends_with(".l2.parquet"));
    assert_eq!(f.catalogue.len(), 1);
    assert_eq!(span_count(&f.engine).await, 8);

    let (_, restarted) = f.restart().await;
    assert_eq!(span_count(&restarted).await, 8);
}

#[tokio::test]
async fn queries_planned_before_and_after_a_merge_agree() {
    let f = fixture().await;
    for seed in 0..6u64 {
        f.flush("payments", seed, 50).await;
    }
    let sql = "SELECT name, COUNT(*) AS spans FROM traces GROUP BY name ORDER BY name";
    let before = f.engine.query(sql).await.expect("before");

    f.compactor(settle_now()).run_once().await.expect("compact");

    let after = f.engine.query(sql).await.expect("after");
    let render = |batches: &[RecordBatch]| {
        arrow::util::pretty::pretty_format_batches(batches)
            .expect("format")
            .to_string()
    };
    assert_eq!(render(&before), render(&after));
}
