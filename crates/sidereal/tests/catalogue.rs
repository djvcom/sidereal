//! End-to-end tests for the storage catalogue.
//!
//! The bucket is the only source of truth and operators may expire objects
//! from it with lifecycle rules. These tests pin the behaviour that makes
//! that safe: new flushes are queryable at once, a vanished file degrades a
//! query rather than failing it, and a restart recovers everything from
//! the bucket alone.

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use futures::TryStreamExt;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use sidereal::buffer::Ingester;
use sidereal::catalogue::CatalogueStore;
use sidereal::config::{BufferConfig, ParquetConfig, StorageConfig};
use sidereal::ingest::convert_traces_to_arrow;
use sidereal::query::{QueryEngine, QueryEngineBuilder};
use sidereal::schema::traces::traces_storage_schema;
use sidereal::storage::{create_object_store, query_base_url, Signal};

/// One span per call, each in its own hour so every flush writes a
/// separate file in a separate partition.
fn trace_request(hour: u64) -> ExportTraceServiceRequest {
    let start = 1_751_400_000_000_000_000 + hour * 3_600_000_000_000;
    let json = format!(
        r#"{{"resourceSpans":[{{"resource":{{"attributes":[
          {{"key":"service.name","value":{{"stringValue":"payments"}}}}
        ]}},"scopeSpans":[{{"spans":[
          {{"traceId":"5b8efff798038103d269b633813fc6{hour:02x}",
           "spanId":"eee19b7ec3c1b1{hour:02x}",
           "name":"Charge","kind":2,
           "startTimeUnixNano":"{start}",
           "endTimeUnixNano":"{end}",
           "status":{{"code":1}}}}
        ]}}]}}]}}"#,
        end = start + 1_000_000
    );
    serde_json::from_str(&json).expect("OTLP JSON should decode")
}

fn ingester(store: Arc<dyn ObjectStore>) -> Ingester {
    Ingester::new(
        Signal::Traces,
        traces_storage_schema(),
        store,
        BufferConfig::default(),
        ParquetConfig::default(),
    )
}

async fn flush_span(ingester: &Ingester, hour: u64) {
    let batch = convert_traces_to_arrow(&trace_request(hour), None)
        .expect("conversion should succeed")
        .batch;
    ingester.ingest(batch).await.expect("ingest should buffer");
    ingester.flush().await.expect("flush should write parquet");
}

async fn span_count(engine: &QueryEngine) -> i64 {
    let batches = engine
        .query("SELECT COUNT(*) FROM traces")
        .await
        .expect("query should succeed");
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count is an integer")
        .value(0)
}

async fn data_files(store: &dyn ObjectStore) -> Vec<object_store::path::Path> {
    let metas: Vec<_> = store.list(None).try_collect().await.expect("list");
    metas.into_iter().map(|meta| meta.location).collect()
}

#[tokio::test]
async fn flushed_data_is_queryable_without_a_refresh() {
    let bucket = Arc::new(InMemory::new());
    let catalogue = Arc::new(CatalogueStore::load(bucket).await.expect("load"));
    let engine = QueryEngine::new(catalogue.clone(), "memory://")
        .await
        .expect("engine");
    let ingester = ingester(catalogue.clone());

    assert_eq!(span_count(&engine).await, 0);
    flush_span(&ingester, 0).await;
    assert_eq!(span_count(&engine).await, 1);
    flush_span(&ingester, 1).await;
    assert_eq!(span_count(&engine).await, 2);
}

#[tokio::test]
async fn a_file_expired_by_the_bucket_degrades_the_query_instead_of_failing_it() {
    let bucket = Arc::new(InMemory::new());
    let catalogue = Arc::new(CatalogueStore::load(bucket.clone()).await.expect("load"));
    // No block cache, so the read really reaches the bucket.
    let engine = QueryEngineBuilder::new(catalogue.clone(), "memory://")
        .with_cache_bytes(0)
        .build()
        .await
        .expect("engine");
    let ingester = ingester(catalogue.clone());

    for hour in 0..3 {
        flush_span(&ingester, hour).await;
    }
    assert_eq!(span_count(&engine).await, 3);

    // A lifecycle rule removes one file directly from the bucket; the
    // catalogue still lists it.
    let files = data_files(bucket.as_ref()).await;
    assert_eq!(files.len(), 3);
    bucket.delete(&files[1]).await.expect("delete");
    assert_eq!(catalogue.len(), 3);

    // SELECT * forces the data pages to be read, not just footers.
    let rows: usize = engine
        .query("SELECT * FROM traces")
        .await
        .expect("query should survive the missing file")
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 2);
    assert_eq!(catalogue.len(), 2, "the missing file was dropped");
}

#[tokio::test]
async fn a_restart_recovers_the_catalogue_from_the_bucket_alone() {
    let bucket = Arc::new(InMemory::new());
    {
        let catalogue = Arc::new(CatalogueStore::load(bucket.clone()).await.expect("load"));
        let ingester = ingester(catalogue);
        flush_span(&ingester, 0).await;
        flush_span(&ingester, 1).await;
    }

    // A new process: nothing carried over except the bucket.
    let catalogue = Arc::new(CatalogueStore::load(bucket).await.expect("load"));
    let engine = QueryEngine::new(catalogue, "memory://")
        .await
        .expect("engine");
    assert_eq!(span_count(&engine).await, 2);
}

#[tokio::test]
async fn local_storage_is_queried_through_the_catalogue() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = StorageConfig::Local {
        path: dir.path().to_path_buf(),
    };
    let store = create_object_store(&config).expect("store");
    let catalogue = Arc::new(CatalogueStore::load(store).await.expect("load"));
    // As the server does for local storage: no block cache, the operating
    // system caches files already.
    let engine = QueryEngineBuilder::new(catalogue.clone(), query_base_url(&config))
        .with_cache_bytes(0)
        .build()
        .await
        .expect("engine");
    let ingester = ingester(catalogue.clone());

    flush_span(&ingester, 0).await;
    flush_span(&ingester, 1).await;
    assert_eq!(span_count(&engine).await, 2);

    // A file removed from disk by hand is tolerated in the same way.
    let files = data_files(catalogue.as_ref()).await;
    std::fs::remove_file(dir.path().join(files[0].as_ref())).expect("remove");
    let rows: usize = engine
        .query("SELECT * FROM traces")
        .await
        .expect("query should survive the missing file")
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(rows, 1);
}
