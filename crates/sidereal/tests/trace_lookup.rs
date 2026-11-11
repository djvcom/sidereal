//! End-to-end tests for fetching a trace by its ID.
//!
//! A trace is asked for by ID alone. These tests pin that the index sends
//! the lookup to the right partitions, that it never hides a trace that
//! exists (whatever state its files are in), and that the HTTP endpoint
//! returns the whole trace.

use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray, UInt64Array};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sidereal::buffer::Ingester;
use sidereal::catalogue::CatalogueStore;
use sidereal::compaction::Compactor;
use sidereal::config::{BufferConfig, CompactionConfig, ParquetConfig};
use sidereal::ingest::convert_traces_to_arrow;
use sidereal::query::{fetch_trace, query_router, QueryApiState, QueryEngine, QueryEngineBuilder};
use sidereal::schema::traces::traces_storage_schema;
use sidereal::storage::Signal;
use sidereal::trace_index::{Partition, TraceIndex};
use tower::ServiceExt;

/// 2025-07-01T20:00:00Z.
const HOUR_20: u64 = 1_751_400_000_000_000_000;
const HOUR_NANOS: u64 = 3_600_000_000_000;

fn trace_id(n: u8) -> [u8; 16] {
    let mut id = [0x5b; 16];
    id[15] = n;
    id
}

/// One span of trace `trace` emitted by `service`, starting `offset_ms`
/// into the hour beginning at `hour_start`.
fn span_request(service: &str, trace: u8, span: u8, start: u64) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_owned(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(service.to_owned())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: trace_id(trace).to_vec(),
                    span_id: vec![trace, span, 0, 0, 0, 0, 0, 1],
                    name: format!("{service}-op"),
                    kind: 2,
                    start_time_unix_nano: start,
                    end_time_unix_nano: start + 1_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

struct Fixture {
    bucket: Arc<InMemory>,
    catalogue: Arc<CatalogueStore>,
    ingester: Ingester,
    engine: Arc<QueryEngine>,
    index: Arc<TraceIndex>,
}

async fn fixture() -> Fixture {
    let bucket = Arc::new(InMemory::new());
    let catalogue = Arc::new(CatalogueStore::load(bucket.clone()).await.expect("load"));
    let ingester = Ingester::new(
        Signal::Traces,
        traces_storage_schema(),
        catalogue.clone(),
        BufferConfig::default(),
        ParquetConfig::default(),
    );
    let engine = Arc::new(
        QueryEngineBuilder::new(catalogue.clone(), "memory://")
            .with_cache_bytes(0)
            .build()
            .await
            .expect("engine"),
    );
    let index = Arc::new(TraceIndex::new(catalogue.clone()));
    Fixture {
        bucket,
        catalogue,
        ingester,
        engine,
        index,
    }
}

impl Fixture {
    /// Ingest one span and flush it into its own file.
    async fn flush(&self, service: &str, trace: u8, span: u8, start: u64) {
        let batch = convert_traces_to_arrow(&span_request(service, trace, span, start), None)
            .expect("convert")
            .batch;
        self.ingester.ingest(batch).await.expect("ingest");
        self.ingester.flush().await.expect("flush");
    }

    /// A trace whose spans are spread over three services, three files
    /// and two hours, among other traces.
    async fn populate(&self) {
        self.flush("gateway", 1, 1, HOUR_20 + 1_000).await;
        self.flush("checkout", 1, 2, HOUR_20 + 2_000).await;
        self.flush("gateway", 2, 1, HOUR_20 + 3_000).await;
        // An asynchronous tail that lands in the next hour.
        self.flush("worker", 1, 3, HOUR_20 + HOUR_NANOS + 5_000)
            .await;
        // Unrelated traffic in a third hour.
        self.flush("gateway", 3, 1, HOUR_20 + 2 * HOUR_NANOS + 1_000)
            .await;
    }

    async fn services_of(&self, trace: u8) -> Vec<String> {
        let batches = fetch_trace(&self.engine, Some(&self.index), &trace_id(trace))
            .await
            .expect("fetch");
        column_strings(&batches, "service.name")
    }
}

fn column_strings(batches: &[RecordBatch], name: &str) -> Vec<String> {
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name(name)
                .expect("column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string column")
                .clone();
            (0..column.len())
                .map(|i| column.value(i).to_owned())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn partition(hour: &str) -> Partition {
    Partition {
        date: "2025-07-01".to_owned(),
        hour: hour.to_owned(),
    }
}

#[tokio::test]
async fn the_index_names_exactly_the_partitions_holding_a_trace() {
    let f = fixture().await;
    f.populate().await;

    assert_eq!(
        f.index.partitions(&trace_id(1)).await,
        vec![partition("20"), partition("21")]
    );
    assert_eq!(
        f.index.partitions(&trace_id(2)).await,
        vec![partition("20")]
    );
    assert_eq!(
        f.index.partitions(&trace_id(3)).await,
        vec![partition("22")]
    );
    assert!(f.index.partitions(&trace_id(99)).await.is_empty());
    assert_eq!(f.index.len(), 5, "one filter per file");
    assert!(f.index.memory_bytes() > 0);
}

#[tokio::test]
async fn a_whole_trace_is_returned_across_files_and_hours_oldest_first() {
    let f = fixture().await;
    f.populate().await;

    let batches = fetch_trace(&f.engine, Some(&f.index), &trace_id(1))
        .await
        .expect("fetch");
    assert_eq!(
        column_strings(&batches, "service.name"),
        vec!["gateway", "checkout", "worker"]
    );
    let starts: Vec<u64> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name("start_time_unix_nano")
                .expect("column")
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("u64 column")
                .clone();
            (0..column.len())
                .map(|i| column.value(i))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(starts.is_sorted(), "{starts:?}");

    // The index gives the same answer as considering every file.
    let unindexed = fetch_trace(&f.engine, None, &trace_id(1))
        .await
        .expect("fetch");
    assert_eq!(
        column_strings(&unindexed, "service.name"),
        vec!["gateway", "checkout", "worker"]
    );

    assert!(f.services_of(99).await.is_empty());
}

#[tokio::test]
async fn spans_flushed_after_the_index_was_built_are_found() {
    let f = fixture().await;
    f.populate().await;
    assert_eq!(f.services_of(1).await.len(), 3);

    f.flush("notifier", 1, 4, HOUR_20 + HOUR_NANOS + 9_000)
        .await;
    f.flush("gateway", 7, 1, HOUR_20 + 4_000).await;

    assert_eq!(
        f.services_of(1).await,
        vec!["gateway", "checkout", "worker", "notifier"]
    );
    assert_eq!(f.services_of(7).await, vec!["gateway"]);
}

#[tokio::test]
async fn a_trace_is_still_found_after_its_files_are_compacted() {
    let f = fixture().await;
    f.populate().await;
    assert_eq!(f.services_of(1).await.len(), 3);
    assert_eq!(f.index.len(), 5);

    let config = CompactionConfig {
        settle_secs: 0,
        ..Default::default()
    };
    let report = Compactor::new(f.catalogue.clone(), config, ParquetConfig::default())
        .run_once()
        .await
        .expect("compact");
    assert_eq!(report.jobs, 1, "hour 20 has three files");

    assert_eq!(
        f.services_of(1).await,
        vec!["gateway", "checkout", "worker"]
    );
    assert_eq!(f.services_of(2).await, vec!["gateway"]);
    assert_eq!(f.index.len(), 3, "filters of merged-away files are dropped");
    assert_eq!(
        f.index.partitions(&trace_id(1)).await,
        vec![partition("20"), partition("21")]
    );
}

#[tokio::test]
async fn a_file_without_a_bloom_filter_is_never_ruled_out() {
    let f = fixture().await;
    f.populate().await;

    // A trace file written without bloom filters, as files from before
    // the index existed were.
    let batch = convert_traces_to_arrow(
        &span_request("legacy", 42, 1, HOUR_20 + 3 * HOUR_NANOS + 1_000),
        None,
    )
    .expect("convert")
    .batch;
    let mut buffer = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut buffer, batch.schema(), None).expect("writer");
    writer.write(&batch).expect("write");
    writer.close().expect("close");
    f.catalogue
        .put(
            &Path::from("traces/date=2025-07-01/hour=23/legacy.parquet"),
            buffer.into(),
        )
        .await
        .expect("put");

    assert_eq!(f.services_of(42).await, vec!["legacy"]);
    // It also counts as a possible home for any other trace, which costs a
    // read but never a wrong answer.
    assert!(f
        .index
        .partitions(&trace_id(99))
        .await
        .contains(&partition("23")));
    assert!(f.services_of(99).await.is_empty());
}

#[tokio::test]
async fn a_file_that_vanished_does_not_break_the_lookup() {
    let f = fixture().await;
    f.populate().await;

    // Expired by the bucket before the index ever read it.
    let checkout = f
        .catalogue
        .objects(None)
        .into_iter()
        .nth(1)
        .expect("second file");
    f.bucket.delete(&checkout.location).await.expect("delete");

    let services = f.services_of(1).await;
    assert_eq!(services.len(), 2, "{services:?}");
}

async fn get(router: axum::Router, uri: &str) -> (StatusCode, serde_json::Value) {
    let response = router
        .oneshot(
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let json = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    (status, json)
}

#[tokio::test]
async fn the_http_endpoint_returns_a_trace_by_id() {
    let f = fixture().await;
    f.populate().await;
    let state = QueryApiState::new(f.engine.clone()).with_trace_index(f.index.clone());
    let router = query_router(state);

    let id = hex::encode(trace_id(1));
    let (status, body) = get(router.clone(), &format!("/traces/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"], 3, "{body}");

    let (status, body) = get(router.clone(), &format!("/traces/0x{}", id.to_uppercase())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"], 3, "{body}");

    let (status, body) = get(
        router.clone(),
        &format!("/traces/{}", hex::encode(trace_id(99))),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["rows"], 0, "{body}");

    let (status, _) = get(router, "/traces/not-a-trace-id").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}
