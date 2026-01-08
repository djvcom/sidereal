//! Benchmarks for DataFusion query engine.
//!
//! These benchmarks measure query latency and throughput for common
//! telemetry query patterns against pre-populated Parquet files.

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use object_store::memory::InMemory;
use tokio::runtime::Runtime;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};

use sidereal_telemetry::buffer::Ingester;
use sidereal_telemetry::config::{BufferConfig, ParquetConfig};
use sidereal_telemetry::ingest::convert_traces_to_arrow;
use sidereal_telemetry::query::QueryEngineBuilder;
use sidereal_telemetry::schema::traces::traces_storage_schema;
use sidereal_telemetry::storage::Signal;

fn bench_config() -> (BufferConfig, ParquetConfig) {
    (
        BufferConfig {
            max_batch_size: 100_000,
            flush_interval_secs: 3600,
            max_buffer_bytes: 1024 * 1024 * 1024,
            max_records_per_request: 1_000_000,
            flush_max_retries: 0,
            flush_initial_delay_ms: 100,
            flush_max_delay_ms: 10_000,
        },
        ParquetConfig {
            row_group_size: 10_000,
            compression: "zstd".to_string(),
        },
    )
}

fn sample_span(id: usize) -> Span {
    Span {
        trace_id: {
            let mut bytes = [0u8; 16];
            bytes[..8].copy_from_slice(&(id as u64).to_be_bytes());
            bytes.to_vec()
        },
        span_id: (id as u64).to_be_bytes().to_vec(),
        name: format!("span-{}", id % 100),
        kind: span::SpanKind::Server as i32,
        start_time_unix_nano: 1_704_067_200_000_000_000 + (id as u64 * 1_000_000),
        end_time_unix_nano: 1_704_067_200_000_000_000 + (id as u64 * 1_000_000) + 50_000_000,
        attributes: vec![
            KeyValue {
                key: "http.request.method".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue("GET".to_string())),
                }),
            },
            KeyValue {
                key: "http.route".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::StringValue(format!(
                        "/api/v1/resource/{}",
                        id % 10
                    ))),
                }),
            },
        ],
        status: Some(Status {
            code: if id % 20 == 0 { 2 } else { 1 }, // 5% errors
            message: String::new(),
        }),
        ..Default::default()
    }
}

fn trace_request(span_count: usize, service: &str) -> ExportTraceServiceRequest {
    let spans: Vec<_> = (0..span_count).map(sample_span).collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(service.to_string())),
                    }),
                }],
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

async fn setup_test_data(store: Arc<InMemory>, row_count: usize) {
    let (buffer_config, parquet_config) = bench_config();
    let ingester = Ingester::new(
        Signal::Traces,
        traces_storage_schema(),
        store.clone(),
        buffer_config,
        parquet_config,
    );

    // Ingest data for multiple services
    let services = ["api-gateway", "user-service", "order-service"];
    let per_service = row_count / services.len();

    for service in services {
        let request = trace_request(per_service, service);
        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
        ingester.ingest(batch).await.unwrap();
    }

    ingester.flush().await.unwrap();
}

fn bench_query_all(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("query_select_all");

    for row_count in [1000, 10000] {
        let store = Arc::new(InMemory::new());
        rt.block_on(setup_test_data(store.clone(), row_count));

        let engine = rt
            .block_on(QueryEngineBuilder::new(store.clone(), "memory://").build())
            .unwrap();

        group.bench_with_input(BenchmarkId::new("rows", row_count), &engine, |b, engine| {
            b.to_async(&rt).iter(|| async {
                engine
                    .query("SELECT * FROM traces LIMIT 100")
                    .await
                    .unwrap()
            });
        });
    }

    group.finish();
}

fn bench_query_by_service(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("query_by_service");

    let store = Arc::new(InMemory::new());
    rt.block_on(setup_test_data(store.clone(), 10000));

    let engine = rt
        .block_on(QueryEngineBuilder::new(store.clone(), "memory://").build())
        .unwrap();

    group.bench_function("filter_service", |b| {
        b.to_async(&rt).iter(|| async {
            engine
                .query("SELECT * FROM traces WHERE \"service.name\" = 'api-gateway' LIMIT 100")
                .await
                .unwrap()
        });
    });

    group.finish();
}

fn bench_query_aggregation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("query_aggregation");

    let store = Arc::new(InMemory::new());
    rt.block_on(setup_test_data(store.clone(), 10000));

    let engine = rt
        .block_on(QueryEngineBuilder::new(store.clone(), "memory://").build())
        .unwrap();

    group.bench_function("count_by_service", |b| {
        b.to_async(&rt).iter(|| async {
            engine
                .query("SELECT \"service.name\", COUNT(*) FROM traces GROUP BY \"service.name\"")
                .await
                .unwrap()
        });
    });

    group.bench_function("avg_duration", |b| {
        b.to_async(&rt).iter(|| async {
            engine
                .query("SELECT AVG(duration_ns) FROM traces")
                .await
                .unwrap()
        });
    });

    group.finish();
}

fn bench_query_errors(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("query_errors");

    let store = Arc::new(InMemory::new());
    rt.block_on(setup_test_data(store.clone(), 10000));

    let engine = rt
        .block_on(QueryEngineBuilder::new(store.clone(), "memory://").build())
        .unwrap();

    group.bench_function("filter_errors", |b| {
        b.to_async(&rt).iter(|| async {
            engine
                .query("SELECT * FROM traces WHERE status_code = 2 LIMIT 100")
                .await
                .unwrap()
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_query_all,
    bench_query_by_service,
    bench_query_aggregation,
    bench_query_errors
);
criterion_main!(benches);
