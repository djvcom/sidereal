//! Benchmarks for telemetry ingestion pipeline.
//!
//! These benchmarks measure the throughput of buffering and flushing telemetry
//! data, including lock contention and Parquet write performance.

use std::sync::Arc;

use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use object_store::memory::InMemory;
use tokio::runtime::Runtime;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};

use sidereal_telemetry::buffer::Ingester;
use sidereal_telemetry::config::{BufferConfig, ParquetConfig};
use sidereal_telemetry::ingest::convert_traces_to_arrow;
use sidereal_telemetry::schema::traces::traces_storage_schema;
use sidereal_telemetry::storage::Signal;

fn bench_config() -> (BufferConfig, ParquetConfig) {
    (
        BufferConfig {
            max_batch_size: 100_000,
            flush_interval_secs: 3600, // Disable auto-flush for benchmarks
            max_buffer_bytes: 1024 * 1024 * 1024,
            max_records_per_request: 1_000_000,
            flush_max_retries: 0,
            flush_initial_delay_ms: 100,
            flush_max_delay_ms: 10_000,
        },
        ParquetConfig {
            row_group_size: 100_000,
            compression: "zstd".to_string(),
        },
    )
}

fn sample_span(id: u8) -> Span {
    Span {
        trace_id: vec![id; 16],
        span_id: vec![id; 8],
        name: format!("benchmark-span-{}", id),
        kind: span::SpanKind::Server as i32,
        start_time_unix_nano: 1_704_067_200_000_000_000,
        end_time_unix_nano: 1_704_067_201_000_000_000,
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
                    value: Some(any_value::Value::StringValue("/api/v1/traces".to_string())),
                }),
            },
            KeyValue {
                key: "http.response.status_code".to_string(),
                value: Some(AnyValue {
                    value: Some(any_value::Value::IntValue(200)),
                }),
            },
        ],
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    }
}

fn trace_request(span_count: usize) -> ExportTraceServiceRequest {
    let spans: Vec<_> = (0..span_count).map(|i| sample_span(i as u8)).collect();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue("bench-service".to_string())),
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

fn bench_ingest(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("ingest");

    for batch_size in [10, 100, 1000] {
        let request = trace_request(batch_size);
        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
        let batch_bytes = batch.get_array_memory_size();

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(BenchmarkId::new("spans", batch_size), &batch, |b, batch| {
            b.to_async(&rt).iter_with_setup(
                || {
                    let store = Arc::new(InMemory::new());
                    let (buffer_config, parquet_config) = bench_config();
                    let ingester = Ingester::new(
                        Signal::Traces,
                        traces_storage_schema(),
                        store,
                        buffer_config,
                        parquet_config,
                    );
                    (ingester, batch.clone())
                },
                |(ingester, batch)| async move {
                    ingester.ingest(black_box(batch)).await.unwrap();
                },
            );
        });

        group.throughput(Throughput::Bytes(batch_bytes as u64));
        group.bench_with_input(BenchmarkId::new("bytes", batch_size), &batch, |b, batch| {
            b.to_async(&rt).iter_with_setup(
                || {
                    let store = Arc::new(InMemory::new());
                    let (buffer_config, parquet_config) = bench_config();
                    let ingester = Ingester::new(
                        Signal::Traces,
                        traces_storage_schema(),
                        store,
                        buffer_config,
                        parquet_config,
                    );
                    (ingester, batch.clone())
                },
                |(ingester, batch)| async move {
                    ingester.ingest(black_box(batch)).await.unwrap();
                },
            );
        });
    }

    group.finish();
}

fn bench_flush(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("flush");

    for row_count in [100, 1000, 10000] {
        let request = trace_request(row_count);
        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;

        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(BenchmarkId::new("rows", row_count), &batch, |b, batch| {
            b.iter_batched(
                || {
                    let store = Arc::new(InMemory::new());
                    let (buffer_config, parquet_config) = bench_config();
                    let ingester = Ingester::new(
                        Signal::Traces,
                        traces_storage_schema(),
                        store,
                        buffer_config,
                        parquet_config,
                    );
                    let batch_clone = batch.clone();
                    rt.block_on(async {
                        ingester.ingest(batch_clone).await.unwrap();
                    });
                    ingester
                },
                |ingester| {
                    rt.block_on(async {
                        ingester.flush().await.unwrap();
                    });
                },
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_ingest, bench_flush);
criterion_main!(benches);
