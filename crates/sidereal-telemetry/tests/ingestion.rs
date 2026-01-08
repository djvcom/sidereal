//! Integration tests for the OTLP ingestion pipeline.
//!
//! Tests the full flow: OTLP proto -> Arrow -> Parquet -> Object Store

use std::sync::Arc;

use arrow::array::{Array, AsArray, FixedSizeBinaryArray, UInt64Array, UInt8Array};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::ObjectStore;
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rstest::{fixture, rstest};

use sidereal_telemetry::buffer::Ingester;
use sidereal_telemetry::config::{BufferConfig, ParquetConfig};
use sidereal_telemetry::ingest::{
    convert_logs_to_arrow, convert_metrics_to_arrow, convert_traces_to_arrow,
};
use sidereal_telemetry::query::QueryEngine;
use sidereal_telemetry::redact::{
    ActionConfig, MatcherConfig, RedactionConfig, RedactionEngine, RedactionRule,
};
use sidereal_telemetry::schema::{
    logs::logs_storage_schema, metrics::number_metrics_storage_schema,
    traces::traces_storage_schema,
};
use sidereal_telemetry::storage::Signal;

// ============================================================================
// Fixtures
// ============================================================================

#[fixture]
fn buffer_config() -> BufferConfig {
    BufferConfig {
        max_batch_size: 100,
        flush_interval_secs: 30,
        max_buffer_bytes: 10 * 1024 * 1024,
        max_records_per_request: 100_000,
        flush_max_retries: 3,
        flush_initial_delay_ms: 10,
        flush_max_delay_ms: 100,
    }
}

#[fixture]
fn parquet_config() -> ParquetConfig {
    ParquetConfig {
        row_group_size: 1000,
        compression: "zstd".to_string(),
    }
}

#[fixture]
fn memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

#[fixture]
fn traces_ingester(
    memory_store: Arc<dyn ObjectStore>,
    buffer_config: BufferConfig,
    parquet_config: ParquetConfig,
) -> (Ingester, Arc<dyn ObjectStore>) {
    let ingester = Ingester::new(
        Signal::Traces,
        traces_storage_schema(), // Storage schema excludes partition columns
        memory_store.clone(),
        buffer_config,
        parquet_config,
    );
    (ingester, memory_store)
}

#[fixture]
fn metrics_ingester(
    memory_store: Arc<dyn ObjectStore>,
    buffer_config: BufferConfig,
    parquet_config: ParquetConfig,
) -> (Ingester, Arc<dyn ObjectStore>) {
    let ingester = Ingester::new(
        Signal::Metrics,
        number_metrics_storage_schema(), // Storage schema excludes partition columns
        memory_store.clone(),
        buffer_config,
        parquet_config,
    );
    (ingester, memory_store)
}

#[fixture]
fn logs_ingester(
    memory_store: Arc<dyn ObjectStore>,
    buffer_config: BufferConfig,
    parquet_config: ParquetConfig,
) -> (Ingester, Arc<dyn ObjectStore>) {
    let ingester = Ingester::new(
        Signal::Logs,
        logs_storage_schema(), // Storage schema excludes partition columns
        memory_store.clone(),
        buffer_config,
        parquet_config,
    );
    (ingester, memory_store)
}

// ============================================================================
// Test data builders
// ============================================================================

fn service_name_kv(name: &str) -> KeyValue {
    KeyValue {
        key: "service.name".to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(name.to_string())),
        }),
    }
}

fn sample_trace_request(service_name: &str, span_name: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![service_name_kv(service_name)],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0xAA; 16],
                    span_id: vec![0xBB; 8],
                    name: span_name.to_string(),
                    start_time_unix_nano: 1_704_067_200_000_000_000, // 2024-01-01 00:00:00
                    end_time_unix_nano: 1_704_067_201_000_000_000,   // +1 second
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn sample_metrics_request(
    service_name: &str,
    metric_name: &str,
    value: i64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![service_name_kv(service_name)],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: "Test metric".to_string(),
                    unit: "1".to_string(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1_704_067_200_000_000_000,
                            value: Some(number_data_point::Value::AsInt(value)),
                            ..Default::default()
                        }],
                    })),
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn sample_logs_request(service_name: &str, body: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![service_name_kv(service_name)],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1_704_067_200_000_000_000,
                    observed_time_unix_nano: 1_704_067_200_000_000_000,
                    severity_number: 9, // INFO
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(body.to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

// ============================================================================
// Pipeline roundtrip tests
// ============================================================================

#[rstest]
#[tokio::test]
async fn traces_pipeline_roundtrip(traces_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = traces_ingester;

    // Ingest a trace
    let request = sample_trace_request("test-service", "test-span");
    let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
    assert_eq!(batch.num_rows(), 1);

    ingester.ingest(batch).await.unwrap();
    assert_eq!(ingester.buffered_rows(), 1);

    // Flush to storage
    ingester.flush().await.unwrap();
    assert_eq!(ingester.buffered_rows(), 0);

    // Read back from storage
    let files: Vec<_> = store.list(None).collect().await;
    assert_eq!(files.len(), 1);

    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(batches.len(), 1);

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify trace_id
    let trace_id = batch
        .column_by_name("trace_id")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(trace_id.value(0), &[0xAA; 16]);

    // Verify span_id
    let span_id = batch
        .column_by_name("span_id")
        .unwrap()
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    assert_eq!(span_id.value(0), &[0xBB; 8]);

    // Verify span name
    let name = batch.column_by_name("name").unwrap().as_string::<i32>();
    assert_eq!(name.value(0), "test-span");

    // Verify service name
    let service_name = batch
        .column_by_name("service.name")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(service_name.value(0), "test-service");

    // Verify duration (1 second = 1_000_000_000 ns)
    let duration = batch
        .column_by_name("duration_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(duration.value(0), 1_000_000_000);
}

#[rstest]
#[tokio::test]
async fn metrics_pipeline_roundtrip(metrics_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = metrics_ingester;

    // Ingest a metric
    let request = sample_metrics_request("metrics-service", "test.counter", 42);
    let batch = convert_metrics_to_arrow(&request, None).unwrap().batch;
    assert_eq!(batch.num_rows(), 1);

    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    // Read back from storage
    let files: Vec<_> = store.list(None).collect().await;
    assert_eq!(files.len(), 1);

    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify metric name
    let metric_name = batch
        .column_by_name("metric_name")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(metric_name.value(0), "test.counter");

    // Verify value
    let value = batch
        .column_by_name("value_int")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(value.value(0), 42);

    // Verify service name
    let service_name = batch
        .column_by_name("service.name")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(service_name.value(0), "metrics-service");
}

#[rstest]
#[tokio::test]
async fn logs_pipeline_roundtrip(logs_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = logs_ingester;

    // Ingest a log
    let request = sample_logs_request("logs-service", "Hello from tests!");
    let batch = convert_logs_to_arrow(&request, None).unwrap().batch;
    assert_eq!(batch.num_rows(), 1);

    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    // Read back from storage
    let files: Vec<_> = store.list(None).collect().await;
    assert_eq!(files.len(), 1);

    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);

    // Verify body
    let body = batch
        .column_by_name("body_string")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(body.value(0), "Hello from tests!");

    // Verify severity
    let severity = batch
        .column_by_name("severity_number")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap();
    assert_eq!(severity.value(0), 9); // INFO

    // Verify service name
    let service_name = batch
        .column_by_name("service.name")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(service_name.value(0), "logs-service");
}

// ============================================================================
// Batching and partitioning tests
// ============================================================================

#[rstest]
#[tokio::test]
async fn multiple_batches_in_single_file(traces_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = traces_ingester;

    // Ingest multiple batches
    for i in 0..5 {
        let request = sample_trace_request("batch-service", &format!("span-{}", i));
        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
        ingester.ingest(batch).await.unwrap();
    }

    assert_eq!(ingester.buffered_rows(), 5);

    // Flush once
    ingester.flush().await.unwrap();
    assert_eq!(ingester.buffered_rows(), 0);

    // Should create a single file with 5 rows
    let files: Vec<_> = store.list(None).collect().await;
    assert_eq!(files.len(), 1);

    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let total_rows: usize = reader
        .map(|b: Result<RecordBatch, _>| b.unwrap().num_rows())
        .sum();
    assert_eq!(total_rows, 5);
}

#[rstest]
#[tokio::test]
async fn partition_path_includes_signal_and_date(
    traces_ingester: (Ingester, Arc<dyn ObjectStore>),
) {
    let (ingester, store) = traces_ingester;

    let request = sample_trace_request("test-service", "test-span");
    let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    let files: Vec<_> = store.list(None).collect().await;
    let path = files[0].as_ref().unwrap().location.to_string();

    // Path should include signal type
    assert!(
        path.starts_with("traces/"),
        "Path should start with signal: {}",
        path
    );

    // Path should include date partition
    assert!(path.contains("date="), "Path should contain date: {}", path);

    // Path should include hour partition
    assert!(path.contains("hour="), "Path should contain hour: {}", path);

    // Path should end with .parquet
    assert!(
        path.ends_with(".parquet"),
        "Path should end with .parquet: {}",
        path
    );

    // Verify timestamp-based partitioning: data timestamp is 2024-01-01 00:00:00 UTC
    assert!(
        path.contains("date=2024-01-01"),
        "Path should be partitioned by data timestamp (2024-01-01), not ingestion time: {}",
        path
    );
    assert!(
        path.contains("hour=00"),
        "Path should be partitioned by data hour (00), not ingestion time: {}",
        path
    );
}

// ============================================================================
// End-to-end ingestion and query tests
// ============================================================================

#[rstest]
#[tokio::test]
async fn end_to_end_ingest_and_query_traces(traces_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = traces_ingester;

    // Ingest multiple traces
    for i in 0..3 {
        let request = sample_trace_request("query-service", &format!("span-{}", i));
        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
        ingester.ingest(batch).await.unwrap();
    }

    // Flush to storage
    ingester.flush().await.unwrap();

    // Create query engine
    let engine = QueryEngine::new(store, "memory://").await.unwrap();

    // Query all traces
    let results = engine
        .query("SELECT name, \"service.name\" FROM traces ORDER BY name")
        .await
        .unwrap();

    // Should have results
    assert!(!results.is_empty());

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Verify span names are in order
    let batch = &results[0];
    let names = batch.column_by_name("name").unwrap().as_string::<i32>();
    assert_eq!(names.value(0), "span-0");
    assert_eq!(names.value(1), "span-1");
    assert_eq!(names.value(2), "span-2");

    // Verify service name
    let service_names = batch
        .column_by_name("service.name")
        .unwrap()
        .as_string::<i32>();
    for i in 0..3 {
        assert_eq!(service_names.value(i), "query-service");
    }
}

#[rstest]
#[tokio::test]
async fn end_to_end_ingest_and_query_metrics(metrics_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = metrics_ingester;

    // Ingest metrics with different values
    for i in 0..5 {
        let request = sample_metrics_request("metrics-query-service", "test.gauge", i * 10);
        let batch = convert_metrics_to_arrow(&request, None).unwrap().batch;
        ingester.ingest(batch).await.unwrap();
    }

    ingester.flush().await.unwrap();

    // Query via DataFusion
    let engine = QueryEngine::new(store, "memory://").await.unwrap();

    let results = engine
        .query("SELECT metric_name, value_int FROM metrics ORDER BY value_int")
        .await
        .unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5);

    // Verify values are in order (0, 10, 20, 30, 40)
    let batch = &results[0];
    let values = batch
        .column_by_name("value_int")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();

    for i in 0..5 {
        assert_eq!(values.value(i), (i * 10) as i64);
    }
}

#[rstest]
#[tokio::test]
async fn end_to_end_ingest_and_query_logs(logs_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = logs_ingester;

    // Ingest logs
    let messages = [
        "First log message",
        "Second log message",
        "Third log message",
    ];
    for msg in messages {
        let request = sample_logs_request("logs-query-service", msg);
        let batch = convert_logs_to_arrow(&request, None).unwrap().batch;
        ingester.ingest(batch).await.unwrap();
    }

    ingester.flush().await.unwrap();

    // Query via DataFusion
    let engine = QueryEngine::new(store, "memory://").await.unwrap();

    let results = engine
        .query("SELECT body_string, severity_number FROM logs ORDER BY body_string")
        .await
        .unwrap();

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3);

    // Verify log bodies are in alphabetical order
    let batch = &results[0];
    let bodies = batch
        .column_by_name("body_string")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(bodies.value(0), "First log message");
    assert_eq!(bodies.value(1), "Second log message");
    assert_eq!(bodies.value(2), "Third log message");
}

// ============================================================================
// SpanEvents and SpanLinks tests
// ============================================================================

fn sample_trace_with_events(service_name: &str, span_name: &str) -> ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::trace::v1::span::Event;

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![service_name_kv(service_name)],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0xAA; 16],
                    span_id: vec![0xBB; 8],
                    name: span_name.to_string(),
                    start_time_unix_nano: 1_704_067_200_000_000_000,
                    end_time_unix_nano: 1_704_067_201_000_000_000,
                    events: vec![
                        Event {
                            name: "event-1".to_string(),
                            time_unix_nano: 1_704_067_200_100_000_000,
                            attributes: vec![KeyValue {
                                key: "event.key".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue(
                                        "event-value".to_string(),
                                    )),
                                }),
                            }],
                            dropped_attributes_count: 0,
                        },
                        Event {
                            name: "event-2".to_string(),
                            time_unix_nano: 1_704_067_200_200_000_000,
                            attributes: vec![],
                            dropped_attributes_count: 0,
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn sample_trace_with_links(service_name: &str, span_name: &str) -> ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::trace::v1::span::Link;

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![service_name_kv(service_name)],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0xAA; 16],
                    span_id: vec![0xBB; 8],
                    name: span_name.to_string(),
                    start_time_unix_nano: 1_704_067_200_000_000_000,
                    end_time_unix_nano: 1_704_067_201_000_000_000,
                    links: vec![
                        Link {
                            trace_id: vec![0xCC; 16],
                            span_id: vec![0xDD; 8],
                            trace_state: "vendor=opaque".to_string(),
                            attributes: vec![KeyValue {
                                key: "link.reason".to_string(),
                                value: Some(AnyValue {
                                    value: Some(any_value::Value::StringValue("retry".to_string())),
                                }),
                            }],
                            dropped_attributes_count: 0,
                            flags: 0,
                        },
                        Link {
                            trace_id: vec![0xEE; 16],
                            span_id: vec![0xFF; 8],
                            trace_state: String::new(),
                            attributes: vec![],
                            dropped_attributes_count: 0,
                            flags: 0,
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[rstest]
#[tokio::test]
async fn span_with_events_roundtrip(traces_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = traces_ingester;

    // Ingest span with events
    let request = sample_trace_with_events("events-service", "span-with-events");
    let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
    assert_eq!(batch.num_rows(), 1);

    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    // Read back and verify events column is populated
    let files: Vec<_> = store.list(None).collect().await;
    assert_eq!(files.len(), 1);

    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];

    // Verify span name
    let name = batch.column_by_name("name").unwrap().as_string::<i32>();
    assert_eq!(name.value(0), "span-with-events");

    // Verify events column is not null and contains CBOR data
    let events_col = batch
        .column_by_name("events")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    assert!(!events_col.is_null(0));

    // Decode CBOR and verify event contents
    let events_data = events_col.value(0);
    let events: Vec<serde_json::Value> = ciborium::from_reader(events_data).unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["name"], "event-1");
    assert_eq!(events[0]["attributes"]["event.key"], "event-value");
    assert_eq!(events[1]["name"], "event-2");
}

#[rstest]
#[tokio::test]
async fn span_with_links_roundtrip(traces_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = traces_ingester;

    // Ingest span with links
    let request = sample_trace_with_links("links-service", "span-with-links");
    let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
    assert_eq!(batch.num_rows(), 1);

    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    // Read back and verify links column is populated
    let files: Vec<_> = store.list(None).collect().await;
    assert_eq!(files.len(), 1);

    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];

    // Verify span name
    let name = batch.column_by_name("name").unwrap().as_string::<i32>();
    assert_eq!(name.value(0), "span-with-links");

    // Verify links column is not null and contains CBOR data
    let links_col = batch
        .column_by_name("links")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    assert!(!links_col.is_null(0));

    // Decode CBOR and verify link contents
    let links_data = links_col.value(0);
    let links: Vec<serde_json::Value> = ciborium::from_reader(links_data).unwrap();
    assert_eq!(links.len(), 2);

    // First link has trace_state and attributes
    assert_eq!(links[0]["trace_state"], "vendor=opaque");
    assert_eq!(links[0]["attributes"]["link.reason"], "retry");

    // Second link is simpler
    assert_eq!(links[1]["trace_state"], "");
}

#[rstest]
#[tokio::test]
async fn span_without_events_has_null_events(traces_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = traces_ingester;

    // Ingest a simple span without events
    let request = sample_trace_request("no-events-service", "span-no-events");
    let batch = convert_traces_to_arrow(&request, None).unwrap().batch;

    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    // Read back
    let files: Vec<_> = store.list(None).collect().await;
    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];

    // Events and links should be null for spans without them
    let events_col = batch
        .column_by_name("events")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    assert!(events_col.is_null(0));

    let links_col = batch
        .column_by_name("links")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();
    assert!(links_col.is_null(0));
}

// ============================================================================
// W3C trace context tests
// ============================================================================

fn sample_trace_with_trace_state(
    service_name: &str,
    span_name: &str,
    trace_state: &str,
) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![service_name_kv(service_name)],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0xAA; 16],
                    span_id: vec![0xBB; 8],
                    name: span_name.to_string(),
                    trace_state: trace_state.to_string(),
                    start_time_unix_nano: 1_704_067_200_000_000_000,
                    end_time_unix_nano: 1_704_067_201_000_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[rstest]
#[tokio::test]
async fn span_with_trace_state_roundtrip(traces_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = traces_ingester;

    // Ingest span with W3C trace_state
    let request = sample_trace_with_trace_state(
        "tracestate-service",
        "span-with-tracestate",
        "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7",
    );
    let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
    assert_eq!(batch.num_rows(), 1);

    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    // Read back and verify trace_state column
    let files: Vec<_> = store.list(None).collect().await;
    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];

    // Verify trace_state is preserved
    let trace_state = batch
        .column_by_name("trace_state")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(
        trace_state.value(0),
        "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7"
    );
}

#[rstest]
#[tokio::test]
async fn span_without_trace_state_has_null(traces_ingester: (Ingester, Arc<dyn ObjectStore>)) {
    let (ingester, store) = traces_ingester;

    // Ingest span without trace_state (empty string in proto)
    let request = sample_trace_with_trace_state("no-tracestate", "span-no-tracestate", "");
    let batch = convert_traces_to_arrow(&request, None).unwrap().batch;

    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    // Read back
    let files: Vec<_> = store.list(None).collect().await;
    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];

    // Empty trace_state should be stored as null
    let trace_state_col = batch.column_by_name("trace_state").unwrap();
    assert!(trace_state_col.is_null(0));
}

// ============================================================================
// Redaction pipeline tests
// ============================================================================

fn sample_trace_with_pii(service_name: &str, span_name: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    service_name_kv(service_name),
                    KeyValue {
                        key: "api.secret".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "sk-12345-secret".to_string(),
                            )),
                        }),
                    },
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0xAA; 16],
                    span_id: vec![0xBB; 8],
                    name: span_name.to_string(),
                    start_time_unix_nano: 1_704_067_200_000_000_000,
                    end_time_unix_nano: 1_704_067_201_000_000_000,
                    attributes: vec![
                        KeyValue {
                            key: "user.email".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "user@example.com".to_string(),
                                )),
                            }),
                        },
                        KeyValue {
                            key: "http.method".to_string(),
                            value: Some(AnyValue {
                                value: Some(any_value::Value::StringValue("GET".to_string())),
                            }),
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn test_redaction_engine() -> RedactionEngine {
    let config = RedactionConfig {
        enabled: true,
        key_file: None,
        key_env: None,
        rules: vec![
            RedactionRule {
                name: "drop-secrets".to_string(),
                signals: vec![],
                scopes: vec![],
                matcher: MatcherConfig::Glob {
                    pattern: "*.secret".to_string(),
                },
                action: ActionConfig::Drop,
            },
            RedactionRule {
                name: "redact-emails".to_string(),
                signals: vec![],
                scopes: vec![],
                matcher: MatcherConfig::Pattern {
                    regex: "builtin:email".to_string(),
                },
                action: ActionConfig::Redact {
                    placeholder: "[EMAIL-REDACTED]".to_string(),
                },
            },
        ],
        redact_names: false,
        redact_errors: true,
    };
    RedactionEngine::new(&config).unwrap()
}

#[rstest]
#[tokio::test]
async fn redaction_drops_secrets_and_redacts_emails(
    traces_ingester: (Ingester, Arc<dyn ObjectStore>),
) {
    let (ingester, store) = traces_ingester;
    let engine = test_redaction_engine();

    // Create a trace with PII
    let request = sample_trace_with_pii("redaction-test", "pii-span");

    // Convert with redaction enabled
    let batch = convert_traces_to_arrow(&request, Some(&engine))
        .unwrap()
        .batch;
    assert_eq!(batch.num_rows(), 1);

    // Ingest and flush
    ingester.ingest(batch).await.unwrap();
    ingester.flush().await.unwrap();

    // Read back from Parquet
    let files: Vec<_> = store.list(None).collect().await;
    let path = &files[0].as_ref().unwrap().location;
    let data = store.get(path).await.unwrap().bytes().await.unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(data)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<_> = reader.collect::<Result<Vec<_>, _>>().unwrap();
    let batch = &batches[0];

    // Verify service name is preserved (not affected by rules)
    let service_name = batch
        .column_by_name("service.name")
        .unwrap()
        .as_string::<i32>();
    assert_eq!(service_name.value(0), "redaction-test");

    // Verify resource_attributes doesn't contain api.secret (it was dropped)
    let resource_attrs = batch
        .column_by_name("resource_attributes")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();

    if !resource_attrs.is_null(0) {
        let attrs_data = resource_attrs.value(0);
        let attrs: std::collections::HashMap<String, serde_json::Value> =
            ciborium::from_reader(attrs_data).unwrap();
        assert!(
            !attrs.contains_key("api.secret"),
            "api.secret should have been dropped by redaction"
        );
    }

    // Verify span_attributes has redacted email
    let span_attrs = batch
        .column_by_name("span_attributes")
        .unwrap()
        .as_any()
        .downcast_ref::<arrow::array::BinaryArray>()
        .unwrap();

    let attrs_data = span_attrs.value(0);
    let attrs: std::collections::HashMap<String, serde_json::Value> =
        ciborium::from_reader(attrs_data).unwrap();

    assert_eq!(
        attrs.get("user.email").and_then(|v| v.as_str()),
        Some("[EMAIL-REDACTED]"),
        "email should have been redacted"
    );
    assert_eq!(
        attrs.get("http.method").and_then(|v| v.as_str()),
        Some("GET"),
        "http.method should be preserved"
    );
}
