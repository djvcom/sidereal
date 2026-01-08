//! Benchmarks for OTLP to Arrow conversion.
//!
//! These benchmarks measure the throughput of converting OTLP protocol buffer
//! messages to Arrow RecordBatches, which is the critical hot path for ingestion.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

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
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};

use sidereal_telemetry::ingest::{
    convert_logs_to_arrow, convert_metrics_to_arrow, convert_traces_to_arrow,
};
use sidereal_telemetry::redact::{
    ActionConfig, MatcherConfig, RedactionConfig, RedactionEngine, RedactionRule,
};

fn service_resource(name: &str) -> Resource {
    Resource {
        attributes: vec![KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(name.to_string())),
            }),
        }],
        ..Default::default()
    }
}

fn sample_attributes(count: usize) -> Vec<KeyValue> {
    (0..count)
        .map(|i| KeyValue {
            key: format!("attr.key.{}", i),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(format!("value-{}", i))),
            }),
        })
        .collect()
}

fn simple_span(id: u8) -> Span {
    Span {
        trace_id: vec![id; 16],
        span_id: vec![id; 8],
        name: format!("span-{}", id),
        kind: span::SpanKind::Server as i32,
        start_time_unix_nano: 1_704_067_200_000_000_000,
        end_time_unix_nano: 1_704_067_201_000_000_000,
        status: Some(Status {
            code: 1,
            message: String::new(),
        }),
        ..Default::default()
    }
}

fn span_with_attributes(id: u8, attr_count: usize) -> Span {
    let mut span = simple_span(id);
    span.attributes = sample_attributes(attr_count);
    span
}

fn span_with_events(id: u8, event_count: usize) -> Span {
    let mut span = simple_span(id);
    span.events = (0..event_count)
        .map(|i| span::Event {
            name: format!("event-{}", i),
            time_unix_nano: 1_704_067_200_500_000_000,
            attributes: sample_attributes(3),
            ..Default::default()
        })
        .collect();
    span
}

fn trace_request(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(service_resource("bench-service")),
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn simple_metric(id: u8) -> Metric {
    Metric {
        name: format!("bench.metric.{}", id),
        description: "Benchmark metric".to_string(),
        unit: "1".to_string(),
        metadata: vec![],
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint {
                time_unix_nano: 1_704_067_200_000_000_000,
                value: Some(number_data_point::Value::AsInt(42)),
                attributes: sample_attributes(5),
                ..Default::default()
            }],
        })),
    }
}

fn metrics_request(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(service_resource("bench-service")),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn simple_log(id: u8) -> LogRecord {
    LogRecord {
        time_unix_nano: 1_704_067_200_000_000_000,
        observed_time_unix_nano: 1_704_067_200_000_000_000,
        severity_number: 9, // INFO
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(format!("Log message {}", id))),
        }),
        attributes: sample_attributes(5),
        ..Default::default()
    }
}

fn logs_request(logs: Vec<LogRecord>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(service_resource("bench-service")),
            scope_logs: vec![ScopeLogs {
                log_records: logs,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn bench_convert_traces(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_traces");

    for span_count in [1, 10, 100, 1000] {
        let spans: Vec<_> = (0..span_count).map(|i| simple_span(i as u8)).collect();
        let request = trace_request(spans);

        group.throughput(Throughput::Elements(span_count as u64));
        group.bench_with_input(
            BenchmarkId::new("simple_spans", span_count),
            &request,
            |b, req| {
                b.iter(|| convert_traces_to_arrow(black_box(req), None).unwrap());
            },
        );
    }

    // Benchmark with varying attribute counts
    for attr_count in [5, 20, 50] {
        let spans: Vec<_> = (0..100)
            .map(|i| span_with_attributes(i as u8, attr_count))
            .collect();
        let request = trace_request(spans);

        group.throughput(Throughput::Elements(100));
        group.bench_with_input(
            BenchmarkId::new("spans_with_attrs", attr_count),
            &request,
            |b, req| {
                b.iter(|| convert_traces_to_arrow(black_box(req), None).unwrap());
            },
        );
    }

    // Benchmark with events
    for event_count in [1, 5, 10] {
        let spans: Vec<_> = (0..100)
            .map(|i| span_with_events(i as u8, event_count))
            .collect();
        let request = trace_request(spans);

        group.throughput(Throughput::Elements(100));
        group.bench_with_input(
            BenchmarkId::new("spans_with_events", event_count),
            &request,
            |b, req| {
                b.iter(|| convert_traces_to_arrow(black_box(req), None).unwrap());
            },
        );
    }

    group.finish();
}

fn bench_convert_metrics(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_metrics");

    for metric_count in [1, 10, 100, 1000] {
        let metrics: Vec<_> = (0..metric_count).map(|i| simple_metric(i as u8)).collect();
        let request = metrics_request(metrics);

        group.throughput(Throughput::Elements(metric_count as u64));
        group.bench_with_input(
            BenchmarkId::new("gauge_metrics", metric_count),
            &request,
            |b, req| {
                b.iter(|| convert_metrics_to_arrow(black_box(req), None).unwrap());
            },
        );
    }

    group.finish();
}

fn bench_convert_logs(c: &mut Criterion) {
    let mut group = c.benchmark_group("convert_logs");

    for log_count in [1, 10, 100, 1000] {
        let logs: Vec<_> = (0..log_count).map(|i| simple_log(i as u8)).collect();
        let request = logs_request(logs);

        group.throughput(Throughput::Elements(log_count as u64));
        group.bench_with_input(
            BenchmarkId::new("simple_logs", log_count),
            &request,
            |b, req| {
                b.iter(|| convert_logs_to_arrow(black_box(req), None).unwrap());
            },
        );
    }

    group.finish();
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
                name: "redact-pii".to_string(),
                signals: vec![],
                scopes: vec![],
                matcher: MatcherConfig::Pattern {
                    regex: "builtin:email".to_string(),
                },
                action: ActionConfig::Redact {
                    placeholder: "[REDACTED]".to_string(),
                },
            },
        ],
        redact_names: false,
        redact_errors: true,
    };
    RedactionEngine::new(&config).unwrap()
}

fn span_with_pii_attributes(id: u8) -> Span {
    let mut span = simple_span(id);
    span.attributes = vec![
        KeyValue {
            key: "user.email".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(
                    "user@example.com".to_string(),
                )),
            }),
        },
        KeyValue {
            key: "api.secret".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("sk-12345".to_string())),
            }),
        },
        KeyValue {
            key: "http.method".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("GET".to_string())),
            }),
        },
    ];
    span
}

fn bench_redaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("redaction");
    let engine = test_redaction_engine();

    for span_count in [10, 100, 1000] {
        let spans: Vec<_> = (0..span_count)
            .map(|i| span_with_pii_attributes(i as u8))
            .collect();
        let request = trace_request(spans);

        group.throughput(Throughput::Elements(span_count as u64));

        group.bench_with_input(
            BenchmarkId::new("without_redaction", span_count),
            &request,
            |b, req| {
                b.iter(|| convert_traces_to_arrow(black_box(req), None).unwrap());
            },
        );

        group.bench_with_input(
            BenchmarkId::new("with_redaction", span_count),
            &request,
            |b, req| {
                b.iter(|| convert_traces_to_arrow(black_box(req), Some(&engine)).unwrap());
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_convert_traces,
    bench_convert_metrics,
    bench_convert_logs,
    bench_redaction
);
criterion_main!(benches);
