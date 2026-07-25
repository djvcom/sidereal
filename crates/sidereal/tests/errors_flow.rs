//! Integration tests for the error tracking module.
//!
//! Exercises the full flow against a real query engine: OTLP error spans are
//! converted, ingested, flushed to Parquet, and read back through every
//! `ErrorAggregator` query path. This covers the aggregation SQL (including
//! the `error_fingerprint` UDF with mixed scalar and column arguments) and
//! the count-column downcasts, which unit tests cannot reach.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::indexing_slicing,
    clippy::float_cmp
)]

use std::sync::Arc;

use chrono::{TimeZone, Utc};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    status::StatusCode, ResourceSpans, ScopeSpans, Span, Status,
};

use sidereal::buffer::Ingester;
use sidereal::config::{BufferConfig, ParquetConfig};
use sidereal::errors::{ErrorAggregator, ErrorFilter, ErrorSortBy};
use sidereal::ingest::convert_traces_to_arrow;
use sidereal::query::QueryEngine;
use sidereal::schema::traces::traces_storage_schema;
use sidereal::storage::Signal;

const DAY_ONE: u64 = 1_704_067_200;
const DAY_TWO: u64 = DAY_ONE + 86_400;
const NANOS_PER_SECOND: u64 = 1_000_000_000;

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_owned())),
        }),
    }
}

struct SpanSpec<'a> {
    service: &'a str,
    version: &'a str,
    operation: &'a str,
    error: Option<(&'a str, &'a str)>,
    start_seconds: u64,
    id: u8,
}

fn span_request(spec: &SpanSpec<'_>) -> ExportTraceServiceRequest {
    let start_nanos = spec.start_seconds * NANOS_PER_SECOND;
    let (status, attributes) = match spec.error {
        Some((error_type, message)) => (
            Status {
                message: message.to_owned(),
                code: StatusCode::Error as i32,
            },
            vec![string_kv("error.type", error_type)],
        ),
        None => (
            Status {
                message: String::new(),
                code: StatusCode::Ok as i32,
            },
            Vec::new(),
        ),
    };

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![
                    string_kv("service.name", spec.service),
                    string_kv("service.version", spec.version),
                    string_kv("deployment.environment.name", "production"),
                ],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![spec.id; 16],
                    span_id: vec![spec.id; 8],
                    name: spec.operation.to_owned(),
                    start_time_unix_nano: start_nanos,
                    end_time_unix_nano: start_nanos + NANOS_PER_SECOND,
                    attributes,
                    status: Some(status),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Two days of traffic for two services.
///
/// Day one: four gateway timeouts on checkout v1.0.0 (two per hour across two
/// hours), two connection errors on payments v2.1.0, and two successful
/// checkout spans that must never appear in error results.
///
/// Day two: the gateway timeout doubles to eight occurrences on checkout
/// v1.1.0, a new cache-miss error appears three times, and the payments
/// connection error is resolved.
fn sample_dataset() -> Vec<ExportTraceServiceRequest> {
    let gateway = Some(("GatewayTimeout", "payment gateway timed out"));
    let connection = Some(("ConnectionError", "connection refused to db"));
    let cache = Some(("CacheMiss", "cache miss for user profile"));

    let mut specs = vec![
        SpanSpec {
            service: "checkout",
            version: "1.0.0",
            operation: "charge-card",
            error: gateway,
            start_seconds: DAY_ONE + 600,
            id: 1,
        },
        SpanSpec {
            service: "checkout",
            version: "1.0.0",
            operation: "charge-card",
            error: gateway,
            start_seconds: DAY_ONE + 1_200,
            id: 2,
        },
        SpanSpec {
            service: "checkout",
            version: "1.0.0",
            operation: "charge-card",
            error: gateway,
            start_seconds: DAY_ONE + 3_660,
            id: 3,
        },
        SpanSpec {
            service: "checkout",
            version: "1.0.0",
            operation: "charge-card",
            error: gateway,
            start_seconds: DAY_ONE + 3_720,
            id: 4,
        },
        SpanSpec {
            service: "payments",
            version: "2.1.0",
            operation: "settle",
            error: connection,
            start_seconds: DAY_ONE + 1_800,
            id: 5,
        },
        SpanSpec {
            service: "payments",
            version: "2.1.0",
            operation: "settle",
            error: connection,
            start_seconds: DAY_ONE + 5_400,
            id: 6,
        },
        SpanSpec {
            service: "checkout",
            version: "1.0.0",
            operation: "browse",
            error: None,
            start_seconds: DAY_ONE + 2_400,
            id: 7,
        },
        SpanSpec {
            service: "checkout",
            version: "1.0.0",
            operation: "browse",
            error: None,
            start_seconds: DAY_ONE + 6_000,
            id: 8,
        },
    ];

    for i in 0..8u8 {
        specs.push(SpanSpec {
            service: "checkout",
            version: "1.1.0",
            operation: "charge-card",
            error: gateway,
            start_seconds: DAY_TWO + 300 * u64::from(i),
            id: 10 + i,
        });
    }
    for i in 0..3u8 {
        specs.push(SpanSpec {
            service: "checkout",
            version: "1.1.0",
            operation: "load-profile",
            error: cache,
            start_seconds: DAY_TWO + 900 * u64::from(i),
            id: 20 + i,
        });
    }

    specs.iter().map(span_request).collect()
}

async fn setup_aggregator() -> ErrorAggregator {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let ingester = Ingester::new(
        Signal::Traces,
        traces_storage_schema(),
        store.clone(),
        BufferConfig::default(),
        ParquetConfig::default(),
    );

    for request in sample_dataset() {
        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
        ingester.ingest(batch).await.unwrap();
    }
    ingester.flush().await.unwrap();

    let engine = QueryEngine::new(store, "memory://").await.unwrap();
    ErrorAggregator::new(Arc::new(engine))
}

fn day_one_filter() -> ErrorFilter {
    ErrorFilter::new(
        Utc.timestamp_opt(DAY_ONE as i64, 0).unwrap(),
        Utc.timestamp_opt((DAY_ONE + 7_200) as i64, 0).unwrap(),
    )
}

fn day_two_filter() -> ErrorFilter {
    ErrorFilter::new(
        Utc.timestamp_opt(DAY_TWO as i64, 0).unwrap(),
        Utc.timestamp_opt((DAY_TWO + 7_200) as i64, 0).unwrap(),
    )
}

#[tokio::test]
async fn error_groups_aggregate_counts_versions_and_exclude_ok_spans() {
    let aggregator = setup_aggregator().await;

    let groups = aggregator
        .get_error_groups(&day_one_filter(), ErrorSortBy::Volume, 50, 0)
        .await
        .unwrap();

    assert_eq!(
        groups.len(),
        2,
        "expected two error groups, got {groups:#?}"
    );

    let gateway = &groups[0];
    assert_eq!(gateway.service_name, "checkout");
    assert_eq!(gateway.error_type.as_deref(), Some("GatewayTimeout"));
    assert_eq!(
        gateway.message.as_deref(),
        Some("payment gateway timed out")
    );
    assert_eq!(gateway.first_version.as_deref(), Some("1.0.0"));
    assert_eq!(gateway.count, 4);
    assert_eq!(gateway.affected_traces, 4);
    assert!(gateway.first_seen < gateway.last_seen);
    assert!(gateway.sample_trace_id.is_some());

    let connection = &groups[1];
    assert_eq!(connection.service_name, "payments");
    assert_eq!(connection.error_type.as_deref(), Some("ConnectionError"));
    assert_eq!(connection.count, 2);
}

#[tokio::test]
async fn error_groups_respect_service_filter() {
    let aggregator = setup_aggregator().await;

    let groups = aggregator
        .get_error_groups(
            &day_one_filter().with_service("payments"),
            ErrorSortBy::Volume,
            50,
            0,
        )
        .await
        .unwrap();

    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].error_type.as_deref(), Some("ConnectionError"));
}

#[tokio::test]
async fn error_stats_count_all_errors_in_range() {
    let aggregator = setup_aggregator().await;

    let stats = aggregator.get_error_stats(&day_one_filter()).await.unwrap();

    assert_eq!(stats.total_errors, 6);
    let hourly_total: u64 = stats.hourly_counts.iter().map(|&c| u64::from(c)).sum();
    assert_eq!(hourly_total, 6);
    assert_eq!(stats.hourly_counts, vec![3, 3]);
}

#[tokio::test]
async fn error_timeline_buckets_by_hour_for_a_fingerprint() {
    let aggregator = setup_aggregator().await;

    let groups = aggregator
        .get_error_groups(&day_one_filter(), ErrorSortBy::Volume, 50, 0)
        .await
        .unwrap();
    let fingerprint = &groups[0].fingerprint;

    let timeline = aggregator
        .get_error_timeline(Some(fingerprint), &day_one_filter())
        .await
        .unwrap();

    let counts: Vec<u64> = timeline.iter().map(|(_, c)| *c).collect();
    assert_eq!(counts, vec![2, 2]);

    let first_bucket = timeline[0].0;
    assert_eq!(first_bucket, Utc.timestamp_opt(DAY_ONE as i64, 0).unwrap());
}

#[tokio::test]
async fn error_samples_return_occurrences_with_context() {
    let aggregator = setup_aggregator().await;

    let groups = aggregator
        .get_error_groups(&day_one_filter(), ErrorSortBy::Volume, 50, 0)
        .await
        .unwrap();
    let fingerprint = &groups[0].fingerprint;

    let samples = aggregator
        .get_error_samples(fingerprint, &day_one_filter(), 10)
        .await
        .unwrap();

    assert_eq!(samples.len(), 4);
    for sample in &samples {
        assert_eq!(sample.service_name, "checkout");
        assert_eq!(sample.operation.as_deref(), Some("charge-card"));
        assert_eq!(sample.error_type.as_deref(), Some("GatewayTimeout"));
        assert_eq!(sample.message.as_deref(), Some("payment gateway timed out"));
        assert_eq!(sample.duration_ns, NANOS_PER_SECOND);
    }

    let mut trace_ids: Vec<_> = samples.iter().map(|s| s.trace_id).collect();
    trace_ids.sort_unstable();
    trace_ids.dedup();
    assert_eq!(trace_ids.len(), 4, "each occurrence has a distinct trace");
}

#[tokio::test]
async fn compare_errors_categorises_new_resolved_and_increased() {
    let aggregator = setup_aggregator().await;

    let comparison = aggregator
        .compare_errors(&day_one_filter(), &day_two_filter())
        .await
        .unwrap();

    assert_eq!(comparison.new_errors.len(), 1);
    assert_eq!(
        comparison.new_errors[0].error_type.as_deref(),
        Some("CacheMiss")
    );
    assert_eq!(comparison.new_errors[0].count, 3);

    assert_eq!(comparison.resolved_errors.len(), 1);
    assert_eq!(
        comparison.resolved_errors[0].error_type.as_deref(),
        Some("ConnectionError")
    );

    assert_eq!(comparison.increased_errors.len(), 1);
    let delta = &comparison.increased_errors[0];
    assert_eq!(delta.error.error_type.as_deref(), Some("GatewayTimeout"));
    assert_eq!(delta.baseline_count, 4);
    assert_eq!(delta.comparison_count, 8);
    assert!((delta.change_percent - 100.0).abs() < 0.01);

    assert!(comparison.decreased_errors.is_empty());
    assert!(comparison.unchanged_errors.is_empty());
}
