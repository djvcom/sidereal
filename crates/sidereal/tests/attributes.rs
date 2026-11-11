//! End-to-end tests for querying by attribute.
//!
//! Any attribute a service sends should be filterable and groupable by
//! name, with its own type, without anyone declaring it first. These tests
//! send attributes through the real ingest path and query them back
//! through SQL, across flushes, compaction and restarts.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int64Array, RecordBatch, StringArray};
use object_store::memory::InMemory;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use sidereal::attributes::AttributeRegistry;
use sidereal::buffer::Ingester;
use sidereal::catalogue::CatalogueStore;
use sidereal::compaction::Compactor;
use sidereal::config::{BufferConfig, CompactionConfig, ParquetConfig};
use sidereal::ingest::{convert_logs_to_arrow, convert_traces_to_arrow};
use sidereal::query::{QueryEngine, QueryEngineBuilder};
use sidereal::schema::logs::logs_storage_schema;
use sidereal::schema::traces::traces_storage_schema;
use sidereal::storage::Signal;

/// 2025-07-01T20:00:00Z.
const START: u64 = 1_751_400_000_000_000_000;

fn attr(key: &str, value: Value) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue { value: Some(value) }),
    }
}

fn s(key: &str, value: &str) -> KeyValue {
    attr(key, Value::StringValue(value.to_owned()))
}

fn i(key: &str, value: i64) -> KeyValue {
    attr(key, Value::IntValue(value))
}

fn b(key: &str, value: bool) -> KeyValue {
    attr(key, Value::BoolValue(value))
}

fn f(key: &str, value: f64) -> KeyValue {
    attr(key, Value::DoubleValue(value))
}

fn resource(service: &str, extra: Vec<KeyValue>) -> Option<Resource> {
    let mut attributes = vec![s("service.name", service)];
    attributes.extend(extra);
    Some(Resource {
        attributes,
        ..Default::default()
    })
}

struct Fixture {
    bucket: Arc<InMemory>,
    catalogue: Arc<CatalogueStore>,
    traces: Ingester,
    logs: Ingester,
    engine: QueryEngine,
    next_id: std::sync::atomic::AtomicU64,
}

async fn engine_over(
    catalogue: &Arc<CatalogueStore>,
    registry: &Arc<AttributeRegistry>,
) -> QueryEngine {
    QueryEngineBuilder::new(catalogue.clone(), "memory://")
        .with_cache_bytes(0)
        .with_attribute_registry(registry.clone())
        .build()
        .await
        .expect("engine")
}

async fn fixture() -> Fixture {
    let bucket = Arc::new(InMemory::new());
    let catalogue = Arc::new(CatalogueStore::load(bucket.clone()).await.expect("load"));
    let registry = Arc::new(AttributeRegistry::new());
    let ingester = |signal, schema| {
        Ingester::new(
            signal,
            schema,
            catalogue.clone(),
            BufferConfig::default(),
            ParquetConfig::default(),
        )
        .with_attribute_registry(registry.clone())
    };
    Fixture {
        traces: ingester(Signal::Traces, traces_storage_schema()),
        logs: ingester(Signal::Logs, logs_storage_schema()),
        engine: engine_over(&catalogue, &registry).await,
        bucket,
        catalogue,
        next_id: std::sync::atomic::AtomicU64::new(1),
    }
}

impl Fixture {
    /// Buffer one span with the given span and resource attributes.
    async fn span(&self, service: &str, resource_attrs: Vec<KeyValue>, attrs: Vec<KeyValue>) {
        let n = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut trace_id = [0u8; 16];
        trace_id[8..].copy_from_slice(&n.to_be_bytes());
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: resource(service, resource_attrs),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: trace_id.to_vec(),
                        span_id: n.to_be_bytes().to_vec(),
                        name: "op".to_owned(),
                        kind: 2,
                        start_time_unix_nano: START + n * 1_000,
                        end_time_unix_nano: START + n * 1_000 + 500,
                        attributes: attrs,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let batch = convert_traces_to_arrow(&request, None)
            .expect("convert")
            .batch;
        self.traces.ingest(batch).await.expect("ingest");
    }

    async fn flush(&self) {
        self.traces.flush().await.expect("flush traces");
        self.logs.flush().await.expect("flush logs");
    }

    async fn count(&self, predicate: &str) -> i64 {
        count(&self.engine, predicate).await
    }
}

async fn count(engine: &QueryEngine, predicate: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM traces WHERE {predicate}");
    let batches = engine
        .query(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("integer count")
        .value(0)
}

fn strings(batches: &[RecordBatch], column: usize) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let array =
                arrow::compute::cast(batch.column(column), &arrow::datatypes::DataType::Utf8)
                    .expect("castable to string");
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array")
                .clone();
            (0..array.len())
                .map(|i| (!array.is_null(i)).then(|| array.value(i).to_owned()))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn any_attribute_is_filterable_by_name_with_its_own_type() {
    let fx = fixture().await;
    fx.span(
        "checkout",
        vec![s("k8s.pod.name", "checkout-1")],
        vec![
            s("tenant.id", "acme"),
            i("retries", 0),
            b("cached", true),
            f("ratio", 0.25),
        ],
    )
    .await;
    fx.span(
        "checkout",
        vec![s("k8s.pod.name", "checkout-2")],
        vec![
            s("tenant.id", "globex"),
            i("retries", 3),
            b("cached", false),
            f("ratio", 0.75),
        ],
    )
    .await;
    fx.span(
        "gateway",
        vec![],
        vec![s("tenant.id", "acme"), i("retries", 5)],
    )
    .await;
    fx.span("gateway", vec![], vec![]).await;
    fx.flush().await;

    // Strings.
    assert_eq!(fx.count("attr('tenant.id') = 'acme'").await, 2);
    assert_eq!(fx.count("attr('tenant.id') LIKE 'glob%'").await, 1);
    assert_eq!(fx.count("attr('tenant.id') IS NULL").await, 1);
    // Integers compare as numbers, not text.
    assert_eq!(fx.count("attr('retries') > 2").await, 2);
    assert_eq!(fx.count("attr('retries') BETWEEN 1 AND 4").await, 1);
    // Booleans and floats.
    assert_eq!(fx.count("attr('cached')").await, 1);
    assert_eq!(fx.count("attr('cached') = false").await, 1);
    assert_eq!(fx.count("attr('ratio') >= 0.5").await, 1);
    // Resource attributes are separate from the record's own.
    assert_eq!(
        fx.count("resource_attr('k8s.pod.name') = 'checkout-2'")
            .await,
        1
    );
    assert_eq!(fx.count("attr('k8s.pod.name') IS NOT NULL").await, 0);
    // Combined, as a real query would.
    assert_eq!(
        fx.count(
            "attr('tenant.id') = 'acme' AND attr('retries') = 0 AND \"service.name\" = 'checkout'"
        )
        .await,
        1
    );
}

#[tokio::test]
async fn attributes_group_and_aggregate_like_any_column() {
    let fx = fixture().await;
    for (tenant, bytes) in [("acme", 100), ("acme", 300), ("globex", 50)] {
        fx.span(
            "api",
            vec![],
            vec![s("tenant.id", tenant), i("response.bytes", bytes)],
        )
        .await;
    }
    fx.flush().await;

    let batches = fx
        .engine
        .query(
            "SELECT attr('tenant.id') AS tenant, SUM(attr('response.bytes')) AS bytes \
             FROM traces GROUP BY attr('tenant.id') ORDER BY tenant",
        )
        .await
        .expect("query");
    assert_eq!(
        strings(&batches, 0),
        vec![Some("acme".to_owned()), Some("globex".to_owned())]
    );
    assert_eq!(
        strings(&batches, 1),
        vec![Some("400".to_owned()), Some("50".to_owned())]
    );
}

#[tokio::test]
async fn attributes_with_dedicated_columns_are_read_through_the_same_function() {
    let fx = fixture().await;
    fx.span(
        "shop",
        vec![],
        vec![
            s("http.route", "/products/{id}"),
            i("http.response.status_code", 200),
        ],
    )
    .await;
    fx.span("shop", vec![], vec![s("http.route", "/cart")])
        .await;
    fx.flush().await;

    assert_eq!(fx.count("attr('http.route') = '/cart'").await, 1);
    assert_eq!(fx.count("attr('http.response.status_code') = 200").await, 1);
    assert_eq!(fx.count("resource_attr('service.name') = 'shop'").await, 2);
}

#[tokio::test]
async fn an_attribute_nobody_has_sent_matches_nothing_rather_than_failing() {
    let fx = fixture().await;
    fx.span("api", vec![], vec![s("tenant.id", "acme")]).await;
    fx.flush().await;

    assert_eq!(fx.count("attr('not.sent.yet') = 'x'").await, 0);
    assert_eq!(fx.count("attr('not.sent.yet') IS NULL").await, 1);
    assert_eq!(fx.count("resource_attr('also.missing') = 'x'").await, 0);

    // On an empty store too, before any attribute column exists.
    let empty = fixture().await;
    assert_eq!(empty.count("attr('tenant.id') = 'acme'").await, 0);
}

#[tokio::test]
async fn a_key_sent_with_two_types_keeps_both_and_reads_as_text() {
    let fx = fixture().await;
    fx.span("old", vec![], vec![s("error.code", "E42")]).await;
    fx.span("new", vec![], vec![i("error.code", 42)]).await;
    fx.flush().await;

    assert_eq!(fx.count("attr('error.code') = 'E42'").await, 1);
    assert_eq!(fx.count("attr('error.code') = '42'").await, 1);
    assert_eq!(fx.count("attr('error.code') IS NOT NULL").await, 2);
}

#[tokio::test]
async fn a_new_attribute_is_queryable_as_soon_as_it_is_flushed() {
    let fx = fixture().await;
    fx.span("api", vec![], vec![s("tenant.id", "acme")]).await;
    fx.flush().await;
    assert_eq!(fx.count("attr('tenant.id') = 'acme'").await, 1);
    assert_eq!(fx.count("attr('feature.flag') = 'new-checkout'").await, 0);

    // A deployment starts sending an attribute that never existed before.
    fx.span("api", vec![], vec![s("feature.flag", "new-checkout")])
        .await;
    fx.flush().await;

    assert_eq!(fx.count("attr('feature.flag') = 'new-checkout'").await, 1);
    // Files written before it existed read as null for it.
    assert_eq!(fx.count("attr('feature.flag') IS NULL").await, 1);
    assert_eq!(fx.count("attr('tenant.id') = 'acme'").await, 1);
}

#[tokio::test]
async fn attributes_survive_compaction_of_files_with_different_columns() {
    let fx = fixture().await;
    fx.span("api", vec![], vec![s("tenant.id", "acme")]).await;
    fx.flush().await;
    fx.span("api", vec![], vec![i("retries", 2)]).await;
    fx.flush().await;
    fx.span("api", vec![s("k8s.pod.name", "api-1")], vec![])
        .await;
    fx.flush().await;

    let config = CompactionConfig {
        settle_secs: 0,
        ..Default::default()
    };
    let report = Compactor::new(fx.catalogue.clone(), config, ParquetConfig::default())
        .run_once()
        .await
        .expect("compact");
    assert_eq!((report.jobs, report.files_in, report.files_out), (1, 3, 1));

    assert_eq!(fx.count("attr('tenant.id') = 'acme'").await, 1);
    assert_eq!(fx.count("attr('retries') = 2").await, 1);
    assert_eq!(fx.count("resource_attr('k8s.pod.name') = 'api-1'").await, 1);
    assert_eq!(fx.count("true").await, 3);
}

#[tokio::test]
async fn a_restart_rediscovers_attribute_columns_from_the_bucket() {
    let fx = fixture().await;
    fx.span(
        "api",
        vec![s("k8s.pod.name", "api-1")],
        vec![s("tenant.id", "acme"), i("retries", 4)],
    )
    .await;
    fx.flush().await;

    // A new process: nothing carried over except the bucket.
    let catalogue = Arc::new(CatalogueStore::load(fx.bucket.clone()).await.expect("load"));
    let registry = Arc::new(AttributeRegistry::new());
    assert_eq!(registry.load(&catalogue).await, 1);
    assert_eq!(registry.len(Signal::Traces), 3);
    let engine = engine_over(&catalogue, &registry).await;

    assert_eq!(count(&engine, "attr('tenant.id') = 'acme'").await, 1);
    assert_eq!(count(&engine, "attr('retries') > 3").await, 1);
    assert_eq!(
        count(&engine, "resource_attr('k8s.pod.name') = 'api-1'").await,
        1
    );
}

#[tokio::test]
async fn the_original_attribute_blob_is_still_complete() {
    let fx = fixture().await;
    fx.span("api", vec![], vec![s("tenant.id", "acme"), i("retries", 4)])
        .await;
    fx.flush().await;

    assert_eq!(
        fx.count("cbor_extract(span_attributes, 'tenant.id') = 'acme'")
            .await,
        1
    );
    assert_eq!(
        fx.count("cbor_extract(span_attributes, 'retries') = '4'")
            .await,
        1
    );
}

#[tokio::test]
async fn a_business_outcome_is_measurable_from_attributes_alone() {
    // The share of crawler requests for product pages that were told the
    // product is in stock: nothing errors and nothing is slow when this
    // drops, so only the attributes can show it.
    let fx = fixture().await;
    let googlebot = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
    let product_page = |agent: &str, in_stock: bool| {
        vec![
            s("http.route", "/products/{id}"),
            s("user_agent.original", agent),
            b("product.in_stock", in_stock),
            f("product.price", 19.99),
        ]
    };
    for in_stock in [true, true, true, false] {
        fx.span("storefront", vec![], product_page(googlebot, in_stock))
            .await;
    }
    // Other traffic must not count either way.
    fx.span(
        "storefront",
        vec![],
        product_page("Mozilla/5.0 Firefox", false),
    )
    .await;
    fx.span(
        "storefront",
        vec![],
        vec![
            s("http.route", "/cart"),
            s("user_agent.original", googlebot),
        ],
    )
    .await;
    fx.flush().await;

    let batches = fx
        .engine
        .query(
            "SELECT CAST(COUNT(*) FILTER (WHERE attr('product.in_stock')) AS DOUBLE) / COUNT(*) \
             FROM traces \
             WHERE attr('user_agent.original') LIKE '%Googlebot%' \
               AND attr('http.route') = '/products/{id}'",
        )
        .await
        .expect("query");
    let ratio = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("ratio")
        .value(0);
    assert!((ratio - 0.75).abs() < 1e-9, "{ratio}");
}

#[tokio::test]
async fn log_attributes_are_queryable_the_same_way() {
    let fx = fixture().await;
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: resource("payments", vec![s("cloud.region", "eu-west-1")]),
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: START,
                    observed_time_unix_nano: START,
                    severity_number: 17,
                    attributes: vec![s("order.id", "o-1"), i("attempt", 2)],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let batch = convert_logs_to_arrow(&request, None)
        .expect("convert")
        .batch;
    fx.logs.ingest(batch).await.expect("ingest");
    fx.flush().await;

    let batches = fx
        .engine
        .query(
            "SELECT COUNT(*) FROM logs WHERE attr('order.id') = 'o-1' AND attr('attempt') = 2 \
             AND resource_attr('cloud.region') = 'eu-west-1'",
        )
        .await
        .expect("query");
    let matched = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count")
        .value(0);
    assert_eq!(matched, 1);
}

#[tokio::test]
async fn the_attribute_key_must_be_a_literal() {
    let fx = fixture().await;
    fx.span("api", vec![], vec![s("tenant.id", "acme")]).await;
    fx.flush().await;

    let error = fx
        .engine
        .query("SELECT COUNT(*) FROM traces WHERE attr(name) = 'acme'")
        .await
        .expect_err("a column is not a key");
    assert!(error.to_string().contains("string literal"), "{error}");
}
