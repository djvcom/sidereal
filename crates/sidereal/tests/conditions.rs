//! End-to-end tests for reliability conditions.
//!
//! The scenario throughout is the one the feature exists for: a crawler
//! asks a storefront for product pages; the storefront asks an
//! availability service, which asks a store locator. The storefront knows
//! who is asking, the availability service knows whether stock was
//! returned, and nothing errors when it was not. Every figure asserted
//! here comes from spans sent through the real ingest path.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use object_store::memory::InMemory;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value as Any, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    ResourceSpans, ScopeSpans, Span, Status as SpanStatus,
};
use serde_json::{json, Value};
use sidereal::attributes::AttributeRegistry;
use sidereal::buffer::Ingester;
use sidereal::catalogue::CatalogueStore;
use sidereal::conditions::api::{condition_router, ConditionApiState, AUTHOR_HEADER};
use sidereal::conditions::evaluate::{self, State};
use sidereal::conditions::sql::Window;
use sidereal::conditions::store::ConditionStore;
use sidereal::conditions::{Condition, Selector, Success};
use sidereal::config::{BufferConfig, ParquetConfig};
use sidereal::ingest::convert_traces_to_arrow;
use sidereal::query::{QueryEngine, QueryEngineBuilder};
use sidereal::schema::traces::traces_storage_schema;
use sidereal::storage::Signal;
use tower::ServiceExt;

const GOOGLEBOT: &str = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)";
const FIREFOX: &str = "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0";
const MS: u64 = 1_000_000;

fn s(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(Any::StringValue(value.to_owned())),
        }),
    }
}

fn b(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(Any::BoolValue(value)),
        }),
    }
}

fn f(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(Any::DoubleValue(value)),
        }),
    }
}

/// One request to the storefront and everything it caused.
#[derive(Clone)]
struct Visit {
    agent: &'static str,
    route: &'static str,
    /// What the availability service recorded, if anything.
    in_stock: Option<bool>,
    /// How long the caller waited.
    duration_ms: u64,
    /// Version of the store locator the request passed through.
    locator: &'static str,
    /// Whether the store locator's span ended in error.
    locator_error: bool,
    /// Extra attributes on the storefront's span.
    extra: Vec<KeyValue>,
    /// A consumer that carries on working after the response is sent.
    async_tail_ms: Option<u64>,
}

impl Visit {
    fn crawler(in_stock: Option<bool>) -> Self {
        Self {
            agent: GOOGLEBOT,
            route: "/products/{id}",
            in_stock,
            duration_ms: 120,
            locator: "2.4.0",
            locator_error: false,
            extra: Vec::new(),
            async_tail_ms: None,
        }
    }
}

struct Fixture {
    bucket: Arc<InMemory>,
    ingester: Ingester,
    engine: Arc<QueryEngine>,
    registry: Arc<AttributeRegistry>,
    catalogue: Arc<CatalogueStore>,
    next: AtomicU64,
    /// Spans waiting to be sent, grouped by (service, version).
    pending: std::sync::Mutex<Vec<(&'static str, &'static str, Span)>>,
}

async fn fixture() -> Fixture {
    let bucket = Arc::new(InMemory::new());
    let catalogue = Arc::new(CatalogueStore::load(bucket.clone()).await.expect("load"));
    let registry = Arc::new(AttributeRegistry::new());
    let ingester = Ingester::new(
        Signal::Traces,
        traces_storage_schema(),
        catalogue.clone(),
        BufferConfig {
            max_records_per_request: 1_000_000,
            ..Default::default()
        },
        ParquetConfig::default(),
    )
    .with_attribute_registry(registry.clone());
    let engine = Arc::new(
        QueryEngineBuilder::new(catalogue.clone(), "memory://")
            .with_cache_bytes(0)
            .with_attribute_registry(registry.clone())
            .build()
            .await
            .expect("engine"),
    );
    Fixture {
        bucket,
        ingester,
        engine,
        registry,
        catalogue,
        next: AtomicU64::new(1),
        pending: std::sync::Mutex::new(Vec::new()),
    }
}

impl Fixture {
    fn id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst)
    }

    /// Queue the spans of one visit, which started a few minutes ago.
    fn visit(&self, visit: &Visit) {
        let n = self.id();
        let mut trace_id = [0u8; 16];
        trace_id[8..].copy_from_slice(&n.to_be_bytes());
        let start = u64::try_from(
            (Utc::now() - chrono::Duration::minutes(10))
                .timestamp_nanos_opt()
                .expect("now"),
        )
        .expect("after 1970")
            + n * 1_000;
        let span = |parent: Option<u64>,
                    name: &str,
                    begin: u64,
                    millis: u64,
                    attrs: Vec<KeyValue>,
                    error: bool| {
            let id = self.id();
            let span = Span {
                trace_id: trace_id.to_vec(),
                span_id: id.to_be_bytes().to_vec(),
                parent_span_id: parent.map(|p| p.to_be_bytes().to_vec()).unwrap_or_default(),
                name: name.to_owned(),
                kind: 2,
                start_time_unix_nano: begin,
                end_time_unix_nano: begin + millis * MS,
                attributes: attrs,
                status: Some(SpanStatus {
                    code: if error { 2 } else { 1 },
                    message: String::new(),
                }),
                ..Default::default()
            };
            (id, span)
        };

        let mut edge_attrs = vec![
            s("http.route", visit.route),
            s("user_agent.original", visit.agent),
        ];
        edge_attrs.extend(visit.extra.clone());
        let (edge, edge_span) = span(None, "GET", start, visit.duration_ms, edge_attrs, false);

        let mut availability_attrs = vec![f("product.price", 19.99)];
        if let Some(in_stock) = visit.in_stock {
            availability_attrs.push(b("product.in_stock", in_stock));
        }
        let (availability, availability_span) = span(
            Some(edge),
            "availability",
            start + MS,
            visit.duration_ms.saturating_sub(4).max(1),
            availability_attrs,
            false,
        );
        let (_, locator_span) = span(
            Some(availability),
            "nearest-stores",
            start + 2 * MS,
            visit.duration_ms.saturating_sub(8).max(1),
            Vec::new(),
            visit.locator_error,
        );

        let mut pending = self.pending.lock().expect("lock");
        pending.push(("storefront", "7.1.0", edge_span));
        pending.push(("availability", "3.0.2", availability_span));
        pending.push(("store-locator", visit.locator, locator_span));
        if let Some(tail) = visit.async_tail_ms {
            let (_, tail_span) = span(
                Some(edge),
                "index-view",
                start + 5 * MS,
                tail,
                Vec::new(),
                false,
            );
            pending.push(("analytics-worker", "1.0.0", tail_span));
        }
    }

    fn visits(&self, count: usize, visit: &Visit) {
        for _ in 0..count {
            self.visit(visit);
        }
    }

    /// Send everything queued, one export per service version as real
    /// SDKs would, and flush it to storage.
    async fn flush(&self) {
        let pending = std::mem::take(&mut *self.pending.lock().expect("lock"));
        let mut groups: std::collections::BTreeMap<(&str, &str), Vec<Span>> = Default::default();
        for (service, version, span) in pending {
            groups.entry((service, version)).or_default().push(span);
        }
        for ((service, version), spans) in groups {
            let request = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![s("service.name", service), s("service.version", version)],
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        spans,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            let batch = convert_traces_to_arrow(&request, None)
                .expect("convert")
                .batch;
            self.ingester.ingest(batch).await.expect("ingest");
        }
        self.ingester.flush().await.expect("flush");
    }

    fn router(&self) -> axum::Router {
        condition_router(ConditionApiState {
            engine: self.engine.clone(),
            store: Arc::new(ConditionStore::new(self.catalogue.clone())),
            attributes: self.registry.clone(),
        })
    }
}

fn crawler_product_pages() -> Selector {
    serde_json::from_value(json!({
        "all": [
            {"attribute": "user_agent.original", "op": "contains", "value": "Googlebot"},
            {"attribute": "http.route", "op": "equals", "value": "/products/{id}"}
        ]
    }))
    .expect("selector")
}

fn told_about_stock() -> Success {
    serde_json::from_value(json!({
        "all": [{"attribute": "product.in_stock", "op": "exists"}]
    }))
    .expect("success")
}

fn last_day() -> Window {
    Window::hours_ending(Utc::now() + chrono::Duration::minutes(1), 24)
}

fn definition(percent: f64, minimum_requests: u64) -> Value {
    json!({
        "id": "googlebot-product-availability",
        "name": "Googlebot sees product availability",
        "requests": crawler_product_pages(),
        "success": told_about_stock(),
        "target": {"percent": percent, "window_days": 7, "minimum_requests": minimum_requests},
        "impact": {"statement": "Listings are disapproved by Google", "severity": "high"},
        "notify": [{"name": "Search marketing", "channel": "email", "address": "search@example.com"}],
        "trial_days": 7
    })
}

/// 190 crawler visits told about stock, 10 not; the 10 all passed through
/// a newer store locator. Other traffic surrounds them.
async fn incident(fx: &Fixture) {
    fx.visits(190, &Visit::crawler(Some(true)));
    fx.visits(
        10,
        &Visit {
            locator: "2.4.1",
            ..Visit::crawler(None)
        },
    );
    // Not the crawler, and the crawler on another page: neither counts.
    fx.visits(
        50,
        &Visit {
            agent: FIREFOX,
            ..Visit::crawler(None)
        },
    );
    fx.visits(
        20,
        &Visit {
            route: "/cart",
            ..Visit::crawler(None)
        },
    );
    fx.flush().await;
}

#[tokio::test]
async fn a_request_is_judged_as_a_whole_trace_across_services() {
    let fx = fixture().await;
    incident(&fx).await;

    // Who is asking is known to the storefront; whether stock was returned
    // is known to the availability service. No span has both.
    let profile = evaluate::profile(
        &fx.engine,
        &crawler_product_pages(),
        &told_about_stock(),
        last_day(),
    )
    .await
    .expect("profile");

    assert_eq!(profile.summary.total, 200);
    assert_eq!(profile.summary.good, 190);
    assert_eq!(profile.summary.success_percent, Some(95.0));
    let durations = profile.summary.durations.expect("durations");
    assert!((durations.p50_ms - 120.0).abs() < 1.0, "{durations:?}");

    assert_eq!(profile.passing_examples.len(), 5);
    assert_eq!(profile.failing_examples.len(), 5);
    assert_eq!(profile.failing_examples[0].trace_id.len(), 32);
    assert_eq!(
        profile
            .series
            .iter()
            .map(|bucket| bucket.total)
            .sum::<u64>(),
        200
    );

    assert_eq!(
        profile.narrative,
        vec![
            "200 requests matched in the last 24 hours.",
            "95% of them met your definition of success; 10 did not.",
            "Half finished within 120 ms, and 99 in 100 within 120 ms.",
        ]
    );
    assert_eq!(profile.suggestion.percent, Some(90.0));
}

#[tokio::test]
async fn the_services_in_failing_requests_point_at_the_change_responsible() {
    let fx = fixture().await;
    incident(&fx).await;

    let profile = evaluate::profile(
        &fx.engine,
        &crawler_product_pages(),
        &told_about_stock(),
        last_day(),
    )
    .await
    .expect("profile");
    let find = |service: &str, version: &str| {
        profile
            .participants
            .iter()
            .find(|p| p.service == service && p.version == version)
            .unwrap_or_else(|| panic!("{service} {version} in {:?}", profile.participants))
    };

    // The storefront is in every request, good or bad: no signal there.
    let storefront = find("storefront", "7.1.0");
    assert_eq!((storefront.requests, storefront.failing), (200, 10));
    assert_eq!(storefront.share_of_requests_percent, 100.0);

    // The new store locator is in 5% of requests and 100% of failures.
    let suspect = find("store-locator", "2.4.1");
    assert_eq!((suspect.requests, suspect.failing), (10, 10));
    assert_eq!(suspect.share_of_requests_percent, 5.0);
    assert_eq!(suspect.share_of_failures_percent, Some(100.0));

    let previous = find("store-locator", "2.4.0");
    assert_eq!((previous.requests, previous.failing), (190, 0));
    assert_eq!(previous.share_of_failures_percent, Some(0.0));
}

#[tokio::test]
async fn speed_is_the_callers_wait_not_work_that_continues_afterwards() {
    let fx = fixture().await;
    fx.visits(90, &Visit::crawler(Some(true)));
    // Slow for the caller.
    fx.visits(
        6,
        &Visit {
            duration_ms: 1_500,
            ..Visit::crawler(Some(true))
        },
    );
    // Quick for the caller, with a long-running consumer afterwards.
    fx.visits(
        4,
        &Visit {
            async_tail_ms: Some(30_000),
            ..Visit::crawler(Some(true))
        },
    );
    fx.flush().await;

    let within = Success {
        within_ms: Some(800),
        ..told_about_stock()
    };
    let profile = evaluate::profile(&fx.engine, &crawler_product_pages(), &within, last_day())
        .await
        .expect("profile");
    assert_eq!((profile.summary.total, profile.summary.good), (100, 94));
}

#[tokio::test]
async fn an_error_anywhere_in_the_trace_counts_unless_told_otherwise() {
    let fx = fixture().await;
    fx.visits(95, &Visit::crawler(Some(true)));
    fx.visits(
        5,
        &Visit {
            locator_error: true,
            ..Visit::crawler(Some(true))
        },
    );
    fx.flush().await;

    let strict = evaluate::profile(
        &fx.engine,
        &crawler_product_pages(),
        &told_about_stock(),
        last_day(),
    )
    .await
    .expect("profile");
    assert_eq!(strict.summary.good, 95);

    let lenient = Success {
        no_errors: false,
        ..told_about_stock()
    };
    let profile = evaluate::profile(&fx.engine, &crawler_product_pages(), &lenient, last_day())
        .await
        .expect("profile");
    assert_eq!(profile.summary.good, 100);
}

#[tokio::test]
async fn requests_can_be_excluded_and_outcomes_forbidden() {
    let fx = fixture().await;
    fx.visits(80, &Visit::crawler(Some(true)));
    // Our own monitoring pretending to be the crawler.
    fx.visits(
        15,
        &Visit {
            extra: vec![b("synthetic", true)],
            ..Visit::crawler(None)
        },
    );
    // Told about stock, but explicitly told there is none.
    fx.visits(5, &Visit::crawler(Some(false)));
    fx.flush().await;

    let requests: Selector = serde_json::from_value(json!({
        "all": crawler_product_pages().all,
        "none": [{"attribute": "synthetic", "op": "is_true"}]
    }))
    .expect("selector");
    let success: Success = serde_json::from_value(json!({
        "all": [{"attribute": "product.in_stock", "op": "exists"}],
        "none": [{"attribute": "product.in_stock", "op": "is_false"}]
    }))
    .expect("success");

    let profile = evaluate::profile(&fx.engine, &requests, &success, last_day())
        .await
        .expect("profile");
    assert_eq!((profile.summary.total, profile.summary.good), (85, 80));
}

#[tokio::test]
async fn requests_nobody_records_are_reported_as_such_not_as_an_error() {
    let fx = fixture().await;
    incident(&fx).await;

    let unknown: Selector = serde_json::from_value(json!({
        "all": [{"attribute": "client.type", "op": "equals", "value": "googlebot"}]
    }))
    .expect("selector");
    let profile = evaluate::profile(&fx.engine, &unknown, &Success::default(), last_day())
        .await
        .expect("profile");

    assert_eq!(profile.summary.total, 0);
    assert!(
        profile.narrative[0].contains("not being recorded"),
        "{:?}",
        profile.narrative
    );
    assert_eq!(profile.suggestion.percent, None);
}

#[tokio::test]
async fn standing_reflects_the_target_and_the_amount_of_data() {
    let fx = fixture().await;
    incident(&fx).await;
    let now = Utc::now() + chrono::Duration::minutes(1);
    let condition = |percent: f64, minimum: u64| -> Condition {
        serde_json::from_value(definition(percent, minimum)).expect("condition")
    };

    // 95% achieved against 99% promised.
    let breached = evaluate::status(&fx.engine, &condition(99.0, 100), now, now)
        .await
        .expect("status");
    assert_eq!(breached.state, State::Breached);
    assert_eq!(
        (breached.total, breached.failures, breached.allowed_failures),
        (200, 10, 2)
    );
    assert!(breached.in_trial, "created just now with a seven-day trial");
    assert_eq!(
        breached.headline,
        "Not met: 95% of requests succeeded in the last 7 days, against a target of 99%. 10 failed; the target allows 2."
    );

    // Against 90%: met, with half the allowance used.
    let healthy = evaluate::status(&fx.engine, &condition(90.0, 100), now, now)
        .await
        .expect("status");
    assert_eq!(healthy.state, State::Healthy);
    assert_eq!(healthy.allowance_used_percent, Some(50.0));

    // Against 94%: met, but nearly out of allowance.
    let at_risk = evaluate::status(&fx.engine, &condition(94.0, 100), now, now)
        .await
        .expect("status");
    assert_eq!(at_risk.state, State::AtRisk);

    // A condition that needs more requests than there were says so.
    let quiet = evaluate::status(&fx.engine, &condition(99.0, 1_000), now, now)
        .await
        .expect("status");
    assert_eq!(quiet.state, State::NotEnoughData);

    // Once the trial is over it is no longer flagged.
    let later = evaluate::status(
        &fx.engine,
        &condition(99.0, 100),
        now - chrono::Duration::days(8),
        now,
    )
    .await
    .expect("status");
    assert!(!later.in_trial);
}

async fn call(
    router: &axum::Router,
    method: &str,
    uri: &str,
    body: Option<&Value>,
    author: Option<&str>,
) -> (StatusCode, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    if let Some(author) = author {
        request = request.header(AUTHOR_HEADER, author);
    }
    let request = match body {
        Some(body) => request
            .header("content-type", "application/json")
            .body(Body::from(body.to_string())),
        None => request.body(Body::empty()),
    }
    .expect("request");
    let response = router.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn the_api_takes_an_author_from_a_first_look_to_a_monitored_condition() {
    let fx = fixture().await;
    incident(&fx).await;
    let router = fx.router();

    // "Show me Googlebot requests for product availability."
    let (status, preview) = call(
        &router,
        "POST",
        "/preview",
        Some(&json!({"requests": crawler_product_pages(), "success": told_about_stock(), "lookback_hours": 24})),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert_eq!(preview["summary"]["total"], 200);
    assert_eq!(preview["summary"]["success_percent"], 95.0);
    assert_eq!(preview["suggestion"]["percent"], 90.0);
    assert_eq!(
        preview["narrative"][0],
        "200 requests matched in the last 24 hours."
    );

    // The attributes a definition can refer to are discoverable.
    let (status, attributes) = call(&router, "GET", "/attributes", None, None).await;
    assert_eq!(status, StatusCode::OK);
    let attributes = attributes.as_array().expect("array").clone();
    assert!(
        attributes
            .contains(&json!({"key": "product.in_stock", "scope": "span", "types": ["boolean"]})),
        "{attributes:?}"
    );
    assert!(
        attributes.contains(&json!({"key": "http.route", "scope": "built_in", "types": ["text"]})),
        "{attributes:?}"
    );

    // "Yes, set that up at 90%."
    let (status, created) = call(
        &router,
        "POST",
        "/",
        Some(&definition(90.0, 100)),
        Some("dana"),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["status"]["state"], "healthy");
    assert_eq!(created["status"]["in_trial"], true);
    assert_eq!(created["backtest"]["days_with_requests"], 1);
    assert_eq!(created["backtest"]["days_below_target"], 0);
    assert_eq!(created["condition"]["impact"]["severity"], "high");

    // It cannot be created twice.
    let (status, conflict) = call(&router, "POST", "/", Some(&definition(90.0, 100)), None).await;
    assert_eq!(status, StatusCode::CONFLICT, "{conflict}");

    // Someone tightens it; the standing changes and the change is recorded.
    tokio::time::sleep(std::time::Duration::from_millis(3)).await;
    let (status, updated) = call(
        &router,
        "PUT",
        "/googlebot-product-availability",
        Some(&definition(99.0, 100)),
        Some("sam"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["status"]["state"], "breached");

    let (_, listed) = call(&router, "GET", "/", None, None).await;
    assert_eq!(listed.as_array().expect("array").len(), 1);
    assert_eq!(listed[0]["status"]["state"], "breached");

    let (status, history) = call(
        &router,
        "GET",
        "/googlebot-product-availability/history",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(history[0]["author"], "dana");
    assert_eq!(history[0]["condition"]["target"]["percent"], 90.0);
    assert_eq!(history[1]["author"], "sam");

    let (status, backtest) = call(
        &router,
        "GET",
        "/googlebot-product-availability/backtest?days=7",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(backtest["days_below_target"], 1);
    assert!(
        backtest["narrative"][0]
            .as_str()
            .expect("text")
            .starts_with("Over the last 7 days, 95% of 200 matching requests succeeded"),
        "{backtest}"
    );

    // A restarted process finds it in the bucket.
    let restarted = ConditionStore::new(fx.bucket.clone());
    let stored = restarted
        .get("googlebot-product-availability")
        .await
        .expect("read")
        .expect("present");
    assert_eq!(stored.condition.target.percent, 99.0);

    // Retired, it is gone from view but not from the record.
    let (status, _) = call(
        &router,
        "DELETE",
        "/googlebot-product-availability",
        None,
        Some("dana"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = call(
        &router,
        "GET",
        "/googlebot-product-availability",
        None,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, history) = call(
        &router,
        "GET",
        "/googlebot-product-availability/history",
        None,
        None,
    )
    .await;
    assert_eq!(history.as_array().expect("array").len(), 3);
    assert_eq!(history[2]["retired"], true);
}

#[tokio::test]
async fn a_definition_that_cannot_work_is_refused_with_the_reason() {
    let fx = fixture().await;
    let router = fx.router();

    let mut no_requests = definition(99.0, 100);
    no_requests["requests"] = json!({"all": []});
    let (status, body) = call(&router, "POST", "/", Some(&no_requests), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"]
            .as_str()
            .expect("reason")
            .contains("which requests"),
        "{body}"
    );

    let mut impossible = definition(100.0, 100);
    impossible["id"] = json!("always");
    let (status, body) = call(&router, "POST", "/", Some(&impossible), None).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        body["error"].as_str().expect("reason").contains("percent"),
        "{body}"
    );

    let (status, _) = call(
        &router,
        "PUT",
        "/does-not-exist",
        Some(&json!({
            "id": "does-not-exist", "name": "x",
            "requests": crawler_product_pages(), "target": {"percent": 99.0}
        })),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
