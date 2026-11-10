//! End-to-end benchmark harness.
//!
//! Generates a realistic multi-service trace workload, pushes it through the
//! real conversion, buffering and flush path onto local disk, then times a
//! fixed set of queries through the real query engine. The output is a
//! table on stdout and, optionally, a JSON report so that two runs can be
//! compared.
//!
//! ```sh
//! cargo run --release --example harness -- --traces 300000 --json before.json
//! cargo run --release --example harness -- --dir /tmp/data --reuse   # query only
//! ```
//!
//! Unlike the criterion benches, which time individual functions against an
//! in-memory store, this measures what a user would see: many flushed files,
//! traces whose spans are split across services and files, and queries that
//! have to find them.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::indexing_slicing,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss,
    clippy::cast_possible_wrap,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    clippy::print_stdout,
    missing_docs
)]

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, BoxStream, StreamExt};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{span, ResourceSpans, ScopeSpans, Span, Status};
use serde::{Deserialize, Serialize};

use sidereal::buffer::Ingester;
use sidereal::config::{BufferConfig, ParquetConfig};
use sidereal::ingest::convert_traces_to_arrow;
use sidereal::query::{QueryEngine, QueryEngineBuilder};
use sidereal::schema::traces::traces_storage_schema;
use sidereal::storage::Signal;

/// 2026-11-01T00:00:00Z.
const BASE_TIME_SECS: u64 = 1_793_491_200;
const NANOS: u64 = 1_000_000_000;
const MILLIS: u64 = 1_000_000;
const META_FILE: &str = "harness-meta.json";
const SAMPLE_TRACES: usize = 256;
const TENANTS: u64 = 1_000;

// ---------------------------------------------------------------------------
// Arguments
// ---------------------------------------------------------------------------

struct Args {
    traces: usize,
    hours: u64,
    flushes_per_hour: u64,
    iterations: usize,
    dir: Option<PathBuf>,
    reuse: bool,
    json: Option<PathBuf>,
    label: String,
    get_latency_ms: u64,
    cache_mib: usize,
    only: Option<String>,
}

impl Args {
    fn parse() -> Self {
        let mut args = Self {
            traces: 300_000,
            hours: 6,
            flushes_per_hour: 30,
            iterations: 20,
            dir: None,
            reuse: false,
            json: None,
            label: "run".to_owned(),
            get_latency_ms: 0,
            cache_mib: 256,
            only: None,
        };
        let mut it = std::env::args().skip(1);
        while let Some(flag) = it.next() {
            let mut value = || {
                it.next()
                    .unwrap_or_else(|| panic!("missing value for {flag}"))
            };
            match flag.as_str() {
                "--traces" => args.traces = value().replace('_', "").parse().unwrap(),
                "--hours" => args.hours = value().parse().unwrap(),
                "--flushes-per-hour" => args.flushes_per_hour = value().parse().unwrap(),
                "--iterations" => args.iterations = value().parse().unwrap(),
                "--dir" => args.dir = Some(PathBuf::from(value())),
                "--json" => args.json = Some(PathBuf::from(value())),
                "--label" => args.label = value(),
                "--get-latency-ms" => args.get_latency_ms = value().parse().unwrap(),
                "--cache-mib" => args.cache_mib = value().parse().unwrap(),
                "--only" => args.only = Some(value()),
                "--reuse" => args.reuse = true,
                "--help" | "-h" => {
                    println!(
                        "harness [--traces N] [--hours H] [--flushes-per-hour F] \
                         [--iterations N] [--dir PATH] [--reuse] [--json PATH] \
                         [--label NAME] [--get-latency-ms MS] [--cache-mib N] [--only QUERY]\n\n\
                         --reuse            query an existing dataset in --dir\n\
                         --get-latency-ms   add a fixed delay to every object read and\n\
                         \x20                  listing, to approximate remote object storage\n\
                         --cache-mib        in-memory object cache size, 0 to disable\n\
                         --only             run only queries whose name contains this"
                    );
                    std::process::exit(0);
                }
                other => panic!("unknown argument: {other}"),
            }
        }
        args
    }
}

// ---------------------------------------------------------------------------
// Deterministic random numbers (no dependency, reproducible datasets)
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn between(&mut self, low: u64, high: u64) -> u64 {
        low + self.below(high - low)
    }

    fn chance(&mut self, one_in: u64) -> bool {
        self.below(one_in) == 0
    }
}

// ---------------------------------------------------------------------------
// Workload model
// ---------------------------------------------------------------------------

const SERVICES: [&str; 10] = [
    "api-gateway",
    "auth-service",
    "catalogue-service",
    "cart-service",
    "checkout-service",
    "payments-service",
    "inventory-service",
    "order-events",
    "fulfilment-worker",
    "notification-service",
];
const GATEWAY: usize = 0;
const AUTH: usize = 1;
const CATALOGUE: usize = 2;
const CART: usize = 3;
const CHECKOUT: usize = 4;
const PAYMENTS: usize = 5;
const INVENTORY: usize = 6;
const ORDER_EVENTS: usize = 7;
const FULFILMENT: usize = 8;
const NOTIFICATION: usize = 9;

const VERSIONS: [&str; 2] = ["1.14.2", "1.15.0"];
const PODS_PER_VERSION: usize = 3;
const BUCKETS: usize = SERVICES.len() * VERSIONS.len() * PODS_PER_VERSION;

fn string_attr(key: &str, value: impl Into<String>) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }),
    }
}

fn int_attr(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn resource_for(bucket: usize) -> Resource {
    let service = bucket / (VERSIONS.len() * PODS_PER_VERSION);
    let version = (bucket / PODS_PER_VERSION) % VERSIONS.len();
    let pod = bucket % PODS_PER_VERSION;
    let name = SERVICES[service];
    Resource {
        attributes: vec![
            string_attr("service.name", name),
            string_attr("service.version", VERSIONS[version]),
            string_attr("service.namespace", "shop"),
            string_attr("deployment.environment.name", "production"),
            string_attr("telemetry.sdk.name", "opentelemetry"),
            string_attr("telemetry.sdk.language", "rust"),
            string_attr("telemetry.sdk.version", "0.31.0"),
            string_attr("host.name", format!("ip-10-0-{service}-{pod}")),
            string_attr("k8s.namespace.name", "shop"),
            string_attr("k8s.pod.name", format!("{name}-{version}-{pod}")),
            string_attr("cloud.provider", "aws"),
            string_attr("cloud.region", "eu-west-1"),
        ],
        ..Default::default()
    }
}

/// One trace under construction: spans are appended to the bucket of the
/// service instance that would have exported them.
struct TraceBuilder<'a> {
    rng: &'a mut Rng,
    buckets: &'a mut [Vec<Span>],
    trace_id: [u8; 16],
    tenant: u64,
    user: u64,
    spans: usize,
}

impl TraceBuilder<'_> {
    /// Emit a span and return its ID. Roughly one request in ten is served
    /// by the newer version of each service.
    fn span(
        &mut self,
        service: usize,
        kind: span::SpanKind,
        name: &str,
        parent: Option<[u8; 8]>,
        start: u64,
        duration: u64,
        mut attributes: Vec<KeyValue>,
        error: Option<&str>,
    ) -> [u8; 8] {
        let span_id = self.rng.next().to_be_bytes();
        let version = usize::from(self.rng.chance(10));
        let pod = self.rng.below(PODS_PER_VERSION as u64) as usize;
        let bucket = service * VERSIONS.len() * PODS_PER_VERSION + version * PODS_PER_VERSION + pod;

        attributes.push(string_attr("tenant.id", format!("tenant-{}", self.tenant)));
        attributes.push(string_attr("user.id", format!("user-{}", self.user)));
        attributes.push(string_attr("thread.name", "tokio-runtime-worker"));

        let mut events = Vec::new();
        let status = error.map_or(
            Status {
                code: 0,
                message: String::new(),
            },
            |message| {
                attributes.push(string_attr("error.type", "PaymentDeclined"));
                events.push(span::Event {
                    time_unix_nano: start + duration,
                    name: "exception".to_owned(),
                    attributes: vec![
                        string_attr("exception.type", "PaymentDeclined"),
                        string_attr("exception.message", message),
                        string_attr(
                            "exception.stacktrace",
                            "PaymentDeclined: card declined\n   at payments::charge (src/charge.rs:118)\n   at payments::handler (src/handler.rs:42)",
                        ),
                    ],
                    dropped_attributes_count: 0,
                });
                Status {
                    code: 2,
                    message: message.to_owned(),
                }
            },
        );

        self.buckets[bucket].push(Span {
            trace_id: self.trace_id.to_vec(),
            span_id: span_id.to_vec(),
            parent_span_id: parent.map(|p| p.to_vec()).unwrap_or_default(),
            name: name.to_owned(),
            kind: kind as i32,
            start_time_unix_nano: start,
            end_time_unix_nano: start + duration,
            attributes,
            events,
            status: Some(status),
            ..Default::default()
        });
        self.spans += 1;
        span_id
    }

    fn http_server(
        &mut self,
        service: usize,
        method: &str,
        route: &str,
        parent: Option<[u8; 8]>,
        start: u64,
        duration: u64,
        status: i64,
    ) -> [u8; 8] {
        let path = route.replace("{id}", &self.rng.below(50_000).to_string());
        let attrs = vec![
            string_attr("http.request.method", method),
            string_attr("http.route", route),
            int_attr("http.response.status_code", status),
            string_attr("url.scheme", "https"),
            string_attr("url.path", path),
            string_attr(
                "server.address",
                format!("{}.shop.internal", SERVICES[service]),
            ),
            int_attr("server.port", 8443),
            string_attr("network.protocol.version", "2"),
        ];
        let error = (status >= 500).then_some("upstream failure");
        self.span(
            service,
            span::SpanKind::Server,
            &format!("{method} {route}"),
            parent,
            start,
            duration,
            attrs,
            error,
        )
    }

    /// A client span in `from` and the matching server span in `to`.
    fn http_call(
        &mut self,
        from: usize,
        to: usize,
        method: &str,
        route: &str,
        parent: [u8; 8],
        start: u64,
        duration: u64,
        status: i64,
    ) -> [u8; 8] {
        let attrs = vec![
            string_attr("http.request.method", method),
            int_attr("http.response.status_code", status),
            string_attr("server.address", format!("{}.shop.internal", SERVICES[to])),
            int_attr("server.port", 8443),
        ];
        let error = (status >= 500).then_some("upstream failure");
        let client = self.span(
            from,
            span::SpanKind::Client,
            method,
            Some(parent),
            start,
            duration,
            attrs,
            error,
        );
        self.http_server(
            to,
            method,
            route,
            Some(client),
            start + MILLIS,
            duration - 2 * MILLIS,
            status,
        )
    }

    fn rpc_call(&mut self, from: usize, to: usize, method: &str, parent: [u8; 8], start: u64) {
        let duration = self.rng.between(2, 12) * MILLIS;
        let attrs = |service: &str| {
            vec![
                string_attr("rpc.system", "grpc"),
                string_attr("rpc.service", service),
                string_attr("rpc.method", method),
            ]
        };
        let rpc_service = format!("shop.{}", SERVICES[to]);
        let name = format!("{rpc_service}/{method}");
        let client = self.span(
            from,
            span::SpanKind::Client,
            &name,
            Some(parent),
            start,
            duration,
            attrs(&rpc_service),
            None,
        );
        self.span(
            to,
            span::SpanKind::Server,
            &name,
            Some(client),
            start + MILLIS / 2,
            duration - MILLIS,
            attrs(&rpc_service),
            None,
        );
    }

    fn db_query(
        &mut self,
        service: usize,
        system: &str,
        operation: &str,
        collection: &str,
        parent: [u8; 8],
        start: u64,
    ) {
        let duration = self.rng.between(1, 25) * MILLIS;
        self.span(
            service,
            span::SpanKind::Client,
            &format!("{operation} {collection}"),
            Some(parent),
            start,
            duration,
            vec![
                string_attr("db.system.name", system),
                string_attr("db.operation.name", operation),
                string_attr("db.collection.name", collection),
                string_attr("server.address", format!("{system}.shop.internal")),
            ],
            None,
        );
    }
}

/// Generate one trace starting at `start` and return its span count.
fn generate_trace(rng: &mut Rng, buckets: &mut [Vec<Span>], start: u64) -> ([u8; 16], usize) {
    let mut trace_id = [0u8; 16];
    trace_id[..8].copy_from_slice(&rng.next().to_be_bytes());
    trace_id[8..].copy_from_slice(&rng.next().to_be_bytes());
    let tenant = rng.below(TENANTS);
    let user = rng.below(100_000);
    let shape = rng.below(100);
    let mut t = TraceBuilder {
        rng,
        buckets,
        trace_id,
        tenant,
        user,
        spans: 0,
    };

    if shape < 10 {
        // Health check: a single span.
        t.http_server(GATEWAY, "GET", "/healthz", None, start, 2 * MILLIS, 200);
    } else if shape < 60 {
        // Browse a product.
        let total = t.rng.between(20, 120) * MILLIS;
        let root = t.http_server(GATEWAY, "GET", "/products/{id}", None, start, total, 200);
        t.rpc_call(GATEWAY, AUTH, "Verify", root, start + MILLIS);
        let catalogue = t.http_call(
            GATEWAY,
            CATALOGUE,
            "GET",
            "/internal/products/{id}",
            root,
            start + 14 * MILLIS,
            total - 16 * MILLIS,
            200,
        );
        t.db_query(
            CATALOGUE,
            "postgresql",
            "SELECT",
            "products",
            catalogue,
            start + 16 * MILLIS,
        );
        if t.rng.chance(3) {
            t.db_query(
                CATALOGUE,
                "redis",
                "GET",
                "product-cache",
                catalogue,
                start + 15 * MILLIS,
            );
        }
    } else if shape < 88 {
        // Add to cart.
        let total = t.rng.between(40, 200) * MILLIS;
        let root = t.http_server(GATEWAY, "POST", "/cart/items", None, start, total, 201);
        t.rpc_call(GATEWAY, AUTH, "Verify", root, start + MILLIS);
        let cart = t.http_call(
            GATEWAY,
            CART,
            "POST",
            "/internal/cart/items",
            root,
            start + 14 * MILLIS,
            total - 16 * MILLIS,
            201,
        );
        t.rpc_call(CART, INVENTORY, "Reserve", cart, start + 16 * MILLIS);
        t.db_query(
            INVENTORY,
            "postgresql",
            "UPDATE",
            "stock",
            cart,
            start + 18 * MILLIS,
        );
        t.db_query(CART, "redis", "HSET", "carts", cart, start + 30 * MILLIS);
    } else {
        // Checkout, including an asynchronous tail through a queue. About
        // one checkout in eight fails at the payment provider.
        let failed = t.rng.chance(8);
        let status = if failed { 502 } else { 200 };
        let total = t.rng.between(150, 900) * MILLIS;
        let root = t.http_server(GATEWAY, "POST", "/checkout", None, start, total, status);
        t.rpc_call(GATEWAY, AUTH, "Verify", root, start + MILLIS);
        let checkout = t.http_call(
            GATEWAY,
            CHECKOUT,
            "POST",
            "/internal/checkout",
            root,
            start + 14 * MILLIS,
            total - 16 * MILLIS,
            status,
        );
        t.db_query(
            CHECKOUT,
            "redis",
            "HGETALL",
            "carts",
            checkout,
            start + 16 * MILLIS,
        );
        t.rpc_call(CHECKOUT, INVENTORY, "Commit", checkout, start + 20 * MILLIS);
        t.db_query(
            INVENTORY,
            "postgresql",
            "UPDATE",
            "stock",
            checkout,
            start + 22 * MILLIS,
        );
        let payment = t.http_call(
            CHECKOUT,
            PAYMENTS,
            "POST",
            "/internal/charges",
            checkout,
            start + 40 * MILLIS,
            total - 60 * MILLIS,
            status,
        );
        let provider_duration = total - 80 * MILLIS;
        t.span(
            PAYMENTS,
            span::SpanKind::Client,
            "POST",
            Some(payment),
            start + 45 * MILLIS,
            provider_duration,
            vec![
                string_attr("http.request.method", "POST"),
                int_attr("http.response.status_code", if failed { 402 } else { 200 }),
                string_attr("server.address", "api.payments.example"),
                int_attr("server.port", 443),
            ],
            failed.then_some("card declined by provider"),
        );
        if !failed {
            t.db_query(
                CHECKOUT,
                "postgresql",
                "INSERT",
                "orders",
                checkout,
                start + total - 30 * MILLIS,
            );
            let messaging = |operation: &str| {
                vec![
                    string_attr("messaging.system", "aws_sqs"),
                    string_attr("messaging.operation.type", operation),
                    string_attr("messaging.destination.name", "order-placed"),
                ]
            };
            let publish = t.span(
                ORDER_EVENTS,
                span::SpanKind::Producer,
                "publish order-placed",
                Some(checkout),
                start + total - 20 * MILLIS,
                4 * MILLIS,
                messaging("publish"),
                None,
            );
            let lag = t.rng.between(50, 2_000) * MILLIS;
            let process = t.span(
                FULFILMENT,
                span::SpanKind::Consumer,
                "process order-placed",
                Some(publish),
                start + total + lag,
                60 * MILLIS,
                messaging("process"),
                None,
            );
            t.db_query(
                FULFILMENT,
                "postgresql",
                "INSERT",
                "shipments",
                process,
                start + total + lag + 5 * MILLIS,
            );
            let notify = t.span(
                NOTIFICATION,
                span::SpanKind::Consumer,
                "process order-placed",
                Some(publish),
                start + total + lag + 10 * MILLIS,
                120 * MILLIS,
                messaging("process"),
                None,
            );
            t.span(
                NOTIFICATION,
                span::SpanKind::Client,
                "POST",
                Some(notify),
                start + total + lag + 15 * MILLIS,
                100 * MILLIS,
                vec![
                    string_attr("http.request.method", "POST"),
                    int_attr("http.response.status_code", 202),
                    string_attr("server.address", "api.email.example"),
                ],
                None,
            );
        }
    }

    (trace_id, t.spans)
}

// ---------------------------------------------------------------------------
// Dataset metadata, persisted next to the data so query-only runs can reuse it
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct SampleTrace {
    trace_id: String,
    spans: usize,
    start_unix_nano: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct DatasetMeta {
    traces: usize,
    spans: usize,
    hours: u64,
    flushes: u64,
    convert_secs: f64,
    flush_secs: f64,
    samples: Vec<SampleTrace>,
}

async fn generate_dataset(args: &Args, dir: &Path) -> DatasetMeta {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let ingester = Ingester::new(
        Signal::Traces,
        traces_storage_schema(),
        store,
        BufferConfig {
            max_batch_size: usize::MAX,
            flush_interval_secs: 3600,
            max_buffer_bytes: usize::MAX,
            max_records_per_request: usize::MAX,
            flush_max_retries: 0,
            flush_initial_delay_ms: 100,
            flush_max_delay_ms: 10_000,
        },
        ParquetConfig::default(),
    );

    let flushes = args.hours * args.flushes_per_hour;
    let window = 3600 * NANOS / args.flushes_per_hour;
    let traces_per_flush = (args.traces as u64 / flushes).max(1);
    let sample_every = ((traces_per_flush * flushes) as usize / SAMPLE_TRACES).max(1);

    let mut rng = Rng(0x5EED);
    let mut buckets: Vec<Vec<Span>> = vec![Vec::new(); BUCKETS];
    let mut samples = Vec::new();
    let (mut traces, mut spans) = (0usize, 0usize);
    let (mut convert, mut flush) = (Duration::ZERO, Duration::ZERO);

    for flush_index in 0..flushes {
        let window_start = BASE_TIME_SECS * NANOS + flush_index * window;
        for _ in 0..traces_per_flush {
            let start = window_start + rng.below(window);
            let (trace_id, span_count) = generate_trace(&mut rng, &mut buckets, start);
            if traces % sample_every == 0 {
                samples.push(SampleTrace {
                    trace_id: hex::encode(trace_id),
                    spans: span_count,
                    start_unix_nano: start,
                });
            }
            traces += 1;
            spans += span_count;
        }

        // Each service instance exports its own spans, as real SDKs do, so
        // one trace arrives spread over many requests.
        for (bucket, bucket_spans) in buckets.iter_mut().enumerate() {
            if bucket_spans.is_empty() {
                continue;
            }
            let request = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: Some(resource_for(bucket)),
                    scope_spans: vec![ScopeSpans {
                        scope: Some(InstrumentationScope {
                            name: "opentelemetry-instrumentation".to_owned(),
                            version: "0.31.0".to_owned(),
                            ..Default::default()
                        }),
                        spans: std::mem::take(bucket_spans),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };
            let started = Instant::now();
            let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
            ingester.ingest(batch).await.unwrap();
            convert += started.elapsed();
        }

        let started = Instant::now();
        ingester.flush().await.unwrap();
        flush += started.elapsed();
    }

    let meta = DatasetMeta {
        traces,
        spans,
        hours: args.hours,
        flushes,
        convert_secs: convert.as_secs_f64(),
        flush_secs: flush.as_secs_f64(),
        samples,
    };
    std::fs::write(
        dir.join(META_FILE),
        serde_json::to_vec_pretty(&meta).unwrap(),
    )
    .unwrap();
    meta
}

fn disk_usage(dir: &Path) -> (usize, u64) {
    let (mut files, mut bytes) = (0, 0);
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        let path = entry.path();
        if path.is_dir() {
            let (f, b) = disk_usage(&path);
            files += f;
            bytes += b;
        } else if path.extension().is_some_and(|ext| ext == "parquet") {
            files += 1;
            bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
        }
    }
    (files, bytes)
}

// ---------------------------------------------------------------------------
// Simulated remote storage
// ---------------------------------------------------------------------------

/// Adds a fixed delay to every read and listing, and counts them.
///
/// Local disk answers in microseconds; remote object storage takes tens of
/// milliseconds per request regardless of size. The delay makes the number
/// of round trips a query needs visible in its latency.
#[derive(Debug)]
struct RemoteLike {
    inner: LocalFileSystem,
    delay: Duration,
    gets: AtomicU64,
    lists: AtomicU64,
}

impl std::fmt::Display for RemoteLike {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteLike({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RemoteLike {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.gets.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.delay).await;
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        let delay = self.delay;
        let listing = self.inner.list(prefix);
        stream::once(async move {
            tokio::time::sleep(delay).await;
            listing
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(self.delay).await;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

// ---------------------------------------------------------------------------
// Queries
// ---------------------------------------------------------------------------

struct QueryCase {
    name: &'static str,
    story: &'static str,
    /// Build the SQL for iteration `i`.
    sql: Box<dyn Fn(usize) -> String>,
    /// Expected row count for iteration `i`, where it is known.
    expected_rows: Box<dyn Fn(usize) -> Option<usize>>,
}

fn partition_of(unix_nano: u64) -> (String, String) {
    let secs = (unix_nano / NANOS) as i64;
    let time = chrono::DateTime::from_timestamp(secs, 0).unwrap();
    (
        time.format("%Y-%m-%d").to_string(),
        time.format("%H").to_string(),
    )
}

fn query_cases(meta: &DatasetMeta) -> Vec<QueryCase> {
    let samples = Arc::new(meta.samples.clone());
    let pick =
        |samples: &Arc<Vec<SampleTrace>>, i: usize| samples[(i * 7 + 3) % samples.len()].clone();
    let last_hour_start = BASE_TIME_SECS * NANOS + (meta.hours - 1) * 3600 * NANOS;
    let (last_date, last_hour) = partition_of(last_hour_start);
    let none = || Box::new(|_| None) as Box<dyn Fn(usize) -> Option<usize>>;

    let mut cases = Vec::new();

    let s = samples.clone();
    let e = samples.clone();
    cases.push(QueryCase {
        name: "trace_by_id",
        story: "4: trace by ID, no time hint",
        sql: Box::new(move |i| {
            format!(
                "SELECT * FROM traces WHERE trace_id = X'{}'",
                pick(&s, i).trace_id
            )
        }),
        expected_rows: Box::new(move |i| Some(pick(&e, i).spans)),
    });

    let s = samples.clone();
    cases.push(QueryCase {
        name: "trace_by_id_in_hour",
        story: "4: trace by ID, hour known",
        sql: Box::new(move |i| {
            let sample = pick(&s, i);
            let (date, hour) = partition_of(sample.start_unix_nano);
            format!(
                "SELECT * FROM traces WHERE trace_id = X'{}' AND date = '{date}' AND hour = '{hour}'",
                sample.trace_id
            )
        }),
        expected_rows: none(),
    });

    // A real ID with its last byte flipped: inside every file's min/max
    // range, so only a bloom filter or a full scan can rule it out.
    let s = samples.clone();
    cases.push(QueryCase {
        name: "trace_by_id_absent",
        story: "4: trace ID that does not exist",
        sql: Box::new(move |i| {
            let mut id = hex::decode(pick(&s, i).trace_id).unwrap();
            id[15] ^= 0xFF;
            format!(
                "SELECT * FROM traces WHERE trace_id = X'{}'",
                hex::encode(id)
            )
        }),
        expected_rows: Box::new(|_| Some(0)),
    });

    let (date, hour) = (last_date.clone(), last_hour.clone());
    cases.push(QueryCase {
        name: "error_rate_by_service_1h",
        story: "10: error rate per service, last hour",
        sql: Box::new(move |_| {
            format!(
                "SELECT \"service.name\", COUNT(*) AS spans, \
                 SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) AS errors \
                 FROM traces WHERE date = '{date}' AND hour = '{hour}' \
                 GROUP BY \"service.name\""
            )
        }),
        expected_rows: Box::new(|_| Some(SERVICES.len())),
    });

    cases.push(QueryCase {
        name: "p99_by_route_all_time",
        story: "10: p99 latency per route, one service",
        sql: Box::new(|_| {
            "SELECT \"http.route\", COUNT(*) AS requests, \
             approx_percentile_cont(duration_ns, 0.99) AS p99 \
             FROM traces WHERE \"service.name\" = 'api-gateway' AND kind = 2 \
             GROUP BY \"http.route\""
                .to_owned()
        }),
        expected_rows: Box::new(|_| Some(4)),
    });

    cases.push(QueryCase {
        name: "recent_spans_for_service",
        story: "8: latest 100 spans for a service",
        sql: Box::new(|i| {
            format!(
                "SELECT trace_id, span_id, name, start_time_unix_nano, duration_ns \
                 FROM traces WHERE \"service.name\" = '{}' \
                 ORDER BY start_time_unix_nano DESC LIMIT 100",
                SERVICES[i % SERVICES.len()]
            )
        }),
        expected_rows: Box::new(|_| Some(100)),
    });

    cases.push(QueryCase {
        name: "custom_attribute_filter",
        story: "6: count spans for one tenant",
        sql: Box::new(|i| {
            format!(
                "SELECT COUNT(*) FROM traces \
                 WHERE cbor_extract(span_attributes, 'tenant.id') = 'tenant-{}'",
                (i as u64 * 37) % TENANTS
            )
        }),
        expected_rows: Box::new(|_| Some(1)),
    });

    let hours = meta.hours;
    cases.push(QueryCase {
        name: "time_range_5m_no_partition",
        story: "8: 5-minute window by timestamp only",
        sql: Box::new(move |i| {
            let offset = (i as u64 * 611) % (hours * 3600 - 300);
            let from = (BASE_TIME_SECS + offset) * NANOS;
            format!(
                "SELECT COUNT(*) FROM traces \
                 WHERE start_time_unix_nano >= {from} AND start_time_unix_nano < {}",
                from + 300 * NANOS
            )
        }),
        expected_rows: Box::new(|_| Some(1)),
    });

    cases.push(QueryCase {
        name: "count_all",
        story: "12: count every span",
        sql: Box::new(|_| "SELECT COUNT(*) FROM traces".to_owned()),
        expected_rows: Box::new(|_| Some(1)),
    });

    cases
}

#[derive(Serialize)]
struct QueryResult {
    name: String,
    story: String,
    iterations: usize,
    first_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
    /// Storage requests made by the first iteration.
    first_requests: u64,
    /// Mean storage requests per iteration after the first.
    later_requests: f64,
    rows: usize,
    error: Option<String>,
}

async fn run_case(
    engine: &QueryEngine,
    remote: &RemoteLike,
    case: &QueryCase,
    iterations: usize,
) -> QueryResult {
    let requests = || remote.gets.load(Ordering::Relaxed) + remote.lists.load(Ordering::Relaxed);
    let mut first_requests = 0;
    let mut later_requests = 0;
    let mut timings = Vec::with_capacity(iterations);
    let mut rows = 0;
    let mut error = None;

    for i in 0..iterations {
        let sql = (case.sql)(i);
        let requests_before = requests();
        let started = Instant::now();
        match engine.query(&sql).await {
            Ok(batches) => {
                timings.push(started.elapsed().as_secs_f64() * 1e3);
                if i == 0 {
                    first_requests = requests() - requests_before;
                } else {
                    later_requests += requests() - requests_before;
                }
                rows = batches.iter().map(|b| b.num_rows()).sum();
                if let Some(expected) = (case.expected_rows)(i) {
                    if rows != expected && error.is_none() {
                        error = Some(format!("expected {expected} rows, got {rows}"));
                    }
                }
            }
            Err(e) => {
                error = Some(e.to_string());
                break;
            }
        }
    }

    let first = timings.first().copied().unwrap_or(f64::NAN);
    let mut sorted = timings.clone();
    sorted.sort_by(f64::total_cmp);
    let percentile = |p: f64| {
        if sorted.is_empty() {
            f64::NAN
        } else {
            sorted[(((sorted.len() - 1) as f64) * p).round() as usize]
        }
    };

    QueryResult {
        name: case.name.to_owned(),
        story: case.story.to_owned(),
        iterations: timings.len(),
        first_ms: first,
        p50_ms: percentile(0.5),
        p95_ms: percentile(0.95),
        max_ms: sorted.last().copied().unwrap_or(f64::NAN),
        first_requests,
        later_requests: later_requests as f64 / (timings.len().max(2) - 1) as f64,
        rows,
        error,
    }
}

#[derive(Serialize)]
struct Report {
    label: String,
    get_latency_ms: u64,
    cache_mib: usize,
    traces: usize,
    spans: usize,
    hours: u64,
    files: usize,
    bytes: u64,
    bytes_per_span: f64,
    convert_spans_per_sec: f64,
    flush_spans_per_sec: f64,
    queries: Vec<QueryResult>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let temp;
    let dir = if let Some(dir) = &args.dir {
        std::fs::create_dir_all(dir).unwrap();
        dir.canonicalize().unwrap()
    } else {
        temp = tempfile::tempdir().unwrap();
        temp.path().canonicalize().unwrap()
    };

    let meta = if args.reuse {
        let raw = std::fs::read(dir.join(META_FILE))
            .unwrap_or_else(|_| panic!("--reuse needs an existing dataset in {}", dir.display()));
        serde_json::from_slice(&raw).unwrap()
    } else {
        if dir.join("traces").exists() {
            std::fs::remove_dir_all(dir.join("traces")).unwrap();
        }
        println!(
            "generating {} traces into {} ...",
            args.traces,
            dir.display()
        );
        generate_dataset(&args, &dir).await
    };
    let (files, bytes) = disk_usage(&dir);

    // Queries read through a store registered under its own URL scheme, so
    // an optional per-request delay can stand in for remote object storage.
    let remote = Arc::new(RemoteLike {
        inner: LocalFileSystem::new_with_prefix(&dir).unwrap(),
        delay: Duration::from_millis(args.get_latency_ms),
        gets: AtomicU64::new(0),
        lists: AtomicU64::new(0),
    });
    let store: Arc<dyn ObjectStore> = remote.clone();
    let engine = QueryEngineBuilder::new(store, "harness://data")
        .with_timeout(Some(Duration::from_secs(600)))
        .with_cache_bytes(args.cache_mib * 1024 * 1024)
        .build()
        .await
        .unwrap();

    let mut results = Vec::new();
    for case in query_cases(&meta) {
        if args
            .only
            .as_ref()
            .is_some_and(|only| !case.name.contains(only.as_str()))
        {
            continue;
        }
        results.push(run_case(&engine, &remote, &case, args.iterations).await);
    }

    let report = Report {
        label: args.label.clone(),
        get_latency_ms: args.get_latency_ms,
        cache_mib: args.cache_mib,
        traces: meta.traces,
        spans: meta.spans,
        hours: meta.hours,
        files,
        bytes,
        bytes_per_span: bytes as f64 / meta.spans as f64,
        convert_spans_per_sec: meta.spans as f64 / meta.convert_secs,
        flush_spans_per_sec: meta.spans as f64 / meta.flush_secs,
        queries: results,
    };

    let mut out = String::new();
    writeln!(out, "\n== {} ==", report.label).unwrap();
    writeln!(
        out,
        "dataset   {} traces, {} spans, {} hours, {} files, {:.1} MiB ({:.0} bytes/span)",
        report.traces,
        report.spans,
        report.hours,
        report.files,
        report.bytes as f64 / (1024.0 * 1024.0),
        report.bytes_per_span
    )
    .unwrap();
    writeln!(
        out,
        "ingest    convert {:.0} spans/s, flush {:.0} spans/s (single thread)",
        report.convert_spans_per_sec, report.flush_spans_per_sec
    )
    .unwrap();
    writeln!(
        out,
        "storage   +{} ms per object read/list\n",
        report.get_latency_ms
    )
    .unwrap();
    writeln!(
        out,
        "{:<28} {:>9} {:>9} {:>9} {:>9} {:>9} {:>7}  {}",
        "query", "first ms", "p50 ms", "p95 ms", "reqs 1st", "reqs next", "rows", "user story"
    )
    .unwrap();
    for q in &report.queries {
        writeln!(
            out,
            "{:<28} {:>9.1} {:>9.1} {:>9.1} {:>9} {:>9.1} {:>7}  {}{}",
            q.name,
            q.first_ms,
            q.p50_ms,
            q.p95_ms,
            q.first_requests,
            q.later_requests,
            q.rows,
            q.story,
            q.error
                .as_ref()
                .map(|e| format!("  !! {e}"))
                .unwrap_or_default()
        )
        .unwrap();
    }
    println!("{out}");

    if let Some(path) = &args.json {
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
        println!("wrote {}", path.display());
    }
}
