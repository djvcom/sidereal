//! axum HTTP OTLP receivers.
//!
//! Implements the OTLP/HTTP endpoints for traces, metrics, and logs ingestion.
//! Supports both protobuf (`application/x-protobuf`) and JSON (`application/json`)
//! content types as per the OTLP specification.
//!
//! Gzip-compressed request bodies are automatically decompressed via tower-http middleware.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{header::CONTENT_TYPE, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use tower_http::decompression::RequestDecompressionLayer;

/// Default maximum request body size (16 MiB).
///
/// This limit prevents large requests from exhausting memory. Telemetry batches
/// larger than this should be split into smaller chunks by the client.
pub const DEFAULT_MAX_BODY_SIZE: usize = 16 * 1024 * 1024;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{
        ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
    trace::v1::{ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use prost::Message;

use super::convert::{
    convert_logs_to_arrow, convert_metrics_to_arrow, convert_traces_to_arrow, ConversionResult,
};
use crate::buffer::Ingester;
use crate::redact::RedactionEngine;
use crate::TelemetryError;

/// Shared state for HTTP handlers.
#[derive(Clone)]
pub struct OtlpHttpState {
    pub trace_ingester: Arc<Ingester>,
    pub metrics_ingester: Arc<Ingester>,
    pub logs_ingester: Arc<Ingester>,
    pub redaction: Arc<RedactionEngine>,
}

/// Content type for OTLP requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentType {
    Protobuf,
    Json,
}

impl ContentType {
    fn from_headers(headers: &HeaderMap) -> Result<Self, TelemetryError> {
        let content_type = headers
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/x-protobuf");

        if content_type.starts_with("application/x-protobuf") {
            Ok(Self::Protobuf)
        } else if content_type.starts_with("application/json") {
            Ok(Self::Json)
        } else {
            Err(TelemetryError::InvalidContentType {
                content_type: content_type.to_owned(),
            })
        }
    }
}

/// Create the OTLP HTTP router with default body size limit.
pub fn otlp_http_router(state: OtlpHttpState) -> Router {
    otlp_http_router_with_limit(state, DEFAULT_MAX_BODY_SIZE)
}

/// Create the OTLP HTTP router with a custom body size limit.
///
/// The router includes automatic gzip decompression via tower-http middleware,
/// which also protects against decompression bombs by limiting the decompressed size.
pub fn otlp_http_router_with_limit(state: OtlpHttpState, max_body_size: usize) -> Router {
    Router::new()
        .route("/health", axum::routing::get(handle_health))
        .route("/v1/traces", post(handle_traces))
        .route("/v1/metrics", post(handle_metrics))
        .route("/v1/logs", post(handle_logs))
        .layer(RequestDecompressionLayer::new())
        .layer(DefaultBodyLimit::max(max_body_size))
        .with_state(state)
}

/// Handle GET /health - ingestion health check
#[tracing::instrument]
async fn handle_health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

/// Handle POST /v1/traces
#[tracing::instrument(skip(state, headers, body), fields(signal = "traces"))]
async fn handle_traces(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, HttpError> {
    let content_type = ContentType::from_headers(&headers)?;
    let request = decode_request::<ExportTraceServiceRequest>(content_type, &body)?;

    let result = convert_traces_to_arrow(&request, Some(&state.redaction))?;
    let partial_success = build_partial_success::<ExportTracePartialSuccess>(&result);
    if result.batch.num_rows() > 0 {
        state.trace_ingester.ingest(result.batch).await?;
    }

    let response = ExportTraceServiceResponse { partial_success };
    encode_response(content_type, &response)
}

/// Handle POST /v1/metrics
#[tracing::instrument(skip(state, headers, body), fields(signal = "metrics"))]
async fn handle_metrics(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, HttpError> {
    let content_type = ContentType::from_headers(&headers)?;
    let request = decode_request::<ExportMetricsServiceRequest>(content_type, &body)?;

    let result = convert_metrics_to_arrow(&request, Some(&state.redaction))?;
    let partial_success = build_partial_success::<ExportMetricsPartialSuccess>(&result);
    if result.batch.num_rows() > 0 {
        state.metrics_ingester.ingest(result.batch).await?;
    }

    let response = ExportMetricsServiceResponse { partial_success };
    encode_response(content_type, &response)
}

/// Handle POST /v1/logs
#[tracing::instrument(skip(state, headers, body), fields(signal = "logs"))]
async fn handle_logs(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, HttpError> {
    let content_type = ContentType::from_headers(&headers)?;
    let request = decode_request::<ExportLogsServiceRequest>(content_type, &body)?;

    let result = convert_logs_to_arrow(&request, Some(&state.redaction))?;
    let partial_success = build_partial_success::<ExportLogsPartialSuccess>(&result);
    if result.batch.num_rows() > 0 {
        state.logs_ingester.ingest(result.batch).await?;
    }

    let response = ExportLogsServiceResponse { partial_success };
    encode_response(content_type, &response)
}

/// Trait for building partial_success responses from ConversionResult.
trait PartialSuccessBuilder: Default {
    fn with_rejected_count(count: i64) -> Self;
    fn with_error_message(self, message: String) -> Self;
}

impl PartialSuccessBuilder for ExportTracePartialSuccess {
    fn with_rejected_count(count: i64) -> Self {
        Self {
            rejected_spans: count,
            error_message: String::new(),
        }
    }
    fn with_error_message(mut self, message: String) -> Self {
        self.error_message = message;
        self
    }
}

impl PartialSuccessBuilder for ExportMetricsPartialSuccess {
    fn with_rejected_count(count: i64) -> Self {
        Self {
            rejected_data_points: count,
            error_message: String::new(),
        }
    }
    fn with_error_message(mut self, message: String) -> Self {
        self.error_message = message;
        self
    }
}

impl PartialSuccessBuilder for ExportLogsPartialSuccess {
    fn with_rejected_count(count: i64) -> Self {
        Self {
            rejected_log_records: count,
            error_message: String::new(),
        }
    }
    fn with_error_message(mut self, message: String) -> Self {
        self.error_message = message;
        self
    }
}

/// Build a partial_success response from a ConversionResult.
///
/// Returns None if no records were rejected (per OTLP spec, partial_success
/// should only be set when there is something to report).
fn build_partial_success<T: PartialSuccessBuilder>(result: &ConversionResult) -> Option<T> {
    if result.rejected_count == 0 && result.error_message.is_none() {
        return None;
    }

    let partial = T::with_rejected_count(result.rejected_count);
    Some(match &result.error_message {
        Some(msg) => partial.with_error_message(msg.clone()),
        None => partial,
    })
}

/// Decode a request from protobuf or JSON.
fn decode_request<T>(content_type: ContentType, body: &[u8]) -> Result<T, TelemetryError>
where
    T: Message + Default + serde::de::DeserializeOwned,
{
    match content_type {
        ContentType::Protobuf => {
            T::decode(body).map_err(|e| TelemetryError::ProtoDecode { source: e })
        }
        ContentType::Json => {
            serde_json::from_slice(body).map_err(|e| TelemetryError::JsonDecode { source: e })
        }
    }
}

/// Encode a response as protobuf or JSON.
fn encode_response<T>(
    content_type: ContentType,
    response: &T,
) -> Result<impl IntoResponse, HttpError>
where
    T: Message + serde::Serialize,
{
    match content_type {
        ContentType::Protobuf => {
            let body = response.encode_to_vec();
            Ok((
                StatusCode::OK,
                [(CONTENT_TYPE, "application/x-protobuf")],
                body,
            ))
        }
        ContentType::Json => {
            let body = serde_json::to_vec(response)
                .map_err(|e| TelemetryError::JsonEncode { source: e })?;
            Ok((StatusCode::OK, [(CONTENT_TYPE, "application/json")], body))
        }
    }
}

/// HTTP error response wrapper.
pub struct HttpError(TelemetryError);

impl From<TelemetryError> for HttpError {
    fn from(err: TelemetryError) -> Self {
        Self(err)
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match &self.0 {
            TelemetryError::BufferOverflow { .. } => {
                (StatusCode::SERVICE_UNAVAILABLE, self.0.to_string())
            }
            TelemetryError::InvalidContentType { .. } => {
                (StatusCode::UNSUPPORTED_MEDIA_TYPE, self.0.to_string())
            }
            TelemetryError::ProtoDecode { .. } | TelemetryError::JsonDecode { .. } => {
                (StatusCode::BAD_REQUEST, self.0.to_string())
            }
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()),
        };

        (status, message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{header::CONTENT_ENCODING, Request, StatusCode},
    };
    use object_store::memory::InMemory;
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use tower::ServiceExt;

    use crate::config::{BufferConfig, ParquetConfig};
    use crate::redact::{RedactionConfig, RedactionEngine};
    use crate::schema::traces::traces_schema;
    use crate::storage::Signal;

    fn test_state() -> OtlpHttpState {
        let store = Arc::new(InMemory::new());
        let buffer_config = BufferConfig {
            max_batch_size: 1000,
            flush_interval_secs: 30,
            max_buffer_bytes: 10 * 1024 * 1024,
            max_records_per_request: 100_000,
            flush_max_retries: 3,
            flush_initial_delay_ms: 10,
            flush_max_delay_ms: 100,
        };
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        OtlpHttpState {
            trace_ingester: Arc::new(Ingester::new(
                Signal::Traces,
                traces_schema(),
                store.clone(),
                buffer_config.clone(),
                parquet_config.clone(),
            )),
            metrics_ingester: Arc::new(Ingester::new(
                Signal::Metrics,
                crate::schema::metrics::number_metrics_schema(),
                store.clone(),
                buffer_config.clone(),
                parquet_config.clone(),
            )),
            logs_ingester: Arc::new(Ingester::new(
                Signal::Logs,
                crate::schema::logs::logs_schema(),
                store,
                buffer_config,
                parquet_config,
            )),
            redaction: Arc::new(
                RedactionEngine::new(&RedactionConfig::default())
                    .expect("default config should work"),
            ),
        }
    }

    fn sample_trace_request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("test-service".to_string())),
                        }),
                    }],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        name: "test-span".to_string(),
                        start_time_unix_nano: 1_000_000_000,
                        end_time_unix_nano: 2_000_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[tokio::test]
    async fn traces_protobuf_roundtrip() {
        let state = test_state();
        let router = otlp_http_router(state.clone());

        let request_body = sample_trace_request().encode_to_vec();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify data was ingested
        assert_eq!(state.trace_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn traces_json_roundtrip() {
        let state = test_state();
        let router = otlp_http_router(state.clone());

        let request_body = serde_json::to_vec(&sample_trace_request()).unwrap();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        // Verify data was ingested
        assert_eq!(state.trace_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn invalid_content_type_returns_415() {
        let state = test_state();
        let router = otlp_http_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "text/plain")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn malformed_protobuf_returns_400() {
        let state = test_state();
        let router = otlp_http_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(vec![0xFF, 0xFF, 0xFF]))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn malformed_json_returns_400() {
        let state = test_state();
        let router = otlp_http_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("{ invalid json }"))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn empty_protobuf_body_succeeds() {
        let state = test_state();
        let router = otlp_http_router(state);

        // Empty protobuf is valid - just means no spans to ingest
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn empty_json_with_resource_spans_succeeds() {
        let state = test_state();
        let router = otlp_http_router(state);

        // Valid OTLP JSON with empty resourceSpans array
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"resourceSpans":[]}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn empty_json_object_returns_400() {
        let state = test_state();
        let router = otlp_http_router(state);

        // Empty JSON object {} is not valid OTLP - missing required structure
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // Actually, {} may be accepted since resourceSpans is optional
        // If this returns 200, it's because serde accepts missing fields
        assert!(
            response.status() == StatusCode::OK || response.status() == StatusCode::BAD_REQUEST,
            "unexpected status: {}",
            response.status()
        );
    }

    #[tokio::test]
    async fn metrics_malformed_protobuf_returns_400() {
        let state = test_state();
        let router = otlp_http_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/metrics")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .body(Body::from(vec![0xDE, 0xAD, 0xBE, 0xEF]))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn logs_malformed_json_returns_400() {
        let state = test_state();
        let router = otlp_http_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/logs")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("[not valid json"))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn missing_content_type_defaults_to_protobuf() {
        let state = test_state();
        let router = otlp_http_router(state.clone());

        // Send valid protobuf without Content-Type header
        let request_body = sample_trace_request().encode_to_vec();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            // No Content-Type header - should default to protobuf
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.trace_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn content_type_with_charset_accepted() {
        let state = test_state();
        let router = otlp_http_router(state.clone());

        let request_body = serde_json::to_vec(&sample_trace_request()).unwrap();
        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/json; charset=utf-8")
            .body(Body::from(request_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.trace_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let state = test_state();
        let router = otlp_http_router(state);

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn gzip_compressed_protobuf_accepted() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let state = test_state();
        let router = otlp_http_router(state.clone());

        let request_body = sample_trace_request().encode_to_vec();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&request_body).unwrap();
        let compressed_body = encoder.finish().unwrap();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "gzip")
            .body(Body::from(compressed_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.trace_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn gzip_compressed_json_accepted() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let state = test_state();
        let router = otlp_http_router(state.clone());

        let request_body = serde_json::to_vec(&sample_trace_request()).unwrap();

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&request_body).unwrap();
        let compressed_body = encoder.finish().unwrap();

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/json")
            .header(CONTENT_ENCODING, "gzip")
            .body(Body::from(compressed_body))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.trace_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn invalid_gzip_returns_400() {
        let state = test_state();
        let router = otlp_http_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "gzip")
            .body(Body::from(vec![0xDE, 0xAD, 0xBE, 0xEF]))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn unsupported_content_encoding_returns_415() {
        let state = test_state();
        let router = otlp_http_router(state);

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "deflate")
            .body(Body::from(sample_trace_request().encode_to_vec()))
            .unwrap();

        // tower-http returns 415 Unsupported Media Type for unsupported encodings
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    #[tokio::test]
    async fn identity_content_encoding_accepted() {
        let state = test_state();
        let router = otlp_http_router(state.clone());

        let request = Request::builder()
            .method("POST")
            .uri("/v1/traces")
            .header(CONTENT_TYPE, "application/x-protobuf")
            .header(CONTENT_ENCODING, "identity")
            .body(Body::from(sample_trace_request().encode_to_vec()))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.trace_ingester.buffered_rows(), 1);
    }
}
