//! HTTP query API.
//!
//! Provides axum routes for querying telemetry data via SQL or structured
//! query builders, returning results as JSON or Arrow IPC.

#![allow(clippy::needless_pass_by_value)]

use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use super::builders::{LogQueryBuilder, MetricQueryBuilder, TraceQueryBuilder};
use super::QueryEngine;
use crate::TelemetryError;

/// Default row limit for queries without explicit LIMIT clause.
///
/// Prevents unbounded memory usage from queries like `SELECT * FROM traces`.
/// Users can specify higher limits explicitly, up to `MAX_ROW_LIMIT`.
pub const DEFAULT_ROW_LIMIT: usize = 10_000;

/// Maximum row limit for any query.
///
/// Even with explicit LIMIT, queries cannot exceed this value.
/// This prevents accidental memory exhaustion from large result sets.
pub const MAX_ROW_LIMIT: usize = 100_000;

/// Query API state.
#[derive(Clone)]
pub struct QueryApiState {
    pub engine: Arc<QueryEngine>,
    /// Default row limit (defaults to `DEFAULT_ROW_LIMIT`).
    pub default_row_limit: usize,
    /// Maximum row limit (defaults to `MAX_ROW_LIMIT`).
    pub max_row_limit: usize,
}

impl QueryApiState {
    /// Create a new query API state with default limits.
    pub const fn new(engine: Arc<QueryEngine>) -> Self {
        Self {
            engine,
            default_row_limit: DEFAULT_ROW_LIMIT,
            max_row_limit: MAX_ROW_LIMIT,
        }
    }

    /// Create with custom row limits.
    pub const fn with_limits(
        engine: Arc<QueryEngine>,
        default_row_limit: usize,
        max_row_limit: usize,
    ) -> Self {
        Self {
            engine,
            default_row_limit,
            max_row_limit,
        }
    }
}

/// Create the query API router.
pub fn query_router(state: QueryApiState) -> Router {
    Router::new()
        .route("/health", axum::routing::get(handle_health))
        .route("/ready", axum::routing::get(handle_ready))
        .route("/sql", post(handle_sql_query))
        .route("/traces", post(handle_traces_query))
        .route("/metrics", post(handle_metrics_query))
        .route("/logs", post(handle_logs_query))
        .with_state(state)
}

/// Health check response.
#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: &'static str,
}

/// Handle GET /health - liveness probe
async fn handle_health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Handle GET /ready - readiness probe
async fn handle_ready(
    State(state): State<QueryApiState>,
) -> Result<Json<HealthResponse>, QueryError> {
    // Check if we can execute a simple query
    state
        .engine
        .query("SELECT 1")
        .await
        .map_err(|e| QueryError::Internal(format!("readiness check failed: {e}")))?;

    Ok(Json(HealthResponse {
        status: "ready",
        version: env!("CARGO_PKG_VERSION"),
    }))
}

/// Request body for SQL queries.
#[derive(Debug, Deserialize)]
pub struct SqlQueryRequest {
    /// The SQL query to execute.
    pub sql: String,
    /// Response format: "json" (default) or "arrow".
    #[serde(default)]
    pub format: ResponseFormat,
}

/// Request body for trace queries.
#[derive(Debug, Deserialize)]
pub struct TraceQueryRequest {
    /// Filter by service name.
    pub service: Option<String>,
    /// Filter by trace ID (hex-encoded).
    pub trace_id: Option<String>,
    /// Filter by span name.
    pub span_name: Option<String>,
    /// Minimum duration in milliseconds.
    pub min_duration_ms: Option<u64>,
    /// Maximum duration in milliseconds.
    pub max_duration_ms: Option<u64>,
    /// Start time (RFC 3339 format).
    pub start_time: Option<String>,
    /// End time (RFC 3339 format).
    pub end_time: Option<String>,
    /// Only include error spans.
    #[serde(default)]
    pub errors_only: bool,
    /// Maximum number of results.
    pub limit: Option<usize>,
    /// Number of results to skip.
    pub offset: Option<usize>,
    /// Response format.
    #[serde(default)]
    pub format: ResponseFormat,
}

/// Request body for metric queries.
#[derive(Debug, Deserialize)]
pub struct MetricQueryRequest {
    /// Filter by exact metric name.
    pub metric_name: Option<String>,
    /// Filter by metric name pattern (SQL LIKE).
    pub metric_name_like: Option<String>,
    /// Filter by service name.
    pub service: Option<String>,
    /// Start time (RFC 3339 format).
    pub start_time: Option<String>,
    /// End time (RFC 3339 format).
    pub end_time: Option<String>,
    /// Maximum number of results.
    pub limit: Option<usize>,
    /// Number of results to skip.
    pub offset: Option<usize>,
    /// Response format.
    #[serde(default)]
    pub format: ResponseFormat,
}

/// Request body for log queries.
#[derive(Debug, Deserialize)]
pub struct LogQueryRequest {
    /// Filter by service name.
    pub service: Option<String>,
    /// Minimum severity level (1-24).
    pub min_severity: Option<u8>,
    /// Maximum severity level (1-24).
    pub max_severity: Option<u8>,
    /// Filter logs containing this text in body.
    pub body_contains: Option<String>,
    /// Filter by trace ID for correlation.
    pub trace_id: Option<String>,
    /// Start time (RFC 3339 format).
    pub start_time: Option<String>,
    /// End time (RFC 3339 format).
    pub end_time: Option<String>,
    /// Maximum number of results.
    pub limit: Option<usize>,
    /// Number of results to skip.
    pub offset: Option<usize>,
    /// Response format.
    #[serde(default)]
    pub format: ResponseFormat,
}

/// Response format for query results.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ResponseFormat {
    #[default]
    Json,
    Arrow,
}

/// JSON response for query results.
#[derive(Debug, Serialize)]
pub struct QueryResponse {
    /// Number of rows returned.
    pub rows: usize,
    /// Column names.
    pub columns: Vec<String>,
    /// Row data as arrays of JSON values.
    pub data: Vec<Vec<serde_json::Value>>,
}

/// Handle POST /query/sql
///
/// Note: For safety, queries without a LIMIT clause will have a default limit
/// applied. Use explicit LIMIT to control result size.
#[tracing::instrument(skip(state, request), fields(sql_len = request.sql.len()))]
async fn handle_sql_query(
    State(state): State<QueryApiState>,
    Json(request): Json<SqlQueryRequest>,
) -> Result<Response, QueryError> {
    // Check if query already has a LIMIT clause
    let sql = if has_limit_clause(&request.sql) {
        request.sql
    } else {
        // Add default limit to prevent unbounded results
        format!(
            "{} LIMIT {}",
            request.sql.trim_end_matches(';'),
            state.default_row_limit
        )
    };

    let results = state.engine.query(&sql).await?;

    // Enforce maximum row limit even if query specified a higher one
    let results = truncate_results(results, state.max_row_limit);

    format_response(results, request.format)
}

/// Check if a SQL query already contains a LIMIT clause.
fn has_limit_clause(sql: &str) -> bool {
    // Simple check - look for LIMIT keyword (case-insensitive)
    // This is a heuristic; a full SQL parser would be more accurate
    sql.to_uppercase().contains(" LIMIT ")
}

/// Truncate results to a maximum number of rows.
fn truncate_results(batches: Vec<RecordBatch>, max_rows: usize) -> Vec<RecordBatch> {
    let mut total = 0;
    let mut result = Vec::new();

    for batch in batches {
        if total >= max_rows {
            break;
        }

        let remaining = max_rows - total;
        if batch.num_rows() <= remaining {
            total += batch.num_rows();
            result.push(batch);
        } else {
            // Slice the batch to fit within limit
            let sliced = batch.slice(0, remaining);
            total += sliced.num_rows();
            result.push(sliced);
        }
    }

    result
}

/// Handle POST /query/traces
#[tracing::instrument(skip(state, request), fields(service = ?request.service))]
async fn handle_traces_query(
    State(state): State<QueryApiState>,
    Json(request): Json<TraceQueryRequest>,
) -> Result<Response, QueryError> {
    let mut builder = TraceQueryBuilder::new();

    if let Some(ref svc) = request.service {
        builder = builder.service(svc);
    }
    if let Some(ref tid) = request.trace_id {
        builder = builder.trace_id(tid);
    }
    if let Some(ref name) = request.span_name {
        builder = builder.span_name(name);
    }
    if let Some(ms) = request.min_duration_ms {
        builder = builder.min_duration_ms(ms);
    }
    if let Some(ms) = request.max_duration_ms {
        builder = builder.max_duration_ns(ms * 1_000_000);
    }
    if let (Some(ref start), Some(ref end)) = (&request.start_time, &request.end_time) {
        let start = chrono::DateTime::parse_from_rfc3339(start)
            .map_err(|e| QueryError::InvalidRequest(format!("invalid start_time: {e}")))?
            .with_timezone(&chrono::Utc);
        let end = chrono::DateTime::parse_from_rfc3339(end)
            .map_err(|e| QueryError::InvalidRequest(format!("invalid end_time: {e}")))?
            .with_timezone(&chrono::Utc);
        builder = builder.time_range(start, end);
    }
    if request.errors_only {
        builder = builder.errors_only();
    }

    // Apply limit, respecting maximum
    let limit = request
        .limit
        .unwrap_or(state.default_row_limit)
        .min(state.max_row_limit);
    builder = builder.limit(limit);

    if let Some(offset) = request.offset {
        builder = builder.offset(offset);
    }

    let sql = builder.newest_first().build();
    let results = state.engine.query(&sql).await?;
    format_response(results, request.format)
}

/// Handle POST /query/metrics
#[tracing::instrument(skip(state, request), fields(metric_name = ?request.metric_name))]
async fn handle_metrics_query(
    State(state): State<QueryApiState>,
    Json(request): Json<MetricQueryRequest>,
) -> Result<Response, QueryError> {
    let mut builder = MetricQueryBuilder::new();

    if let Some(ref name) = request.metric_name {
        builder = builder.metric_name(name);
    }
    if let Some(ref pattern) = request.metric_name_like {
        builder = builder.metric_name_like(pattern);
    }
    if let Some(ref svc) = request.service {
        builder = builder.service(svc);
    }
    if let (Some(ref start), Some(ref end)) = (&request.start_time, &request.end_time) {
        let start = chrono::DateTime::parse_from_rfc3339(start)
            .map_err(|e| QueryError::InvalidRequest(format!("invalid start_time: {e}")))?
            .with_timezone(&chrono::Utc);
        let end = chrono::DateTime::parse_from_rfc3339(end)
            .map_err(|e| QueryError::InvalidRequest(format!("invalid end_time: {e}")))?
            .with_timezone(&chrono::Utc);
        builder = builder.time_range(start, end);
    }

    // Apply limit, respecting maximum
    let limit = request
        .limit
        .unwrap_or(state.default_row_limit)
        .min(state.max_row_limit);
    builder = builder.limit(limit);

    if let Some(offset) = request.offset {
        builder = builder.offset(offset);
    }

    let sql = builder.build();
    let results = state.engine.query(&sql).await?;
    format_response(results, request.format)
}

/// Handle POST /query/logs
#[tracing::instrument(skip(state, request), fields(service = ?request.service, min_severity = ?request.min_severity))]
async fn handle_logs_query(
    State(state): State<QueryApiState>,
    Json(request): Json<LogQueryRequest>,
) -> Result<Response, QueryError> {
    let mut builder = LogQueryBuilder::new();

    if let Some(ref svc) = request.service {
        builder = builder.service(svc);
    }
    if let Some(severity) = request.min_severity {
        builder = builder.min_severity(severity);
    }
    if let Some(severity) = request.max_severity {
        builder = builder.max_severity(severity);
    }
    if let Some(ref text) = request.body_contains {
        builder = builder.body_contains(text);
    }
    if let Some(ref tid) = request.trace_id {
        builder = builder.trace_id(tid);
    }
    if let (Some(ref start), Some(ref end)) = (&request.start_time, &request.end_time) {
        let start = chrono::DateTime::parse_from_rfc3339(start)
            .map_err(|e| QueryError::InvalidRequest(format!("invalid start_time: {e}")))?
            .with_timezone(&chrono::Utc);
        let end = chrono::DateTime::parse_from_rfc3339(end)
            .map_err(|e| QueryError::InvalidRequest(format!("invalid end_time: {e}")))?
            .with_timezone(&chrono::Utc);
        builder = builder.time_range(start, end);
    }

    // Apply limit, respecting maximum
    let limit = request
        .limit
        .unwrap_or(state.default_row_limit)
        .min(state.max_row_limit);
    builder = builder.limit(limit);

    if let Some(offset) = request.offset {
        builder = builder.offset(offset);
    }

    let sql = builder.build();
    let results = state.engine.query(&sql).await?;
    format_response(results, request.format)
}

/// Format query results based on the requested format.
fn format_response(
    batches: Vec<RecordBatch>,
    format: ResponseFormat,
) -> Result<Response, QueryError> {
    match format {
        ResponseFormat::Json => format_json_response(batches),
        ResponseFormat::Arrow => format_arrow_response(batches),
    }
}

/// Format results as JSON.
fn format_json_response(batches: Vec<RecordBatch>) -> Result<Response, QueryError> {
    if batches.is_empty() {
        let response = QueryResponse {
            rows: 0,
            columns: vec![],
            data: vec![],
        };
        return Ok(Json(response).into_response());
    }

    let Some(first_batch) = batches.first() else {
        return Err(QueryError::Internal("no batches available".to_owned()));
    };
    let schema = first_batch.schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let mut data = Vec::new();
    let mut total_rows = 0;

    for batch in &batches {
        total_rows += batch.num_rows();

        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());

            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = array_value_to_json(column, row_idx);
                row.push(value);
            }

            data.push(row);
        }
    }

    let response = QueryResponse {
        rows: total_rows,
        columns,
        data,
    };

    Ok(Json(response).into_response())
}

/// Format results as Arrow IPC stream.
fn format_arrow_response(batches: Vec<RecordBatch>) -> Result<Response, QueryError> {
    if batches.is_empty() {
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")
            .body(Body::empty())
            .map_err(|e| QueryError::Internal(format!("failed to build response: {e}")));
    }

    let Some(first_batch) = batches.first() else {
        return Err(QueryError::Internal("no batches available".to_owned()));
    };
    let schema = first_batch.schema();
    let mut buffer = Vec::new();

    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)
            .map_err(|e| QueryError::Internal(format!("failed to create Arrow writer: {e}")))?;

        for batch in &batches {
            writer
                .write(batch)
                .map_err(|e| QueryError::Internal(format!("failed to write batch: {e}")))?;
        }

        writer
            .finish()
            .map_err(|e| QueryError::Internal(format!("failed to finish Arrow stream: {e}")))?;
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/vnd.apache.arrow.stream")
        .body(Body::from(buffer))
        .map_err(|e| QueryError::Internal(format!("failed to build response: {e}")))
}

/// Convert an Arrow array value to JSON.
///
/// Returns `Value::Null` if the value is null or if the downcast fails unexpectedly.
#[allow(clippy::wildcard_imports)]
fn array_value_to_json(array: &dyn arrow::array::Array, idx: usize) -> serde_json::Value {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    // Helper macro to safely downcast and extract value
    macro_rules! downcast_value {
        ($array_type:ty, $value_expr:expr) => {
            match array.as_any().downcast_ref::<$array_type>() {
                Some(arr) => $value_expr(arr),
                None => serde_json::Value::Null,
            }
        };
    }

    if array.is_null(idx) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Null => serde_json::Value::Null,
        DataType::Boolean => {
            downcast_value!(BooleanArray, |arr: &BooleanArray| {
                serde_json::Value::Bool(arr.value(idx))
            })
        }
        DataType::Int8 => {
            downcast_value!(Int8Array, |arr: &Int8Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::Int16 => {
            downcast_value!(Int16Array, |arr: &Int16Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::Int32 => {
            downcast_value!(Int32Array, |arr: &Int32Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::Int64 => {
            downcast_value!(Int64Array, |arr: &Int64Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::UInt8 => {
            downcast_value!(UInt8Array, |arr: &UInt8Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::UInt16 => {
            downcast_value!(UInt16Array, |arr: &UInt16Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::UInt32 => {
            downcast_value!(UInt32Array, |arr: &UInt32Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::UInt64 => {
            downcast_value!(UInt64Array, |arr: &UInt64Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::Float32 => {
            downcast_value!(Float32Array, |arr: &Float32Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::Float64 => {
            downcast_value!(Float64Array, |arr: &Float64Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::Utf8 => {
            downcast_value!(StringArray, |arr: &StringArray| {
                serde_json::Value::String(arr.value(idx).to_owned())
            })
        }
        DataType::LargeUtf8 => {
            downcast_value!(LargeStringArray, |arr: &LargeStringArray| {
                serde_json::Value::String(arr.value(idx).to_owned())
            })
        }
        DataType::Binary => {
            downcast_value!(BinaryArray, |arr: &BinaryArray| {
                serde_json::Value::String(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    arr.value(idx),
                ))
            })
        }
        DataType::FixedSizeBinary(_) => {
            downcast_value!(FixedSizeBinaryArray, |arr: &FixedSizeBinaryArray| {
                // Hex-encode trace_id and span_id for readability
                serde_json::Value::String(hex::encode(arr.value(idx)))
            })
        }
        DataType::Date32 => {
            downcast_value!(Date32Array, |arr: &Date32Array| {
                serde_json::json!(arr.value(idx))
            })
        }
        DataType::Timestamp(_, _) => {
            downcast_value!(
                TimestampNanosecondArray,
                |arr: &TimestampNanosecondArray| { serde_json::json!(arr.value(idx)) }
            )
        }
        _ => serde_json::Value::String(format!("<unsupported type: {:?}>", array.data_type())),
    }
}

/// Query API error type.
#[derive(Debug)]
pub enum QueryError {
    InvalidRequest(String),
    Query(TelemetryError),
    Internal(String),
}

impl From<TelemetryError> for QueryError {
    fn from(err: TelemetryError) -> Self {
        Self::Query(err)
    }
}

impl IntoResponse for QueryError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::InvalidRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Self::Query(err) => {
                // Log the full error server-side for debugging
                tracing::error!(error = %err, "query execution failed");
                // Return a sanitised message to the client (avoid leaking internal details)
                let sanitised = match err {
                    TelemetryError::QueryTimeout { duration } => {
                        format!("query timed out after {duration:?}")
                    }
                    TelemetryError::Config(_) => "configuration error".to_owned(),
                    TelemetryError::DataFusion(_) => "query execution error".to_owned(),
                    TelemetryError::Arrow(_) => "data processing error".to_owned(),
                    TelemetryError::Parquet(_) => "storage read error".to_owned(),
                    TelemetryError::ObjectStore(_) => "storage access error".to_owned(),
                    _ => "internal error".to_owned(),
                };
                (StatusCode::INTERNAL_SERVER_ERROR, sanitised)
            }
            Self::Internal(msg) => {
                // Log internal errors but return generic message
                tracing::error!(error = %msg, "internal API error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_owned(),
                )
            }
        };

        let body = serde_json::json!({
            "error": message
        });

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use object_store::memory::InMemory;
    use tower::ServiceExt;

    async fn setup_test_api() -> Router {
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(QueryEngine::new(store, "memory://").await.unwrap());
        let state = QueryApiState::new(engine);
        query_router(state)
    }

    #[tokio::test]
    async fn sql_query_empty_table() {
        let router = setup_test_api().await;

        let request = Request::builder()
            .method("POST")
            .uri("/sql")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"sql": "SELECT * FROM traces LIMIT 10"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn traces_query_empty_table() {
        let router = setup_test_api().await;

        let request = Request::builder()
            .method("POST")
            .uri("/traces")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"service": "test-service", "limit": 10}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_query_empty_table() {
        let router = setup_test_api().await;

        let request = Request::builder()
            .method("POST")
            .uri("/metrics")
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"metric_name": "test.counter", "limit": 10}"#,
            ))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn logs_query_empty_table() {
        let router = setup_test_api().await;

        let request = Request::builder()
            .method("POST")
            .uri("/logs")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"min_severity": 17, "limit": 10}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn invalid_sql_returns_error() {
        let router = setup_test_api().await;

        let request = Request::builder()
            .method("POST")
            .uri("/sql")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"sql": "INVALID SQL QUERY"}"#))
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let router = setup_test_api().await;

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
    }

    #[tokio::test]
    async fn ready_endpoint_returns_ready() {
        let router = setup_test_api().await;

        let request = Request::builder()
            .method("GET")
            .uri("/ready")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ready");
    }
}
