//! HTTP API for error tracking.
//!
//! Provides REST endpoints for listing, viewing, and analysing errors.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::{
    trace_id_to_hex, ErrorAggregator, ErrorFilter, ErrorSortBy, DEFAULT_ERROR_LIMIT,
    DEFAULT_SAMPLE_LIMIT,
};
use crate::TelemetryError;

/// Error tracking API state.
#[derive(Clone)]
pub struct ErrorApiState {
    /// Error aggregator for querying error data.
    pub aggregator: Arc<ErrorAggregator>,
}

impl ErrorApiState {
    /// Create a new error API state.
    pub const fn new(aggregator: Arc<ErrorAggregator>) -> Self {
        Self { aggregator }
    }
}

/// Create the error tracking API router.
///
/// # Endpoints
///
/// - `GET /errors` - List error groups
/// - `GET /errors/stats` - Get error statistics
/// - `GET /errors/compare` - Compare errors between time periods or versions
/// - `GET /errors/:fingerprint` - Get error group details
/// - `GET /errors/:fingerprint/samples` - Get error samples
/// - `GET /errors/:fingerprint/timeline` - Get error timeline
pub fn error_router(state: ErrorApiState) -> Router {
    Router::new()
        .route("/", get(list_errors))
        .route("/stats", get(get_stats))
        .route("/compare", get(compare_errors))
        .route("/:fingerprint", get(get_error_detail))
        .route("/:fingerprint/samples", get(get_error_samples))
        .route("/:fingerprint/timeline", get(get_error_timeline))
        .with_state(state)
}

/// Query parameters for listing errors.
#[derive(Debug, Default, Deserialize)]
pub struct ListErrorsQuery {
    /// Start of time range (RFC 3339 format).
    pub start_time: Option<String>,
    /// End of time range (RFC 3339 format).
    pub end_time: Option<String>,
    /// Filter by service name.
    pub service: Option<String>,
    /// Filter by deployment environment.
    pub environment: Option<String>,
    /// Filter by error type.
    pub error_type: Option<String>,
    /// Filter by service version.
    pub version: Option<String>,
    /// Minimum occurrence count.
    pub min_count: Option<u64>,
    /// Sort order: "volume" (default), "first_seen", "last_seen", "error_rate".
    pub sort_by: Option<String>,
    /// Maximum number of results (default: 1000).
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: Option<usize>,
}

/// Response for listing errors.
#[derive(Debug, Serialize)]
pub struct ListErrorsResponse {
    /// Error groups matching the filter.
    pub errors: Vec<ErrorGroupSummary>,
    /// Total count (may be an estimate for large result sets).
    pub total: usize,
    /// Whether there are more results.
    pub has_more: bool,
}

/// Summary of an error group for list views.
#[derive(Debug, Serialize)]
pub struct ErrorGroupSummary {
    /// SHA-256 fingerprint identifying this error group.
    pub fingerprint: String,
    /// Exception type.
    pub error_type: Option<String>,
    /// Error message (truncated).
    pub message: Option<String>,
    /// Service that produced this error.
    pub service_name: String,
    /// Total number of occurrences.
    pub count: u64,
    /// Number of unique traces affected.
    pub affected_traces: u64,
    /// Timestamp of first occurrence (RFC 3339).
    pub first_seen: String,
    /// Timestamp of most recent occurrence (RFC 3339).
    pub last_seen: String,
    /// Whether this error is new within the query window.
    pub is_new: bool,
    /// Trend direction: "increasing", "decreasing", "stable".
    pub trend: String,
}

/// Handle GET /errors - List error groups.
#[tracing::instrument(skip(state))]
async fn list_errors(
    State(state): State<ErrorApiState>,
    Query(params): Query<ListErrorsQuery>,
) -> Result<Json<ListErrorsResponse>, ErrorApiError> {
    let filter = build_filter(&params)?;
    let sort_by = parse_sort_by(params.sort_by.as_deref());
    let limit = params.limit.unwrap_or(DEFAULT_ERROR_LIMIT);
    let offset = params.offset.unwrap_or(0);

    let groups = state
        .aggregator
        .get_error_groups(&filter, sort_by, limit + 1, offset)
        .await?;

    let has_more = groups.len() > limit;

    let errors: Vec<ErrorGroupSummary> = groups
        .into_iter()
        .take(limit)
        .map(|g| {
            let is_new = filter.start_time.is_some_and(|start| g.first_seen >= start);

            ErrorGroupSummary {
                fingerprint: g.fingerprint,
                error_type: g.error_type,
                message: g.message.map(|m| truncate_message(&m, 200)),
                service_name: g.service_name,
                count: g.count,
                affected_traces: g.affected_traces,
                first_seen: g.first_seen.to_rfc3339(),
                last_seen: g.last_seen.to_rfc3339(),
                is_new,
                trend: "stable".to_owned(),
            }
        })
        .collect();

    let total = errors.len() + offset;

    Ok(Json(ListErrorsResponse {
        errors,
        total,
        has_more,
    }))
}

/// Detailed response for a single error group.
#[derive(Debug, Serialize)]
pub struct ErrorDetailResponse {
    /// SHA-256 fingerprint.
    pub fingerprint: String,
    /// Exception type.
    pub error_type: Option<String>,
    /// Full error message.
    pub message: Option<String>,
    /// Service that produced this error.
    pub service_name: String,
    /// Service version when first seen.
    pub first_version: Option<String>,
    /// Timestamp of first occurrence (RFC 3339).
    pub first_seen: String,
    /// Timestamp of most recent occurrence (RFC 3339).
    pub last_seen: String,
    /// Total number of occurrences.
    pub count: u64,
    /// Number of unique traces affected.
    pub affected_traces: u64,
    /// Sample trace ID for drill-down.
    pub sample_trace_id: Option<String>,
}

/// Handle GET /errors/:fingerprint - Get error details.
#[tracing::instrument(skip(state))]
async fn get_error_detail(
    State(state): State<ErrorApiState>,
    Path(fingerprint): Path<String>,
    Query(params): Query<ListErrorsQuery>,
) -> Result<Json<ErrorDetailResponse>, ErrorApiError> {
    let filter = build_filter(&params)?;

    let groups = state
        .aggregator
        .get_error_groups(&filter, ErrorSortBy::Volume, 1000, 0)
        .await?;

    let group = groups
        .into_iter()
        .find(|g| g.fingerprint == fingerprint)
        .ok_or(ErrorApiError::NotFound)?;

    Ok(Json(ErrorDetailResponse {
        fingerprint: group.fingerprint,
        error_type: group.error_type,
        message: group.message,
        service_name: group.service_name,
        first_version: group.first_version,
        first_seen: group.first_seen.to_rfc3339(),
        last_seen: group.last_seen.to_rfc3339(),
        count: group.count,
        affected_traces: group.affected_traces,
        sample_trace_id: group.sample_trace_id.as_ref().map(trace_id_to_hex),
    }))
}

/// Query parameters for error samples.
#[derive(Debug, Default, Deserialize)]
pub struct SamplesQuery {
    /// Start of time range (RFC 3339 format).
    pub start_time: Option<String>,
    /// End of time range (RFC 3339 format).
    pub end_time: Option<String>,
    /// Filter by service name.
    pub service: Option<String>,
    /// Maximum number of samples (default: 100).
    pub limit: Option<usize>,
}

/// Response for error samples.
#[derive(Debug, Serialize)]
pub struct SamplesResponse {
    /// Individual error occurrences.
    pub samples: Vec<ErrorSampleSummary>,
}

/// Summary of an individual error occurrence.
#[derive(Debug, Serialize)]
pub struct ErrorSampleSummary {
    /// Trace ID (hex-encoded).
    pub trace_id: String,
    /// Span ID (hex-encoded).
    pub span_id: String,
    /// Timestamp (RFC 3339).
    pub timestamp: String,
    /// Duration in milliseconds.
    pub duration_ms: f64,
    /// Exception type.
    pub error_type: Option<String>,
    /// Error message.
    pub message: Option<String>,
    /// Service name.
    pub service_name: String,
    /// Service version.
    pub service_version: Option<String>,
    /// Operation name (span name).
    pub operation: Option<String>,
}

/// Handle GET /errors/:fingerprint/samples - Get error samples.
#[tracing::instrument(skip(state))]
async fn get_error_samples(
    State(state): State<ErrorApiState>,
    Path(fingerprint): Path<String>,
    Query(params): Query<SamplesQuery>,
) -> Result<Json<SamplesResponse>, ErrorApiError> {
    let filter = build_samples_filter(&params)?;
    let limit = params.limit.unwrap_or(DEFAULT_SAMPLE_LIMIT);

    let samples = state
        .aggregator
        .get_error_samples(&fingerprint, &filter, limit)
        .await?;

    let samples: Vec<ErrorSampleSummary> = samples
        .into_iter()
        .map(|s| {
            #[allow(clippy::cast_precision_loss, clippy::as_conversions)]
            let duration_ms = s.duration_ns as f64 / 1_000_000.0;
            ErrorSampleSummary {
                trace_id: hex::encode(s.trace_id),
                span_id: hex::encode(s.span_id),
                timestamp: s.timestamp.to_rfc3339(),
                duration_ms,
                error_type: s.error_type,
                message: s.message,
                service_name: s.service_name,
                service_version: s.service_version,
                operation: s.operation,
            }
        })
        .collect();

    Ok(Json(SamplesResponse { samples }))
}

/// Query parameters for error timeline.
#[derive(Debug, Default, Deserialize)]
pub struct TimelineQuery {
    /// Start of time range (RFC 3339 format).
    pub start_time: Option<String>,
    /// End of time range (RFC 3339 format).
    pub end_time: Option<String>,
    /// Filter by service name.
    pub service: Option<String>,
}

/// Response for error timeline.
#[derive(Debug, Serialize)]
pub struct TimelineResponse {
    /// Hourly error counts.
    pub buckets: Vec<TimelineBucket>,
    /// Time granularity.
    pub granularity: String,
}

/// A single time bucket in the timeline.
#[derive(Debug, Serialize)]
pub struct TimelineBucket {
    /// Bucket timestamp (RFC 3339).
    pub timestamp: String,
    /// Error count in this bucket.
    pub count: u64,
}

/// Handle GET /errors/:fingerprint/timeline - Get error timeline.
#[tracing::instrument(skip(state))]
async fn get_error_timeline(
    State(state): State<ErrorApiState>,
    Path(fingerprint): Path<String>,
    Query(params): Query<TimelineQuery>,
) -> Result<Json<TimelineResponse>, ErrorApiError> {
    let filter = build_timeline_filter(&params)?;

    let timeline = state
        .aggregator
        .get_error_timeline(Some(&fingerprint), &filter)
        .await?;

    let buckets: Vec<TimelineBucket> = timeline
        .into_iter()
        .map(|(ts, count)| TimelineBucket {
            timestamp: ts.to_rfc3339(),
            count,
        })
        .collect();

    Ok(Json(TimelineResponse {
        buckets,
        granularity: "1h".to_owned(),
    }))
}

/// Response for error statistics.
#[derive(Debug, Serialize)]
pub struct StatsResponse {
    /// Total number of errors.
    pub total_errors: u64,
    /// Error rate (if calculable).
    pub error_rate: Option<f64>,
    /// Trend direction.
    pub trend: String,
    /// Hourly counts for sparkline.
    pub hourly_counts: Vec<u32>,
}

/// Handle GET /errors/stats - Get error statistics.
#[tracing::instrument(skip(state))]
async fn get_stats(
    State(state): State<ErrorApiState>,
    Query(params): Query<ListErrorsQuery>,
) -> Result<Json<StatsResponse>, ErrorApiError> {
    let filter = build_filter(&params)?;

    let stats = state.aggregator.get_error_stats(&filter).await?;

    let trend = match stats.trend {
        super::ErrorTrend::Increasing => "increasing",
        super::ErrorTrend::Decreasing => "decreasing",
        super::ErrorTrend::Stable => "stable",
    };

    Ok(Json(StatsResponse {
        total_errors: stats.total_errors,
        error_rate: stats.error_rate,
        trend: trend.to_owned(),
        hourly_counts: stats.hourly_counts,
    }))
}

/// Query parameters for comparing errors between periods or versions.
#[derive(Debug, Default, Deserialize)]
pub struct CompareQuery {
    /// Baseline start time (RFC 3339 format).
    pub baseline_start: Option<String>,
    /// Baseline end time (RFC 3339 format).
    pub baseline_end: Option<String>,
    /// Comparison start time (RFC 3339 format).
    pub comparison_start: Option<String>,
    /// Comparison end time (RFC 3339 format).
    pub comparison_end: Option<String>,
    /// Filter by service name.
    pub service: Option<String>,
    /// Baseline service version (alternative to time-based comparison).
    pub baseline_version: Option<String>,
    /// Comparison service version (alternative to time-based comparison).
    pub comparison_version: Option<String>,
}

/// Response for error comparison.
#[derive(Debug, Serialize)]
pub struct CompareResponse {
    /// Errors only in the comparison period (new errors).
    pub new_errors: Vec<ErrorGroupSummary>,
    /// Errors only in the baseline period (resolved errors).
    pub resolved_errors: Vec<ErrorGroupSummary>,
    /// Errors with increased occurrence count.
    pub increased_errors: Vec<ErrorDeltaSummary>,
    /// Errors with decreased occurrence count.
    pub decreased_errors: Vec<ErrorDeltaSummary>,
    /// Errors with similar occurrence count.
    pub unchanged_count: usize,
}

/// Summary of an error count change between periods.
#[derive(Debug, Serialize)]
pub struct ErrorDeltaSummary {
    /// The error group summary.
    pub error: ErrorGroupSummary,
    /// Count in the baseline period.
    pub baseline_count: u64,
    /// Count in the comparison period.
    pub comparison_count: u64,
    /// Percentage change from baseline.
    pub change_percent: f64,
}

/// Handle GET /errors/compare - Compare errors between time periods or versions.
#[tracing::instrument(skip(state))]
async fn compare_errors(
    State(state): State<ErrorApiState>,
    Query(params): Query<CompareQuery>,
) -> Result<Json<CompareResponse>, ErrorApiError> {
    let baseline_filter = build_compare_filter(
        params.baseline_start.as_deref(),
        params.baseline_end.as_deref(),
        params.service.as_deref(),
        params.baseline_version.as_deref(),
    )?;

    let comparison_filter = build_compare_filter(
        params.comparison_start.as_deref(),
        params.comparison_end.as_deref(),
        params.service.as_deref(),
        params.comparison_version.as_deref(),
    )?;

    let comparison = state
        .aggregator
        .compare_errors(&baseline_filter, &comparison_filter)
        .await?;

    let new_errors: Vec<ErrorGroupSummary> = comparison
        .new_errors
        .into_iter()
        .map(|g| group_to_summary(g, None))
        .collect();

    let resolved_errors: Vec<ErrorGroupSummary> = comparison
        .resolved_errors
        .into_iter()
        .map(|g| group_to_summary(g, None))
        .collect();

    let increased_errors: Vec<ErrorDeltaSummary> = comparison
        .increased_errors
        .into_iter()
        .map(|d| ErrorDeltaSummary {
            error: group_to_summary(d.error, None),
            baseline_count: d.baseline_count,
            comparison_count: d.comparison_count,
            change_percent: d.change_percent,
        })
        .collect();

    let decreased_errors: Vec<ErrorDeltaSummary> = comparison
        .decreased_errors
        .into_iter()
        .map(|d| ErrorDeltaSummary {
            error: group_to_summary(d.error, None),
            baseline_count: d.baseline_count,
            comparison_count: d.comparison_count,
            change_percent: d.change_percent,
        })
        .collect();

    Ok(Json(CompareResponse {
        new_errors,
        resolved_errors,
        increased_errors,
        decreased_errors,
        unchanged_count: comparison.unchanged_errors.len(),
    }))
}

fn build_compare_filter(
    start: Option<&str>,
    end: Option<&str>,
    service: Option<&str>,
    version: Option<&str>,
) -> Result<ErrorFilter, ErrorApiError> {
    let mut filter = ErrorFilter::default();

    if let Some(s) = start {
        filter.start_time = Some(parse_datetime(s)?);
    }
    if let Some(e) = end {
        filter.end_time = Some(parse_datetime(e)?);
    }
    if let Some(svc) = service {
        filter.service = Some(svc.to_owned());
    }
    if let Some(v) = version {
        filter.version = Some(v.to_owned());
    }

    Ok(filter)
}

fn group_to_summary(g: super::ErrorGroup, start_time: Option<DateTime<Utc>>) -> ErrorGroupSummary {
    let is_new = start_time.is_some_and(|start| g.first_seen >= start);

    ErrorGroupSummary {
        fingerprint: g.fingerprint,
        error_type: g.error_type,
        message: g.message.map(|m| truncate_message(&m, 200)),
        service_name: g.service_name,
        count: g.count,
        affected_traces: g.affected_traces,
        first_seen: g.first_seen.to_rfc3339(),
        last_seen: g.last_seen.to_rfc3339(),
        is_new,
        trend: "stable".to_owned(),
    }
}

fn build_filter(params: &ListErrorsQuery) -> Result<ErrorFilter, ErrorApiError> {
    let mut filter = ErrorFilter::default();

    if let Some(ref s) = params.start_time {
        filter.start_time = Some(parse_datetime(s)?);
    }
    if let Some(ref e) = params.end_time {
        filter.end_time = Some(parse_datetime(e)?);
    }
    if let Some(ref svc) = params.service {
        filter.service = Some(svc.clone());
    }
    if let Some(ref env) = params.environment {
        filter.environment = Some(env.clone());
    }
    if let Some(ref et) = params.error_type {
        filter.error_type = Some(et.clone());
    }
    if let Some(ref v) = params.version {
        filter.version = Some(v.clone());
    }
    if let Some(mc) = params.min_count {
        filter.min_count = Some(mc);
    }

    Ok(filter)
}

fn build_samples_filter(params: &SamplesQuery) -> Result<ErrorFilter, ErrorApiError> {
    let mut filter = ErrorFilter::default();

    if let Some(ref s) = params.start_time {
        filter.start_time = Some(parse_datetime(s)?);
    }
    if let Some(ref e) = params.end_time {
        filter.end_time = Some(parse_datetime(e)?);
    }
    if let Some(ref svc) = params.service {
        filter.service = Some(svc.clone());
    }

    Ok(filter)
}

fn build_timeline_filter(params: &TimelineQuery) -> Result<ErrorFilter, ErrorApiError> {
    let mut filter = ErrorFilter::default();

    if let Some(ref s) = params.start_time {
        filter.start_time = Some(parse_datetime(s)?);
    }
    if let Some(ref e) = params.end_time {
        filter.end_time = Some(parse_datetime(e)?);
    }
    if let Some(ref svc) = params.service {
        filter.service = Some(svc.clone());
    }

    Ok(filter)
}

fn parse_datetime(s: &str) -> Result<DateTime<Utc>, ErrorApiError> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| ErrorApiError::InvalidRequest(format!("invalid datetime: {e}")))
}

fn parse_sort_by(s: Option<&str>) -> ErrorSortBy {
    match s {
        Some("first_seen") => ErrorSortBy::FirstSeen,
        Some("last_seen") => ErrorSortBy::LastSeen,
        Some("error_rate") => ErrorSortBy::ErrorRate,
        Some("volume" | _) | None => ErrorSortBy::Volume,
    }
}

fn truncate_message(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_owned()
    } else {
        format!("{}...", &s[..max_len])
    }
}

/// Error type for the error tracking API.
#[derive(Debug)]
pub enum ErrorApiError {
    /// Invalid request parameters.
    InvalidRequest(String),
    /// Requested resource not found.
    NotFound,
    /// Internal error during query execution.
    Query(TelemetryError),
}

impl From<TelemetryError> for ErrorApiError {
    fn from(err: TelemetryError) -> Self {
        Self::Query(err)
    }
}

impl IntoResponse for ErrorApiError {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Self::InvalidRequest(msg) => (StatusCode::BAD_REQUEST, msg.clone()),
            Self::NotFound => (StatusCode::NOT_FOUND, "error group not found".to_owned()),
            Self::Query(err) => {
                tracing::error!(error = %err, "error query execution failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "query execution error".to_owned(),
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

    #[test]
    fn parse_sort_by_default() {
        assert_eq!(parse_sort_by(None), ErrorSortBy::Volume);
    }

    #[test]
    fn parse_sort_by_options() {
        assert_eq!(parse_sort_by(Some("volume")), ErrorSortBy::Volume);
        assert_eq!(parse_sort_by(Some("first_seen")), ErrorSortBy::FirstSeen);
        assert_eq!(parse_sort_by(Some("last_seen")), ErrorSortBy::LastSeen);
        assert_eq!(parse_sort_by(Some("error_rate")), ErrorSortBy::ErrorRate);
        assert_eq!(parse_sort_by(Some("unknown")), ErrorSortBy::Volume);
    }

    #[test]
    fn truncate_message_short() {
        assert_eq!(truncate_message("hello", 10), "hello");
    }

    #[test]
    fn truncate_message_long() {
        let long_msg = "a".repeat(300);
        let truncated = truncate_message(&long_msg, 200);
        assert_eq!(truncated.len(), 203);
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn parse_datetime_valid() {
        let result = parse_datetime("2024-01-15T10:30:00Z");
        assert!(result.is_ok());
    }

    #[test]
    fn parse_datetime_invalid() {
        let result = parse_datetime("not-a-date");
        assert!(result.is_err());
    }

    #[test]
    fn build_filter_empty() {
        let params = ListErrorsQuery::default();
        let filter = build_filter(&params).unwrap();
        assert!(filter.start_time.is_none());
        assert!(filter.service.is_none());
    }

    #[test]
    fn build_filter_with_values() {
        let params = ListErrorsQuery {
            start_time: Some("2024-01-15T00:00:00Z".to_string()),
            service: Some("api-server".to_string()),
            min_count: Some(5),
            ..Default::default()
        };
        let filter = build_filter(&params).unwrap();
        assert!(filter.start_time.is_some());
        assert_eq!(filter.service, Some("api-server".to_string()));
        assert_eq!(filter.min_count, Some(5));
    }
}
