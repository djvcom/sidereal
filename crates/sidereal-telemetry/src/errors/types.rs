//! Core types for error tracking.
//!
//! These types represent aggregated error groups, individual error samples,
//! and statistics for error tracking and analysis.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// An aggregated group of similar errors identified by fingerprint.
///
/// Errors are grouped by their fingerprint, which is computed from the
/// normalised exception type, message, and stacktrace combined with the
/// service name.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorGroup {
    /// SHA-256 fingerprint identifying this error group.
    pub fingerprint: String,

    /// Exception type (e.g., "NullPointerException", "TimeoutError").
    pub error_type: Option<String>,

    /// Error message from the first occurrence.
    pub message: Option<String>,

    /// Service that produced this error.
    pub service_name: String,

    /// Service version when first seen (for deployment correlation).
    pub first_version: Option<String>,

    /// Timestamp of first occurrence.
    pub first_seen: DateTime<Utc>,

    /// Timestamp of most recent occurrence.
    pub last_seen: DateTime<Utc>,

    /// Total number of occurrences.
    pub count: u64,

    /// Number of unique traces affected by this error.
    pub affected_traces: u64,

    /// Sample trace ID for drill-down.
    pub sample_trace_id: Option<TraceId>,

    /// Sample span ID for drill-down.
    pub sample_span_id: Option<SpanId>,
}

/// A single error occurrence with full context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorSample {
    /// Trace ID containing this error.
    pub trace_id: TraceId,

    /// Span ID where error occurred.
    pub span_id: SpanId,

    /// Timestamp of the error.
    pub timestamp: DateTime<Utc>,

    /// Duration of the span in nanoseconds.
    pub duration_ns: u64,

    /// Exception type.
    pub error_type: Option<String>,

    /// Error message.
    pub message: Option<String>,

    /// Full stacktrace.
    pub stacktrace: Option<String>,

    /// Additional attributes from span/log.
    pub attributes: HashMap<String, String>,

    /// Service name.
    pub service_name: String,

    /// Service version.
    pub service_version: Option<String>,

    /// Operation name (span name).
    pub operation: Option<String>,
}

/// Statistics for an error group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorStats {
    /// Total number of errors.
    pub total_errors: u64,

    /// Error rate as a fraction of total requests (0.0 to 1.0).
    pub error_rate: Option<f64>,

    /// Trend direction based on recent activity.
    pub trend: ErrorTrend,

    /// Hourly error counts for sparkline visualisation.
    pub hourly_counts: Vec<u32>,
}

/// Direction of error trend.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorTrend {
    /// Error count is increasing.
    Increasing,
    /// Error count is decreasing.
    Decreasing,
    /// Error count is stable.
    Stable,
}

impl ErrorTrend {
    /// Compute trend from two time periods.
    ///
    /// A change of more than 10% is considered significant.
    #[must_use]
    pub fn from_counts(previous: u64, current: u64) -> Self {
        if previous == 0 && current == 0 {
            return Self::Stable;
        }
        if previous == 0 {
            return Self::Increasing;
        }

        #[allow(clippy::cast_precision_loss, clippy::as_conversions)]
        let change_ratio = current as f64 / previous as f64;

        if change_ratio > 1.1 {
            Self::Increasing
        } else if change_ratio < 0.9 {
            Self::Decreasing
        } else {
            Self::Stable
        }
    }
}

/// Filter criteria for error queries.
#[derive(Debug, Clone, Default)]
pub struct ErrorFilter {
    /// Start of time range (inclusive).
    pub start_time: Option<DateTime<Utc>>,

    /// End of time range (inclusive).
    pub end_time: Option<DateTime<Utc>>,

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
}

impl ErrorFilter {
    /// Create a new filter with the given time range.
    #[must_use]
    pub fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        Self {
            start_time: Some(start_time),
            end_time: Some(end_time),
            ..Default::default()
        }
    }

    /// Filter by service name.
    #[must_use]
    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = Some(service.into());
        self
    }

    /// Filter by environment.
    #[must_use]
    pub fn with_environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = Some(environment.into());
        self
    }

    /// Filter by error type.
    #[must_use]
    pub fn with_error_type(mut self, error_type: impl Into<String>) -> Self {
        self.error_type = Some(error_type.into());
        self
    }

    /// Filter by service version.
    #[must_use]
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Filter by minimum count.
    #[must_use]
    pub const fn with_min_count(mut self, min_count: u64) -> Self {
        self.min_count = Some(min_count);
        self
    }
}

/// Sorting options for error lists.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorSortBy {
    /// Sort by occurrence count (descending).
    #[default]
    Volume,
    /// Sort by first seen timestamp (descending - newest first).
    FirstSeen,
    /// Sort by last seen timestamp (descending - most recent first).
    LastSeen,
    /// Sort by error rate (descending).
    ErrorRate,
}

/// Error source - whether from a span or log entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorSource {
    /// Error detected from a span with status_code = ERROR.
    Span,
    /// Error detected from a log entry.
    Log,
}

/// Trace ID as a 16-byte array.
pub type TraceId = [u8; 16];

/// Span ID as an 8-byte array.
pub type SpanId = [u8; 8];

/// Convert trace ID to hex string.
#[must_use]
pub fn trace_id_to_hex(trace_id: &TraceId) -> String {
    hex::encode(trace_id)
}

/// Convert span ID to hex string.
#[must_use]
pub fn span_id_to_hex(span_id: &SpanId) -> String {
    hex::encode(span_id)
}

/// Parse trace ID from hex string.
pub fn trace_id_from_hex(hex_str: &str) -> Result<TraceId, hex::FromHexError> {
    let bytes = hex::decode(hex_str)?;
    if bytes.len() != 16 {
        return Err(hex::FromHexError::InvalidStringLength);
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

/// Parse span ID from hex string.
pub fn span_id_from_hex(hex_str: &str) -> Result<SpanId, hex::FromHexError> {
    let bytes = hex::decode(hex_str)?;
    if bytes.len() != 8 {
        return Err(hex::FromHexError::InvalidStringLength);
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes);
    Ok(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trend_from_counts_stable() {
        assert_eq!(ErrorTrend::from_counts(100, 100), ErrorTrend::Stable);
        assert_eq!(ErrorTrend::from_counts(100, 105), ErrorTrend::Stable);
        assert_eq!(ErrorTrend::from_counts(100, 95), ErrorTrend::Stable);
    }

    #[test]
    fn trend_from_counts_increasing() {
        assert_eq!(ErrorTrend::from_counts(100, 120), ErrorTrend::Increasing);
        assert_eq!(ErrorTrend::from_counts(0, 10), ErrorTrend::Increasing);
    }

    #[test]
    fn trend_from_counts_decreasing() {
        assert_eq!(ErrorTrend::from_counts(100, 80), ErrorTrend::Decreasing);
    }

    #[test]
    fn trend_from_counts_both_zero() {
        assert_eq!(ErrorTrend::from_counts(0, 0), ErrorTrend::Stable);
    }

    #[test]
    fn trace_id_hex_roundtrip() {
        let original: TraceId = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let hex = trace_id_to_hex(&original);
        let parsed = trace_id_from_hex(&hex).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn span_id_hex_roundtrip() {
        let original: SpanId = [1, 2, 3, 4, 5, 6, 7, 8];
        let hex = span_id_to_hex(&original);
        let parsed = span_id_from_hex(&hex).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn error_filter_builder() {
        let now = Utc::now();
        let filter = ErrorFilter::new(now, now)
            .with_service("api")
            .with_environment("production")
            .with_min_count(10);

        assert_eq!(filter.service, Some("api".to_string()));
        assert_eq!(filter.environment, Some("production".to_string()));
        assert_eq!(filter.min_count, Some(10));
    }
}
