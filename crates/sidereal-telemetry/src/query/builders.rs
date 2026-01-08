//! Type-safe query builders for telemetry queries.
//!
//! Provides ergonomic builders for common trace, metric, and log queries
//! that generate optimised SQL with appropriate filters.

#![allow(clippy::format_push_string)]

use chrono::{DateTime, Timelike, Utc};

/// Convert a timestamp to a partition date string (YYYY-MM-DD).
fn partition_date(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%d").to_string()
}

/// Convert a timestamp to a partition hour string (00-23).
fn partition_hour(ts: DateTime<Utc>) -> String {
    format!("{:02}", ts.hour())
}

/// Generate partition predicates for a time range.
///
/// Returns SQL predicates that enable DataFusion partition pruning.
/// When querying data with `date=YYYY-MM-DD/hour=HH` partitioning,
/// adding explicit date/hour filters allows DataFusion to skip
/// scanning irrelevant partition directories.
fn partition_predicates(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> String {
    let mut predicates = String::new();

    if let Some(s) = start {
        predicates.push_str(&format!(" AND date >= '{}'", partition_date(s)));
        // Only add hour filter if querying a single day
        if let Some(e) = end {
            if partition_date(s) == partition_date(e) {
                // Same day - can add hour bounds
                predicates.push_str(&format!(" AND hour >= '{}'", partition_hour(s)));
            }
        }
    }

    if let Some(e) = end {
        predicates.push_str(&format!(" AND date <= '{}'", partition_date(e)));
        // Only add hour filter if querying a single day
        if let Some(s) = start {
            if partition_date(s) == partition_date(e) {
                // Same day - can add hour bounds
                predicates.push_str(&format!(" AND hour <= '{}'", partition_hour(e)));
            }
        }
    }

    predicates
}

/// Builder for trace queries.
///
/// # Example
///
/// ```
/// use chrono::{Duration, Utc};
/// use sidereal_telemetry::query::TraceQueryBuilder;
///
/// let end = Utc::now();
/// let start = end - Duration::hours(1);
///
/// let sql = TraceQueryBuilder::new()
///     .service("my-service")
///     .time_range(start, end)
///     .min_duration_ms(100)
///     .limit(100)
///     .build();
///
/// assert!(sql.contains("service.name"));
/// ```
#[derive(Debug, Clone, Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct TraceQueryBuilder {
    service_name: Option<String>,
    trace_id: Option<String>,
    span_name: Option<String>,
    min_duration_ns: Option<u64>,
    max_duration_ns: Option<u64>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    status_error_only: bool,
    limit: Option<usize>,
    offset: Option<usize>,
    order_by: Option<String>,
    order_desc: bool,
}

impl TraceQueryBuilder {
    /// Create a new trace query builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by service name.
    pub fn service(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Filter by trace ID (hex-encoded).
    pub fn trace_id(mut self, id: impl Into<String>) -> Self {
        self.trace_id = Some(id.into());
        self
    }

    /// Filter by span name (exact match).
    pub fn span_name(mut self, name: impl Into<String>) -> Self {
        self.span_name = Some(name.into());
        self
    }

    /// Filter spans with duration >= the given value in nanoseconds.
    pub const fn min_duration_ns(mut self, ns: u64) -> Self {
        self.min_duration_ns = Some(ns);
        self
    }

    /// Filter spans with duration >= the given value in milliseconds.
    pub const fn min_duration_ms(mut self, ms: u64) -> Self {
        self.min_duration_ns = Some(ms * 1_000_000);
        self
    }

    /// Filter spans with duration <= the given value in nanoseconds.
    pub const fn max_duration_ns(mut self, ns: u64) -> Self {
        self.max_duration_ns = Some(ns);
        self
    }

    /// Filter by time range.
    pub const fn time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    /// Filter to only error spans (status_code = 2).
    pub const fn errors_only(mut self) -> Self {
        self.status_error_only = true;
        self
    }

    /// Limit the number of results.
    pub const fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip the first N results.
    pub const fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Order by a column.
    pub fn order_by(mut self, column: impl Into<String>, desc: bool) -> Self {
        self.order_by = Some(column.into());
        self.order_desc = desc;
        self
    }

    /// Order by start time (newest first).
    pub fn newest_first(self) -> Self {
        self.order_by("start_time_unix_nano", true)
    }

    /// Order by duration (slowest first).
    pub fn slowest_first(self) -> Self {
        self.order_by("duration_ns", true)
    }

    /// Build the SQL query string.
    pub fn build(&self) -> String {
        let mut sql = String::from(
            "SELECT trace_id, span_id, parent_span_id, name, \"service.name\", \
             start_time_unix_nano, end_time_unix_nano, duration_ns, kind, status_code \
             FROM traces WHERE 1=1",
        );

        if let Some(ref svc) = self.service_name {
            sql.push_str(&format!(" AND \"service.name\" = '{}'", escape_string(svc)));
        }

        if let Some(ref tid) = self.trace_id {
            if let Some(hex) = validate_hex(tid) {
                sql.push_str(&format!(" AND trace_id = X'{hex}'"));
            }
        }

        if let Some(ref name) = self.span_name {
            sql.push_str(&format!(" AND name = '{}'", escape_string(name)));
        }

        if let Some(min) = self.min_duration_ns {
            sql.push_str(&format!(" AND duration_ns >= {min}"));
        }

        if let Some(max) = self.max_duration_ns {
            sql.push_str(&format!(" AND duration_ns <= {max}"));
        }

        if let Some(start) = self.start_time {
            #[allow(clippy::cast_sign_loss, clippy::as_conversions)]
            let nanos = start.timestamp_nanos_opt().unwrap_or(0) as u64;
            sql.push_str(&format!(" AND start_time_unix_nano >= {nanos}"));
        }

        if let Some(end) = self.end_time {
            #[allow(clippy::cast_sign_loss, clippy::as_conversions)]
            let nanos = end.timestamp_nanos_opt().unwrap_or(0) as u64;
            sql.push_str(&format!(" AND start_time_unix_nano <= {nanos}"));
        }

        // Add partition predicates for DataFusion partition pruning
        sql.push_str(&partition_predicates(self.start_time, self.end_time));

        if self.status_error_only {
            sql.push_str(" AND status_code = 2");
        }

        if let Some(ref col) = self.order_by {
            if let Some(valid_col) = validate_identifier(col) {
                sql.push_str(&format!(
                    " ORDER BY {} {}",
                    valid_col,
                    if self.order_desc { "DESC" } else { "ASC" }
                ));
            }
        }

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        sql
    }
}

/// Builder for metric queries.
#[derive(Debug, Clone, Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct MetricQueryBuilder {
    metric_name: Option<String>,
    metric_name_pattern: Option<String>,
    service_name: Option<String>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl MetricQueryBuilder {
    /// Create a new metric query builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by exact metric name.
    pub fn metric_name(mut self, name: impl Into<String>) -> Self {
        self.metric_name = Some(name.into());
        self
    }

    /// Filter by metric name pattern (SQL LIKE).
    pub fn metric_name_like(mut self, pattern: impl Into<String>) -> Self {
        self.metric_name_pattern = Some(pattern.into());
        self
    }

    /// Filter by service name.
    pub fn service(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Filter by time range.
    pub const fn time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    /// Limit the number of results.
    pub const fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip the first N results.
    pub const fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Build the SQL query string.
    pub fn build(&self) -> String {
        let mut sql = String::from(
            "SELECT metric_name, metric_type, time_unix_nano, value_int, value_double, \
             \"service.name\" FROM metrics WHERE 1=1",
        );

        if let Some(ref name) = self.metric_name {
            sql.push_str(&format!(" AND metric_name = '{}'", escape_string(name)));
        }

        if let Some(ref pattern) = self.metric_name_pattern {
            sql.push_str(&format!(
                " AND metric_name LIKE '{}'",
                escape_string(pattern)
            ));
        }

        if let Some(ref svc) = self.service_name {
            sql.push_str(&format!(" AND \"service.name\" = '{}'", escape_string(svc)));
        }

        if let Some(start) = self.start_time {
            #[allow(clippy::cast_sign_loss, clippy::as_conversions)]
            let nanos = start.timestamp_nanos_opt().unwrap_or(0) as u64;
            sql.push_str(&format!(" AND time_unix_nano >= {nanos}"));
        }

        if let Some(end) = self.end_time {
            #[allow(clippy::cast_sign_loss, clippy::as_conversions)]
            let nanos = end.timestamp_nanos_opt().unwrap_or(0) as u64;
            sql.push_str(&format!(" AND time_unix_nano <= {nanos}"));
        }

        // Add partition predicates for DataFusion partition pruning
        sql.push_str(&partition_predicates(self.start_time, self.end_time));

        sql.push_str(" ORDER BY time_unix_nano DESC");

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        sql
    }
}

/// Builder for log queries.
#[derive(Debug, Clone, Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct LogQueryBuilder {
    service_name: Option<String>,
    min_severity: Option<u8>,
    max_severity: Option<u8>,
    body_contains: Option<String>,
    trace_id: Option<String>,
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    limit: Option<usize>,
    offset: Option<usize>,
}

impl LogQueryBuilder {
    /// Create a new log query builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by service name.
    pub fn service(mut self, name: impl Into<String>) -> Self {
        self.service_name = Some(name.into());
        self
    }

    /// Filter logs with severity >= the given level.
    ///
    /// Severity numbers: 1-4 (TRACE), 5-8 (DEBUG), 9-12 (INFO),
    /// 13-16 (WARN), 17-20 (ERROR), 21-24 (FATAL).
    pub const fn min_severity(mut self, severity: u8) -> Self {
        self.min_severity = Some(severity);
        self
    }

    /// Filter logs with severity <= the given level.
    pub const fn max_severity(mut self, severity: u8) -> Self {
        self.max_severity = Some(severity);
        self
    }

    /// Filter to WARN level and above (severity >= 13).
    pub const fn warnings_and_above(self) -> Self {
        self.min_severity(13)
    }

    /// Filter to ERROR level and above (severity >= 17).
    pub const fn errors_and_above(self) -> Self {
        self.min_severity(17)
    }

    /// Filter logs containing the given text in body.
    pub fn body_contains(mut self, text: impl Into<String>) -> Self {
        self.body_contains = Some(text.into());
        self
    }

    /// Filter logs associated with a trace ID.
    pub fn trace_id(mut self, id: impl Into<String>) -> Self {
        self.trace_id = Some(id.into());
        self
    }

    /// Filter by time range.
    pub const fn time_range(mut self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        self.start_time = Some(start);
        self.end_time = Some(end);
        self
    }

    /// Limit the number of results.
    pub const fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Skip the first N results.
    pub const fn offset(mut self, n: usize) -> Self {
        self.offset = Some(n);
        self
    }

    /// Build the SQL query string.
    pub fn build(&self) -> String {
        let mut sql = String::from(
            "SELECT time_unix_nano, severity_number, severity_text, body_string, \
             trace_id, span_id, \"service.name\" FROM logs WHERE 1=1",
        );

        if let Some(ref svc) = self.service_name {
            sql.push_str(&format!(" AND \"service.name\" = '{}'", escape_string(svc)));
        }

        if let Some(min) = self.min_severity {
            sql.push_str(&format!(" AND severity_number >= {min}"));
        }

        if let Some(max) = self.max_severity {
            sql.push_str(&format!(" AND severity_number <= {max}"));
        }

        if let Some(ref text) = self.body_contains {
            sql.push_str(&format!(
                " AND body_string LIKE '%{}%' ESCAPE '\\'",
                escape_like_literal(text)
            ));
        }

        if let Some(ref tid) = self.trace_id {
            if let Some(hex) = validate_hex(tid) {
                sql.push_str(&format!(" AND trace_id = X'{hex}'"));
            }
        }

        if let Some(start) = self.start_time {
            #[allow(clippy::cast_sign_loss, clippy::as_conversions)]
            let nanos = start.timestamp_nanos_opt().unwrap_or(0) as u64;
            sql.push_str(&format!(" AND time_unix_nano >= {nanos}"));
        }

        if let Some(end) = self.end_time {
            #[allow(clippy::cast_sign_loss, clippy::as_conversions)]
            let nanos = end.timestamp_nanos_opt().unwrap_or(0) as u64;
            sql.push_str(&format!(" AND time_unix_nano <= {nanos}"));
        }

        // Add partition predicates for DataFusion partition pruning
        sql.push_str(&partition_predicates(self.start_time, self.end_time));

        sql.push_str(" ORDER BY time_unix_nano DESC");

        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }

        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {offset}"));
        }

        sql
    }
}

/// Escape a string literal for use in SQL.
///
/// Handles:
/// - Single quotes (') → ''
/// - Backslashes (\) → \\
/// - Null bytes → rejected (returns sanitised string)
fn escape_string(s: &str) -> String {
    s.chars()
        .filter(|&c| c != '\0') // Remove null bytes
        .flat_map(|c| match c {
            '\'' => vec!['\'', '\''],
            '\\' => vec!['\\', '\\'],
            _ => vec![c],
        })
        .collect()
}

/// Escape a string for use in a LIKE pattern, escaping wildcards.
///
/// Use this when the user input should be matched literally, not as a pattern.
/// Escapes: % → \%, _ → \_
fn escape_like_literal(s: &str) -> String {
    let escaped = escape_string(s);
    escaped.replace('%', "\\%").replace('_', "\\_")
}

/// Validate and sanitise a column identifier.
///
/// Only allows alphanumeric characters and underscores.
/// Returns None if the identifier contains invalid characters.
fn validate_identifier(s: &str) -> Option<&str> {
    if s.is_empty() {
        return None;
    }
    if s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        Some(s)
    } else {
        None
    }
}

/// Validate a hex string (for trace_id, span_id).
///
/// Returns the validated hex string without any prefix.
fn validate_hex(s: &str) -> Option<String> {
    let hex = s.trim_start_matches("0x").trim_start_matches("0X");
    if hex.is_empty() {
        return None;
    }
    if hex.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(hex.to_lowercase())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn trace_query_basic() {
        let sql = TraceQueryBuilder::new()
            .service("my-service")
            .limit(100)
            .build();

        assert!(sql.contains("\"service.name\" = 'my-service'"));
        assert!(sql.contains("LIMIT 100"));
    }

    #[test]
    fn trace_query_with_duration() {
        let sql = TraceQueryBuilder::new()
            .min_duration_ms(100)
            .slowest_first()
            .build();

        assert!(sql.contains("duration_ns >= 100000000"));
        assert!(sql.contains("ORDER BY duration_ns DESC"));
    }

    #[test]
    fn trace_query_with_time_range() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 2, 0, 0, 0).unwrap();

        let sql = TraceQueryBuilder::new().time_range(start, end).build();

        assert!(sql.contains("start_time_unix_nano >= "));
        assert!(sql.contains("start_time_unix_nano <= "));
        // Verify partition predicates for DataFusion pruning
        assert!(sql.contains("date >= '2024-01-01'"));
        assert!(sql.contains("date <= '2024-01-02'"));
    }

    #[test]
    fn trace_query_same_day_includes_hour_predicates() {
        let start = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 15, 14, 0, 0).unwrap();

        let sql = TraceQueryBuilder::new().time_range(start, end).build();

        // Same day should include hour predicates
        assert!(sql.contains("date >= '2024-01-15'"));
        assert!(sql.contains("date <= '2024-01-15'"));
        assert!(sql.contains("hour >= '10'"));
        assert!(sql.contains("hour <= '14'"));
    }

    #[test]
    fn trace_query_multi_day_excludes_hour_predicates() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 3, 14, 0, 0).unwrap();

        let sql = TraceQueryBuilder::new().time_range(start, end).build();

        // Multi-day should only have date predicates, not hour
        assert!(sql.contains("date >= '2024-01-01'"));
        assert!(sql.contains("date <= '2024-01-03'"));
        assert!(!sql.contains("hour >= '10'"));
        assert!(!sql.contains("hour <= '14'"));
    }

    #[test]
    fn trace_query_errors_only() {
        let sql = TraceQueryBuilder::new().errors_only().build();
        assert!(sql.contains("status_code = 2"));
    }

    #[test]
    fn metric_query_basic() {
        let sql = MetricQueryBuilder::new()
            .metric_name("http.request.duration")
            .service("api-gateway")
            .limit(1000)
            .build();

        assert!(sql.contains("metric_name = 'http.request.duration'"));
        assert!(sql.contains("\"service.name\" = 'api-gateway'"));
        assert!(sql.contains("LIMIT 1000"));
    }

    #[test]
    fn metric_query_with_pattern() {
        let sql = MetricQueryBuilder::new().metric_name_like("http.%").build();

        assert!(sql.contains("metric_name LIKE 'http.%'"));
    }

    #[test]
    fn log_query_basic() {
        let sql = LogQueryBuilder::new()
            .service("worker")
            .errors_and_above()
            .limit(50)
            .build();

        assert!(sql.contains("\"service.name\" = 'worker'"));
        assert!(sql.contains("severity_number >= 17"));
        assert!(sql.contains("LIMIT 50"));
    }

    #[test]
    fn log_query_with_body_search() {
        let sql = LogQueryBuilder::new()
            .body_contains("connection refused")
            .build();

        assert!(sql.contains("body_string LIKE '%connection refused%' ESCAPE '\\'"));
    }

    #[test]
    fn log_query_with_trace_correlation() {
        let sql = LogQueryBuilder::new()
            .trace_id("0xabcdef1234567890abcdef1234567890")
            .build();

        assert!(sql.contains("trace_id = X'abcdef1234567890abcdef1234567890'"));
    }

    #[test]
    fn escape_string_prevents_injection() {
        let sql = TraceQueryBuilder::new()
            .service("test'; DROP TABLE traces; --")
            .build();

        // Should have escaped the single quote
        assert!(sql.contains("test''; DROP TABLE traces; --"));
        assert!(!sql.contains("test'; DROP"));
    }

    #[test]
    fn escape_string_handles_backslashes() {
        let escaped = escape_string("test\\path");
        assert_eq!(escaped, "test\\\\path");
    }

    #[test]
    fn escape_string_removes_null_bytes() {
        let escaped = escape_string("test\0injection");
        assert_eq!(escaped, "testinjection");
    }

    #[test]
    fn escape_like_literal_escapes_wildcards() {
        let escaped = escape_like_literal("100% match_here");
        assert_eq!(escaped, "100\\% match\\_here");
    }

    #[test]
    fn validate_identifier_rejects_special_chars() {
        assert!(validate_identifier("valid_column").is_some());
        assert!(validate_identifier("column123").is_some());
        assert!(validate_identifier("column; DROP TABLE").is_none());
        assert!(validate_identifier("").is_none());
        assert!(validate_identifier("col-name").is_none());
    }

    #[test]
    fn validate_hex_accepts_valid_hex() {
        assert_eq!(validate_hex("abcdef").as_deref(), Some("abcdef"));
        assert_eq!(validate_hex("0xABCDEF").as_deref(), Some("abcdef"));
        assert_eq!(validate_hex("0X123456").as_deref(), Some("123456"));
    }

    #[test]
    fn validate_hex_rejects_invalid_hex() {
        assert!(validate_hex("").is_none());
        assert!(validate_hex("0x").is_none());
        assert!(validate_hex("ghijk").is_none());
        assert!(validate_hex("abc; DROP TABLE").is_none());
    }

    #[test]
    fn invalid_trace_id_is_ignored() {
        let sql = TraceQueryBuilder::new()
            .trace_id("invalid; DROP TABLE")
            .build();

        // Should not contain any trace_id filter since it's invalid
        assert!(!sql.contains("trace_id = X'"));
    }

    #[test]
    fn invalid_order_by_is_ignored() {
        let sql = TraceQueryBuilder::new()
            .order_by("column; DROP TABLE", true)
            .build();

        // Should not contain any ORDER BY since the column is invalid
        assert!(!sql.contains("ORDER BY"));
    }

    #[test]
    fn body_contains_with_wildcards_escapes_them() {
        let sql = LogQueryBuilder::new().body_contains("50% complete").build();

        // The % should be escaped so it's matched literally
        assert!(sql.contains("50\\% complete"));
    }

    // Property-based tests for SQL injection prevention
    mod proptest_sql_injection {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            /// Verify that arbitrary strings never produce unescaped single quotes
            #[test]
            fn escape_string_never_leaves_unescaped_quotes(s in ".*") {
                let escaped = escape_string(&s);
                // Count quotes - should always be even (all escaped as '')
                let quote_count = escaped.matches('\'').count();
                prop_assert!(quote_count % 2 == 0, "unescaped quote in: {}", escaped);
            }

            /// Verify escape_string never contains null bytes
            #[test]
            fn escape_string_never_contains_null(s in ".*") {
                let escaped = escape_string(&s);
                prop_assert!(!escaped.contains('\0'), "null byte found in: {:?}", escaped);
            }

            /// Verify arbitrary service names produce valid SQL
            #[test]
            fn trace_query_with_arbitrary_service(service in ".*") {
                let sql = TraceQueryBuilder::new()
                    .service(&service)
                    .build();
                // Should contain the service filter (escaped)
                prop_assert!(sql.contains("\"service.name\" = '"));
                // Should never contain unescaped injection attempts
                prop_assert!(!sql.contains("'; DROP"));
                prop_assert!(!sql.contains("'; --"));
            }

            /// Verify arbitrary span names produce valid SQL
            #[test]
            fn trace_query_with_arbitrary_span_name(name in ".*") {
                let sql = TraceQueryBuilder::new()
                    .span_name(&name)
                    .build();
                prop_assert!(sql.contains("name = '") || sql.contains("name LIKE '"));
            }

            /// Verify arbitrary metric names produce valid SQL
            #[test]
            fn metric_query_with_arbitrary_metric_name(name in ".*") {
                let sql = MetricQueryBuilder::new()
                    .metric_name(&name)
                    .build();
                prop_assert!(sql.contains("metric_name = '"));
            }

            /// Verify arbitrary body search produces valid SQL
            #[test]
            fn log_query_with_arbitrary_body_contains(text in ".+") {
                // Note: Using .+ instead of .* to require at least one character
                // since empty strings don't produce a LIKE clause
                let sql = LogQueryBuilder::new()
                    .body_contains(&text)
                    .build();
                prop_assert!(sql.contains("body_string LIKE '%"));
                prop_assert!(sql.contains("ESCAPE '\\'"));
            }

            /// Verify LIKE pattern special chars are escaped in body search
            #[test]
            fn body_contains_escapes_like_wildcards(text in "[%_\\\\]+") {
                let sql = LogQueryBuilder::new()
                    .body_contains(&text)
                    .build();
                // Check that LIKE wildcards are escaped
                let escaped = escape_like_literal(&text);
                prop_assert!(sql.contains(&escaped));
            }

            /// Verify hex validation rejects all non-hex
            #[test]
            fn validate_hex_rejects_non_hex(s in "[g-z!@#$%^&*()]+") {
                let result = validate_hex(&s);
                prop_assert!(result.is_none(), "accepted non-hex: {}", s);
            }

            /// Verify identifier validation rejects special chars
            #[test]
            fn validate_identifier_rejects_special(s in ".*[;\"'`\\-\\+\\*\\/].*") {
                let result = validate_identifier(&s);
                prop_assert!(result.is_none(), "accepted invalid identifier: {}", s);
            }
        }
    }
}
