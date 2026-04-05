//! SQL query builders for error tracking.
//!
//! Generates SQL queries for error detection, aggregation, and analysis
//! using the custom UDFs registered in the query engine.

#![allow(clippy::format_push_string)]

use chrono::{DateTime, Timelike, Utc};

use super::{ErrorFilter, ErrorSortBy, SPAN_ERROR_CONDITION};

/// Generate partition predicates for DataFusion partition pruning.
fn partition_predicates(start: Option<DateTime<Utc>>, end: Option<DateTime<Utc>>) -> String {
    let mut predicates = String::new();

    if let Some(s) = start {
        let date = s.format("%Y-%m-%d");
        predicates.push_str(&format!(" AND date >= '{date}'"));

        if let Some(e) = end {
            let start_date = s.format("%Y-%m-%d").to_string();
            let end_date = e.format("%Y-%m-%d").to_string();
            if start_date == end_date {
                predicates.push_str(&format!(" AND hour >= '{:02}'", s.hour()));
            }
        }
    }

    if let Some(e) = end {
        let date = e.format("%Y-%m-%d");
        predicates.push_str(&format!(" AND date <= '{date}'"));

        if let Some(s) = start {
            let start_date = s.format("%Y-%m-%d").to_string();
            let end_date = e.format("%Y-%m-%d").to_string();
            if start_date == end_date {
                predicates.push_str(&format!(" AND hour <= '{:02}'", e.hour()));
            }
        }
    }

    predicates
}

/// Escape a string literal for SQL.
fn escape_string(s: &str) -> String {
    s.chars()
        .filter(|&c| c != '\0')
        .flat_map(|c| match c {
            '\'' => vec!['\'', '\''],
            '\\' => vec!['\\', '\\'],
            _ => vec![c],
        })
        .collect()
}

/// Default limit for error queries to prevent unbounded results.
pub const DEFAULT_ERROR_LIMIT: usize = 1000;

/// Default limit for error samples.
pub const DEFAULT_SAMPLE_LIMIT: usize = 100;

/// Builder for error aggregation queries.
///
/// Generates SQL that uses the `error_fingerprint` UDF to group similar errors.
#[derive(Debug, Clone, Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ErrorAggregationQueryBuilder {
    filter: ErrorFilter,
    sort_by: ErrorSortBy,
    limit: usize,
    offset: usize,
}

impl ErrorAggregationQueryBuilder {
    /// Create a new builder with the given filter.
    pub const fn new(filter: ErrorFilter) -> Self {
        Self {
            filter,
            sort_by: ErrorSortBy::Volume,
            limit: DEFAULT_ERROR_LIMIT,
            offset: 0,
        }
    }

    /// Set the sort order.
    pub const fn sort_by(mut self, sort: ErrorSortBy) -> Self {
        self.sort_by = sort;
        self
    }

    /// Set the result limit.
    pub const fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    /// Set the result offset.
    pub const fn offset(mut self, n: usize) -> Self {
        self.offset = n;
        self
    }

    /// Build the SQL query for aggregating errors by fingerprint.
    pub fn build(&self) -> String {
        let mut sql = String::from(
            r#"SELECT
    error_fingerprint("error.type", status_message, NULL, "service.name") AS fingerprint,
    "service.name" AS service_name,
    "service.version" AS first_version,
    "error.type" AS error_type,
    FIRST_VALUE(status_message) AS message,
    COUNT(*) AS count,
    COUNT(DISTINCT trace_id) AS affected_traces,
    MIN(start_time_unix_nano) AS first_seen_nanos,
    MAX(start_time_unix_nano) AS last_seen_nanos,
    FIRST_VALUE(trace_id) AS sample_trace_id,
    FIRST_VALUE(span_id) AS sample_span_id
FROM traces
WHERE "#,
        );

        sql.push_str(SPAN_ERROR_CONDITION);

        self.append_filter_predicates(&mut sql);
        sql.push_str(&partition_predicates(
            self.filter.start_time,
            self.filter.end_time,
        ));

        sql.push_str(
            r"
GROUP BY fingerprint, service_name, first_version, error_type",
        );

        if let Some(min_count) = self.filter.min_count {
            sql.push_str(&format!("\nHAVING COUNT(*) >= {min_count}"));
        }

        let order_clause = match self.sort_by {
            ErrorSortBy::Volume | ErrorSortBy::ErrorRate => "count DESC",
            ErrorSortBy::FirstSeen => "first_seen_nanos DESC",
            ErrorSortBy::LastSeen => "last_seen_nanos DESC",
        };
        sql.push_str(&format!("\nORDER BY {order_clause}"));

        sql.push_str(&format!("\nLIMIT {}", self.limit));

        if self.offset > 0 {
            sql.push_str(&format!("\nOFFSET {}", self.offset));
        }

        sql
    }

    fn append_filter_predicates(&self, sql: &mut String) {
        if let Some(start) = self.filter.start_time {
            if let Some(nanos) = start.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND start_time_unix_nano >= {nanos}"));
            }
        }

        if let Some(end) = self.filter.end_time {
            if let Some(nanos) = end.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND start_time_unix_nano <= {nanos}"));
            }
        }

        if let Some(ref svc) = self.filter.service {
            sql.push_str(&format!(" AND \"service.name\" = '{}'", escape_string(svc)));
        }

        if let Some(ref env) = self.filter.environment {
            sql.push_str(&format!(
                " AND \"deployment.environment.name\" = '{}'",
                escape_string(env)
            ));
        }

        if let Some(ref error_type) = self.filter.error_type {
            sql.push_str(&format!(
                " AND \"error.type\" = '{}'",
                escape_string(error_type)
            ));
        }

        if let Some(ref version) = self.filter.version {
            sql.push_str(&format!(
                " AND \"service.version\" = '{}'",
                escape_string(version)
            ));
        }
    }
}

/// Builder for fetching error samples (individual error occurrences).
#[derive(Debug, Clone)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ErrorSamplesQueryBuilder {
    fingerprint: String,
    filter: ErrorFilter,
    limit: usize,
}

impl ErrorSamplesQueryBuilder {
    /// Create a new builder for fetching samples of a specific error fingerprint.
    pub fn new(fingerprint: impl Into<String>, filter: ErrorFilter) -> Self {
        Self {
            fingerprint: fingerprint.into(),
            filter,
            limit: DEFAULT_SAMPLE_LIMIT,
        }
    }

    /// Set the sample limit.
    pub const fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    /// Build the SQL query.
    pub fn build(&self) -> String {
        let mut sql = String::from(
            r#"SELECT
    trace_id,
    span_id,
    name AS operation,
    "service.name" AS service_name,
    "service.version" AS service_version,
    start_time_unix_nano,
    duration_ns,
    "error.type" AS error_type,
    status_message AS message,
    span_attributes
FROM traces
WHERE "#,
        );

        sql.push_str(SPAN_ERROR_CONDITION);
        sql.push_str(&format!(
            " AND error_fingerprint(\"error.type\", status_message, NULL, \"service.name\") = '{}'",
            escape_string(&self.fingerprint)
        ));

        if let Some(start) = self.filter.start_time {
            if let Some(nanos) = start.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND start_time_unix_nano >= {nanos}"));
            }
        }

        if let Some(end) = self.filter.end_time {
            if let Some(nanos) = end.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND start_time_unix_nano <= {nanos}"));
            }
        }

        if let Some(ref svc) = self.filter.service {
            sql.push_str(&format!(" AND \"service.name\" = '{}'", escape_string(svc)));
        }

        sql.push_str(&partition_predicates(
            self.filter.start_time,
            self.filter.end_time,
        ));

        sql.push_str("\nORDER BY start_time_unix_nano DESC");
        sql.push_str(&format!("\nLIMIT {}", self.limit));

        sql
    }
}

/// Builder for error timeline queries (hourly aggregation for sparklines).
#[derive(Debug, Clone)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ErrorTimelineQueryBuilder {
    fingerprint: Option<String>,
    filter: ErrorFilter,
}

impl ErrorTimelineQueryBuilder {
    /// Create a new timeline builder.
    pub const fn new(filter: ErrorFilter) -> Self {
        Self {
            fingerprint: None,
            filter,
        }
    }

    /// Filter to a specific fingerprint.
    pub fn fingerprint(mut self, fp: impl Into<String>) -> Self {
        self.fingerprint = Some(fp.into());
        self
    }

    /// Build the SQL query.
    pub fn build(&self) -> String {
        let mut sql = String::from(
            r"SELECT
    date_trunc('hour', to_timestamp(start_time_unix_nano / 1000000000)) AS bucket,
    COUNT(*) AS count
FROM traces
WHERE ",
        );

        sql.push_str(SPAN_ERROR_CONDITION);

        if let Some(ref fp) = self.fingerprint {
            sql.push_str(&format!(
                " AND error_fingerprint(\"error.type\", status_message, NULL, \"service.name\") = '{}'",
                escape_string(fp)
            ));
        }

        if let Some(start) = self.filter.start_time {
            if let Some(nanos) = start.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND start_time_unix_nano >= {nanos}"));
            }
        }

        if let Some(end) = self.filter.end_time {
            if let Some(nanos) = end.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND start_time_unix_nano <= {nanos}"));
            }
        }

        if let Some(ref svc) = self.filter.service {
            sql.push_str(&format!(" AND \"service.name\" = '{}'", escape_string(svc)));
        }

        sql.push_str(&partition_predicates(
            self.filter.start_time,
            self.filter.end_time,
        ));

        sql.push_str("\nGROUP BY bucket\nORDER BY bucket ASC");

        sql
    }
}

/// Builder for counting total errors (for computing error rate).
#[derive(Debug, Clone)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ErrorCountQueryBuilder {
    filter: ErrorFilter,
}

impl ErrorCountQueryBuilder {
    /// Create a new count builder.
    pub const fn new(filter: ErrorFilter) -> Self {
        Self { filter }
    }

    /// Build the SQL query.
    pub fn build(&self) -> String {
        let mut sql = String::from(
            r"SELECT
    COUNT(*) AS error_count,
    COUNT(DISTINCT trace_id) AS affected_traces
FROM traces
WHERE ",
        );

        sql.push_str(SPAN_ERROR_CONDITION);

        if let Some(start) = self.filter.start_time {
            if let Some(nanos) = start.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND start_time_unix_nano >= {nanos}"));
            }
        }

        if let Some(end) = self.filter.end_time {
            if let Some(nanos) = end.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND start_time_unix_nano <= {nanos}"));
            }
        }

        if let Some(ref svc) = self.filter.service {
            sql.push_str(&format!(" AND \"service.name\" = '{}'", escape_string(svc)));
        }

        sql.push_str(&partition_predicates(
            self.filter.start_time,
            self.filter.end_time,
        ));

        sql
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn aggregation_query_basic() {
        let filter = ErrorFilter::default();
        let sql = ErrorAggregationQueryBuilder::new(filter).build();

        assert!(sql.contains("error_fingerprint("));
        assert!(sql.contains("FROM traces"));
        assert!(sql.contains("status_code = 2"));
        assert!(sql.contains("GROUP BY"));
        assert!(sql.contains("ORDER BY count DESC"));
    }

    #[test]
    fn aggregation_query_with_service_filter() {
        let filter = ErrorFilter::default().with_service("api-server");
        let sql = ErrorAggregationQueryBuilder::new(filter).build();

        assert!(sql.contains("\"service.name\" = 'api-server'"));
    }

    #[test]
    fn aggregation_query_with_time_range() {
        let start = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 15, 14, 0, 0).unwrap();
        let filter = ErrorFilter::new(start, end);
        let sql = ErrorAggregationQueryBuilder::new(filter).build();

        assert!(sql.contains("start_time_unix_nano >= "));
        assert!(sql.contains("start_time_unix_nano <= "));
        assert!(sql.contains("date >= '2024-01-15'"));
        assert!(sql.contains("date <= '2024-01-15'"));
        assert!(sql.contains("hour >= '10'"));
        assert!(sql.contains("hour <= '14'"));
    }

    #[test]
    fn aggregation_query_with_min_count() {
        let filter = ErrorFilter::default().with_min_count(5);
        let sql = ErrorAggregationQueryBuilder::new(filter).build();

        assert!(sql.contains("HAVING COUNT(*) >= 5"));
    }

    #[test]
    fn aggregation_query_sort_by_first_seen() {
        let filter = ErrorFilter::default();
        let sql = ErrorAggregationQueryBuilder::new(filter)
            .sort_by(ErrorSortBy::FirstSeen)
            .build();

        assert!(sql.contains("ORDER BY first_seen_nanos DESC"));
    }

    #[test]
    fn samples_query_basic() {
        let filter = ErrorFilter::default();
        let sql = ErrorSamplesQueryBuilder::new("abc123", filter).build();

        assert!(sql.contains("FROM traces"));
        assert!(sql.contains("status_code = 2"));
        assert!(sql.contains("error_fingerprint("));
        assert!(sql.contains("= 'abc123'"));
        assert!(sql.contains("ORDER BY start_time_unix_nano DESC"));
        assert!(sql.contains("LIMIT 100"));
    }

    #[test]
    fn samples_query_with_limit() {
        let filter = ErrorFilter::default();
        let sql = ErrorSamplesQueryBuilder::new("abc123", filter)
            .limit(50)
            .build();

        assert!(sql.contains("LIMIT 50"));
    }

    #[test]
    fn timeline_query_basic() {
        let filter = ErrorFilter::default();
        let sql = ErrorTimelineQueryBuilder::new(filter).build();

        assert!(sql.contains("date_trunc('hour'"));
        assert!(sql.contains("GROUP BY bucket"));
        assert!(sql.contains("ORDER BY bucket ASC"));
    }

    #[test]
    fn timeline_query_with_fingerprint() {
        let filter = ErrorFilter::default();
        let sql = ErrorTimelineQueryBuilder::new(filter)
            .fingerprint("xyz789")
            .build();

        assert!(sql.contains("error_fingerprint("));
        assert!(sql.contains("= 'xyz789'"));
    }

    #[test]
    fn count_query_basic() {
        let filter = ErrorFilter::default();
        let sql = ErrorCountQueryBuilder::new(filter).build();

        assert!(sql.contains("COUNT(*) AS error_count"));
        assert!(sql.contains("COUNT(DISTINCT trace_id) AS affected_traces"));
        assert!(sql.contains("status_code = 2"));
    }

    #[test]
    fn escape_sql_injection() {
        let filter = ErrorFilter::default().with_service("test'; DROP TABLE traces; --");
        let sql = ErrorAggregationQueryBuilder::new(filter).build();

        assert!(sql.contains("test''; DROP TABLE traces; --"));
        assert!(!sql.contains("test'; DROP"));
    }
}
