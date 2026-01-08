//! SQL query builders for deployment tracking.
//!
//! Generates SQL queries for querying deployment events from the logs table.
//! Deployments are identified by `event.name = 'deployment'`.

#![allow(clippy::format_push_string)]

use chrono::{DateTime, Timelike, Utc};

use super::types::DeploymentFilter;

/// Event name for deployment events.
pub const DEPLOYMENT_EVENT_NAME: &str = "deployment";

/// Default limit for deployment queries.
pub const DEFAULT_DEPLOYMENT_LIMIT: usize = 100;

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

/// Builder for deployment list queries.
#[derive(Debug, Clone, Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct DeploymentQueryBuilder {
    filter: DeploymentFilter,
    limit: usize,
    offset: usize,
}

impl DeploymentQueryBuilder {
    /// Create a new builder with the given filter.
    pub const fn new(filter: DeploymentFilter) -> Self {
        Self {
            filter,
            limit: DEFAULT_DEPLOYMENT_LIMIT,
            offset: 0,
        }
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

    /// Build the SQL query for listing deployments.
    pub fn build(&self) -> String {
        let mut sql = String::from(
            r#"SELECT
    time_unix_nano,
    "service.name" AS service_name,
    "service.version" AS service_version,
    "deployment.environment.name" AS environment,
    cbor_extract(log_attributes, 'deployment.id') AS deployment_id,
    cbor_extract(log_attributes, 'deployment.status') AS deployment_status,
    cbor_extract(log_attributes, 'vcs.revision') AS vcs_revision,
    cbor_extract(log_attributes, 'vcs.branch') AS vcs_branch,
    cbor_extract(log_attributes, 'vcs.message') AS vcs_message
FROM logs
WHERE "event.name" = '"#,
        );

        sql.push_str(DEPLOYMENT_EVENT_NAME);
        sql.push('\'');

        self.append_filter_predicates(&mut sql);
        sql.push_str(&partition_predicates(
            self.filter.start_time,
            self.filter.end_time,
        ));

        sql.push_str("\nORDER BY time_unix_nano DESC");
        sql.push_str(&format!("\nLIMIT {}", self.limit));

        if self.offset > 0 {
            sql.push_str(&format!("\nOFFSET {}", self.offset));
        }

        sql
    }

    fn append_filter_predicates(&self, sql: &mut String) {
        if let Some(start) = self.filter.start_time {
            if let Some(nanos) = start.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND time_unix_nano >= {nanos}"));
            }
        }

        if let Some(end) = self.filter.end_time {
            if let Some(nanos) = end.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND time_unix_nano <= {nanos}"));
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

        if let Some(status) = self.filter.status {
            sql.push_str(&format!(
                " AND cbor_extract(log_attributes, 'deployment.status') = '{}'",
                escape_string(status.as_str())
            ));
        }
    }
}

/// Builder for querying service versions.
#[derive(Debug, Clone)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ServiceVersionsQueryBuilder {
    service_name: String,
    filter: DeploymentFilter,
    limit: usize,
}

impl ServiceVersionsQueryBuilder {
    /// Create a new builder for a specific service.
    pub fn new(service_name: impl Into<String>, filter: DeploymentFilter) -> Self {
        Self {
            service_name: service_name.into(),
            filter,
            limit: DEFAULT_DEPLOYMENT_LIMIT,
        }
    }

    /// Set the result limit.
    pub const fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    /// Build the SQL query for listing service versions.
    pub fn build(&self) -> String {
        let mut sql = format!(
            r#"SELECT
    "service.version" AS version,
    MIN(time_unix_nano) AS first_deployed_nanos,
    MAX(time_unix_nano) AS last_deployed_nanos,
    COUNT(*) AS deployment_count,
    ARRAY_AGG(DISTINCT "deployment.environment.name") AS environments
FROM logs
WHERE "event.name" = '{}'
  AND "service.name" = '{}'"#,
            DEPLOYMENT_EVENT_NAME,
            escape_string(&self.service_name)
        );

        if let Some(start) = self.filter.start_time {
            if let Some(nanos) = start.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND time_unix_nano >= {nanos}"));
            }
        }

        if let Some(end) = self.filter.end_time {
            if let Some(nanos) = end.timestamp_nanos_opt() {
                sql.push_str(&format!(" AND time_unix_nano <= {nanos}"));
            }
        }

        sql.push_str(&partition_predicates(
            self.filter.start_time,
            self.filter.end_time,
        ));

        sql.push_str(
            r"
GROUP BY version
ORDER BY last_deployed_nanos DESC",
        );

        sql.push_str(&format!("\nLIMIT {}", self.limit));

        sql
    }
}

/// Builder for querying errors introduced by a deployment.
///
/// Finds errors that first appeared after the deployment timestamp for the
/// deployed version.
#[derive(Debug, Clone)]
#[must_use = "builders do nothing until .build() is called"]
pub struct DeploymentErrorsQueryBuilder {
    service_name: String,
    version: String,
    deployment_time_nanos: u64,
    window_hours: u64,
    limit: usize,
}

impl DeploymentErrorsQueryBuilder {
    /// Create a new builder for errors introduced by a deployment.
    ///
    /// # Arguments
    ///
    /// * `service_name` - Service that was deployed
    /// * `version` - Version that was deployed
    /// * `deployment_time_nanos` - When the deployment occurred (nanoseconds)
    /// * `window_hours` - How many hours after deployment to look for new errors
    pub fn new(
        service_name: impl Into<String>,
        version: impl Into<String>,
        deployment_time_nanos: u64,
        window_hours: u64,
    ) -> Self {
        Self {
            service_name: service_name.into(),
            version: version.into(),
            deployment_time_nanos,
            window_hours,
            limit: 100,
        }
    }

    /// Set the result limit.
    pub const fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }

    /// Build the SQL query for finding deployment-introduced errors.
    ///
    /// Returns errors that:
    /// 1. Occurred after the deployment time
    /// 2. Are for the deployed service and version
    /// 3. Have fingerprints not seen before this deployment
    pub fn build(&self) -> String {
        let window_nanos = self.window_hours * 3_600_000_000_000;
        let window_end = self.deployment_time_nanos.saturating_add(window_nanos);

        format!(
            r#"WITH new_errors AS (
    SELECT
        error_fingerprint("error.type", status_message, NULL, "service.name") AS fingerprint,
        "service.name" AS service_name,
        "error.type" AS error_type,
        FIRST_VALUE(status_message) AS message,
        COUNT(*) AS error_count,
        MIN(start_time_unix_nano) AS first_seen_nanos,
        FIRST_VALUE(trace_id) AS sample_trace_id,
        FIRST_VALUE(span_id) AS sample_span_id
    FROM traces
    WHERE status_code = 2
      AND "service.name" = '{service}'
      AND "service.version" = '{version}'
      AND start_time_unix_nano >= {deploy_time}
      AND start_time_unix_nano <= {window_end}
    GROUP BY fingerprint, service_name, error_type
),
prior_errors AS (
    SELECT DISTINCT error_fingerprint("error.type", status_message, NULL, "service.name") AS fingerprint
    FROM traces
    WHERE status_code = 2
      AND "service.name" = '{service}'
      AND start_time_unix_nano < {deploy_time}
)
SELECT new_errors.*
FROM new_errors
LEFT JOIN prior_errors ON new_errors.fingerprint = prior_errors.fingerprint
WHERE prior_errors.fingerprint IS NULL
ORDER BY error_count DESC
LIMIT {limit}"#,
            service = escape_string(&self.service_name),
            version = escape_string(&self.version),
            deploy_time = self.deployment_time_nanos,
            window_end = window_end,
            limit = self.limit
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::deployments::types::DeploymentStatus;
    use chrono::TimeZone;

    #[test]
    fn deployment_query_basic() {
        let filter = DeploymentFilter::default();
        let sql = DeploymentQueryBuilder::new(filter).build();

        assert!(sql.contains("FROM logs"));
        assert!(sql.contains("event.name\" = 'deployment'"));
        assert!(sql.contains("ORDER BY time_unix_nano DESC"));
    }

    #[test]
    fn deployment_query_with_service_filter() {
        let filter = DeploymentFilter::default().with_service("api-server");
        let sql = DeploymentQueryBuilder::new(filter).build();

        assert!(sql.contains("\"service.name\" = 'api-server'"));
    }

    #[test]
    fn deployment_query_with_time_range() {
        let start = Utc.with_ymd_and_hms(2024, 1, 15, 10, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 15, 14, 0, 0).unwrap();
        let filter = DeploymentFilter::new(start, end);
        let sql = DeploymentQueryBuilder::new(filter).build();

        assert!(sql.contains("time_unix_nano >= "));
        assert!(sql.contains("time_unix_nano <= "));
        assert!(sql.contains("date >= '2024-01-15'"));
    }

    #[test]
    fn deployment_query_with_status_filter() {
        let filter = DeploymentFilter::default().with_status(DeploymentStatus::Succeeded);
        let sql = DeploymentQueryBuilder::new(filter).build();

        assert!(sql.contains("deployment.status') = 'succeeded'"));
    }

    #[test]
    fn service_versions_query_basic() {
        let filter = DeploymentFilter::default();
        let sql = ServiceVersionsQueryBuilder::new("api-server", filter).build();

        assert!(sql.contains("FROM logs"));
        assert!(sql.contains("\"service.name\" = 'api-server'"));
        assert!(sql.contains("GROUP BY version"));
        assert!(sql.contains("ARRAY_AGG(DISTINCT"));
    }

    #[test]
    fn deployment_errors_query_basic() {
        let sql = DeploymentErrorsQueryBuilder::new(
            "api-server",
            "abc123",
            1_700_000_000_000_000_000,
            24,
        )
        .build();

        assert!(sql.contains("WITH new_errors AS"));
        assert!(sql.contains("prior_errors"));
        assert!(sql.contains("\"service.name\" = 'api-server'"));
        assert!(sql.contains("\"service.version\" = 'abc123'"));
        assert!(sql.contains("status_code = 2"));
    }

    #[test]
    fn escape_sql_injection() {
        let filter = DeploymentFilter::default().with_service("test'; DROP TABLE logs; --");
        let sql = DeploymentQueryBuilder::new(filter).build();

        assert!(sql.contains("test''; DROP TABLE logs; --"));
        assert!(!sql.contains("test'; DROP"));
    }
}
