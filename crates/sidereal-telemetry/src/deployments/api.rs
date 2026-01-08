//! HTTP API for deployment tracking.
//!
//! Provides REST endpoints for querying deployment events and correlating
//! them with errors.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::instrument;

use super::queries::{
    DeploymentErrorsQueryBuilder, DeploymentQueryBuilder, ServiceVersionsQueryBuilder,
    DEFAULT_DEPLOYMENT_LIMIT,
};
use super::types::{Deployment, DeploymentFilter, ServiceVersion};
use crate::query::QueryEngine;
use crate::TelemetryError;

/// Shared state for deployment API handlers.
#[derive(Clone)]
pub struct DeploymentApiState {
    /// Query engine for executing SQL queries.
    pub engine: Arc<QueryEngine>,
}

/// Create the deployment API router.
pub fn deployment_router(state: DeploymentApiState) -> Router {
    Router::new()
        .route("/", get(list_deployments))
        .route("/services/{service}/versions", get(list_service_versions))
        .route(
            "/services/{service}/versions/{version}/errors",
            get(get_deployment_errors),
        )
        .with_state(state)
}

/// Request parameters for listing deployments.
#[derive(Debug, Deserialize)]
pub struct ListDeploymentsRequest {
    /// Start of time range (RFC 3339).
    pub start_time: Option<String>,
    /// End of time range (RFC 3339).
    pub end_time: Option<String>,
    /// Filter by service name.
    pub service: Option<String>,
    /// Filter by environment.
    pub environment: Option<String>,
    /// Filter by deployment status.
    pub status: Option<String>,
    /// Maximum results to return.
    pub limit: Option<usize>,
    /// Offset for pagination.
    pub offset: Option<usize>,
}

/// Response for deployment list.
#[derive(Debug, Serialize)]
pub struct ListDeploymentsResponse {
    /// List of deployments.
    pub deployments: Vec<Deployment>,
    /// Whether more results are available.
    pub has_more: bool,
}

/// List deployments with filtering.
#[instrument(skip(state))]
async fn list_deployments(
    State(state): State<DeploymentApiState>,
    Query(params): Query<ListDeploymentsRequest>,
) -> Result<impl IntoResponse, DeploymentApiError> {
    let mut filter = DeploymentFilter::default();

    if let Some(ref start) = params.start_time {
        filter.start_time = Some(parse_datetime(start)?);
    }
    if let Some(ref end) = params.end_time {
        filter.end_time = Some(parse_datetime(end)?);
    }
    if let Some(ref svc) = params.service {
        filter.service = Some(svc.clone());
    }
    if let Some(ref env) = params.environment {
        filter.environment = Some(env.clone());
    }
    if let Some(ref status) = params.status {
        filter.status = Some(status.parse().unwrap());
    }

    let limit = params.limit.unwrap_or(DEFAULT_DEPLOYMENT_LIMIT);
    let offset = params.offset.unwrap_or(0);

    let sql = DeploymentQueryBuilder::new(filter)
        .limit(limit + 1)
        .offset(offset)
        .build();

    let batches = state.engine.query(&sql).await?;
    let mut deployments = parse_deployments(&batches)?;

    let has_more = deployments.len() > limit;
    if has_more {
        deployments.truncate(limit);
    }

    Ok(Json(ListDeploymentsResponse {
        deployments,
        has_more,
    }))
}

/// Request parameters for listing service versions.
#[derive(Debug, Deserialize)]
pub struct ListVersionsRequest {
    /// Start of time range (RFC 3339).
    pub start_time: Option<String>,
    /// End of time range (RFC 3339).
    pub end_time: Option<String>,
    /// Maximum results to return.
    pub limit: Option<usize>,
}

/// Response for service versions list.
#[derive(Debug, Serialize)]
pub struct ListVersionsResponse {
    /// Service name.
    pub service: String,
    /// List of versions with deployment info.
    pub versions: Vec<ServiceVersion>,
}

/// List versions deployed for a service.
#[instrument(skip(state))]
async fn list_service_versions(
    State(state): State<DeploymentApiState>,
    Path(service): Path<String>,
    Query(params): Query<ListVersionsRequest>,
) -> Result<impl IntoResponse, DeploymentApiError> {
    let mut filter = DeploymentFilter::default();

    if let Some(ref start) = params.start_time {
        filter.start_time = Some(parse_datetime(start)?);
    }
    if let Some(ref end) = params.end_time {
        filter.end_time = Some(parse_datetime(end)?);
    }

    let limit = params.limit.unwrap_or(DEFAULT_DEPLOYMENT_LIMIT);

    let sql = ServiceVersionsQueryBuilder::new(&service, filter)
        .limit(limit)
        .build();

    let batches = state.engine.query(&sql).await?;
    let versions = parse_service_versions(&batches)?;

    Ok(Json(ListVersionsResponse { service, versions }))
}

/// Request parameters for deployment errors.
#[derive(Debug, Deserialize)]
pub struct DeploymentErrorsRequest {
    /// Deployment timestamp in nanoseconds (required).
    pub deployment_time_nanos: u64,
    /// Hours after deployment to check for new errors.
    pub window_hours: Option<u64>,
    /// Maximum results to return.
    pub limit: Option<usize>,
}

/// An error introduced by a deployment.
#[derive(Debug, Serialize)]
pub struct DeploymentError {
    /// Error fingerprint.
    pub fingerprint: String,
    /// Service name.
    pub service_name: String,
    /// Error type.
    pub error_type: Option<String>,
    /// Error message.
    pub message: Option<String>,
    /// Count of occurrences.
    pub error_count: u64,
    /// When first seen.
    pub first_seen: String,
    /// Sample trace ID (hex).
    pub sample_trace_id: Option<String>,
    /// Sample span ID (hex).
    pub sample_span_id: Option<String>,
}

/// Response for deployment errors.
#[derive(Debug, Serialize)]
pub struct DeploymentErrorsResponse {
    /// Service name.
    pub service: String,
    /// Version that was deployed.
    pub version: String,
    /// Errors introduced by this deployment.
    pub new_errors: Vec<DeploymentError>,
}

/// Get errors introduced by a specific deployment.
#[instrument(skip(state))]
async fn get_deployment_errors(
    State(state): State<DeploymentApiState>,
    Path((service, version)): Path<(String, String)>,
    Query(params): Query<DeploymentErrorsRequest>,
) -> Result<impl IntoResponse, DeploymentApiError> {
    let window_hours = params.window_hours.unwrap_or(24);
    let limit = params.limit.unwrap_or(100);

    let sql = DeploymentErrorsQueryBuilder::new(
        &service,
        &version,
        params.deployment_time_nanos,
        window_hours,
    )
    .limit(limit)
    .build();

    let batches = state.engine.query(&sql).await?;
    let new_errors = parse_deployment_errors(&batches)?;

    Ok(Json(DeploymentErrorsResponse {
        service,
        version,
        new_errors,
    }))
}

/// Parse RFC 3339 datetime string.
fn parse_datetime(s: &str) -> Result<DateTime<Utc>, DeploymentApiError> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| DeploymentApiError::InvalidParameter(format!("Invalid datetime: {s}")))
}

/// Parse deployments from query results.
fn parse_deployments(
    batches: &[arrow::array::RecordBatch],
) -> Result<Vec<Deployment>, DeploymentApiError> {
    use arrow::array::{Array, StringArray, UInt64Array};

    let mut deployments = Vec::new();

    for batch in batches {
        let time_nanos = batch
            .column_by_name("time_unix_nano")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| DeploymentApiError::Internal("Missing time_unix_nano".to_owned()))?;

        let service_names = batch
            .column_by_name("service_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| DeploymentApiError::Internal("Missing service_name".to_owned()))?;

        let versions = batch
            .column_by_name("service_version")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let environments = batch
            .column_by_name("environment")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let deployment_ids = batch
            .column_by_name("deployment_id")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let statuses = batch
            .column_by_name("deployment_status")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let vcs_revisions = batch
            .column_by_name("vcs_revision")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let vcs_branches = batch
            .column_by_name("vcs_branch")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let vcs_messages = batch
            .column_by_name("vcs_message")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        for i in 0..batch.num_rows() {
            let nanos = time_nanos.value(i);
            #[allow(clippy::cast_possible_wrap, clippy::as_conversions)]
            let timestamp = DateTime::from_timestamp_nanos(nanos as i64);

            let status_str =
                statuses.and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) });

            deployments.push(Deployment {
                id: deployment_ids.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                }),
                service_name: service_names.value(i).to_owned(),
                version: versions.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                }),
                environment: environments.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                }),
                status: status_str.map(|s| s.parse().unwrap()).unwrap_or_default(),
                timestamp,
                vcs_revision: vcs_revisions.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                }),
                vcs_branch: vcs_branches.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                }),
                vcs_message: vcs_messages.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                }),
            });
        }
    }

    Ok(deployments)
}

/// Parse service versions from query results.
fn parse_service_versions(
    batches: &[arrow::array::RecordBatch],
) -> Result<Vec<ServiceVersion>, DeploymentApiError> {
    use arrow::array::{Array, ListArray, StringArray, UInt64Array};

    let mut versions = Vec::new();

    for batch in batches {
        let version_col = batch
            .column_by_name("version")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| DeploymentApiError::Internal("Missing version".to_owned()))?;

        let first_deployed = batch
            .column_by_name("first_deployed_nanos")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                DeploymentApiError::Internal("Missing first_deployed_nanos".to_owned())
            })?;

        let last_deployed = batch
            .column_by_name("last_deployed_nanos")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                DeploymentApiError::Internal("Missing last_deployed_nanos".to_owned())
            })?;

        let deployment_counts = batch
            .column_by_name("deployment_count")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| DeploymentApiError::Internal("Missing deployment_count".to_owned()))?;

        let environments_col = batch
            .column_by_name("environments")
            .and_then(|c| c.as_any().downcast_ref::<ListArray>());

        for i in 0..batch.num_rows() {
            if version_col.is_null(i) {
                continue;
            }

            let envs = environments_col
                .map(|list| {
                    let values = list.value(i);
                    if let Some(str_array) = values.as_any().downcast_ref::<StringArray>() {
                        (0..str_array.len())
                            .filter(|&j| !str_array.is_null(j))
                            .map(|j| str_array.value(j).to_owned())
                            .collect()
                    } else {
                        Vec::new()
                    }
                })
                .unwrap_or_default();

            #[allow(clippy::cast_possible_wrap, clippy::as_conversions)]
            versions.push(ServiceVersion {
                version: version_col.value(i).to_owned(),
                first_deployed: DateTime::from_timestamp_nanos(first_deployed.value(i) as i64),
                last_deployed: DateTime::from_timestamp_nanos(last_deployed.value(i) as i64),
                deployment_count: deployment_counts.value(i),
                environments: envs,
            });
        }
    }

    Ok(versions)
}

/// Parse deployment errors from query results.
fn parse_deployment_errors(
    batches: &[arrow::array::RecordBatch],
) -> Result<Vec<DeploymentError>, DeploymentApiError> {
    use arrow::array::{Array, FixedSizeBinaryArray, StringArray, UInt64Array};

    let mut errors = Vec::new();

    for batch in batches {
        let fingerprints = batch
            .column_by_name("fingerprint")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| DeploymentApiError::Internal("Missing fingerprint".to_owned()))?;

        let service_names = batch
            .column_by_name("service_name")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| DeploymentApiError::Internal("Missing service_name".to_owned()))?;

        let error_types = batch
            .column_by_name("error_type")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let messages = batch
            .column_by_name("message")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let counts = batch
            .column_by_name("error_count")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| DeploymentApiError::Internal("Missing error_count".to_owned()))?;

        let first_seen = batch
            .column_by_name("first_seen_nanos")
            .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| DeploymentApiError::Internal("Missing first_seen_nanos".to_owned()))?;

        let trace_ids = batch
            .column_by_name("sample_trace_id")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

        let span_ids = batch
            .column_by_name("sample_span_id")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

        for i in 0..batch.num_rows() {
            #[allow(clippy::cast_possible_wrap, clippy::as_conversions)]
            let timestamp = DateTime::from_timestamp_nanos(first_seen.value(i) as i64).to_rfc3339();

            errors.push(DeploymentError {
                fingerprint: fingerprints.value(i).to_owned(),
                service_name: service_names.value(i).to_owned(),
                error_type: error_types.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                }),
                message: messages.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                }),
                error_count: counts.value(i),
                first_seen: timestamp,
                sample_trace_id: trace_ids.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(hex::encode(a.value(i)))
                    }
                }),
                sample_span_id: span_ids.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(hex::encode(a.value(i)))
                    }
                }),
            });
        }
    }

    Ok(errors)
}

/// API error type for deployment endpoints.
#[derive(Debug)]
pub enum DeploymentApiError {
    /// Invalid parameter in request.
    InvalidParameter(String),
    /// Query execution failed.
    Query(TelemetryError),
    /// Internal error.
    Internal(String),
}

impl From<TelemetryError> for DeploymentApiError {
    fn from(e: TelemetryError) -> Self {
        Self::Query(e)
    }
}

impl IntoResponse for DeploymentApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, message) = match self {
            Self::InvalidParameter(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::Query(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        (status, Json(serde_json::json!({ "error": message }))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::super::types::DeploymentStatus;
    use super::*;
    use chrono::Datelike;

    #[test]
    fn parse_datetime_valid() {
        let dt = parse_datetime("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
    }

    #[test]
    fn parse_datetime_invalid() {
        let result = parse_datetime("not a date");
        assert!(result.is_err());
    }

    #[test]
    fn deployment_status_from_string() {
        assert_eq!(
            "started".parse::<DeploymentStatus>().unwrap(),
            DeploymentStatus::Started
        );
        assert_eq!(
            "succeeded".parse::<DeploymentStatus>().unwrap(),
            DeploymentStatus::Succeeded
        );
    }
}
