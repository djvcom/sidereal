//! Deployment management endpoints.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::deployment::DeploymentRequest;
use crate::error::ControlError;
use crate::provisioner::WorkerProvisioner;
use crate::store::{DeploymentFilter, DeploymentStore};
use crate::types::{FunctionMetadata, PersistedState, ProjectId};

use super::AppState;

/// Request to create a new deployment.
#[derive(Debug, Deserialize)]
pub struct CreateDeploymentRequest {
    /// Project identifier.
    pub project_id: String,
    /// Environment name.
    pub environment: String,
    /// Git commit SHA.
    pub commit_sha: String,
    /// URL to the artifact in object storage.
    pub artifact_url: String,
    /// Functions to deploy.
    pub functions: Vec<FunctionMetadata>,
}

/// Query parameters for listing deployments.
#[derive(Debug, Default, Deserialize)]
pub struct ListDeploymentsQuery {
    /// Filter by project ID.
    pub project_id: Option<String>,
    /// Filter by environment.
    pub environment: Option<String>,
    /// Filter by state.
    pub state: Option<String>,
    /// Maximum number of results.
    pub limit: Option<u32>,
    /// Offset for pagination.
    pub offset: Option<u32>,
}

/// Response for a deployment.
#[derive(Debug, Serialize)]
pub struct DeploymentResponse {
    /// Deployment ID.
    pub id: String,
    /// Project ID.
    pub project_id: String,
    /// Environment name.
    pub environment: String,
    /// Git commit SHA.
    pub commit_sha: String,
    /// Artifact URL.
    pub artifact_url: String,
    /// Current state.
    pub state: String,
    /// Error message (if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Creation timestamp.
    pub created_at: String,
    /// Last update timestamp.
    pub updated_at: String,
}

/// Response for creating a deployment.
#[derive(Debug, Serialize)]
pub struct CreateDeploymentResponse {
    /// The assigned deployment ID.
    pub id: String,
    /// Initial state.
    pub state: String,
}

/// Error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Error message.
    pub error: String,
}

/// Create a new deployment.
pub async fn create_deployment<S, P>(
    State(state): State<AppState<S, P>>,
    Json(request): Json<CreateDeploymentRequest>,
) -> Result<(StatusCode, Json<CreateDeploymentResponse>), (StatusCode, Json<ErrorResponse>)>
where
    S: DeploymentStore + 'static,
    P: WorkerProvisioner + 'static,
{
    let deployment_request = DeploymentRequest {
        project_id: ProjectId::new(&request.project_id),
        environment: request.environment.clone(),
        commit_sha: request.commit_sha,
        artifact_url: request.artifact_url,
        functions: request.functions,
    };

    info!(
        project_id = %request.project_id,
        environment = %request.environment,
        "creating deployment via API"
    );

    match state.manager.deploy(deployment_request).await {
        Ok(deployment_id) => {
            info!(deployment_id = %deployment_id, "deployment created");
            Ok((
                StatusCode::ACCEPTED,
                Json(CreateDeploymentResponse {
                    id: deployment_id.to_string(),
                    state: "pending".to_owned(),
                }),
            ))
        }
        Err(e) => {
            let status = error_to_status(&e);
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// Get a deployment by ID.
pub async fn get_deployment<S, P>(
    State(state): State<AppState<S, P>>,
    Path(id): Path<String>,
) -> Result<Json<DeploymentResponse>, (StatusCode, Json<ErrorResponse>)>
where
    S: DeploymentStore + 'static,
    P: WorkerProvisioner + 'static,
{
    let deployment_id = crate::types::DeploymentId::new(&id);

    match state.manager.get(&deployment_id).await {
        Ok(Some(record)) => Ok(Json(record_to_response(record))),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("deployment not found: {id}"),
            }),
        )),
        Err(e) => {
            let status = error_to_status(&e);
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// List deployments with optional filters.
pub async fn list_deployments<S, P>(
    State(state): State<AppState<S, P>>,
    Query(query): Query<ListDeploymentsQuery>,
) -> Result<Json<Vec<DeploymentResponse>>, (StatusCode, Json<ErrorResponse>)>
where
    S: DeploymentStore + 'static,
    P: WorkerProvisioner + 'static,
{
    let mut filter = DeploymentFilter::new();

    if let Some(project_id) = query.project_id {
        filter = filter.with_project(ProjectId::new(&project_id));
    }
    if let Some(environment) = query.environment {
        filter = filter.with_environment(&environment);
    }
    if let Some(state_str) = query.state {
        if let Some(parsed_state) = parse_state(&state_str) {
            filter = filter.with_state(parsed_state);
        }
    }
    if let Some(limit) = query.limit {
        filter = filter.with_limit(limit);
    }
    if let Some(offset) = query.offset {
        filter = filter.with_offset(offset);
    }

    match state.store.list(&filter).await {
        Ok(records) => Ok(Json(records.into_iter().map(record_to_response).collect())),
        Err(e) => {
            let status = error_to_status(&e);
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// Terminate a deployment.
pub async fn terminate_deployment<S, P>(
    State(state): State<AppState<S, P>>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)>
where
    S: DeploymentStore + 'static,
    P: WorkerProvisioner + 'static,
{
    let deployment_id = crate::types::DeploymentId::new(&id);

    info!(deployment_id = %id, "terminating deployment via API");

    match state.manager.terminate(&deployment_id).await {
        Ok(()) => {
            info!(deployment_id = %id, "deployment terminated");
            Ok(StatusCode::NO_CONTENT)
        }
        Err(e) => {
            let status = error_to_status(&e);
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

fn record_to_response(record: crate::types::DeploymentRecord) -> DeploymentResponse {
    DeploymentResponse {
        id: record.data.id.to_string(),
        project_id: record.data.project_id.to_string(),
        environment: record.data.environment,
        commit_sha: record.data.commit_sha,
        artifact_url: record.data.artifact_url,
        state: record.state.as_str().to_owned(),
        error: record.data.error,
        created_at: record.data.created_at.to_rfc3339(),
        updated_at: record.data.updated_at.to_rfc3339(),
    }
}

const fn error_to_status(error: &ControlError) -> StatusCode {
    match error {
        ControlError::DeploymentNotFound(_) => StatusCode::NOT_FOUND,
        ControlError::InvalidStateTransition { .. } => StatusCode::CONFLICT,
        ControlError::Config(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

fn parse_state(s: &str) -> Option<PersistedState> {
    match s.to_lowercase().as_str() {
        "pending" => Some(PersistedState::Pending),
        "registering" => Some(PersistedState::Registering),
        "active" => Some(PersistedState::Active),
        "superseded" => Some(PersistedState::Superseded),
        "failed" => Some(PersistedState::Failed),
        "terminated" => Some(PersistedState::Terminated),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ArtifactConfig, DeploymentConfig};
    use crate::deployment::DeploymentManager;
    use crate::provisioner::MockProvisioner;
    use crate::scheduler::SchedulerClient;
    use crate::store::MemoryStore;
    use std::sync::Arc;

    fn make_app_state() -> AppState<MemoryStore, MockProvisioner> {
        let store = Arc::new(MemoryStore::new());
        let provisioner = Arc::new(MockProvisioner::default());
        let scheduler = SchedulerClient::with_url("http://localhost:8082").unwrap();
        let artifact_config = ArtifactConfig::default();
        let deployment_config = DeploymentConfig::default();

        let manager = Arc::new(DeploymentManager::new(
            Arc::clone(&store),
            provisioner,
            scheduler,
            artifact_config,
            deployment_config,
        ));

        AppState { manager, store }
    }

    #[tokio::test]
    async fn list_deployments_empty() {
        let state = make_app_state();
        let app = super::super::router(state);

        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/deployments")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_deployment_not_found() {
        let state = make_app_state();
        let app = super::super::router(state);

        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/deployments/nonexistent-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn terminate_deployment_not_found() {
        let state = make_app_state();
        let app = super::super::router(state);

        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/deployments/nonexistent-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
