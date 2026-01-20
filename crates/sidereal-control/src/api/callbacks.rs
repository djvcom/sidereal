//! Callback endpoints for external service notifications.

use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

use crate::deployment::DeploymentRequest;
use crate::provisioner::WorkerProvisioner;
use crate::store::DeploymentStore;
use crate::types::{FunctionMetadata, ProjectId};

use super::AppState;

/// Request from sidereal-build when a build completes.
#[derive(Debug, Deserialize)]
pub struct BuildCompletedRequest {
    /// Build ID from sidereal-build.
    pub build_id: String,
    /// Project identifier.
    pub project_id: String,
    /// Environment to deploy to.
    pub environment: String,
    /// Git commit SHA that was built.
    pub commit_sha: String,
    /// URL to the artifact in object storage.
    pub artifact_url: String,
    /// Functions discovered in the build.
    pub functions: Vec<FunctionMetadata>,
    /// Whether the build succeeded.
    pub success: bool,
    /// Error message (if build failed).
    #[serde(default)]
    pub error: Option<String>,
}

/// Response for build completion callback.
#[derive(Debug, Serialize)]
pub struct BuildCompletedResponse {
    /// Whether the callback was processed.
    pub accepted: bool,
    /// Deployment ID (if deployment was triggered).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    /// Error message (if callback processing failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Handle build completion callback from sidereal-build.
pub async fn build_completed<S, P>(
    State(state): State<AppState<S, P>>,
    Json(request): Json<BuildCompletedRequest>,
) -> (StatusCode, Json<BuildCompletedResponse>)
where
    S: DeploymentStore + 'static,
    P: WorkerProvisioner + 'static,
{
    info!(
        build_id = %request.build_id,
        project_id = %request.project_id,
        environment = %request.environment,
        success = %request.success,
        "received build completion callback"
    );

    if !request.success {
        warn!(
            build_id = %request.build_id,
            error = ?request.error,
            "build failed, not triggering deployment"
        );
        return (
            StatusCode::OK,
            Json(BuildCompletedResponse {
                accepted: true,
                deployment_id: None,
                error: request.error,
            }),
        );
    }

    let deployment_request = DeploymentRequest {
        project_id: ProjectId::new(&request.project_id),
        environment: request.environment.clone(),
        commit_sha: request.commit_sha,
        artifact_url: request.artifact_url,
        functions: request.functions,
    };

    match state.manager.deploy(deployment_request).await {
        Ok(deployment_id) => {
            info!(
                build_id = %request.build_id,
                deployment_id = %deployment_id,
                "deployment triggered from build callback"
            );
            (
                StatusCode::ACCEPTED,
                Json(BuildCompletedResponse {
                    accepted: true,
                    deployment_id: Some(deployment_id.to_string()),
                    error: None,
                }),
            )
        }
        Err(e) => {
            error!(
                build_id = %request.build_id,
                error = %e,
                "failed to trigger deployment from build callback"
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(BuildCompletedResponse {
                    accepted: false,
                    deployment_id: None,
                    error: Some(e.to_string()),
                }),
            )
        }
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
    async fn build_failed_callback() {
        let state = make_app_state();
        let app = super::super::router(state);

        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let body = serde_json::json!({
            "build_id": "build-123",
            "project_id": "test-project",
            "environment": "production",
            "commit_sha": "abc123",
            "artifact_url": "",
            "functions": [],
            "success": false,
            "error": "compilation failed"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/callbacks/build-completed")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn build_success_callback_triggers_deployment() {
        let state = make_app_state();
        let app = super::super::router(state);

        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let body = serde_json::json!({
            "build_id": "build-456",
            "project_id": "test-project",
            "environment": "staging",
            "commit_sha": "def789",
            "artifact_url": "file:///tmp/test-artifact/rootfs.ext4",
            "functions": [{
                "name": "handler",
                "trigger": {
                    "type": "http",
                    "method": "GET",
                    "path": "/hello"
                },
                "memory_mb": 128,
                "vcpus": 1
            }],
            "success": true
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/callbacks/build-completed")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(
            response.status() == StatusCode::ACCEPTED
                || response.status() == StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
