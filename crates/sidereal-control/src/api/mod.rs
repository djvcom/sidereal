//! HTTP API for the control service.
//!
//! Provides endpoints for:
//! - Deployment management (create, query, list, terminate)
//! - Build completion callbacks
//! - Health and readiness checks
//! - Prometheus metrics

mod callbacks;
mod deployments;

use std::fmt::Write as _;
use std::sync::Arc;

use axum::{
    routing::{delete, get, post},
    Router,
};

use crate::deployment::DeploymentManager;
use crate::store::DeploymentStore;

pub use callbacks::BuildCompletedRequest;
pub use deployments::{CreateDeploymentRequest, DeploymentResponse, ListDeploymentsQuery};

/// Shared application state for the control service.
#[derive(Clone)]
pub struct AppState {
    /// Deployment manager for orchestrating deployments.
    pub manager: Arc<DeploymentManager>,
    /// Deployment store for direct queries.
    pub store: Arc<dyn DeploymentStore>,
}

/// Creates the API router.
pub fn router(state: AppState) -> Router {
    Router::new()
        // Health endpoints
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        // Deployment management
        .route("/deployments", post(deployments::create_deployment))
        .route("/deployments", get(deployments::list_deployments))
        .route("/deployments/{id}", get(deployments::get_deployment))
        .route(
            "/deployments/{id}",
            delete(deployments::terminate_deployment),
        )
        // Callbacks
        .route(
            "/callbacks/build-completed",
            post(callbacks::build_completed),
        )
        // Metrics
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Health check endpoint.
async fn health_check() -> axum::Json<HealthResponse> {
    axum::Json(HealthResponse { status: "healthy" })
}

/// Readiness check endpoint.
async fn readiness_check(
    axum::extract::State(state): axum::extract::State<AppState>,
) -> (axum::http::StatusCode, axum::Json<ReadyResponse>) {
    let filter =
        crate::store::DeploymentFilter::new().with_state(crate::types::PersistedState::Active);

    match state.store.list(&filter).await {
        Ok(deployments) => (
            axum::http::StatusCode::OK,
            axum::Json(ReadyResponse {
                ready: true,
                active_deployments: deployments.len(),
            }),
        ),
        Err(_) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::Json(ReadyResponse {
                ready: false,
                active_deployments: 0,
            }),
        ),
    }
}

/// Metrics endpoint.
async fn metrics(axum::extract::State(state): axum::extract::State<AppState>) -> String {
    let mut output = String::new();

    let states = [
        ("pending", crate::types::PersistedState::Pending),
        ("registering", crate::types::PersistedState::Registering),
        ("active", crate::types::PersistedState::Active),
        ("superseded", crate::types::PersistedState::Superseded),
        ("failed", crate::types::PersistedState::Failed),
        ("terminated", crate::types::PersistedState::Terminated),
    ];

    output.push_str("# HELP control_deployments_total Number of deployments by state\n");
    output.push_str("# TYPE control_deployments_total gauge\n");

    for (label, state_val) in states {
        let filter = crate::store::DeploymentFilter::new().with_state(state_val);
        let count = state
            .store
            .list(&filter)
            .await
            .map(|d| d.len())
            .unwrap_or(0);
        let _ = writeln!(
            output,
            "control_deployments_total{{state=\"{label}\"}} {count}"
        );
    }

    output
}

/// Health response.
#[derive(serde::Serialize)]
struct HealthResponse {
    status: &'static str,
}

/// Readiness response.
#[derive(serde::Serialize)]
struct ReadyResponse {
    ready: bool,
    active_deployments: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ArtifactConfig, DeploymentConfig};
    use crate::deployment::DeploymentManager;
    use crate::provisioner::{MockProvisioner, WorkerProvisioner};
    use crate::scheduler::SchedulerClient;
    use crate::store::MemoryStore;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn make_app_state() -> AppState {
        let store: Arc<dyn DeploymentStore> = Arc::new(MemoryStore::new());
        let provisioner: Arc<dyn WorkerProvisioner> = Arc::new(MockProvisioner::default());
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
    async fn health_endpoint() {
        let state = make_app_state();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn ready_endpoint() {
        let state = make_app_state();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ready")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }

    #[tokio::test]
    async fn metrics_endpoint() {
        let state = make_app_state();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), axum::http::StatusCode::OK);
    }
}
