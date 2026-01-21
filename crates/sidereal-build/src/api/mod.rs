//! HTTP API for the build service.
//!
//! Provides endpoints for submitting builds, checking status, and cancellation.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::CancelReason;
use crate::queue::BuildQueue;
use crate::types::{BuildId, BuildRequest, BuildStatus};

/// Shared application state for the build service.
pub struct AppState {
    /// Build queue for managing pending and in-progress builds.
    pub queue: Arc<BuildQueue>,
}

/// Creates the API router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health endpoints
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        // Build management
        .route("/builds", post(submit_build))
        .route("/builds/{id}", get(get_build))
        .route("/builds/{id}/cancel", post(cancel_build))
        // Queue info
        .route("/queue/stats", get(queue_stats))
        .with_state(state)
}

/// Health check endpoint.
async fn health_check() -> impl IntoResponse {
    Json(HealthResponse { status: "healthy" })
}

/// Readiness check endpoint.
async fn readiness_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let pending = state.queue.pending_count().await;
    let in_progress = state.queue.in_progress_count();

    (
        StatusCode::OK,
        Json(ReadyResponse {
            ready: true,
            pending_builds: pending,
            in_progress_builds: in_progress,
        }),
    )
}

/// Submit a new build request.
async fn submit_build(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SubmitBuildRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut build_request = BuildRequest::new(
        request.project_id,
        request.repo_url,
        request.branch,
        request.commit_sha,
    );

    if let Some(env) = request.environment {
        build_request = build_request.with_environment(env);
    }
    if let Some(url) = request.callback_url {
        build_request = build_request.with_callback_url(url);
    }

    let build_id = build_request.id.clone();

    match state.queue.submit(build_request).await {
        Ok(_) => {
            info!(build_id = %build_id, "build submitted via API");
            Ok((
                StatusCode::ACCEPTED,
                Json(SubmitBuildResponse {
                    id: build_id.to_string(),
                    status: "queued".to_owned(),
                }),
            ))
        }
        Err(e) => {
            let status = match &e {
                crate::error::BuildError::QueueFull => StatusCode::SERVICE_UNAVAILABLE,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// Get the status of a build.
async fn get_build(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<BuildStatusResponse>, (StatusCode, Json<ErrorResponse>)> {
    let build_id = BuildId::new(&id);

    match state.queue.status(&build_id) {
        Some(status) => Ok(Json(BuildStatusResponse::from_status(&id, &status))),
        None => Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("build not found: {id}"),
            }),
        )),
    }
}

/// Cancel a build.
async fn cancel_build(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let build_id = BuildId::new(&id);

    match state
        .queue
        .cancel(&build_id, CancelReason::UserRequested)
        .await
    {
        Ok(()) => {
            info!(build_id = %id, "build cancelled via API");
            Ok((
                StatusCode::OK,
                Json(CancelBuildResponse {
                    id,
                    status: "cancelled".to_owned(),
                }),
            ))
        }
        Err(e) => {
            let status = match &e {
                crate::error::BuildError::BuildNotFound(_) => StatusCode::NOT_FOUND,
                crate::error::BuildError::BuildCompleted(_) => StatusCode::CONFLICT,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            };
            Err((
                status,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            ))
        }
    }
}

/// Get queue statistics.
async fn queue_stats(State(state): State<Arc<AppState>>) -> Json<QueueStatsResponse> {
    Json(QueueStatsResponse {
        pending: state.queue.pending_count().await,
        in_progress: state.queue.in_progress_count(),
    })
}

// Request types

/// Request to submit a new build.
#[derive(Debug, Deserialize)]
pub struct SubmitBuildRequest {
    /// Project identifier.
    pub project_id: String,
    /// Repository URL (HTTPS or SSH).
    pub repo_url: String,
    /// Branch name.
    pub branch: String,
    /// Commit SHA to build.
    pub commit_sha: String,
    /// Target environment (e.g., "production", "staging").
    #[serde(default)]
    pub environment: Option<String>,
    /// URL to POST build completion notification to.
    #[serde(default)]
    pub callback_url: Option<String>,
}

// Response types

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ReadyResponse {
    ready: bool,
    pending_builds: usize,
    in_progress_builds: usize,
}

/// Response for a submitted build.
#[derive(Serialize)]
pub struct SubmitBuildResponse {
    /// The assigned build ID.
    pub id: String,
    /// Initial status (always "queued").
    pub status: String,
}

/// Response for build status queries.
#[derive(Serialize)]
pub struct BuildStatusResponse {
    /// Build ID.
    pub id: String,
    /// Current status.
    pub status: String,
    /// Additional status details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
    /// Whether the build has completed (success, failure, or cancelled).
    pub is_terminal: bool,
    /// Artifact ID (only present if completed successfully).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    /// Error message (only present if failed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl BuildStatusResponse {
    fn from_status(id: &str, status: &BuildStatus) -> Self {
        let is_terminal = status.is_terminal();
        let (status_str, details, artifact_id, error) = match status {
            BuildStatus::Queued => ("queued".to_owned(), None, None, None),
            BuildStatus::CheckingOut => ("checking_out".to_owned(), None, None, None),
            BuildStatus::FetchingDeps => ("fetching_deps".to_owned(), None, None, None),
            BuildStatus::Auditing => ("auditing".to_owned(), None, None, None),
            BuildStatus::Compiling { progress } => {
                ("compiling".to_owned(), progress.clone(), None, None)
            }
            BuildStatus::GeneratingArtifact => ("generating_artifact".to_owned(), None, None, None),
            BuildStatus::Completed { artifact_id } => (
                "completed".to_owned(),
                None,
                Some(artifact_id.to_string()),
                None,
            ),
            BuildStatus::Failed { error, stage } => (
                "failed".to_owned(),
                Some(format!("failed at {stage}")),
                None,
                Some(error.clone()),
            ),
            BuildStatus::Cancelled { reason } => {
                ("cancelled".to_owned(), Some(reason.to_string()), None, None)
            }
        };

        Self {
            id: id.to_owned(),
            status: status_str,
            details,
            is_terminal,
            artifact_id,
            error,
        }
    }
}

/// Response for build cancellation.
#[derive(Serialize)]
pub struct CancelBuildResponse {
    /// Build ID.
    pub id: String,
    /// New status (always "cancelled").
    pub status: String,
}

/// Response for queue statistics.
#[derive(Serialize)]
pub struct QueueStatsResponse {
    /// Number of pending builds.
    pub pending: usize,
    /// Number of in-progress builds.
    pub in_progress: usize,
}

/// Error response.
#[derive(Serialize)]
pub struct ErrorResponse {
    /// Error message.
    pub error: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn make_app_state() -> Arc<AppState> {
        Arc::new(AppState {
            queue: Arc::new(BuildQueue::new(100)),
        })
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

        assert_eq!(response.status(), StatusCode::OK);
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

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn submit_build_endpoint() {
        let state = make_app_state();
        let app = router(state);

        let body = serde_json::json!({
            "project_id": "test-project",
            "repo_url": "https://github.com/example/repo.git",
            "branch": "main",
            "commit_sha": "abc123def456"
        });

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/builds")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
    }

    #[tokio::test]
    async fn get_build_not_found() {
        let state = make_app_state();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/builds/nonexistent-id")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn queue_stats_endpoint() {
        let state = make_app_state();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/queue/stats")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn submit_and_get_build() {
        let state = make_app_state();
        let queue = Arc::clone(&state.queue);

        // Submit a build directly to the queue
        let request = BuildRequest::new(
            "test-project",
            "https://github.com/example/repo.git",
            "main",
            "abc123",
        );
        let build_id = request.id.clone();
        queue.submit(request).await.unwrap();

        // Now query via API
        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/builds/{build_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn cancel_build_not_found() {
        let state = make_app_state();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/builds/nonexistent-id/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
