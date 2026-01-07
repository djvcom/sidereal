//! HTTP API handlers for the scheduler.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{delete, get, post},
    Json, Router,
};
use serde::Serialize;
use std::sync::Arc;

use crate::health::HealthTracker;
use crate::registry::{WorkerInfo, WorkerRegistry, WorkerStatus};
use crate::scaling::{ClusterMetrics, ScalingPolicy};
use crate::store::PlacementStore;

/// Shared application state.
pub struct AppState {
    pub registry: Arc<WorkerRegistry>,
    pub health_tracker: Arc<HealthTracker>,
    pub scaling_policy: Arc<ScalingPolicy>,
    pub placement_store: Arc<dyn PlacementStore>,
}

/// Creates the API router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        // Health endpoints
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        // Worker management
        .route("/workers", get(list_workers))
        .route("/workers/{id}", get(get_worker))
        .route("/workers/{id}", delete(remove_worker))
        .route("/workers/{id}/drain", post(drain_worker))
        // Placements
        .route("/placements/{function}", get(get_placement))
        // Metrics
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Health check endpoint.
async fn health_check() -> impl IntoResponse {
    Json(HealthResponse { status: "healthy" })
}

/// Readiness check endpoint.
async fn readiness_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let worker_count = state.registry.len();
    if worker_count > 0 {
        (
            StatusCode::OK,
            Json(ReadyResponse {
                ready: true,
                workers: worker_count,
            }),
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadyResponse {
                ready: false,
                workers: 0,
            }),
        )
    }
}

/// List all workers.
async fn list_workers(State(state): State<Arc<AppState>>) -> Json<Vec<WorkerResponse>> {
    let workers = state.registry.list_all();
    Json(workers.into_iter().map(WorkerResponse::from).collect())
}

/// Get a specific worker.
async fn get_worker(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<WorkerResponse>, StatusCode> {
    state
        .registry
        .get(&id)
        .map(|w| Json(WorkerResponse::from(w)))
        .ok_or(StatusCode::NOT_FOUND)
}

/// Remove a worker.
async fn remove_worker(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    state
        .registry
        .deregister(&id)
        .map(|_| StatusCode::NO_CONTENT)
        .map_err(|_| StatusCode::NOT_FOUND)
}

/// Drain a worker.
async fn drain_worker(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    state
        .registry
        .update_status(&id, WorkerStatus::Draining)
        .map(|_| StatusCode::ACCEPTED)
        .map_err(|_| StatusCode::NOT_FOUND)
}

/// Get placement for a function.
async fn get_placement(
    State(state): State<Arc<AppState>>,
    Path(function): Path<String>,
) -> Result<Json<PlacementResponse>, StatusCode> {
    let availability = state
        .placement_store
        .get_workers(&function)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(PlacementResponse::from_availability(
        function,
        availability,
    )))
}

/// Metrics endpoint.
async fn metrics(State(state): State<Arc<AppState>>) -> String {
    let workers = state.registry.list_all();
    let metrics = calculate_cluster_metrics(&workers);

    format!(
        "# HELP scheduler_workers_total Total number of registered workers\n\
         # TYPE scheduler_workers_total gauge\n\
         scheduler_workers_total {}\n\n\
         # HELP scheduler_workers_healthy Number of healthy workers\n\
         # TYPE scheduler_workers_healthy gauge\n\
         scheduler_workers_healthy {}\n\n\
         # HELP scheduler_total_capacity Total capacity across all workers\n\
         # TYPE scheduler_total_capacity gauge\n\
         scheduler_total_capacity {}\n\n\
         # HELP scheduler_total_load Total current load across all workers\n\
         # TYPE scheduler_total_load gauge\n\
         scheduler_total_load {}\n\n\
         # HELP scheduler_utilisation Current cluster utilisation\n\
         # TYPE scheduler_utilisation gauge\n\
         scheduler_utilisation {:.4}\n",
        metrics.worker_count,
        workers
            .iter()
            .filter(|w| w.status == WorkerStatus::Healthy)
            .count(),
        metrics.total_capacity,
        metrics.total_load,
        metrics.utilisation(),
    )
}

fn calculate_cluster_metrics(workers: &[WorkerInfo]) -> ClusterMetrics {
    ClusterMetrics {
        worker_count: workers.len() as u32,
        total_capacity: workers.iter().map(|w| w.capacity.max_concurrent).sum(),
        total_load: workers.iter().map(|w| w.capacity.current_load).sum(),
        pending_invocations: 0,
        avg_latency: std::time::Duration::ZERO,
    }
}

// Response types

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ReadyResponse {
    ready: bool,
    workers: usize,
}

#[derive(Serialize)]
pub struct WorkerResponse {
    pub id: String,
    pub address: String,
    pub vsock_cid: Option<u32>,
    pub functions: Vec<String>,
    pub status: String,
    pub capacity: CapacityResponse,
    pub registered_at_secs_ago: u64,
    pub last_heartbeat_secs_ago: u64,
}

impl From<WorkerInfo> for WorkerResponse {
    fn from(w: WorkerInfo) -> Self {
        Self {
            id: w.id,
            address: w.address.to_string(),
            vsock_cid: w.vsock_cid,
            functions: w.functions,
            status: format!("{:?}", w.status),
            capacity: CapacityResponse {
                max_concurrent: w.capacity.max_concurrent,
                current_load: w.capacity.current_load,
                memory_mb: w.capacity.memory_mb,
                memory_used_mb: w.capacity.memory_used_mb,
            },
            registered_at_secs_ago: w.registered_at.elapsed().as_secs(),
            last_heartbeat_secs_ago: w.last_heartbeat.elapsed().as_secs(),
        }
    }
}

#[derive(Serialize)]
pub struct CapacityResponse {
    pub max_concurrent: u32,
    pub current_load: u32,
    pub memory_mb: u32,
    pub memory_used_mb: u32,
}

#[derive(Serialize)]
pub struct PlacementResponse {
    pub function: String,
    pub status: String,
    pub workers: Vec<PlacementWorker>,
}

#[derive(Serialize)]
pub struct PlacementWorker {
    pub worker_id: String,
    pub address: String,
    pub status: String,
}

impl PlacementResponse {
    fn from_availability(function: String, availability: crate::store::WorkerAvailability) -> Self {
        use crate::store::WorkerAvailability;

        match availability {
            WorkerAvailability::Available(endpoints) => Self {
                function,
                status: "available".to_string(),
                workers: endpoints
                    .into_iter()
                    .map(|e| PlacementWorker {
                        worker_id: e.worker_id,
                        address: e.address.to_string(),
                        status: format!("{:?}", e.status),
                    })
                    .collect(),
            },
            WorkerAvailability::Provisioning => Self {
                function,
                status: "provisioning".to_string(),
                workers: vec![],
            },
            WorkerAvailability::AllUnhealthy(endpoints) => Self {
                function,
                status: "all_unhealthy".to_string(),
                workers: endpoints
                    .into_iter()
                    .map(|e| PlacementWorker {
                        worker_id: e.worker_id,
                        address: e.address.to_string(),
                        status: format!("{:?}", e.status),
                    })
                    .collect(),
            },
            WorkerAvailability::NotFound => Self {
                function,
                status: "not_found".to_string(),
                workers: vec![],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{HealthConfig, ScalingConfig};
    use crate::store::InMemoryPlacementStore;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn make_app_state() -> Arc<AppState> {
        let registry = Arc::new(WorkerRegistry::new());
        let health_tracker = Arc::new(HealthTracker::new(
            HealthConfig::default(),
            registry.clone(),
        ));
        let scaling_policy = Arc::new(ScalingPolicy::new(ScalingConfig::default()));
        let placement_store = Arc::new(InMemoryPlacementStore::new());

        Arc::new(AppState {
            registry,
            health_tracker,
            scaling_policy,
            placement_store,
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
    async fn workers_list_empty() {
        let state = make_app_state();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/workers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
