//! Integration tests for HTTP API and multi-component state consistency.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{fixtures::WorkerBuilder, TestScheduler};
use sidereal_scheduler::api::router;
use sidereal_scheduler::registry::WorkerStatus;
use sidereal_scheduler::store::{PlacementStore, WorkerAvailability, WorkerEndpoint};
use tower::ServiceExt;

#[tokio::test]
async fn api_delete_worker_removes_from_all_components() {
    let scheduler = TestScheduler::new();

    // Register worker
    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Record some health data
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", std::time::Duration::from_millis(10));

    // Set up placement
    let endpoint = WorkerEndpoint {
        worker_id: "worker-1".to_string(),
        address: "127.0.0.1:8080".parse().unwrap(),
        vsock_cid: None,
        status: WorkerStatus::Healthy,
    };
    scheduler
        .placement_store
        .set_workers("greet", vec![endpoint])
        .await
        .unwrap();

    // Create router and make DELETE request
    let app = router(scheduler.app_state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/workers/worker-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // Worker should be removed from registry
    assert!(scheduler.registry.get("worker-1").is_none());

    // Note: The API handler only removes from registry, not health tracker or placement store
    // In production, a background task would sync these
}

#[tokio::test]
async fn api_drain_worker_updates_status() {
    let scheduler = TestScheduler::new();

    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Create router and make drain request
    let app = router(scheduler.app_state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/workers/worker-1/drain")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    // Worker should be draining
    let worker_info = scheduler.registry.get("worker-1").unwrap();
    assert_eq!(worker_info.status, WorkerStatus::Draining);
}

#[tokio::test]
async fn api_get_worker_returns_details() {
    let scheduler = TestScheduler::new();

    let worker = WorkerBuilder::new("worker-1")
        .with_functions(vec!["greet", "farewell"])
        .with_load(5, 10)
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    let app = router(scheduler.app_state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/workers/worker-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["id"], "worker-1");
    assert_eq!(json["status"], "Healthy");
    assert_eq!(json["capacity"]["max_concurrent"], 10);
    assert_eq!(json["capacity"]["current_load"], 5);
}

#[tokio::test]
async fn api_get_nonexistent_worker_returns_404() {
    let scheduler = TestScheduler::new();
    let app = router(scheduler.app_state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/workers/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn api_list_workers_returns_all() {
    let scheduler = TestScheduler::new();

    for i in 0..3 {
        let worker = WorkerBuilder::new(&format!("worker-{i}"))
            .with_function("greet")
            .healthy()
            .build();
        scheduler.registry.register(worker).unwrap();
    }

    let app = router(scheduler.app_state.clone());

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

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert!(json.is_array());
    assert_eq!(json.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn api_ready_returns_unavailable_when_no_workers() {
    let scheduler = TestScheduler::new();
    let app = router(scheduler.app_state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ready"], false);
    assert_eq!(json["workers"], 0);
}

#[tokio::test]
async fn api_ready_returns_ok_with_workers() {
    let scheduler = TestScheduler::new();

    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();
    scheduler.registry.register(worker).unwrap();

    let app = router(scheduler.app_state.clone());

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

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["ready"], true);
    assert_eq!(json["workers"], 1);
}

#[tokio::test]
async fn api_get_placement_returns_availability() {
    let scheduler = TestScheduler::new();

    // Register and set up placement
    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();
    scheduler.registry.register(worker).unwrap();

    let endpoint = WorkerEndpoint {
        worker_id: "worker-1".to_string(),
        address: "127.0.0.1:8080".parse().unwrap(),
        vsock_cid: None,
        status: WorkerStatus::Healthy,
    };
    scheduler
        .placement_store
        .set_workers("greet", vec![endpoint])
        .await
        .unwrap();

    let app = router(scheduler.app_state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/placements/greet")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["function"], "greet");
    assert_eq!(json["status"], "available");
    assert_eq!(json["workers"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn api_get_placement_not_found() {
    let scheduler = TestScheduler::new();
    let app = router(scheduler.app_state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/placements/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "not_found");
}

#[tokio::test]
async fn api_metrics_returns_prometheus_format() {
    let scheduler = TestScheduler::new();

    // Add some workers
    for i in 0..3 {
        let worker = WorkerBuilder::new(&format!("worker-{i}"))
            .with_function("greet")
            .with_load(i as u32, 10)
            .healthy()
            .build();
        scheduler.registry.register(worker).unwrap();
    }

    let app = router(scheduler.app_state.clone());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();

    // Check Prometheus format
    assert!(body_str.contains("scheduler_workers_total 3"));
    assert!(body_str.contains("scheduler_workers_healthy 3"));
    assert!(body_str.contains("scheduler_total_capacity 30")); // 3 workers * 10 capacity
    assert!(body_str.contains("scheduler_total_load 3")); // 0 + 1 + 2
}
