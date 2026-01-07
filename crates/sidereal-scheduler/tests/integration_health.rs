//! Integration tests for health tracking and placement synchronisation.

mod common;

use common::{fixtures::WorkerBuilder, TestScheduler};
use sidereal_scheduler::config::HealthConfig;
use sidereal_scheduler::registry::WorkerStatus;
use sidereal_scheduler::store::{PlacementStore, WorkerAvailability, WorkerEndpoint};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn heartbeat_timeout_marks_worker_unhealthy() {
    let scheduler = TestScheduler::with_fast_health_checks();

    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Record initial heartbeat
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", Duration::from_millis(5));

    // Wait for timeout
    sleep(Duration::from_millis(250)).await;

    // Check for timeouts
    let timed_out = scheduler.health_tracker.check_heartbeat_timeouts();
    assert_eq!(timed_out, vec!["worker-1".to_string()]);

    // Record another failure to reach unhealthy threshold
    scheduler.health_tracker.record_failure("worker-1");

    // Worker should be unhealthy now
    let worker_info = scheduler.registry.get("worker-1").unwrap();
    assert_eq!(worker_info.status, WorkerStatus::Unhealthy);
}

#[tokio::test]
async fn unhealthy_worker_placement_reflects_status() {
    let scheduler = TestScheduler::new();

    // Register healthy and unhealthy workers
    let healthy = WorkerBuilder::new("healthy-1")
        .with_function("greet")
        .healthy()
        .build();

    let unhealthy = WorkerBuilder::new("unhealthy-1")
        .with_function("greet")
        .unhealthy()
        .build();

    scheduler.registry.register(healthy).unwrap();
    scheduler.registry.register(unhealthy).unwrap();

    // Update placement with all workers
    let all_workers = scheduler.registry.list_for_function("greet");
    let endpoints: Vec<WorkerEndpoint> = all_workers
        .iter()
        .map(|w| WorkerEndpoint {
            worker_id: w.id.clone(),
            address: w.address,
            vsock_cid: w.vsock_cid,
            status: w.status,
        })
        .collect();

    scheduler
        .placement_store
        .set_workers("greet", endpoints)
        .await
        .unwrap();

    // Placement returns only healthy workers as Available
    let availability = scheduler
        .placement_store
        .get_workers("greet")
        .await
        .unwrap();
    match availability {
        WorkerAvailability::Available(workers) => {
            assert_eq!(workers.len(), 1);
            assert_eq!(workers[0].worker_id, "healthy-1");
        }
        other => panic!("Expected Available with 1 worker, got {other:?}"),
    }
}

#[tokio::test]
async fn all_unhealthy_workers_returns_all_unhealthy_availability() {
    let scheduler = TestScheduler::new();

    // Register only unhealthy workers
    for i in 0..3 {
        let worker = WorkerBuilder::new(&format!("worker-{i}"))
            .with_function("greet")
            .unhealthy()
            .build();
        scheduler.registry.register(worker).unwrap();
    }

    // Update placement
    let all_workers = scheduler.registry.list_for_function("greet");
    let endpoints: Vec<WorkerEndpoint> = all_workers
        .iter()
        .map(|w| WorkerEndpoint {
            worker_id: w.id.clone(),
            address: w.address,
            vsock_cid: w.vsock_cid,
            status: w.status,
        })
        .collect();

    scheduler
        .placement_store
        .set_workers("greet", endpoints)
        .await
        .unwrap();

    // Placement returns AllUnhealthy
    let availability = scheduler
        .placement_store
        .get_workers("greet")
        .await
        .unwrap();
    match availability {
        WorkerAvailability::AllUnhealthy(workers) => {
            assert_eq!(workers.len(), 3);
        }
        other => panic!("Expected AllUnhealthy, got {other:?}"),
    }
}

#[tokio::test]
async fn starting_workers_returns_provisioning() {
    let scheduler = TestScheduler::new();

    // Register only starting workers
    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .with_status(WorkerStatus::Starting)
        .build();

    scheduler.registry.register(worker).unwrap();

    // Update placement
    let all_workers = scheduler.registry.list_for_function("greet");
    let endpoints: Vec<WorkerEndpoint> = all_workers
        .iter()
        .map(|w| WorkerEndpoint {
            worker_id: w.id.clone(),
            address: w.address,
            vsock_cid: w.vsock_cid,
            status: w.status,
        })
        .collect();

    scheduler
        .placement_store
        .set_workers("greet", endpoints)
        .await
        .unwrap();

    // Placement returns Provisioning
    let availability = scheduler
        .placement_store
        .get_workers("greet")
        .await
        .unwrap();
    assert!(matches!(availability, WorkerAvailability::Provisioning));
}

#[tokio::test]
async fn worker_recovery_transitions_back_to_healthy() {
    let health_config = HealthConfig {
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_timeout: Duration::from_millis(200),
        unhealthy_threshold: 2,
        healthy_threshold: 2,
        ping_interval: Duration::from_millis(100),
    };

    let scheduler = TestScheduler::with_config(health_config, Default::default());

    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Mark unhealthy through failures
    scheduler.health_tracker.record_failure("worker-1");
    scheduler.health_tracker.record_failure("worker-1");

    let worker_info = scheduler.registry.get("worker-1").unwrap();
    assert_eq!(worker_info.status, WorkerStatus::Unhealthy);

    // Recover through successful heartbeats (need threshold + 1 because check happens before increment)
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", Duration::from_millis(10));
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", Duration::from_millis(10));
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", Duration::from_millis(10));

    // Worker should be healthy again
    let worker_info = scheduler.registry.get("worker-1").unwrap();
    assert_eq!(worker_info.status, WorkerStatus::Healthy);
}

#[tokio::test]
async fn health_data_removed_on_deregister() {
    let scheduler = TestScheduler::new();

    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Record some health data
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", Duration::from_millis(50));

    assert!(scheduler.health_tracker.get("worker-1").is_some());

    // Deregister and clean up
    scheduler.registry.deregister("worker-1").unwrap();
    scheduler.health_tracker.remove("worker-1");

    // Health data should be gone
    assert!(scheduler.health_tracker.get("worker-1").is_none());
}

#[tokio::test]
async fn latency_tracking_accumulates() {
    let scheduler = TestScheduler::new();

    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Record multiple heartbeats with different latencies
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", Duration::from_millis(10));
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", Duration::from_millis(20));
    scheduler
        .health_tracker
        .record_heartbeat("worker-1", Duration::from_millis(30));

    let health = scheduler.health_tracker.get("worker-1").unwrap();

    // Average latency should be ~20ms
    let avg = health.latency_avg();
    assert!(avg >= Duration::from_millis(15) && avg <= Duration::from_millis(25));
}

#[tokio::test]
async fn mixed_status_workers_placement_prioritises_healthy() {
    let scheduler = TestScheduler::new();

    // Mix of Starting, Healthy, Degraded, and Unhealthy workers
    let starting = WorkerBuilder::new("starting-1")
        .with_function("greet")
        .with_status(WorkerStatus::Starting)
        .build();

    let healthy = WorkerBuilder::new("healthy-1")
        .with_function("greet")
        .healthy()
        .build();

    let degraded = WorkerBuilder::new("degraded-1")
        .with_function("greet")
        .with_status(WorkerStatus::Degraded)
        .build();

    let unhealthy = WorkerBuilder::new("unhealthy-1")
        .with_function("greet")
        .unhealthy()
        .build();

    scheduler.registry.register(starting).unwrap();
    scheduler.registry.register(healthy).unwrap();
    scheduler.registry.register(degraded).unwrap();
    scheduler.registry.register(unhealthy).unwrap();

    // Update placement
    let all_workers = scheduler.registry.list_for_function("greet");
    let endpoints: Vec<WorkerEndpoint> = all_workers
        .iter()
        .map(|w| WorkerEndpoint {
            worker_id: w.id.clone(),
            address: w.address,
            vsock_cid: w.vsock_cid,
            status: w.status,
        })
        .collect();

    scheduler
        .placement_store
        .set_workers("greet", endpoints)
        .await
        .unwrap();

    // Placement returns Available with Healthy and Degraded workers
    let availability = scheduler
        .placement_store
        .get_workers("greet")
        .await
        .unwrap();
    match availability {
        WorkerAvailability::Available(workers) => {
            assert_eq!(workers.len(), 2);
            let ids: Vec<_> = workers.iter().map(|w| w.worker_id.as_str()).collect();
            assert!(ids.contains(&"healthy-1"));
            assert!(ids.contains(&"degraded-1"));
        }
        other => panic!("Expected Available with 2 workers, got {other:?}"),
    }
}
