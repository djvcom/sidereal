//! Integration tests for worker lifecycle scenarios.

mod common;

use common::{fixtures::WorkerBuilder, TestScheduler};
use sidereal_scheduler::registry::WorkerStatus;
use sidereal_scheduler::store::{PlacementStore, WorkerAvailability, WorkerEndpoint};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn worker_registration_through_to_healthy_placement() {
    let scheduler = TestScheduler::with_fast_health_checks();

    // Register worker in Starting state
    let worker = WorkerBuilder::new("worker-1")
        .with_functions(vec!["greet"])
        .with_status(WorkerStatus::Starting)
        .build();

    scheduler.registry.register(worker).unwrap();

    // Worker registered but not yet healthy
    let retrieved = scheduler.registry.get("worker-1").unwrap();
    assert_eq!(retrieved.status, WorkerStatus::Starting);

    // Simulate successful heartbeats to transition to Healthy
    for _ in 0..3 {
        scheduler
            .health_tracker
            .record_heartbeat("worker-1", Duration::from_millis(10));
        sleep(Duration::from_millis(20)).await;
    }

    // Worker should now be Healthy
    let retrieved = scheduler.registry.get("worker-1").unwrap();
    assert_eq!(retrieved.status, WorkerStatus::Healthy);

    // Update placement in store (simulating scheduler's placement loop)
    let healthy_workers = scheduler.registry.list_healthy_for_function("greet");
    let endpoints: Vec<WorkerEndpoint> = healthy_workers
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

    // Placement store returns worker as available
    let availability = scheduler
        .placement_store
        .get_workers("greet")
        .await
        .unwrap();

    match availability {
        WorkerAvailability::Available(workers) => {
            assert_eq!(workers.len(), 1);
            assert_eq!(workers[0].worker_id, "worker-1");
            assert_eq!(workers[0].status, WorkerStatus::Healthy);
        }
        other => panic!("Expected Available, got {other:?}"),
    }
}

#[tokio::test]
async fn worker_deregistration_removes_from_all_placements() {
    let scheduler = TestScheduler::new();

    // Register worker handling multiple functions
    let worker = WorkerBuilder::new("worker-1")
        .with_functions(vec!["greet", "farewell"])
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Setup placements for both functions
    let endpoint = WorkerEndpoint {
        worker_id: "worker-1".to_string(),
        address: "127.0.0.1:8080".parse().unwrap(),
        vsock_cid: None,
        status: WorkerStatus::Healthy,
    };

    scheduler
        .placement_store
        .set_workers("greet", vec![endpoint.clone()])
        .await
        .unwrap();

    scheduler
        .placement_store
        .set_workers("farewell", vec![endpoint])
        .await
        .unwrap();

    // Verify placements exist
    assert!(matches!(
        scheduler
            .placement_store
            .get_workers("greet")
            .await
            .unwrap(),
        WorkerAvailability::Available(_)
    ));

    // Deregister worker and clean up all state
    scheduler.registry.deregister("worker-1").unwrap();
    scheduler.health_tracker.remove("worker-1");
    scheduler
        .placement_store
        .remove_worker(&"worker-1".to_string())
        .await
        .unwrap();

    // Worker removed from registry
    assert!(scheduler.registry.get("worker-1").is_none());

    // Worker removed from function index
    assert_eq!(scheduler.registry.list_for_function("greet").len(), 0);
    assert_eq!(scheduler.registry.list_for_function("farewell").len(), 0);

    // Worker removed from placements (empty placements = NotFound)
    let greet_availability = scheduler
        .placement_store
        .get_workers("greet")
        .await
        .unwrap();
    let farewell_availability = scheduler
        .placement_store
        .get_workers("farewell")
        .await
        .unwrap();

    assert!(matches!(greet_availability, WorkerAvailability::NotFound));
    assert!(matches!(
        farewell_availability,
        WorkerAvailability::NotFound
    ));

    // Health data cleaned up
    assert!(scheduler.health_tracker.get("worker-1").is_none());
}

#[tokio::test]
async fn multiple_workers_for_same_function() {
    let scheduler = TestScheduler::new();

    // Register three workers for the same function
    for i in 0..3 {
        let worker = WorkerBuilder::new(&format!("worker-{i}"))
            .with_function("greet")
            .healthy()
            .build();
        scheduler.registry.register(worker).unwrap();
    }

    // All workers appear in function listing
    let workers = scheduler.registry.list_for_function("greet");
    assert_eq!(workers.len(), 3);

    // Update placement store
    let endpoints: Vec<WorkerEndpoint> = workers
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

    // Placement returns all workers
    let availability = scheduler
        .placement_store
        .get_workers("greet")
        .await
        .unwrap();
    match availability {
        WorkerAvailability::Available(workers) => {
            assert_eq!(workers.len(), 3);
        }
        other => panic!("Expected Available, got {other:?}"),
    }

    // Remove one worker
    scheduler.registry.deregister("worker-1").unwrap();
    scheduler
        .placement_store
        .remove_worker(&"worker-1".to_string())
        .await
        .unwrap();

    // Placement now returns two workers
    let availability = scheduler
        .placement_store
        .get_workers("greet")
        .await
        .unwrap();
    match availability {
        WorkerAvailability::Available(workers) => {
            assert_eq!(workers.len(), 2);
            assert!(!workers.iter().any(|w| w.worker_id == "worker-1"));
        }
        other => panic!("Expected Available, got {other:?}"),
    }
}

#[tokio::test]
async fn worker_drain_updates_status_but_remains_registered() {
    let scheduler = TestScheduler::new();

    let worker = WorkerBuilder::new("worker-1")
        .with_function("greet")
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Drain the worker
    scheduler
        .registry
        .update_status("worker-1", WorkerStatus::Draining)
        .unwrap();

    // Worker still exists but is draining
    let retrieved = scheduler.registry.get("worker-1").unwrap();
    assert_eq!(retrieved.status, WorkerStatus::Draining);

    // Worker still in registry
    assert_eq!(scheduler.registry.len(), 1);

    // Worker no longer appears in healthy list
    let healthy = scheduler.registry.list_healthy_for_function("greet");
    assert!(healthy.is_empty());

    // But still appears in full function list
    let all = scheduler.registry.list_for_function("greet");
    assert_eq!(all.len(), 1);
}

#[tokio::test]
async fn worker_with_vsock_cid_preserved_in_placement() {
    let scheduler = TestScheduler::new();

    let worker = WorkerBuilder::new("vm-worker-1")
        .with_function("compute")
        .with_vsock_cid(42)
        .healthy()
        .build();

    scheduler.registry.register(worker).unwrap();

    // Update placement
    let workers = scheduler.registry.list_healthy_for_function("compute");
    let endpoints: Vec<WorkerEndpoint> = workers
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
        .set_workers("compute", endpoints)
        .await
        .unwrap();

    // vsock CID preserved in placement
    let availability = scheduler
        .placement_store
        .get_workers("compute")
        .await
        .unwrap();

    match availability {
        WorkerAvailability::Available(workers) => {
            assert_eq!(workers.len(), 1);
            assert_eq!(workers[0].vsock_cid, Some(42));
        }
        other => panic!("Expected Available, got {other:?}"),
    }
}
