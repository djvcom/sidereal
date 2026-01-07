//! Common test utilities for scheduler integration tests.

pub mod fixtures;

use sidereal_scheduler::{
    api::AppState,
    config::{HealthConfig, ScalingConfig},
    store::{InMemoryPlacementStore, PlacementStore},
    HealthTracker, ScalingPolicy, WorkerRegistry,
};
use std::sync::Arc;

/// Complete test scheduler setup with all components wired together.
pub struct TestScheduler {
    pub registry: Arc<WorkerRegistry>,
    pub health_tracker: Arc<HealthTracker>,
    pub scaling_policy: Arc<ScalingPolicy>,
    pub placement_store: Arc<dyn PlacementStore>,
    pub app_state: Arc<AppState>,
}

impl TestScheduler {
    /// Creates a new test scheduler with default configuration.
    pub fn new() -> Self {
        Self::with_config(HealthConfig::default(), ScalingConfig::default())
    }

    /// Creates a new test scheduler with custom health and scaling configuration.
    pub fn with_config(health_config: HealthConfig, scaling_config: ScalingConfig) -> Self {
        let registry = Arc::new(WorkerRegistry::new());
        let health_tracker = Arc::new(HealthTracker::new(health_config, registry.clone()));
        let scaling_policy = Arc::new(ScalingPolicy::new(scaling_config));
        let placement_store: Arc<dyn PlacementStore> = Arc::new(InMemoryPlacementStore::new());

        let app_state = Arc::new(AppState {
            registry: registry.clone(),
            health_tracker: health_tracker.clone(),
            scaling_policy: scaling_policy.clone(),
            placement_store: placement_store.clone(),
        });

        Self {
            registry,
            health_tracker,
            scaling_policy,
            placement_store,
            app_state,
        }
    }

    /// Creates a test scheduler with fast health checks for time-sensitive tests.
    pub fn with_fast_health_checks() -> Self {
        use std::time::Duration;

        let health_config = HealthConfig {
            heartbeat_interval: Duration::from_millis(50),
            heartbeat_timeout: Duration::from_millis(200),
            unhealthy_threshold: 2,
            healthy_threshold: 2,
            ping_interval: Duration::from_millis(100),
        };

        Self::with_config(health_config, ScalingConfig::default())
    }
}

impl Default for TestScheduler {
    fn default() -> Self {
        Self::new()
    }
}
