//! Sidereal scheduler binary.
//!
//! Runs the scheduler service for worker registration, health tracking,
//! and placement management.

use figment::providers::{Env, Format, Toml};
use figment::Figment;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use sidereal_scheduler::{
    api, HealthTracker, PlacementAlgorithmType, PlacementStore, PowerOfTwoChoices,
    RoundRobin, ScalingPolicy, SchedulerConfig, ValkeyPlacementStore, WorkerRegistry, LeastLoaded,
    PlacementAlgorithm,
};
use sidereal_scheduler::store::InMemoryPlacementStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialise tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("sidereal_scheduler=info".parse()?))
        .init();

    info!("Sidereal scheduler starting");

    // Load configuration
    let config: SchedulerConfig = Figment::new()
        .merge(Toml::file("scheduler.toml"))
        .merge(Env::prefixed("SCHEDULER_").split("_"))
        .extract()?;

    info!(listen_addr = %config.api.listen_addr, "Configuration loaded");

    // Create registry
    let registry = Arc::new(WorkerRegistry::new());
    info!("Worker registry initialised");

    // Create health tracker
    let health_tracker = Arc::new(HealthTracker::new(config.health.clone(), registry.clone()));
    info!(
        heartbeat_timeout_secs = config.health.heartbeat_timeout.as_secs(),
        "Health tracker initialised"
    );

    // Create placement algorithm
    let placement_algorithm: Arc<dyn PlacementAlgorithm> = match config.placement.algorithm {
        PlacementAlgorithmType::RoundRobin => Arc::new(RoundRobin::new()),
        PlacementAlgorithmType::PowerOfTwo => Arc::new(PowerOfTwoChoices::new()),
        PlacementAlgorithmType::LeastLoaded => Arc::new(LeastLoaded::new()),
    };
    info!(algorithm = placement_algorithm.name(), "Placement algorithm configured");

    // Create scaling policy
    let scaling_policy = Arc::new(ScalingPolicy::new(config.scaling.clone()));
    info!(
        min_workers = config.scaling.min_workers,
        max_workers = config.scaling.max_workers,
        "Scaling policy configured"
    );

    // Create placement store
    let placement_store: Arc<dyn PlacementStore> = match try_connect_valkey(&config).await {
        Ok(store) => {
            info!(url = %config.valkey.url, "Connected to Valkey");
            Arc::new(store)
        }
        Err(e) => {
            error!(error = %e, "Failed to connect to Valkey, using in-memory store");
            Arc::new(InMemoryPlacementStore::new())
        }
    };

    // Build application state
    let state = Arc::new(api::AppState {
        registry: registry.clone(),
        health_tracker: health_tracker.clone(),
        scaling_policy,
        placement_store,
    });

    // Start background tasks
    let registry_clone = registry.clone();
    let health_clone = health_tracker.clone();
    tokio::spawn(async move {
        run_health_check_loop(registry_clone, health_clone).await;
    });

    // Build router
    let app = api::router(state);

    // Start HTTP server
    let listener = TcpListener::bind(&config.api.listen_addr).await?;
    info!(addr = %config.api.listen_addr, "Scheduler API listening");

    axum::serve(listener, app).await?;

    Ok(())
}

async fn try_connect_valkey(config: &SchedulerConfig) -> Result<ValkeyPlacementStore, Box<dyn std::error::Error>> {
    Ok(ValkeyPlacementStore::new(&config.valkey).await?)
}

async fn run_health_check_loop(_registry: Arc<WorkerRegistry>, health_tracker: Arc<HealthTracker>) {
    let interval = health_tracker.heartbeat_interval();
    let mut ticker = tokio::time::interval(interval);

    loop {
        ticker.tick().await;

        // Check for timed-out workers
        let timed_out = health_tracker.check_heartbeat_timeouts();
        for worker_id in timed_out {
            info!(worker_id = %worker_id, "Worker heartbeat timeout");
        }
    }
}
