//! Sidereal build service binary.
//!
//! Runs the build service for compiling user code in sandboxed environments
//! and generating deployment artifacts.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use sidereal_build::{
    api, artifact::ArtifactStore, service::BuildWorker, BuildQueue, ServiceConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("sidereal_build=info".parse()?),
        )
        .init();

    info!("Sidereal build service starting");

    let config = ServiceConfig::load().unwrap_or_else(|e| {
        info!(error = %e, "failed to load config, using defaults");
        ServiceConfig::default()
    });

    info!(
        listen_addr = %config.server.listen_addr,
        worker_count = config.worker.count,
        "configuration loaded"
    );

    // Ensure directories exist
    ensure_directories(&config).await?;

    // Create artifact store
    let artifact_store = Arc::new(ArtifactStore::new(&config.storage)?);
    info!("artifact store initialised");

    // Create build queue
    let queue = Arc::new(BuildQueue::new(config.server.max_queue_size));
    info!(
        max_size = config.server.max_queue_size,
        "build queue initialised"
    );

    // Create cancellation token for graceful shutdown
    let cancel = CancellationToken::new();

    // Spawn build workers
    let worker_handles = spawn_workers(
        config.worker.count,
        Arc::clone(&queue),
        &config,
        Arc::clone(&artifact_store),
        cancel.clone(),
    );
    info!(count = config.worker.count, "build workers started");

    // Create API state and router
    let state = Arc::new(api::AppState {
        queue: Arc::clone(&queue),
    });
    let app = api::router(state);

    // Start HTTP server
    let listener = TcpListener::bind(&config.server.listen_addr).await?;
    info!(addr = %config.server.listen_addr, "build service listening");

    // Run server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(cancel.clone()))
        .await?;

    // Wait for workers to finish
    info!("waiting for build workers to finish");
    cancel.cancel();

    for handle in worker_handles {
        if let Err(e) = handle.await {
            error!(error = %e, "worker task failed");
        }
    }

    // Cancel any remaining builds
    queue
        .cancel_all(sidereal_build::CancelReason::Shutdown)
        .await;

    info!("build service shutdown complete");
    Ok(())
}

async fn ensure_directories(config: &ServiceConfig) -> Result<(), std::io::Error> {
    tokio::fs::create_dir_all(&config.paths.checkouts).await?;
    tokio::fs::create_dir_all(&config.paths.caches).await?;
    tokio::fs::create_dir_all(&config.paths.artifacts).await?;
    Ok(())
}

fn spawn_workers(
    count: usize,
    queue: Arc<BuildQueue>,
    config: &ServiceConfig,
    artifact_store: Arc<ArtifactStore>,
    cancel: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..count)
        .map(|id| {
            let worker = BuildWorker::new(
                id,
                Arc::clone(&queue),
                &config.paths,
                &config.limits,
                Arc::clone(&artifact_store),
            );
            let cancel = cancel.clone();
            tokio::spawn(async move {
                worker.run(cancel).await;
            })
        })
        .collect()
}

async fn shutdown_signal(cancel: CancellationToken) {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            error!(error = %e, "failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                error!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            info!("received Ctrl+C, initiating shutdown");
        }
        () = terminate => {
            info!("received SIGTERM, initiating shutdown");
        }
        () = cancel.cancelled() => {
            info!("shutdown requested");
        }
    }
}
