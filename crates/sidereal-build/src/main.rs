//! Sidereal build service binary.
//!
//! Runs the build service for compiling user code in sandboxed environments
//! and generating deployment artifacts.

use std::sync::Arc;

use sidereal_core::Transport;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use sidereal_build::{api, service::BuildWorker, BuildQueue, ForgeAuth, ServiceConfig};

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
        listen = %config.server.listen,
        worker_count = config.worker.count,
        "configuration loaded"
    );

    // Ensure directories exist
    ensure_directories(&config).await?;

    // Create build queue
    let queue = Arc::new(BuildQueue::new(config.server.max_queue_size));
    info!(
        max_size = config.server.max_queue_size,
        "build queue initialised"
    );

    let cancel = CancellationToken::new();

    let forge_auth = ForgeAuth::from_config(&config.forge_auth)?;

    let worker_handles = spawn_workers(config.worker.count, &queue, &config, &cancel, &forge_auth)?;
    info!(count = config.worker.count, "build workers started");

    let state = Arc::new(api::AppState {
        queue: Arc::clone(&queue),
        forge_auth: forge_auth.clone(),
    });
    let app = api::router(state);

    // Start HTTP server
    info!(transport = %config.server.listen, "build service listening");
    serve_transport(config.server.listen, app, cancel.clone()).await?;

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
    queue: &Arc<BuildQueue>,
    config: &ServiceConfig,
    cancel: &CancellationToken,
    forge_auth: &ForgeAuth,
) -> Result<Vec<tokio::task::JoinHandle<()>>, sidereal_build::BuildError> {
    let mut handles = Vec::with_capacity(count);
    for id in 0..count {
        let worker = BuildWorker::new(
            id,
            Arc::clone(queue),
            &config.paths,
            &config.limits,
            forge_auth.clone(),
            &config.vm,
            &config.storage,
        )?;
        let cancel = cancel.clone();
        handles.push(tokio::spawn(async move {
            worker.run(cancel).await;
        }));
    }
    Ok(handles)
}

/// Serve an axum router over the given transport with graceful shutdown.
async fn serve_transport(
    transport: Transport,
    app: axum::Router,
    cancel: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    match transport {
        Transport::Tcp { addr } => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal(cancel))
                .await?;
        }
        Transport::Unix { path } => {
            if let Some(parent) = path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if path.exists() {
                tokio::fs::remove_file(&path).await?;
            }
            let listener = tokio::net::UnixListener::bind(&path)?;
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal(cancel))
                .await?;
        }
    }
    Ok(())
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
