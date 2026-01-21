//! Sidereal unified server binary.
//!
//! Runs all Sidereal services in a single process for single-node deployments.

use clap::Parser;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

mod config;
mod services;

use config::ServerConfig;
use services::Services;

/// Sidereal unified server.
#[derive(Parser, Debug)]
#[command(name = "sidereal-server")]
#[command(about = "Run all Sidereal services in a single process")]
#[command(version)]
struct Cli {
    /// Path to configuration file.
    #[arg(short, long, value_name = "FILE")]
    config: Option<String>,

    /// Enable verbose logging.
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Initialise tracing
    let filter = if cli.verbose {
        "debug,hyper=info,tower=info"
    } else {
        "info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter)),
        )
        .init();

    info!("Sidereal server starting");

    // Load configuration
    let config = ServerConfig::load(cli.config.as_deref()).unwrap_or_else(|e| {
        info!(error = %e, "Failed to load config, using defaults");
        ServerConfig::default()
    });

    info!(
        mode = ?config.server.mode,
        socket_dir = %config.server.socket_dir.display(),
        gateway_enabled = config.gateway.enabled,
        scheduler_enabled = config.scheduler.enabled,
        control_enabled = config.control.enabled,
        build_enabled = config.build.enabled,
        "Configuration loaded"
    );

    // Ensure socket directory exists
    if let Err(e) = tokio::fs::create_dir_all(&config.server.socket_dir).await {
        error!(
            error = %e,
            path = %config.server.socket_dir.display(),
            "Failed to create socket directory"
        );
    }

    // Ensure data directory exists
    if let Err(e) = tokio::fs::create_dir_all(&config.server.data_dir).await {
        error!(
            error = %e,
            path = %config.server.data_dir.display(),
            "Failed to create data directory"
        );
    }

    // Create and start services
    let mut services = Services::new(config);

    services.start()?;

    // Set up signal handlers
    let cancel = services.cancel_token();
    tokio::spawn(async move {
        shutdown_signal().await;
        cancel.cancel();
    });

    // Wait for services or shutdown
    services.wait().await;

    info!("Sidereal server shutdown complete");
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            error!(error = %e, "Failed to install Ctrl+C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(e) => {
                error!(error = %e, "Failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            info!("Received Ctrl+C, initiating shutdown");
        }
        () = terminate => {
            info!("Received SIGTERM, initiating shutdown");
        }
    }
}
