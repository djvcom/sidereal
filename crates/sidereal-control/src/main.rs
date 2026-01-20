//! Sidereal control service binary.
//!
//! Runs the control plane for orchestrating deployments.

use tracing::info;
use tracing_subscriber::EnvFilter;

use sidereal_control::ControlConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialise tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("sidereal_control=info".parse()?),
        )
        .init();

    info!("Sidereal control service starting");

    // Load configuration
    let config = ControlConfig::load().unwrap_or_else(|e| {
        info!(error = %e, "failed to load config, using defaults");
        ControlConfig::default()
    });

    info!(
        listen_addr = %config.server.listen_addr,
        database = %config.database.url,
        scheduler = %config.scheduler.url,
        "configuration loaded"
    );

    // TODO: Implement service startup
    // - Connect to database
    // - Create deployment store
    // - Create scheduler client
    // - Create worker provisioner
    // - Create deployment manager
    // - Start HTTP API server
    // - Run until shutdown signal

    info!("sidereal-control service not yet fully implemented");

    Ok(())
}
