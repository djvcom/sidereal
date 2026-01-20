//! Sidereal control service binary.
//!
//! Runs the control plane for orchestrating deployments.

use tracing::info;
use tracing_subscriber::EnvFilter;

use sidereal_control::ControlConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("sidereal_control=info".parse()?),
        )
        .init();

    info!("Sidereal control service starting");

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

    info!("sidereal-control service not yet fully implemented");

    Ok(())
}
