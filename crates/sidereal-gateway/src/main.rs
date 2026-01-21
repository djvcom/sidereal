//! Sidereal gateway binary.
//!
//! HTTP ingress gateway that routes requests to backend workers.

use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use sidereal_gateway::{run, GatewayConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _otel_guard = opentelemetry_configuration::OtelSdkBuilder::new()
        .with_standard_env()
        .service_name(env!("CARGO_PKG_NAME"))
        .build()
        .ok();

    info!("Sidereal gateway starting");

    let config = load_config();

    info!(
        bind_address = %config.server.bind_address,
        routing_mode = routing_mode_name(&config.routing),
        "Configuration loaded"
    );

    let cancel = CancellationToken::new();

    let cancel_on_signal = cancel.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        info!("Shutdown signal received, initiating graceful shutdown");
        cancel_on_signal.cancel();
    });

    if let Err(e) = run(config, cancel).await {
        error!(error = %e, "Gateway error");
        return Err(e.into());
    }

    info!("Gateway shutdown complete");
    Ok(())
}

fn load_config() -> GatewayConfig {
    match GatewayConfig::load() {
        Ok(config) => config,
        Err(e) => {
            info!(error = %e, "Failed to load gateway.toml, using default configuration");
            default_config()
        }
    }
}

fn default_config() -> GatewayConfig {
    use sidereal_gateway::config::RoutingConfig;
    use std::collections::HashMap;

    GatewayConfig {
        server: Default::default(),
        routing: RoutingConfig::Static {
            functions: HashMap::new(),
            load_balance: Default::default(),
        },
        middleware: Default::default(),
        limits: Default::default(),
        metrics: None,
    }
}

fn routing_mode_name(routing: &sidereal_gateway::config::RoutingConfig) -> &'static str {
    match routing {
        sidereal_gateway::config::RoutingConfig::Static { .. } => "static",
        sidereal_gateway::config::RoutingConfig::Discovery { .. } => "discovery",
        sidereal_gateway::config::RoutingConfig::Scheduler(_) => "scheduler",
    }
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
            info!("Received Ctrl+C");
        }
        () = terminate => {
            info!("Received SIGTERM");
        }
    }
}
