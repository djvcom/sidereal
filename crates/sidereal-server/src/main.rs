//! Sidereal unified server binary.
//!
//! Runs all Sidereal services in a single process for single-node deployments.

use clap::Parser;
use sqlx::postgres::PgPoolOptions;
use tokio::signal;
use tracing::{error, info, warn};
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
    let mut config = ServerConfig::load(cli.config.as_deref()).unwrap_or_else(|e| {
        info!(error = %e, "Failed to load config, using defaults");
        ServerConfig::default()
    });

    // Load credentials from file if configured
    if let Err(e) = config.storage.load_credentials() {
        error!(error = %e, "Failed to load storage credentials from file");
    }

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

    // Run database migrations
    run_migrations(&config.database.url).await?;

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

/// Run database migrations before starting services.
async fn run_migrations(database_url: &str) -> anyhow::Result<()> {
    info!(url = %mask_password(database_url), "Connecting to database");

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await;

    let pool = match pool {
        Ok(pool) => pool,
        Err(e) => {
            warn!(
                error = %e,
                "Failed to connect to database, skipping migrations"
            );
            return Ok(());
        }
    };

    info!("Running database migrations");

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .map_err(|e| anyhow::anyhow!("Migration failed: {e}"))?;

    info!("Database migrations complete");

    pool.close().await;
    Ok(())
}

/// Mask the password in a database URL for logging.
fn mask_password(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            if let Some(slash_pos) = url[..colon_pos].rfind('/') {
                let prefix = &url[..slash_pos + 3];
                let suffix = &url[at_pos..];
                if let Some(user_end) = url[slash_pos + 3..colon_pos].find(':') {
                    let user = &url[slash_pos + 3..slash_pos + 3 + user_end];
                    return format!("{prefix}{user}:***{suffix}");
                }
                return format!("{prefix}***{suffix}");
            }
        }
    }
    url.to_string()
}
