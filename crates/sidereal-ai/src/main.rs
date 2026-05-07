//! AI query companion for Sidereal.

mod auth;
mod client;
mod config;

use std::process;

use client::SiderealClient;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();

    if let Err(e) = run().await {
        tracing::error!(
            "error.type" = std::any::type_name_of_val(&e),
            "error.message" = %e,
            "error.stack" = %error_source_chain(&e),
        );
        process::exit(1);
    }
}

async fn run() -> Result<(), RunError> {
    let cfg = config::load()?;

    let token = auth::authenticate(&cfg.oidc).await?;

    let client = SiderealClient::new(cfg.sidereal.url.clone(), token);
    client.health().await?;

    tracing::info!(url = %cfg.sidereal.url, "connected to Sidereal");
    tracing::debug!(listen_address = %cfg.listen_address, "future HTTP API will bind here");

    let version_result = client.sql("SELECT 1 AS version").await;
    tracing::debug!(?version_result, "connectivity probe");

    Ok(())
}

fn error_source_chain(e: &dyn std::error::Error) -> String {
    let mut parts = Vec::new();
    let mut source = e.source();
    while let Some(s) = source {
        parts.push(s.to_string());
        source = s.source();
    }
    parts.join(" → ")
}

#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error("failed to load configuration: {0}")]
    Config(#[from] config::ConfigError),
    #[error("authentication failed: {0}")]
    Auth(#[from] auth::AuthError),
    #[error("connection to Sidereal failed: {0}")]
    Client(#[from] client::ClientError),
}
