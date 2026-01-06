//! Implementation of the `sidereal dev` command.

use serde::Deserialize;
use std::path::PathBuf;
use std::process::Command;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DevError {
    #[error("Configuration file not found: sidereal.toml")]
    ConfigNotFound,

    #[error("Build failed: {0}")]
    BuildFailed(String),

    #[error("cargo-watch not installed. Install with: cargo install cargo-watch")]
    CargoWatchNotInstalled,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Toml(#[from] toml::de::Error),
}

#[derive(Deserialize)]
struct SiderealConfig {
    project: ProjectConfig,
    #[serde(default)]
    dev: DevConfig,
}

#[derive(Deserialize)]
struct ProjectConfig {
    name: String,
    #[allow(dead_code)]
    version: String,
}

#[derive(Deserialize, Default)]
struct DevConfig {
    #[serde(default = "default_port")]
    port: u16,
}

fn default_port() -> u16 {
    7850
}

fn load_config() -> Result<SiderealConfig, DevError> {
    let config_path = PathBuf::from("sidereal.toml");
    if !config_path.exists() {
        return Err(DevError::ConfigNotFound);
    }

    let content = std::fs::read_to_string(config_path)?;
    let config: SiderealConfig = toml::from_str(&content)?;
    Ok(config)
}

fn check_cargo_watch() -> Result<(), DevError> {
    let output = Command::new("cargo").args(["watch", "--version"]).output();

    match output {
        Ok(output) if output.status.success() => Ok(()),
        _ => Err(DevError::CargoWatchNotInstalled),
    }
}

pub async fn run(port_override: Option<u16>) -> Result<(), DevError> {
    // Load configuration
    let config = load_config()?;
    let port = port_override.unwrap_or(config.dev.port);

    // Check for cargo-watch
    check_cargo_watch()?;

    println!(
        "Starting development server for '{}'...",
        config.project.name
    );
    println!();
    println!("Using cargo-watch for hot reload.");
    println!("Server will start on http://localhost:{}", port);
    println!();

    // Set the port via environment variable
    // The user's main.rs can read SIDEREAL_PORT to configure the server
    let status = Command::new("cargo")
        .args(["watch", "-x", "run"])
        .env("SIDEREAL_PORT", port.to_string())
        .status()?;

    if !status.success() {
        return Err(DevError::BuildFailed("cargo watch failed".to_string()));
    }

    Ok(())
}
