//! Build-time validation for Sidereal projects.
//!
//! This crate provides configuration parsing and validation for use in
//! `build.rs` scripts.
//!
//! # Example
//!
//! ```ignore
//! // In build.rs
//! fn main() {
//!     sidereal_build::configure()
//!         .config_path("sidereal.toml")
//!         .validate()
//!         .expect("Sidereal validation failed");
//! }
//! ```

mod config;

pub use config::{DevConfig, ProjectConfig, QueueConfig, ResourcesConfig, SiderealConfig};

use std::path::PathBuf;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BuildError {
    #[error("Configuration file not found: {0}")]
    ConfigNotFound(PathBuf),

    #[error("Failed to read configuration: {0}")]
    ReadError(#[from] std::io::Error),

    #[error("Failed to parse configuration: {0}")]
    ParseError(#[from] toml::de::Error),

    #[error("Validation error: {0}")]
    ValidationError(String),
}

/// Builder for configuring and running build-time validation.
pub struct ConfigureBuilder {
    config_path: PathBuf,
}

impl Default for ConfigureBuilder {
    fn default() -> Self {
        Self {
            config_path: PathBuf::from("sidereal.toml"),
        }
    }
}

impl ConfigureBuilder {
    /// Set the path to the sidereal.toml configuration file.
    pub fn config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.config_path = path.into();
        self
    }

    /// Parse and validate the configuration.
    ///
    /// Returns the parsed configuration if successful.
    pub fn validate(self) -> Result<SiderealConfig, BuildError> {
        if !self.config_path.exists() {
            return Err(BuildError::ConfigNotFound(self.config_path));
        }

        let content = std::fs::read_to_string(&self.config_path)?;
        let config: SiderealConfig = toml::from_str(&content)?;

        // Emit rerun-if-changed for cargo
        println!("cargo::rerun-if-changed={}", self.config_path.display());

        Ok(config)
    }

    /// Parse the configuration without validation.
    pub fn parse(self) -> Result<SiderealConfig, BuildError> {
        self.validate()
    }
}

/// Start configuring build-time validation.
///
/// # Example
///
/// ```ignore
/// sidereal_build::configure()
///     .config_path("sidereal.toml")
///     .validate()
///     .expect("Sidereal validation failed");
/// ```
pub fn configure() -> ConfigureBuilder {
    ConfigureBuilder::default()
}

/// Parse sidereal.toml from the default location.
///
/// This is a convenience function for simple use cases.
pub fn parse_config() -> Result<SiderealConfig, BuildError> {
    configure().validate()
}

/// Get the list of declared queue names from the configuration.
pub fn get_declared_queues(config: &SiderealConfig) -> Vec<&str> {
    config
        .resources
        .as_ref()
        .and_then(|r| r.queue.as_ref())
        .map(|queues| queues.keys().map(|s| s.as_str()).collect())
        .unwrap_or_default()
}
