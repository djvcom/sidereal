//! Configuration parsing for runtime validation.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct SiderealConfig {
    pub project: ProjectConfig,
    #[serde(default)]
    pub dev: DevConfig,
    pub resources: Option<ResourcesConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProjectConfig {
    pub name: String,
    #[serde(default = "default_version")]
    pub version: String,
}

fn default_version() -> String {
    "0.1.0".to_string()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DevConfig {
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_port() -> u16 {
    7850
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourcesConfig {
    pub queue: Option<HashMap<String, QueueConfig>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    pub retention: Option<String>,
    #[serde(default)]
    pub dead_letter: bool,
}

impl SiderealConfig {
    /// Load configuration from sidereal.toml in the current directory.
    pub fn load() -> Option<Self> {
        Self::load_from("sidereal.toml")
    }

    /// Load configuration from a specific path.
    pub fn load_from(path: impl AsRef<Path>) -> Option<Self> {
        let content = std::fs::read_to_string(path).ok()?;
        toml::from_str(&content).ok()
    }

    /// Get the names of all declared queues.
    pub fn declared_queues(&self) -> Vec<&str> {
        self.resources
            .as_ref()
            .and_then(|r| r.queue.as_ref())
            .map(|queues| queues.keys().map(|s| s.as_str()).collect())
            .unwrap_or_default()
    }
}
