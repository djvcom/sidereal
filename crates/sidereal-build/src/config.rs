//! Configuration structures for sidereal.toml.

use serde::Deserialize;
use std::collections::HashMap;

/// Root configuration structure for sidereal.toml.
#[derive(Debug, Clone, Deserialize)]
pub struct SiderealConfig {
    /// Project metadata.
    pub project: ProjectConfig,

    /// Development server settings.
    #[serde(default)]
    pub dev: DevConfig,

    /// Resource definitions (queues, databases, etc.).
    pub resources: Option<ResourcesConfig>,
}

/// Project metadata.
#[derive(Debug, Clone, Deserialize)]
pub struct ProjectConfig {
    /// The project name.
    pub name: String,

    /// The project version.
    #[serde(default = "default_version")]
    pub version: String,
}

fn default_version() -> String {
    "0.1.0".to_owned()
}

/// Development server configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct DevConfig {
    /// The port to run the dev server on.
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for DevConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
        }
    }
}

const fn default_port() -> u16 {
    7850
}

/// Resource definitions.
#[derive(Debug, Clone, Deserialize)]
pub struct ResourcesConfig {
    /// Queue definitions.
    pub queue: Option<HashMap<String, QueueConfig>>,

    /// PostgreSQL database definitions.
    pub postgres: Option<HashMap<String, PostgresConfig>>,

    /// Redis definitions.
    pub redis: Option<HashMap<String, RedisConfig>>,
}

/// Queue resource configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    /// Message retention period (e.g., "7d", "24h").
    pub retention: Option<String>,

    /// Whether to enable dead letter queue for failed messages.
    #[serde(default)]
    pub dead_letter: bool,
}

/// PostgreSQL resource configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    /// Connection string or environment variable reference.
    pub connection: Option<String>,
}

/// Redis resource configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct RedisConfig {
    /// Connection string or environment variable reference.
    pub connection: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let toml = r#"
            [project]
            name = "test-project"
        "#;

        let config: SiderealConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.project.name, "test-project");
        assert_eq!(config.project.version, "0.1.0");
        assert_eq!(config.dev.port, 7850);
    }

    #[test]
    fn test_parse_full_config() {
        let toml = r#"
            [project]
            name = "order-service"
            version = "1.0.0"

            [dev]
            port = 8080

            [resources.queue.order-events]
            retention = "7d"
            dead_letter = true

            [resources.queue.notifications]
            retention = "1d"
        "#;

        let config: SiderealConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.project.name, "order-service");
        assert_eq!(config.project.version, "1.0.0");
        assert_eq!(config.dev.port, 8080);

        let queues = config.resources.unwrap().queue.unwrap();
        assert_eq!(queues.len(), 2);
        assert!(queues.contains_key("order-events"));
        assert!(queues.contains_key("notifications"));

        let order_events = &queues["order-events"];
        assert_eq!(order_events.retention.as_deref(), Some("7d"));
        assert!(order_events.dead_letter);
    }
}
