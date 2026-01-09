//! Configuration types for secrets backends.

use serde::Deserialize;

use crate::types::SecretContext;

/// Configuration for secrets management.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct SecretsConfig {
    /// The backend to use for secrets storage.
    #[serde(default)]
    pub backend: SecretsBackendConfig,

    /// The current project context.
    #[serde(default)]
    pub project: Option<String>,

    /// The current environment context.
    #[serde(default)]
    pub environment: Option<String>,
}

impl SecretsConfig {
    /// Creates a context from the configuration.
    pub fn to_context(&self) -> SecretContext {
        let mut ctx = SecretContext::new();
        if let Some(project) = &self.project {
            ctx = ctx.with_project(project);
        }
        if let Some(environment) = &self.environment {
            ctx = ctx.with_environment(environment);
        }
        ctx
    }
}

/// Backend configuration variants.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(tag = "backend", rename_all = "lowercase")]
pub enum SecretsBackendConfig {
    /// In-memory backend for testing.
    #[default]
    Memory,

    /// Environment variable backend.
    #[cfg(feature = "env")]
    Env {
        /// Environment variable prefix (default: `SIDEREAL`).
        #[serde(default = "default_env_prefix")]
        prefix: String,
    },

    /// Native encrypted backend using age and SQLite.
    #[cfg(feature = "native")]
    Native {
        /// Path to the SQLite database file.
        db_path: String,
        /// Path to the age identity file.
        identity_path: String,
    },
}

#[cfg(feature = "env")]
fn default_env_prefix() -> String {
    "SIDEREAL".to_owned()
}
