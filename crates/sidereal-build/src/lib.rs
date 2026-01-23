//! Sandboxed build service for Sidereal projects.
//!
//! This crate provides:
//! - Sandboxed compilation using bubblewrap
//! - Dependency validation and auditing
//! - Build caching with per-commit isolation
//! - Artifact generation (rootfs for Firecracker)
//!
//! # Architecture
//!
//! The build service receives requests from Forge (git integration layer),
//! compiles Rust code in isolation, and produces deployment artifacts.
//!
//! ```text
//! ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
//! │  HTTP API    │───▶│ Build Queue  │───▶│   Executor   │
//! └──────────────┘    └──────────────┘    └──────────────┘
//!                                                │
//!         ┌──────────────────────────────────────┤
//!         ▼                                      ▼
//!  ┌──────────────┐                      ┌──────────────┐
//!  │    Source    │                      │   Sandbox    │
//!  │   Checkout   │                      │  (bwrap)     │
//!  └──────────────┘                      └──────────────┘
//! ```

pub mod api;
pub mod artifact;
pub mod cache;
mod config;
pub mod discovery;
pub mod env;
pub mod error;
pub mod forge_auth;
pub mod project;
pub mod protocol;
pub mod queue;
pub mod sandbox;
pub mod service;
pub mod source;
pub mod types;
pub mod vm;

// Re-export configuration types (legacy build.rs support)
pub use config::{DevConfig, ProjectConfig, QueueConfig, ResourcesConfig, SiderealConfig};

// Re-export error types
pub use error::{BuildError, BuildResult, BuildStage, CancelReason};

// Re-export core types
pub use types::{
    ArtifactId, BuildId, BuildMetadata, BuildRequest, BuildStatus, FunctionMetadata, ProjectId,
};

// Re-export environment validation
pub use env::EnvironmentCheck;

// Re-export queue types
pub use queue::{BuildHandle, BuildQueue};

// Re-export source types
pub use source::{SourceCheckout, SourceManager};

// Re-export sandbox types
pub use sandbox::{
    BinaryInfo, CompileOutput, SandboxConfig, SandboxedCompiler, WorkspaceCompileOutput,
};

// Re-export cache types
pub use cache::{CacheConfig, CacheManager, CacheResult};

// Re-export project types
pub use project::{discover_projects, DeployableProject};

// Re-export artifact types
pub use artifact::{Artifact, ArtifactBuilder, ArtifactManifest, RootfsBuilder};

// Re-export API types
pub use api::{router as api_router, AppState as ApiAppState};

// Re-export service types
pub use service::{BuildWorker, ServiceConfig};

// Re-export forge authentication types
pub use forge_auth::{ForgeAuth, ForgeAuthConfig};

// Re-export protocol types (with aliases to avoid conflicts with existing types)
pub use protocol::{
    BinaryInfo as VmBinaryInfo, BuildMessage, BuildOutput, BuildRequest as VmBuildRequest,
    BuildResult as VmBuildResult, BUILD_PORT,
};

// Re-export VM compiler types
pub use vm::{FirecrackerCompiler, VmCompilerConfig};

use std::path::PathBuf;

/// Builder for configuring and running build-time validation.
///
/// This is primarily for use in `build.rs` scripts.
#[must_use]
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
        let config: SiderealConfig =
            toml::from_str(&content).map_err(|e| BuildError::ConfigParse(e.to_string()))?;

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
        .map(|queues| queues.keys().map(std::string::String::as_str).collect())
        .unwrap_or_default()
}
