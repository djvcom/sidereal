//! Error types for the build service.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Build stage for error context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildStage {
    /// Source checkout from git.
    Checkout,
    /// Dependency fetching.
    FetchDeps,
    /// Dependency auditing.
    Audit,
    /// Compilation.
    Compile,
    /// Artifact generation.
    Artifact,
}

impl std::fmt::Display for BuildStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Checkout => write!(f, "checkout"),
            Self::FetchDeps => write!(f, "fetch-deps"),
            Self::Audit => write!(f, "audit"),
            Self::Compile => write!(f, "compile"),
            Self::Artifact => write!(f, "artifact"),
        }
    }
}

/// Errors that can occur during build operations.
#[derive(Debug, Error)]
pub enum BuildError {
    // ─────────────────────────────────────────────────────────────────────────
    // Environment errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Required tools are missing from the environment.
    #[error("missing required tools: {}", tools.join(", "))]
    MissingTools {
        /// List of missing tool names.
        tools: Vec<String>,
    },

    // ─────────────────────────────────────────────────────────────────────────
    // Configuration errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Configuration file not found.
    #[error("configuration file not found: {0}")]
    ConfigNotFound(PathBuf),

    /// Failed to read configuration.
    #[error("failed to read configuration: {0}")]
    ConfigRead(#[from] std::io::Error),

    /// Failed to parse configuration.
    #[error("failed to parse configuration: {0}")]
    ConfigParse(String),

    /// Invalid configuration value.
    #[error("invalid configuration: {0}")]
    ConfigInvalid(String),

    // ─────────────────────────────────────────────────────────────────────────
    // Source checkout errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Git clone failed.
    #[error("failed to clone repository {url}: {message}")]
    GitClone {
        /// Repository URL.
        url: String,
        /// Error message.
        message: String,
    },

    /// Git fetch failed.
    #[error("failed to fetch from repository: {0}")]
    GitFetch(String),

    /// Git checkout failed.
    #[error("failed to checkout commit {commit}: {message}")]
    GitCheckout {
        /// Commit SHA.
        commit: String,
        /// Error message.
        message: String,
    },

    /// Invalid branch name (path traversal, special characters).
    #[error("invalid branch name: {name} ({reason})")]
    InvalidBranchName {
        /// The invalid branch name.
        name: String,
        /// Reason for rejection.
        reason: String,
    },

    /// Symlink outside repository detected.
    #[error("symlink escape detected: {path} points outside repository")]
    SymlinkEscape {
        /// Path to the offending symlink.
        path: PathBuf,
    },

    // ─────────────────────────────────────────────────────────────────────────
    // Dependency errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Cargo fetch failed.
    #[error("failed to fetch dependencies: {0}")]
    CargoFetch(String),

    /// Dependency audit found vulnerabilities.
    #[error("audit found {count} {}: {}", if *count == 1 { "vulnerability" } else { "vulnerabilities" }, advisories.join(", "))]
    AuditFailed {
        /// Number of vulnerabilities found.
        count: usize,
        /// List of advisory IDs.
        advisories: Vec<String>,
    },

    /// Blocked crate detected.
    #[error("blocked crate: {name} ({reason})")]
    BlockedCrate {
        /// Crate name.
        name: String,
        /// Reason for blocking.
        reason: String,
    },

    /// Git or path dependency detected (not allowed by default).
    #[error("external dependency source not allowed: {dependency} uses {source_type}")]
    ExternalDependency {
        /// Dependency name.
        dependency: String,
        /// Source type (git, path).
        source_type: String,
    },

    /// Cargo.lock missing.
    #[error("Cargo.lock not found - reproducible builds require a lockfile")]
    MissingLockfile,

    // ─────────────────────────────────────────────────────────────────────────
    // Sandbox errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Sandbox setup failed.
    #[error("sandbox setup failed: {0}")]
    SandboxSetup(String),

    /// Compilation failed.
    #[error("compilation failed (exit code {exit_code}): {stderr}")]
    CompileFailed {
        /// Compiler stderr output.
        stderr: String,
        /// Exit code.
        exit_code: i32,
    },

    /// Build timed out.
    #[error("build timed out after {limit:?}")]
    Timeout {
        /// Timeout limit.
        limit: Duration,
    },

    /// Build was cancelled.
    #[error("build cancelled: {reason}")]
    Cancelled {
        /// Cancellation reason.
        reason: CancelReason,
    },

    // ─────────────────────────────────────────────────────────────────────────
    // Cache errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Cache operation failed.
    #[error("cache error: {0}")]
    Cache(String),

    /// Snapshot not found.
    #[error("cache snapshot not found for commit {0}")]
    SnapshotNotFound(String),

    // ─────────────────────────────────────────────────────────────────────────
    // Artifact errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Rootfs generation failed.
    #[error("rootfs generation failed: {0}")]
    RootfsGeneration(String),

    /// Artifact too large.
    #[error("artifact exceeds size limit: {size} bytes (limit: {limit} bytes)")]
    ArtifactTooLarge {
        /// Actual size.
        size: u64,
        /// Maximum allowed size.
        limit: u64,
    },

    /// Artifact storage failed.
    #[error("failed to store artifact: {0}")]
    ArtifactStorage(String),

    /// Artifact signing failed.
    #[error("artifact signing failed: {0}")]
    ArtifactSigning(String),

    /// Artifact verification failed.
    #[error("artifact verification failed: {0}")]
    ArtifactVerification(String),

    // ─────────────────────────────────────────────────────────────────────────
    // Queue errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Build queue is full.
    #[error("build queue is full")]
    QueueFull,

    /// Build not found.
    #[error("build not found: {0}")]
    BuildNotFound(String),

    /// Build already completed.
    #[error("build already completed: {0}")]
    BuildCompleted(String),

    // ─────────────────────────────────────────────────────────────────────────
    // Internal errors
    // ─────────────────────────────────────────────────────────────────────────
    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

/// Reason for build cancellation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CancelReason {
    /// Superseded by a newer commit.
    SupersededByCommit {
        /// The new commit SHA.
        new_commit: String,
    },
    /// Cancelled by user request.
    UserRequested,
    /// Cancelled due to timeout.
    Timeout,
    /// Service shutting down.
    Shutdown,
}

impl std::fmt::Display for CancelReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SupersededByCommit { new_commit } => {
                write!(
                    f,
                    "superseded by commit {}",
                    &new_commit[..8.min(new_commit.len())]
                )
            }
            Self::UserRequested => write!(f, "user requested"),
            Self::Timeout => write!(f, "timeout"),
            Self::Shutdown => write!(f, "service shutdown"),
        }
    }
}

impl From<figment::Error> for BuildError {
    fn from(e: figment::Error) -> Self {
        Self::ConfigParse(e.to_string())
    }
}

/// Result type alias for build operations.
pub type BuildResult<T> = Result<T, BuildError>;
