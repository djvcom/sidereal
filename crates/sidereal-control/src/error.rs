//! Error types for sidereal-control.

use std::fmt;

/// Result type alias using [`ControlError`].
pub type ControlResult<T> = Result<T, ControlError>;

/// Errors that can occur in the control plane.
#[derive(Debug, thiserror::Error)]
pub enum ControlError {
    /// Database error.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Scheduler communication error.
    #[error("scheduler error: {0}")]
    Scheduler(String),

    /// Worker provisioning error.
    #[error("provisioning error: {0}")]
    Provisioning(String),

    /// Deployment not found.
    #[error("deployment not found: {0}")]
    DeploymentNotFound(String),

    /// Invalid state transition attempted.
    #[error("invalid state transition: cannot transition from {from} to {to}")]
    InvalidStateTransition {
        /// Current state.
        from: &'static str,
        /// Attempted target state.
        to: &'static str,
    },

    /// Deployment already exists for this project/environment.
    #[error("deployment already active for {project}/{environment}")]
    DeploymentAlreadyActive {
        /// Project identifier.
        project: String,
        /// Environment name.
        environment: String,
    },

    /// Artifact not found or inaccessible.
    #[error("artifact not found: {0}")]
    ArtifactNotFound(String),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// HTTP client error.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Serialisation error.
    #[error("serialisation error: {0}")]
    Serialisation(String),

    /// Internal error.
    #[error("internal error: {0}")]
    Internal(String),
}

impl ControlError {
    /// Create a scheduler error.
    #[must_use]
    pub fn scheduler(msg: impl Into<String>) -> Self {
        Self::Scheduler(msg.into())
    }

    /// Create a provisioning error.
    #[must_use]
    pub fn provisioning(msg: impl Into<String>) -> Self {
        Self::Provisioning(msg.into())
    }

    /// Create an internal error.
    #[must_use]
    pub fn internal(msg: impl Into<String>) -> Self {
        Self::Internal(msg.into())
    }
}

/// Deployment state name for error reporting.
#[derive(Debug, Clone, Copy)]
pub struct StateName(&'static str);

impl StateName {
    /// Create a new state name.
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }
}

impl fmt::Display for StateName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
