//! Core types for sidereal-control.

use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Unique identifier for a deployment.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DeploymentId(String);

impl DeploymentId {
    /// Create a new deployment ID.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Generate a new unique deployment ID using ULID.
    #[must_use]
    pub fn generate() -> Self {
        Self(ulid::Ulid::new().to_string().to_lowercase())
    }

    /// Get the ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DeploymentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for DeploymentId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Unique identifier for a project.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProjectId(String);

impl ProjectId {
    /// Create a new project ID.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ProjectId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Metadata about a function within a deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionMetadata {
    /// Function name (unique within the deployment).
    pub name: String,
    /// Trigger type for the function.
    pub trigger: FunctionTrigger,
    /// Memory limit in MB.
    #[serde(default = "default_memory_mb")]
    pub memory_mb: u32,
    /// Number of vCPUs.
    #[serde(default = "default_vcpus")]
    pub vcpus: u8,
}

const fn default_memory_mb() -> u32 {
    128
}

const fn default_vcpus() -> u8 {
    1
}

/// How a function is triggered.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FunctionTrigger {
    /// HTTP request trigger.
    Http {
        /// HTTP method (GET, POST, etc.).
        method: String,
        /// URL path pattern.
        path: String,
    },
    /// Queue message trigger.
    Queue {
        /// Queue name to consume from.
        queue: String,
    },
    /// Scheduled trigger (cron-like).
    Schedule {
        /// Cron expression.
        cron: String,
    },
}

/// Common data shared across all deployment states.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentData {
    /// Unique deployment identifier.
    pub id: DeploymentId,
    /// Project this deployment belongs to.
    pub project_id: ProjectId,
    /// Environment name (e.g., "production", "staging", "preview-123").
    pub environment: String,
    /// Git commit SHA.
    pub commit_sha: String,
    /// URL to the artifact (rootfs) in object storage.
    pub artifact_url: String,
    /// Functions included in this deployment.
    pub functions: Vec<FunctionMetadata>,
    /// When the deployment was created.
    pub created_at: DateTime<Utc>,
    /// When the deployment was last updated.
    pub updated_at: DateTime<Utc>,
    /// Error message if the deployment failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl DeploymentData {
    /// Create new deployment data.
    #[must_use]
    pub fn new(
        project_id: ProjectId,
        environment: String,
        commit_sha: String,
        artifact_url: String,
        functions: Vec<FunctionMetadata>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: DeploymentId::generate(),
            project_id,
            environment,
            commit_sha,
            artifact_url,
            functions,
            created_at: now,
            updated_at: now,
            error: None,
        }
    }
}

/// Persisted state representation for database storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistedState {
    /// Deployment created, waiting to start.
    Pending,
    /// Registering functions with the scheduler.
    Registering,
    /// Deployment is live and serving traffic.
    Active,
    /// Deployment was replaced by a newer deployment.
    Superseded,
    /// Deployment failed.
    Failed,
    /// Deployment was explicitly terminated.
    Terminated,
}

impl PersistedState {
    /// Get the state name as a static string.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Registering => "registering",
            Self::Active => "active",
            Self::Superseded => "superseded",
            Self::Failed => "failed",
            Self::Terminated => "terminated",
        }
    }
}

impl fmt::Display for PersistedState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for PersistedState {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "registering" => Ok(Self::Registering),
            "active" => Ok(Self::Active),
            "superseded" => Ok(Self::Superseded),
            "failed" => Ok(Self::Failed),
            "terminated" => Ok(Self::Terminated),
            _ => Err(format!("unknown deployment state: {s}")),
        }
    }
}

/// A deployment record as stored in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentRecord {
    /// The deployment data.
    #[serde(flatten)]
    pub data: DeploymentData,
    /// Current state.
    pub state: PersistedState,
}

impl DeploymentRecord {
    /// Create a new deployment record in the pending state.
    #[must_use]
    pub const fn new(data: DeploymentData) -> Self {
        Self {
            data,
            state: PersistedState::Pending,
        }
    }
}
