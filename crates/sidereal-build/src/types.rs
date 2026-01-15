//! Core types for the build service.

use std::fmt;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::error::{BuildStage, CancelReason};

/// Unique identifier for a build.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BuildId(String);

impl BuildId {
    /// Create a new build ID from a string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Generate a new unique build ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Return the ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for BuildId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for BuildId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for BuildId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Unique identifier for a project.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProjectId(String);

impl ProjectId {
    /// Create a new project ID from a string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Return the ID as a string slice.
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

impl From<String> for ProjectId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ProjectId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// Unique identifier for an artifact.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ArtifactId(String);

impl ArtifactId {
    /// Create a new artifact ID from a string.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Generate a new unique artifact ID.
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Return the ID as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ArtifactId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ArtifactId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for ArtifactId {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

/// A request to build a project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildRequest {
    /// Unique build identifier.
    pub id: BuildId,
    /// Project to build.
    pub project_id: ProjectId,
    /// Repository URL (HTTPS or SSH).
    pub repo_url: String,
    /// Branch name.
    pub branch: String,
    /// Commit SHA to build.
    pub commit_sha: String,
    /// When the request was created.
    pub created_at: SystemTime,
}

impl BuildRequest {
    /// Create a new build request.
    #[must_use]
    pub fn new(
        project_id: impl Into<ProjectId>,
        repo_url: impl Into<String>,
        branch: impl Into<String>,
        commit_sha: impl Into<String>,
    ) -> Self {
        Self {
            id: BuildId::generate(),
            project_id: project_id.into(),
            repo_url: repo_url.into(),
            branch: branch.into(),
            commit_sha: commit_sha.into(),
            created_at: SystemTime::now(),
        }
    }

    /// Create a build request with a specific ID.
    #[must_use]
    pub fn with_id(
        id: BuildId,
        project_id: impl Into<ProjectId>,
        repo_url: impl Into<String>,
        branch: impl Into<String>,
        commit_sha: impl Into<String>,
    ) -> Self {
        Self {
            id,
            project_id: project_id.into(),
            repo_url: repo_url.into(),
            branch: branch.into(),
            commit_sha: commit_sha.into(),
            created_at: SystemTime::now(),
        }
    }

    /// Return a short version of the commit SHA (first 8 characters).
    #[must_use]
    pub fn short_sha(&self) -> &str {
        &self.commit_sha[..8.min(self.commit_sha.len())]
    }
}

/// Current status of a build.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum BuildStatus {
    /// Build is waiting in the queue.
    Queued,

    /// Checking out source code.
    CheckingOut,

    /// Fetching dependencies.
    FetchingDeps,

    /// Running dependency audit.
    Auditing,

    /// Compiling the project.
    Compiling {
        /// Optional progress message.
        #[serde(skip_serializing_if = "Option::is_none")]
        progress: Option<String>,
    },

    /// Generating artifact (rootfs).
    GeneratingArtifact,

    /// Build completed successfully.
    Completed {
        /// The generated artifact ID.
        artifact_id: ArtifactId,
    },

    /// Build failed.
    Failed {
        /// Error message.
        error: String,
        /// Stage at which the build failed.
        stage: BuildStage,
    },

    /// Build was cancelled.
    Cancelled {
        /// Reason for cancellation.
        reason: CancelReason,
    },
}

impl BuildStatus {
    /// Check if this is a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed { .. } | Self::Failed { .. } | Self::Cancelled { .. }
        )
    }

    /// Check if the build is currently in progress.
    #[must_use]
    pub fn is_in_progress(&self) -> bool {
        matches!(
            self,
            Self::CheckingOut
                | Self::FetchingDeps
                | Self::Auditing
                | Self::Compiling { .. }
                | Self::GeneratingArtifact
        )
    }

    /// Return the current stage if in progress.
    #[must_use]
    pub fn current_stage(&self) -> Option<BuildStage> {
        match self {
            Self::CheckingOut => Some(BuildStage::Checkout),
            Self::FetchingDeps => Some(BuildStage::FetchDeps),
            Self::Auditing => Some(BuildStage::Audit),
            Self::Compiling { .. } => Some(BuildStage::Compile),
            Self::GeneratingArtifact => Some(BuildStage::Artifact),
            _ => None,
        }
    }
}

impl fmt::Display for BuildStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queued => write!(f, "queued"),
            Self::CheckingOut => write!(f, "checking out"),
            Self::FetchingDeps => write!(f, "fetching dependencies"),
            Self::Auditing => write!(f, "auditing dependencies"),
            Self::Compiling { progress } => {
                if let Some(msg) = progress {
                    write!(f, "compiling: {msg}")
                } else {
                    write!(f, "compiling")
                }
            }
            Self::GeneratingArtifact => write!(f, "generating artifact"),
            Self::Completed { artifact_id } => write!(f, "completed (artifact: {artifact_id})"),
            Self::Failed { error, stage } => write!(f, "failed at {stage}: {error}"),
            Self::Cancelled { reason } => write!(f, "cancelled: {reason}"),
        }
    }
}

/// Metadata about a completed build.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildMetadata {
    /// Build ID.
    pub id: BuildId,
    /// Project ID.
    pub project_id: ProjectId,
    /// Branch name.
    pub branch: String,
    /// Commit SHA.
    pub commit_sha: String,
    /// Artifact ID (if successful).
    pub artifact_id: Option<ArtifactId>,
    /// Final status.
    pub status: BuildStatus,
    /// When the build started.
    pub started_at: SystemTime,
    /// When the build finished.
    pub finished_at: SystemTime,
    /// Build duration in seconds.
    pub duration_secs: f64,
}

/// Information about a discovered function in the built artifact.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionMetadata {
    /// Function name.
    pub name: String,
    /// HTTP route (if HTTP-triggered).
    pub route: Option<String>,
    /// HTTP method (if HTTP-triggered).
    pub method: Option<String>,
    /// Queue name (if queue-triggered).
    pub queue: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_id_generates_unique_values() {
        let id1 = BuildId::generate();
        let id2 = BuildId::generate();
        assert_ne!(id1, id2);
    }

    #[test]
    fn build_request_short_sha() {
        let req = BuildRequest::new(
            "my-project",
            "https://github.com/example/repo.git",
            "main",
            "abc123def456789",
        );
        assert_eq!(req.short_sha(), "abc123de");
    }

    #[test]
    fn build_status_terminal_states() {
        assert!(!BuildStatus::Queued.is_terminal());
        assert!(!BuildStatus::Compiling { progress: None }.is_terminal());
        assert!(BuildStatus::Completed {
            artifact_id: ArtifactId::generate()
        }
        .is_terminal());
        assert!(BuildStatus::Failed {
            error: "test".into(),
            stage: BuildStage::Compile
        }
        .is_terminal());
        assert!(BuildStatus::Cancelled {
            reason: CancelReason::UserRequested
        }
        .is_terminal());
    }

    #[test]
    fn build_status_in_progress() {
        assert!(!BuildStatus::Queued.is_in_progress());
        assert!(BuildStatus::CheckingOut.is_in_progress());
        assert!(BuildStatus::Compiling { progress: None }.is_in_progress());
        assert!(!BuildStatus::Completed {
            artifact_id: ArtifactId::generate()
        }
        .is_in_progress());
    }

    #[test]
    fn cancel_reason_display() {
        let reason = CancelReason::SupersededByCommit {
            new_commit: "abc123def".into(),
        };
        assert_eq!(reason.to_string(), "superseded by commit abc123de");

        assert_eq!(CancelReason::UserRequested.to_string(), "user requested");
        assert_eq!(CancelReason::Timeout.to_string(), "timeout");
        assert_eq!(CancelReason::Shutdown.to_string(), "service shutdown");
    }
}
