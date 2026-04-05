//! Types for deployment tracking.
//!
//! Deployments are captured as OTEL log events with `event.name = "deployment"`
//! following the semantic convention for events.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A deployment event captured from OTEL logs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deployment {
    /// Unique deployment identifier (from deployment.id attribute).
    pub id: Option<String>,
    /// Service being deployed.
    pub service_name: String,
    /// Version being deployed (git commit SHA or tag).
    pub version: Option<String>,
    /// Target environment (production, staging, etc.).
    pub environment: Option<String>,
    /// Deployment status.
    pub status: DeploymentStatus,
    /// When the deployment event was recorded.
    pub timestamp: DateTime<Utc>,
    /// VCS revision (git commit SHA).
    pub vcs_revision: Option<String>,
    /// VCS branch name.
    pub vcs_branch: Option<String>,
    /// VCS commit message.
    pub vcs_message: Option<String>,
}

/// Deployment status values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentStatus {
    /// Deployment has started.
    Started,
    /// Deployment completed successfully.
    #[default]
    Succeeded,
    /// Deployment failed.
    Failed,
    /// Deployment was rolled back.
    RolledBack,
    /// Unknown status.
    Unknown,
}

impl DeploymentStatus {
    /// Convert to string representation.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::RolledBack => "rolled_back",
            Self::Unknown => "unknown",
        }
    }
}

impl std::str::FromStr for DeploymentStatus {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.to_lowercase().as_str() {
            "started" => Self::Started,
            "succeeded" | "success" | "completed" => Self::Succeeded,
            "failed" | "failure" | "error" => Self::Failed,
            "rolled_back" | "rollback" => Self::RolledBack,
            _ => Self::Unknown,
        })
    }
}

/// Filter options for deployment queries.
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct DeploymentFilter {
    /// Filter by service name.
    pub service: Option<String>,
    /// Filter by environment.
    pub environment: Option<String>,
    /// Start of time range.
    pub start_time: Option<DateTime<Utc>>,
    /// End of time range.
    pub end_time: Option<DateTime<Utc>>,
    /// Filter by deployment status.
    pub status: Option<DeploymentStatus>,
}

impl DeploymentFilter {
    /// Create a new filter with a time range.
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            start_time: Some(start),
            end_time: Some(end),
            ..Default::default()
        }
    }

    /// Filter by service name.
    pub fn with_service(mut self, service: impl Into<String>) -> Self {
        self.service = Some(service.into());
        self
    }

    /// Filter by environment.
    pub fn with_environment(mut self, env: impl Into<String>) -> Self {
        self.environment = Some(env.into());
        self
    }

    /// Filter by status.
    pub const fn with_status(mut self, status: DeploymentStatus) -> Self {
        self.status = Some(status);
        self
    }
}

/// A service version with deployment information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceVersion {
    /// The version identifier.
    pub version: String,
    /// When this version was first deployed.
    pub first_deployed: DateTime<Utc>,
    /// When this version was last deployed.
    pub last_deployed: DateTime<Utc>,
    /// Number of deployment events for this version.
    pub deployment_count: u64,
    /// Environments this version has been deployed to.
    pub environments: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_status_parsing() {
        assert_eq!(
            "started".parse::<DeploymentStatus>().unwrap(),
            DeploymentStatus::Started
        );
        assert_eq!(
            "succeeded".parse::<DeploymentStatus>().unwrap(),
            DeploymentStatus::Succeeded
        );
        assert_eq!(
            "success".parse::<DeploymentStatus>().unwrap(),
            DeploymentStatus::Succeeded
        );
        assert_eq!(
            "failed".parse::<DeploymentStatus>().unwrap(),
            DeploymentStatus::Failed
        );
        assert_eq!(
            "rolled_back".parse::<DeploymentStatus>().unwrap(),
            DeploymentStatus::RolledBack
        );
        assert_eq!(
            "unknown_value".parse::<DeploymentStatus>().unwrap(),
            DeploymentStatus::Unknown
        );
    }

    #[test]
    fn deployment_status_roundtrip() {
        for status in [
            DeploymentStatus::Started,
            DeploymentStatus::Succeeded,
            DeploymentStatus::Failed,
            DeploymentStatus::RolledBack,
        ] {
            let s = status.as_str();
            assert_eq!(s.parse::<DeploymentStatus>().unwrap(), status);
        }
    }

    #[test]
    fn filter_builder() {
        let start = Utc::now();
        let end = start + chrono::Duration::hours(1);
        let filter = DeploymentFilter::new(start, end)
            .with_service("api-server")
            .with_environment("production")
            .with_status(DeploymentStatus::Succeeded);

        assert_eq!(filter.service, Some("api-server".to_string()));
        assert_eq!(filter.environment, Some("production".to_string()));
        assert_eq!(filter.status, Some(DeploymentStatus::Succeeded));
    }
}
