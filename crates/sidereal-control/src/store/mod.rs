//! Deployment storage backends.
//!
//! This module provides traits and implementations for persisting deployment
//! state. The primary implementation uses PostgreSQL, but an in-memory
//! implementation is provided for testing.

mod memory;
mod postgres;

pub use memory::MemoryStore;
pub use postgres::PostgresStore;

use async_trait::async_trait;

use crate::error::ControlResult;
use crate::types::{DeploymentId, DeploymentRecord, PersistedState, ProjectId};

/// Filter criteria for listing deployments.
#[derive(Debug, Clone, Default)]
pub struct DeploymentFilter {
    /// Filter by project ID.
    pub project_id: Option<ProjectId>,
    /// Filter by environment name.
    pub environment: Option<String>,
    /// Filter by state.
    pub state: Option<PersistedState>,
    /// Maximum number of results.
    pub limit: Option<u32>,
    /// Offset for pagination.
    pub offset: Option<u32>,
}

impl DeploymentFilter {
    /// Create a new empty filter.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            project_id: None,
            environment: None,
            state: None,
            limit: None,
            offset: None,
        }
    }

    /// Filter by project ID.
    #[must_use]
    pub fn with_project(mut self, project_id: ProjectId) -> Self {
        self.project_id = Some(project_id);
        self
    }

    /// Filter by environment.
    #[must_use]
    pub fn with_environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = Some(environment.into());
        self
    }

    /// Filter by state.
    #[must_use]
    pub const fn with_state(mut self, state: PersistedState) -> Self {
        self.state = Some(state);
        self
    }

    /// Set maximum results.
    #[must_use]
    pub const fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set pagination offset.
    #[must_use]
    pub const fn with_offset(mut self, offset: u32) -> Self {
        self.offset = Some(offset);
        self
    }
}

/// Backend for storing deployment records.
///
/// Implementations must ensure that all operations are atomic and durable.
/// The `Simple` deployment strategy relies on database transactions for
/// crash safety.
#[async_trait]
pub trait DeploymentStore: Send + Sync {
    /// Insert a new deployment record.
    ///
    /// Returns an error if a deployment with the same ID already exists.
    async fn insert(&self, record: &DeploymentRecord) -> ControlResult<()>;

    /// Get a deployment by ID.
    ///
    /// Returns `None` if the deployment does not exist.
    async fn get(&self, id: &DeploymentId) -> ControlResult<Option<DeploymentRecord>>;

    /// Update a deployment's state.
    ///
    /// Also updates the `updated_at` timestamp and optionally sets an error message.
    async fn update_state(
        &self,
        id: &DeploymentId,
        state: PersistedState,
        error: Option<&str>,
    ) -> ControlResult<()>;

    /// List deployments matching the filter criteria.
    ///
    /// Results are ordered by `created_at` descending (newest first).
    async fn list(&self, filter: &DeploymentFilter) -> ControlResult<Vec<DeploymentRecord>>;

    /// Get the active deployment for a project/environment.
    ///
    /// Returns `None` if no active deployment exists.
    async fn get_active(
        &self,
        project_id: &ProjectId,
        environment: &str,
    ) -> ControlResult<Option<DeploymentRecord>>;

    /// Set the active deployment for a project/environment.
    ///
    /// This atomically replaces any existing active deployment. The previous
    /// active deployment (if any) should be transitioned to `Superseded` before
    /// calling this method.
    async fn set_active(
        &self,
        project_id: &ProjectId,
        environment: &str,
        deployment_id: &DeploymentId,
    ) -> ControlResult<()>;

    /// Clear the active deployment for a project/environment.
    ///
    /// This is called when a deployment is terminated.
    async fn clear_active(&self, project_id: &ProjectId, environment: &str) -> ControlResult<()>;

    /// Delete a deployment record.
    ///
    /// This is primarily for testing and cleanup. In production, deployments
    /// should typically be kept for audit purposes.
    async fn delete(&self, id: &DeploymentId) -> ControlResult<()>;
}
