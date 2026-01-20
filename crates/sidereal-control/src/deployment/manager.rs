//! Core deployment orchestration logic.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::config::{ArtifactConfig, DeploymentConfig};
use crate::error::{ControlError, ControlResult};
use crate::provisioner::{ProvisionRequest, ProvisionedWorker, WorkerProvisioner};
use crate::scheduler::SchedulerClient;
use crate::state::{Active, Deployment, Pending, Registering};
use crate::store::DeploymentStore;
use crate::types::{
    DeploymentData, DeploymentId, DeploymentRecord, FunctionMetadata, PersistedState, ProjectId,
};

/// Request to create a new deployment.
#[derive(Debug, Clone)]
pub struct DeploymentRequest {
    /// Project identifier.
    pub project_id: ProjectId,
    /// Environment name.
    pub environment: String,
    /// Git commit SHA.
    pub commit_sha: String,
    /// URL to the artifact in object storage.
    pub artifact_url: String,
    /// Functions to deploy.
    pub functions: Vec<FunctionMetadata>,
}

impl DeploymentRequest {
    /// Create deployment data from this request.
    #[must_use]
    pub fn into_data(self) -> DeploymentData {
        DeploymentData::new(
            self.project_id,
            self.environment,
            self.commit_sha,
            self.artifact_url,
            self.functions,
        )
    }
}

/// Tracks workers associated with a deployment.
#[derive(Debug, Default)]
struct DeploymentWorkers {
    worker_ids: Vec<String>,
}

/// Orchestrates deployment lifecycle operations.
pub struct DeploymentManager {
    store: Arc<dyn DeploymentStore>,
    provisioner: Arc<dyn WorkerProvisioner>,
    _scheduler: SchedulerClient,
    artifact_config: ArtifactConfig,
    deployment_config: DeploymentConfig,
    workers: RwLock<HashMap<String, DeploymentWorkers>>,
}

impl DeploymentManager {
    /// Create a new deployment manager.
    pub fn new(
        store: Arc<dyn DeploymentStore>,
        provisioner: Arc<dyn WorkerProvisioner>,
        scheduler: SchedulerClient,
        artifact_config: ArtifactConfig,
        deployment_config: DeploymentConfig,
    ) -> Self {
        Self {
            store,
            provisioner,
            _scheduler: scheduler,
            artifact_config,
            deployment_config,
            workers: RwLock::new(HashMap::new()),
        }
    }

    /// Deploy a new version of a project to an environment.
    ///
    /// This orchestrates the full deployment lifecycle:
    /// 1. Create deployment record in pending state
    /// 2. Download artifact to local cache
    /// 3. Provision worker(s)
    /// 4. Wait for workers to become ready
    /// 5. Supersede any existing active deployment
    /// 6. Mark new deployment as active
    pub async fn deploy(&self, request: DeploymentRequest) -> ControlResult<DeploymentId> {
        let project_id = request.project_id.clone();
        let environment = request.environment.clone();

        let data = request.into_data();
        let deployment_id = data.id.clone();

        info!(
            deployment_id = %deployment_id,
            project = %project_id,
            environment = %environment,
            "starting deployment"
        );

        let pending = Deployment::<Pending>::create(data);
        let record = DeploymentRecord::new(pending.data().clone());
        self.store.insert(&record).await?;

        match self.execute_deployment(pending).await {
            Ok(active) => {
                self.handle_activation(&project_id, &environment, active)
                    .await?;
                info!(deployment_id = %deployment_id, "deployment completed successfully");
                Ok(deployment_id)
            }
            Err((failed_deployment, error)) => {
                error!(deployment_id = %deployment_id, error = %error, "deployment failed");
                self.store
                    .update_state(
                        &deployment_id,
                        PersistedState::Failed,
                        Some(&error.to_string()),
                    )
                    .await?;

                if let Some(data) = failed_deployment {
                    self.cleanup_workers(data.id.as_ref()).await;
                }

                Err(error)
            }
        }
    }

    async fn execute_deployment(
        &self,
        pending: Deployment<Pending>,
    ) -> Result<Deployment<Active>, (Option<DeploymentData>, ControlError)> {
        let deployment_id = pending.id().clone();

        let registering = pending.start_registering();
        self.store
            .update_state(&deployment_id, PersistedState::Registering, None)
            .await
            .map_err(|e| (Some(registering.data().clone()), e))?;

        let artifact_path = self
            .download_artifact(registering.data())
            .await
            .map_err(|e| (Some(registering.data().clone()), e))?;

        let worker = self
            .provision_worker(&registering, &artifact_path)
            .await
            .map_err(|e| (Some(registering.data().clone()), e))?;

        self.wait_worker_ready(&worker.id)
            .await
            .map_err(|e| (Some(registering.data().clone()), e))?;

        {
            let mut workers = self.workers.write().await;
            let entry = workers
                .entry(deployment_id.to_string())
                .or_insert_with(DeploymentWorkers::default);
            entry.worker_ids.push(worker.id);
        }

        let active = registering.activate();
        self.store
            .update_state(&deployment_id, PersistedState::Active, None)
            .await
            .map_err(|e| (Some(active.data().clone()), e))?;

        Ok(active)
    }

    async fn handle_activation(
        &self,
        project_id: &ProjectId,
        environment: &str,
        active: Deployment<Active>,
    ) -> ControlResult<()> {
        if let Some(previous) = self.store.get_active(project_id, environment).await? {
            info!(
                previous_id = %previous.data.id,
                new_id = %active.id(),
                "superseding previous deployment"
            );

            self.store
                .update_state(&previous.data.id, PersistedState::Superseded, None)
                .await?;

            self.cleanup_workers(previous.data.id.as_ref()).await;
        }

        self.store
            .set_active(project_id, environment, active.id())
            .await?;

        Ok(())
    }

    async fn download_artifact(&self, data: &DeploymentData) -> ControlResult<PathBuf> {
        let cache_path = self
            .artifact_config
            .cache_dir
            .join(data.project_id.as_str())
            .join(&data.environment)
            .join(&data.commit_sha)
            .join("rootfs.ext4");

        if cache_path.exists() {
            debug!(path = %cache_path.display(), "artifact already cached");
            return Ok(cache_path);
        }

        info!(
            url = %data.artifact_url,
            cache_path = %cache_path.display(),
            "downloading artifact"
        );

        if let Some(parent) = cache_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                ControlError::provisioning(format!("failed to create cache directory: {e}"))
            })?;
        }

        if data.artifact_url.starts_with("file://") {
            let source_path = data.artifact_url.trim_start_matches("file://");
            tokio::fs::copy(source_path, &cache_path)
                .await
                .map_err(|e| ControlError::provisioning(format!("failed to copy artifact: {e}")))?;
        } else {
            return Err(ControlError::provisioning(format!(
                "unsupported artifact URL scheme: {}",
                data.artifact_url
            )));
        }

        Ok(cache_path)
    }

    async fn provision_worker(
        &self,
        deployment: &Deployment<Registering>,
        artifact_path: &Path,
    ) -> ControlResult<ProvisionedWorker> {
        let data = deployment.data();

        let request = ProvisionRequest {
            deployment_id: data.id.to_string(),
            project_id: data.project_id.to_string(),
            environment: data.environment.clone(),
            artifact_path: artifact_path.to_path_buf(),
            functions: data.functions.clone(),
        };

        info!(
            deployment_id = %data.id,
            "provisioning worker"
        );

        self.provisioner.provision(&request).await
    }

    async fn wait_worker_ready(&self, worker_id: &str) -> ControlResult<()> {
        let timeout = Duration::from_secs(self.deployment_config.timeout_secs);

        debug!(worker_id = %worker_id, timeout = ?timeout, "waiting for worker ready");

        self.provisioner.wait_ready(worker_id, timeout).await
    }

    async fn cleanup_workers(&self, deployment_id: &str) {
        let worker_ids = {
            let mut workers = self.workers.write().await;
            workers
                .remove(deployment_id)
                .map(|w| w.worker_ids)
                .unwrap_or_default()
        };

        for worker_id in worker_ids {
            debug!(worker_id = %worker_id, deployment_id = %deployment_id, "terminating worker");
            if let Err(e) = self.provisioner.terminate(&worker_id).await {
                warn!(worker_id = %worker_id, error = %e, "failed to terminate worker");
            }
        }
    }

    /// Terminate a deployment.
    ///
    /// Gracefully shuts down all workers and marks the deployment as terminated.
    pub async fn terminate(&self, deployment_id: &DeploymentId) -> ControlResult<()> {
        let record = self
            .store
            .get(deployment_id)
            .await?
            .ok_or_else(|| ControlError::DeploymentNotFound(deployment_id.to_string()))?;

        if record.state != PersistedState::Active {
            return Err(ControlError::InvalidStateTransition {
                from: record.state.as_str(),
                to: "terminated",
            });
        }

        info!(deployment_id = %deployment_id, "terminating deployment");

        self.store
            .clear_active(&record.data.project_id, &record.data.environment)
            .await?;

        self.cleanup_workers(deployment_id.as_ref()).await;

        self.store
            .update_state(deployment_id, PersistedState::Terminated, None)
            .await?;

        info!(deployment_id = %deployment_id, "deployment terminated");

        Ok(())
    }

    /// Get deployment status.
    pub async fn get(
        &self,
        deployment_id: &DeploymentId,
    ) -> ControlResult<Option<DeploymentRecord>> {
        self.store.get(deployment_id).await
    }

    /// Get the active deployment for a project/environment.
    pub async fn get_active(
        &self,
        project_id: &ProjectId,
        environment: &str,
    ) -> ControlResult<Option<DeploymentRecord>> {
        self.store.get_active(project_id, environment).await
    }

    /// List workers for a deployment.
    pub async fn list_workers(&self, deployment_id: &str) -> Vec<String> {
        let workers = self.workers.read().await;
        workers
            .get(deployment_id)
            .map(|w| w.worker_ids.clone())
            .unwrap_or_default()
    }
}

impl std::fmt::Debug for DeploymentManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeploymentManager").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provisioner::MockProvisioner;
    use crate::store::MemoryStore;
    use crate::types::FunctionTrigger;

    fn test_request() -> DeploymentRequest {
        DeploymentRequest {
            project_id: ProjectId::new("test-project"),
            environment: "production".to_owned(),
            commit_sha: "abc123".to_owned(),
            artifact_url: "file:///tmp/test-artifact/rootfs.ext4".to_owned(),
            functions: vec![FunctionMetadata {
                name: "handler".to_owned(),
                trigger: FunctionTrigger::Http {
                    method: "GET".to_owned(),
                    path: "/hello".to_owned(),
                },
                memory_mb: 128,
                vcpus: 1,
            }],
        }
    }

    async fn create_manager() -> DeploymentManager {
        let store: Arc<dyn DeploymentStore> = Arc::new(MemoryStore::new());
        let provisioner: Arc<dyn WorkerProvisioner> = Arc::new(MockProvisioner::default());
        let scheduler = SchedulerClient::with_url("http://localhost:8082").unwrap();
        let artifact_config = ArtifactConfig::default();
        let deployment_config = DeploymentConfig::default();

        DeploymentManager::new(
            store,
            provisioner,
            scheduler,
            artifact_config,
            deployment_config,
        )
    }

    #[test]
    fn deployment_request_into_data() {
        let request = test_request();
        let data = request.into_data();

        assert_eq!(data.project_id.as_str(), "test-project");
        assert_eq!(data.environment, "production");
        assert_eq!(data.commit_sha, "abc123");
        assert_eq!(data.functions.len(), 1);
    }

    #[tokio::test]
    async fn manager_creation() {
        let manager = create_manager().await;
        assert!(manager.list_workers("nonexistent").await.is_empty());
    }
}
