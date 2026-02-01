//! Worker provisioner for managing VM lifecycle.
//!
//! This module provides abstractions for provisioning worker VMs that run
//! function code. The primary implementation uses Firecracker microVMs.

mod firecracker;

pub use firecracker::FirecrackerProvisioner;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::config::ProvisionerConfig;
use crate::error::{ControlError, ControlResult};
use crate::types::FunctionMetadata;

/// Information about a provisioned worker.
#[derive(Debug, Clone)]
pub struct ProvisionedWorker {
    /// Unique worker identifier.
    pub id: String,
    /// Network address for the worker (host:port).
    pub address: String,
    /// vsock CID if using Firecracker.
    pub vsock_cid: Option<u32>,
    /// Functions this worker can handle.
    pub functions: Vec<String>,
    /// Memory allocated in MB.
    pub memory_mb: u32,
    /// vCPUs allocated.
    pub vcpus: u8,
}

/// Request to provision a new worker.
#[derive(Debug, Clone)]
pub struct ProvisionRequest {
    /// Deployment ID this worker belongs to.
    pub deployment_id: String,
    /// Project identifier.
    pub project_id: String,
    /// Environment name.
    pub environment: String,
    /// Path to the rootfs artifact.
    pub artifact_path: PathBuf,
    /// Functions to deploy on this worker.
    pub functions: Vec<FunctionMetadata>,
}

impl ProvisionRequest {
    /// Generate a worker ID for this request.
    #[must_use]
    pub fn worker_id(&self) -> String {
        format!(
            "{}-{}-{}",
            self.project_id,
            self.environment,
            ulid::Ulid::new().to_string().to_lowercase()
        )
    }

    /// Get the total memory required in MB.
    #[must_use]
    pub fn total_memory_mb(&self) -> u32 {
        self.functions
            .iter()
            .map(|f| f.memory_mb)
            .max()
            .unwrap_or(128)
    }

    /// Get the total vCPUs required.
    #[must_use]
    pub fn total_vcpus(&self) -> u8 {
        self.functions.iter().map(|f| f.vcpus).max().unwrap_or(1)
    }
}

/// Trait for worker provisioning implementations.
#[async_trait]
pub trait WorkerProvisioner: Send + Sync {
    /// Provision a new worker.
    ///
    /// Downloads the artifact if needed and boots a VM.
    async fn provision(&self, request: &ProvisionRequest) -> ControlResult<ProvisionedWorker>;

    /// Terminate a worker.
    ///
    /// Gracefully shuts down the VM if possible.
    async fn terminate(&self, worker_id: &str) -> ControlResult<()>;

    /// Check if a worker is running.
    async fn is_running(&self, worker_id: &str) -> ControlResult<bool>;

    /// List all active workers.
    async fn list_workers(&self) -> ControlResult<Vec<ProvisionedWorker>>;

    /// Wait for a worker to become ready.
    async fn wait_ready(&self, worker_id: &str, timeout: Duration) -> ControlResult<()>;

    /// Wait for a worker to send its function registration.
    ///
    /// Returns the list of function names that the worker reports it can handle.
    /// This is used to discover functions at runtime rather than relying on
    /// static configuration.
    async fn wait_for_registration(
        &self,
        worker_id: &str,
        timeout: Duration,
    ) -> ControlResult<Vec<String>>;
}

/// Create a provisioner from configuration.
pub fn create_provisioner(config: &ProvisionerConfig) -> ControlResult<Arc<dyn WorkerProvisioner>> {
    use crate::config::ProvisionerType;

    match config.provisioner_type {
        ProvisionerType::Firecracker => {
            let provisioner = FirecrackerProvisioner::new(config)?;
            Ok(Arc::new(provisioner))
        }
        ProvisionerType::Mock => Ok(Arc::new(MockProvisioner::default())),
    }
}

/// Mock provisioner for testing.
#[derive(Debug, Default)]
pub struct MockProvisioner {
    workers: std::sync::RwLock<std::collections::HashMap<String, ProvisionedWorker>>,
}

#[async_trait]
impl WorkerProvisioner for MockProvisioner {
    async fn provision(&self, request: &ProvisionRequest) -> ControlResult<ProvisionedWorker> {
        let worker_id = request.worker_id();
        let worker = ProvisionedWorker {
            id: worker_id.clone(),
            address: format!("127.0.0.1:{}", 10000 + rand_port()),
            vsock_cid: None,
            functions: request.functions.iter().map(|f| f.name.clone()).collect(),
            memory_mb: request.total_memory_mb(),
            vcpus: request.total_vcpus(),
        };

        let mut workers = self
            .workers
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;
        workers.insert(worker_id, worker.clone());

        Ok(worker)
    }

    async fn terminate(&self, worker_id: &str) -> ControlResult<()> {
        let mut workers = self
            .workers
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        if workers.remove(worker_id).is_none() {
            return Err(ControlError::provisioning(format!(
                "worker not found: {worker_id}"
            )));
        }

        Ok(())
    }

    async fn is_running(&self, worker_id: &str) -> ControlResult<bool> {
        let workers = self
            .workers
            .read()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        Ok(workers.contains_key(worker_id))
    }

    async fn list_workers(&self) -> ControlResult<Vec<ProvisionedWorker>> {
        let workers = self
            .workers
            .read()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        Ok(workers.values().cloned().collect())
    }

    async fn wait_ready(&self, worker_id: &str, _timeout: Duration) -> ControlResult<()> {
        let workers = self
            .workers
            .read()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        if workers.contains_key(worker_id) {
            Ok(())
        } else {
            Err(ControlError::provisioning(format!(
                "worker not found: {worker_id}"
            )))
        }
    }

    async fn wait_for_registration(
        &self,
        worker_id: &str,
        _timeout: Duration,
    ) -> ControlResult<Vec<String>> {
        let workers = self
            .workers
            .read()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        if let Some(worker) = workers.get(worker_id) {
            Ok(worker.functions.clone())
        } else {
            Err(ControlError::provisioning(format!(
                "worker not found: {worker_id}"
            )))
        }
    }
}

fn rand_port() -> u16 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);

    #[allow(clippy::as_conversions)]
    let port = (nanos % 10000) as u16;
    port
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FunctionTrigger;

    fn test_request() -> ProvisionRequest {
        ProvisionRequest {
            deployment_id: "dep-123".to_owned(),
            project_id: "my-project".to_owned(),
            environment: "production".to_owned(),
            artifact_path: PathBuf::from("/tmp/rootfs.ext4"),
            functions: vec![FunctionMetadata {
                name: "handler".to_owned(),
                trigger: FunctionTrigger::Http {
                    method: "GET".to_owned(),
                    path: "/hello".to_owned(),
                },
                memory_mb: 256,
                vcpus: 2,
            }],
        }
    }

    #[test]
    fn provision_request_worker_id() {
        let request = test_request();
        let id = request.worker_id();
        assert!(id.starts_with("my-project-production-"));
    }

    #[test]
    fn provision_request_resources() {
        let request = test_request();
        assert_eq!(request.total_memory_mb(), 256);
        assert_eq!(request.total_vcpus(), 2);
    }

    #[tokio::test]
    async fn mock_provisioner_lifecycle() {
        let provisioner = MockProvisioner::default();
        let request = test_request();

        let worker = provisioner.provision(&request).await.unwrap();
        assert!(!worker.id.is_empty());
        assert_eq!(worker.functions, vec!["handler"]);
        assert_eq!(worker.memory_mb, 256);
        assert_eq!(worker.vcpus, 2);

        assert!(provisioner.is_running(&worker.id).await.unwrap());

        let workers = provisioner.list_workers().await.unwrap();
        assert_eq!(workers.len(), 1);

        provisioner.terminate(&worker.id).await.unwrap();
        assert!(!provisioner.is_running(&worker.id).await.unwrap());
    }
}
