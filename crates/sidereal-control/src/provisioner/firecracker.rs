//! Firecracker microVM provisioner implementation.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;
use std::time::Duration;

use async_trait::async_trait;
use sidereal_firecracker::{VmConfig, VmInstance, VmManager, VsockClient};
use sidereal_proto::ports::FUNCTION as VSOCK_PORT;
use tracing::{debug, info, warn};

use crate::config::ProvisionerConfig;
use crate::error::{ControlError, ControlResult};

use super::{ProvisionRequest, ProvisionedWorker, WorkerProvisioner};

/// Default timeout for VM ready check.
const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Minimum vsock CID (0=hypervisor, 1=local, 2=host, so we start at 3).
const MIN_VSOCK_CID: u32 = 3;

/// Firecracker microVM provisioner.
pub struct FirecrackerProvisioner {
    vm_manager: VmManager,
    kernel_path: PathBuf,
    instances: RwLock<HashMap<String, WorkerEntry>>,
    next_cid: AtomicU32,
}

struct WorkerEntry {
    instance: VmInstance,
    info: ProvisionedWorker,
    vsock_uds_path: PathBuf,
}

impl FirecrackerProvisioner {
    /// Create a new Firecracker provisioner.
    pub fn new(config: &ProvisionerConfig) -> ControlResult<Self> {
        let vm_manager = VmManager::new(&config.work_dir)
            .map_err(|e| ControlError::provisioning(format!("failed to create VM manager: {e}")))?;

        Ok(Self {
            vm_manager,
            kernel_path: config.kernel_path.clone(),
            instances: RwLock::new(HashMap::new()),
            next_cid: AtomicU32::new(MIN_VSOCK_CID),
        })
    }

    /// Allocate a unique vsock CID.
    fn allocate_cid(&self) -> u32 {
        self.next_cid.fetch_add(1, Ordering::SeqCst)
    }

    /// Get a mutable reference to a worker entry.
    fn get_instance_mut<F, R>(&self, worker_id: &str, f: F) -> ControlResult<R>
    where
        F: FnOnce(&mut WorkerEntry) -> ControlResult<R>,
    {
        let mut instances = self
            .instances
            .write()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        let entry = instances
            .get_mut(worker_id)
            .ok_or_else(|| ControlError::provisioning(format!("worker not found: {worker_id}")))?;

        f(entry)
    }
}

impl std::fmt::Debug for FirecrackerProvisioner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FirecrackerProvisioner")
            .field("kernel_path", &self.kernel_path)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl WorkerProvisioner for FirecrackerProvisioner {
    async fn provision(&self, request: &ProvisionRequest) -> ControlResult<ProvisionedWorker> {
        let worker_id = request.worker_id();
        let cid = self.allocate_cid();

        info!(
            worker_id = %worker_id,
            deployment_id = %request.deployment_id,
            cid = cid,
            "provisioning Firecracker VM"
        );

        if !self.kernel_path.exists() {
            return Err(ControlError::provisioning(format!(
                "kernel not found: {}",
                self.kernel_path.display()
            )));
        }

        if !request.artifact_path.exists() {
            return Err(ControlError::provisioning(format!(
                "artifact not found: {}",
                request.artifact_path.display()
            )));
        }

        let vm_config = VmConfig::new(self.kernel_path.clone(), request.artifact_path.clone())
            .with_memory(request.total_memory_mb())
            .with_vcpus(request.total_vcpus())
            .with_cid(cid);

        debug!(worker_id = %worker_id, config = ?vm_config, "starting VM");

        let instance = self
            .vm_manager
            .start(vm_config)
            .await
            .map_err(|e| ControlError::provisioning(format!("failed to start VM: {e}")))?;

        let vsock_path = instance.vsock_uds_path().to_owned();

        let worker_info = ProvisionedWorker {
            id: worker_id.clone(),
            address: vsock_path.display().to_string(),
            vsock_cid: Some(cid),
            functions: request.functions.iter().map(|f| f.name.clone()).collect(),
            memory_mb: request.total_memory_mb(),
            vcpus: request.total_vcpus(),
        };

        let entry = WorkerEntry {
            instance,
            info: worker_info.clone(),
            vsock_uds_path: vsock_path,
        };

        {
            let mut instances = self
                .instances
                .write()
                .map_err(|_| ControlError::internal("lock poisoned"))?;
            instances.insert(worker_id.clone(), entry);
        }

        info!(worker_id = %worker_id, cid = cid, "VM provisioned");

        Ok(worker_info)
    }

    async fn terminate(&self, worker_id: &str) -> ControlResult<()> {
        info!(worker_id = %worker_id, "terminating worker");

        let mut entry = {
            let mut instances = self
                .instances
                .write()
                .map_err(|_| ControlError::internal("lock poisoned"))?;

            instances.remove(worker_id).ok_or_else(|| {
                ControlError::provisioning(format!("worker not found: {worker_id}"))
            })?
        };

        if let Err(e) = entry.instance.shutdown().await {
            warn!(worker_id = %worker_id, error = %e, "shutdown error (continuing)");
        }

        info!(worker_id = %worker_id, "worker terminated");
        Ok(())
    }

    async fn is_running(&self, worker_id: &str) -> ControlResult<bool> {
        self.get_instance_mut(worker_id, |entry| Ok(entry.instance.is_running()))
    }

    async fn list_workers(&self) -> ControlResult<Vec<ProvisionedWorker>> {
        let instances = self
            .instances
            .read()
            .map_err(|_| ControlError::internal("lock poisoned"))?;

        Ok(instances.values().map(|e| e.info.clone()).collect())
    }

    async fn wait_ready(&self, worker_id: &str, timeout: Duration) -> ControlResult<()> {
        let timeout = if timeout.is_zero() {
            DEFAULT_READY_TIMEOUT
        } else {
            timeout
        };

        debug!(worker_id = %worker_id, timeout = ?timeout, "waiting for VM ready");

        let vsock_path = {
            let instances = self
                .instances
                .read()
                .map_err(|_| ControlError::internal("lock poisoned"))?;

            let entry = instances.get(worker_id).ok_or_else(|| {
                ControlError::provisioning(format!("worker not found: {worker_id}"))
            })?;

            entry.vsock_uds_path.clone()
        };

        let client = VsockClient::new(vsock_path, VSOCK_PORT);
        let deadline = tokio::time::Instant::now() + timeout;
        let poll_interval = Duration::from_millis(500);

        while tokio::time::Instant::now() < deadline {
            match client.ping().await {
                Ok(()) => {
                    info!(worker_id = %worker_id, "VM ready");
                    return Ok(());
                }
                Err(e) => {
                    debug!(worker_id = %worker_id, error = %e, "VM not ready yet");
                    tokio::time::sleep(poll_interval).await;
                }
            }
        }

        Err(ControlError::provisioning(format!(
            "worker not ready within {}s",
            timeout.as_secs()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn cid_allocation() {
        let config = ProvisionerConfig {
            provisioner_type: crate::config::ProvisionerType::Firecracker,
            kernel_path: PathBuf::from("/nonexistent/kernel"),
            work_dir: PathBuf::from("/tmp/test-vms"),
        };

        let provisioner = match FirecrackerProvisioner::new(&config) {
            Ok(p) => p,
            Err(_) => return,
        };

        let cid1 = provisioner.allocate_cid();
        let cid2 = provisioner.allocate_cid();
        let cid3 = provisioner.allocate_cid();

        assert_eq!(cid1, 3);
        assert_eq!(cid2, 4);
        assert_eq!(cid3, 5);
    }
}
