//! Firecracker VM lifecycle management.

use crate::config::{api, VmConfig};
use crate::error::{FirecrackerError, Result};
use crate::vsock::VsockClient;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyperlocal::{UnixClientExt, UnixConnector, Uri};
use sidereal_proto::ports::FUNCTION as VSOCK_PORT;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::process::{Child, Command};
use tracing::{debug, info, warn};

const PROCESS_START_DELAY: Duration = Duration::from_millis(100);
const API_SOCKET_POLL_INTERVAL: Duration = Duration::from_millis(100);
const API_SOCKET_POLL_ATTEMPTS: usize = 50;
const VM_READY_POLL_INTERVAL: Duration = Duration::from_millis(500);
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_millis(500);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Manages Firecracker VM instances.
pub struct VmManager {
    firecracker_bin: PathBuf,
    work_dir: PathBuf,
}

/// A running Firecracker VM instance.
pub struct VmInstance {
    process: Child,
    api_socket: PathBuf,
    vsock_uds_path: PathBuf,
    console_log_path: PathBuf,
    config: VmConfig,
}

impl VmManager {
    /// Create a new VM manager.
    pub fn new(work_dir: impl Into<PathBuf>) -> Result<Self> {
        let firecracker_bin = Self::find_firecracker()?;
        let work_dir = work_dir.into();

        std::fs::create_dir_all(&work_dir)?;

        Ok(Self {
            firecracker_bin,
            work_dir,
        })
    }

    /// Find the Firecracker binary in PATH or common locations.
    fn find_firecracker() -> Result<PathBuf> {
        if let Ok(path) = which::which("firecracker") {
            return Ok(path);
        }

        let common_paths = [
            "/usr/bin/firecracker",
            "/usr/local/bin/firecracker",
            "~/.local/bin/firecracker",
        ];

        for path in common_paths {
            let expanded = shellexpand::tilde(path);
            let path = PathBuf::from(expanded.as_ref());
            if path.exists() {
                return Ok(path);
            }
        }

        Err(FirecrackerError::BinaryNotFound(PathBuf::from(
            "firecracker",
        )))
    }

    /// Check if KVM is available.
    pub fn check_kvm() -> Result<()> {
        let kvm_path = Path::new("/dev/kvm");
        if !kvm_path.exists() {
            return Err(FirecrackerError::KvmNotAvailable(
                "/dev/kvm does not exist".to_owned(),
            ));
        }

        match std::fs::metadata(kvm_path) {
            Ok(meta) => {
                use std::os::unix::fs::MetadataExt;
                let mode = meta.mode();
                if mode & 0o006 == 0 {
                    return Err(FirecrackerError::KvmNotAvailable(
                        "/dev/kvm is not accessible (check permissions)".to_owned(),
                    ));
                }
                Ok(())
            }
            Err(e) => Err(FirecrackerError::KvmNotAvailable(format!(
                "Cannot access /dev/kvm: {e}"
            ))),
        }
    }

    /// Start a new VM with the given configuration.
    pub async fn start(&self, config: VmConfig) -> Result<VmInstance> {
        Self::check_kvm()?;

        if !config.kernel_path.exists() {
            return Err(FirecrackerError::KernelNotFound(config.kernel_path.clone()));
        }
        if !config.rootfs_path.exists() {
            return Err(FirecrackerError::RootfsNotFound(config.rootfs_path.clone()));
        }

        let vm_id = uuid::Uuid::new_v4().to_string();
        let api_socket = self.work_dir.join(format!("{vm_id}.sock"));
        let vsock_uds_path = self.work_dir.join(format!("{vm_id}_vsock.sock"));
        let console_log_path = self.work_dir.join(format!("{vm_id}_console.log"));

        info!(vm_id = %vm_id, console_log = %console_log_path.display(), "Starting Firecracker VM");

        // Create console log file for capturing serial output
        let console_log_file = std::fs::File::create(&console_log_path).map_err(|e| {
            FirecrackerError::VmStartFailed(format!("Failed to create console log: {e}"))
        })?;
        let console_log_file_stderr = console_log_file.try_clone().map_err(|e| {
            FirecrackerError::VmStartFailed(format!("Failed to clone console log handle: {e}"))
        })?;

        let mut process = Command::new(&self.firecracker_bin)
            .arg("--api-sock")
            .arg(&api_socket)
            .stdin(Stdio::null())
            .stdout(Stdio::from(console_log_file))
            .stderr(Stdio::from(console_log_file_stderr))
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| FirecrackerError::VmStartFailed(e.to_string()))?;

        tokio::time::sleep(PROCESS_START_DELAY).await;

        if let Ok(Some(status)) = process.try_wait() {
            // Read the first few lines from the console log for error context
            let stderr = std::fs::read_to_string(&console_log_path)
                .map(|s| s.lines().take(10).collect::<Vec<_>>().join("\n"))
                .unwrap_or_default();
            return Err(FirecrackerError::VmStartFailed(format!(
                "Process exited immediately with status {status}: {stderr}"
            )));
        }

        for _ in 0..API_SOCKET_POLL_ATTEMPTS {
            if api_socket.exists() {
                break;
            }
            tokio::time::sleep(API_SOCKET_POLL_INTERVAL).await;
        }

        if !api_socket.exists() {
            let _ = process.kill().await;
            return Err(FirecrackerError::VmStartFailed(
                "API socket not created".to_owned(),
            ));
        }

        let instance = VmInstance {
            process,
            api_socket,
            vsock_uds_path,
            console_log_path,
            config,
        };

        instance.configure().await?;
        instance.boot().await?;

        Ok(instance)
    }
}

impl VmInstance {
    /// Send a request to the Firecracker API.
    async fn api_request(&self, method: &str, path: &str, body: Option<&str>) -> Result<()> {
        let client: Client<UnixConnector, Full<Bytes>> = Client::unix();
        let url: hyper::Uri = Uri::new(&self.api_socket, path).into();

        let method = method
            .parse::<Method>()
            .map_err(|e| FirecrackerError::VmConfigFailed(format!("Invalid HTTP method: {e}")))?;

        let req_body = body.unwrap_or("").to_owned();
        let request = Request::builder()
            .method(method)
            .uri(url)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(req_body)))
            .map_err(|e| {
                FirecrackerError::VmConfigFailed(format!("Failed to build request: {e}"))
            })?;

        debug!("Sending {} {}", request.method(), path);

        let response = client
            .request(request)
            .await
            .map_err(|e| FirecrackerError::VmConfigFailed(format!("API request failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.collect().await.map_err(|e| {
                FirecrackerError::VmConfigFailed(format!("Failed to read response: {e}"))
            })?;
            let body_bytes = body.to_bytes();
            let message = String::from_utf8_lossy(&body_bytes).to_string();
            #[allow(clippy::as_conversions)]
            return Err(FirecrackerError::ApiError {
                status: status.as_u16(),
                message,
            });
        }

        Ok(())
    }

    /// Configure the VM (kernel, drives, vsock, etc.).
    async fn configure(&self) -> Result<()> {
        debug!("Configuring VM");

        let boot_source = api::BootSource {
            kernel_image_path: self.config.kernel_path.display().to_string(),
            boot_args: self.config.boot_args.clone(),
        };
        self.api_request(
            "PUT",
            "/boot-source",
            Some(&serde_json::to_string(&boot_source)?),
        )
        .await?;

        let drive = api::Drive {
            drive_id: "rootfs".to_owned(),
            path_on_host: self.config.rootfs_path.display().to_string(),
            is_root_device: true,
            is_read_only: false,
        };
        self.api_request(
            "PUT",
            "/drives/rootfs",
            Some(&serde_json::to_string(&drive)?),
        )
        .await?;

        let machine = api::MachineConfig {
            vcpu_count: self.config.vcpu_count,
            mem_size_mib: self.config.mem_size_mib,
        };
        self.api_request(
            "PUT",
            "/machine-config",
            Some(&serde_json::to_string(&machine)?),
        )
        .await?;

        let vsock = api::Vsock {
            vsock_id: "vsock0".to_owned(),
            guest_cid: self.config.vsock_cid,
            uds_path: self.vsock_uds_path.display().to_string(),
        };
        self.api_request("PUT", "/vsock", Some(&serde_json::to_string(&vsock)?))
            .await?;

        debug!("VM configured");
        Ok(())
    }

    /// Boot the VM.
    async fn boot(&self) -> Result<()> {
        let action = api::InstanceActionInfo {
            action_type: "InstanceStart".to_owned(),
        };
        self.api_request("PUT", "/actions", Some(&serde_json::to_string(&action)?))
            .await?;

        info!("VM booted");
        Ok(())
    }

    /// Get the vsock CID for this VM.
    pub const fn cid(&self) -> u32 {
        self.config.vsock_cid
    }

    /// Get the vsock UDS path for host-side connections.
    pub fn vsock_uds_path(&self) -> &Path {
        &self.vsock_uds_path
    }

    /// Get the path to the VM console log file.
    pub fn console_log_path(&self) -> &Path {
        &self.console_log_path
    }

    /// Create a vsock client for this VM.
    pub fn vsock_client(&self) -> VsockClient {
        VsockClient::new(self.vsock_uds_path.clone(), VSOCK_PORT)
    }

    /// Wait for the VM to be ready (responds to ping).
    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let client = self.vsock_client();
        let deadline = tokio::time::Instant::now() + timeout;
        let mut last_error = String::new();

        while tokio::time::Instant::now() < deadline {
            match client.ping().await {
                Ok(()) => {
                    info!("VM is ready");
                    return Ok(());
                }
                Err(e) => {
                    last_error = e.to_string();
                    debug!("VM not ready yet: {}", e);
                    tokio::time::sleep(VM_READY_POLL_INTERVAL).await;
                }
            }
        }

        // Include console log content in the error for debugging
        let console_log = std::fs::read_to_string(&self.console_log_path)
            .map(|s| {
                let lines: Vec<_> = s.lines().collect();
                let start = lines.len().saturating_sub(50);
                lines[start..].join("\n")
            })
            .unwrap_or_else(|e| format!("(failed to read console log: {e})"));

        warn!(
            console_log_path = %self.console_log_path.display(),
            "VM not ready after {}s, last error: {}",
            timeout.as_secs(),
            last_error
        );

        Err(FirecrackerError::VmNotReady {
            timeout_secs: timeout.as_secs(),
            last_error,
            console_log,
        })
    }

    /// Request graceful shutdown.
    pub async fn shutdown(&mut self) -> Result<()> {
        info!("Requesting VM shutdown");

        let client = self.vsock_client();
        match client.shutdown().await {
            Ok(()) => {
                tokio::time::sleep(SHUTDOWN_GRACE_PERIOD).await;
            }
            Err(e) => {
                warn!("Graceful shutdown failed: {}", e);
            }
        }

        match tokio::time::timeout(SHUTDOWN_TIMEOUT, self.process.wait()).await {
            Ok(Ok(status)) => {
                info!("VM exited with status: {}", status);
            }
            Ok(Err(e)) => {
                warn!("Error waiting for VM: {}", e);
            }
            Err(_) => {
                warn!("VM did not exit in time, killing");
                let _ = self.process.kill().await;
            }
        }

        let _ = std::fs::remove_file(&self.api_socket);
        let _ = std::fs::remove_file(&self.vsock_uds_path);

        Ok(())
    }

    /// Check if the VM process is still running.
    pub fn is_running(&mut self) -> bool {
        matches!(self.process.try_wait(), Ok(None))
    }
}

impl Drop for VmInstance {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.api_socket);
        let _ = std::fs::remove_file(&self.vsock_uds_path);
    }
}
