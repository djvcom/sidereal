//! Firecracker-based compiler implementation.
//!
//! Provides compilation inside Firecracker microVMs for stronger isolation
//! than bubblewrap sandboxes.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use rkyv::api::high::HighSerializer;
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Serialize};
use sidereal_firecracker::config::{DriveConfig, VmConfig};
use sidereal_firecracker::VmManager;
use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::error::{BuildError, BuildResult};
use crate::protocol::{
    BuildMessage, BuildOutput, BuildRequest, BuildResult as ProtocolBuildResult, BUILD_PORT,
};
use crate::proxy::{CargoProxy, ProxyServer, PROXY_PORT};
use crate::sandbox::{BinaryInfo, WorkspaceCompileOutput};
use crate::source::SourceCheckout;
use crate::types::{BuildStatus, ProjectId};

/// Configuration for the VM-based compiler.
#[derive(Debug, Clone)]
pub struct VmCompilerConfig {
    /// Path to the Linux kernel for VMs.
    pub kernel_path: PathBuf,

    /// Path to the builder rootfs image.
    pub builder_rootfs: PathBuf,

    /// Path to the cargo registry cache.
    pub cargo_cache_dir: PathBuf,

    /// Number of vCPUs for builder VMs.
    pub vcpu_count: u8,

    /// Memory size in MB for builder VMs.
    pub mem_size_mib: u32,

    /// Rust target triple.
    pub target: String,

    /// Build timeout.
    pub timeout: Duration,
}

impl Default for VmCompilerConfig {
    fn default() -> Self {
        Self {
            kernel_path: PathBuf::from("/var/lib/sidereal/kernel/vmlinux"),
            builder_rootfs: PathBuf::from("/var/lib/sidereal/rootfs/builder.ext4"),
            cargo_cache_dir: PathBuf::from("/var/lib/sidereal/cargo"),
            vcpu_count: 2,
            mem_size_mib: 4096,
            target: "x86_64-unknown-linux-musl".to_owned(),
            timeout: Duration::from_secs(600),
        }
    }
}

/// Compiler that uses Firecracker VMs for isolation.
pub struct FirecrackerCompiler {
    config: VmCompilerConfig,
    vm_manager: VmManager,
}

impl FirecrackerCompiler {
    /// Create a new Firecracker-based compiler.
    pub fn new(config: VmCompilerConfig, work_dir: impl Into<PathBuf>) -> BuildResult<Self> {
        let vm_manager = VmManager::new(work_dir)
            .map_err(|e| BuildError::SandboxSetup(format!("Failed to create VM manager: {e}")))?;

        Ok(Self { config, vm_manager })
    }

    /// Return the cargo home directory path.
    #[must_use]
    pub fn cargo_home(&self) -> &Path {
        &self.config.cargo_cache_dir
    }

    /// Compile the workspace in a Firecracker VM.
    pub async fn compile_workspace(
        &self,
        project_id: &ProjectId,
        checkout: &SourceCheckout,
        target_dir: &Path,
        cancel: CancellationToken,
        status_tx: Option<&tokio::sync::mpsc::Sender<BuildStatus>>,
    ) -> BuildResult<WorkspaceCompileOutput> {
        info!(project = %project_id, "Starting VM-based workspace compilation");

        let build_dir = target_dir.parent().unwrap_or(target_dir);
        std::fs::create_dir_all(build_dir)?;

        // Create source drive from checkout directory
        let source_img = build_dir.join("source.ext4");
        create_ext4_from_dir(&checkout.path, &source_img, "source", 512)?;

        // Create empty target drive for build output (2GB)
        let target_img = build_dir.join("target.ext4");
        create_empty_ext4(&target_img, 2048, "target")?;

        // Configure VM with source and target as additional drives
        let vm_config = VmConfig {
            kernel_path: self.config.kernel_path.clone(),
            rootfs_path: self.config.builder_rootfs.clone(),
            vcpu_count: self.config.vcpu_count,
            mem_size_mib: self.config.mem_size_mib,
            vsock_cid: 3,
            boot_args: "console=ttyS0 reboot=k panic=1 pci=off".to_owned(),
            additional_drives: vec![
                DriveConfig {
                    drive_id: "source".to_owned(),
                    path: source_img.clone(),
                    is_root_device: false,
                    is_read_only: true,
                },
                DriveConfig {
                    drive_id: "target".to_owned(),
                    path: target_img.clone(),
                    is_root_device: false,
                    is_read_only: false,
                },
            ],
        };

        // Start VM
        let mut vm = self
            .vm_manager
            .start(vm_config)
            .await
            .map_err(|e| BuildError::SandboxSetup(format!("Failed to start VM: {e}")))?;

        // Connect to builder via vsock
        let vsock_path = vm.vsock_uds_path().to_path_buf();
        debug!(path = %vsock_path.display(), "Connecting to builder VM via vsock");

        // Start the cargo proxy server for this VM
        // The proxy listens on {vsock_path}_{PROXY_PORT} for connections from the VM
        let proxy_path = PathBuf::from(format!("{}_{PROXY_PORT}", vsock_path.display()));
        let proxy_server = ProxyServer::new(CargoProxy::with_defaults());
        let proxy_cancel = cancel.clone();

        let proxy_handle = tokio::spawn(async move {
            if let Err(e) = proxy_server.run(&proxy_path, proxy_cancel).await {
                warn!(error = %e, "Cargo proxy server error");
            }
        });
        debug!(port = PROXY_PORT, "Cargo proxy server started");

        // Wait for VM to be ready
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Connect to the builder runtime
        let stream = connect_to_builder(&vsock_path, BUILD_PORT).await?;

        // Send build request
        let request = BuildRequest::new(uuid::Uuid::new_v4().to_string(), &self.config.target)
            .with_release(true)
            .with_locked(true);

        send_message(&stream, &BuildMessage::Request(request.clone())).await?;

        // Stream output and collect result
        let result = receive_build_output(stream, status_tx, cancel.clone()).await?;

        // Shutdown VM
        if let Err(e) = vm.shutdown().await {
            warn!("Failed to shutdown VM cleanly: {e}");
        }

        // Cancel and wait for proxy to stop
        cancel.cancel();
        let _ = proxy_handle.await;

        // Clean up drive images
        let _ = std::fs::remove_file(&source_img);
        let _ = std::fs::remove_file(&target_img);

        // Convert protocol result to compile output
        if result.success {
            let binaries: Vec<BinaryInfo> = result
                .binaries
                .into_iter()
                .map(|b| BinaryInfo {
                    name: b.name,
                    path: PathBuf::from(&b.path),
                    crate_dir: PathBuf::from(&b.crate_dir),
                })
                .collect();

            info!(
                binary_count = binaries.len(),
                duration_secs = result.duration_secs,
                "VM compilation complete"
            );

            Ok(WorkspaceCompileOutput {
                binaries,
                duration: Duration::from_secs_f64(result.duration_secs),
                stdout: result.stdout,
                stderr: result.stderr,
            })
        } else {
            Err(BuildError::CompileFailed {
                stderr: result.stderr,
                exit_code: result.exit_code,
            })
        }
    }
}

/// Connect to the builder runtime via vsock using Firecracker's CONNECT protocol.
async fn connect_to_builder(vsock_path: &Path, port: u32) -> BuildResult<UnixStream> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    for attempt in 0..30 {
        // Connect to the vsock UDS
        match UnixStream::connect(vsock_path).await {
            Ok(mut stream) => {
                // Send CONNECT command
                let connect_cmd = format!("CONNECT {port}\n");
                if let Err(e) = stream.write_all(connect_cmd.as_bytes()).await {
                    debug!(attempt, error = %e, "Failed to send CONNECT command");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }

                // Read response
                let mut reader = BufReader::new(&mut stream);
                let mut response = String::new();
                if let Err(e) = reader.read_line(&mut response).await {
                    debug!(attempt, error = %e, "Failed to read CONNECT response");
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    continue;
                }

                if response.starts_with("OK ") {
                    debug!(port, "vsock connection established");
                    return Ok(stream);
                }

                debug!(attempt, response = %response.trim(), "CONNECT failed");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => {
                debug!(attempt, error = %e, "Failed to connect to vsock UDS, retrying...");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }

    Err(BuildError::SandboxSetup(
        "Failed to connect to builder VM after 30 attempts".to_owned(),
    ))
}

/// Send a message over the stream.
async fn send_message<T>(stream: &UnixStream, message: &T) -> BuildResult<()>
where
    T: Archive,
    T: for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
{
    let bytes = rkyv::to_bytes::<RkyvError>(message)
        .map_err(|e| BuildError::SandboxSetup(format!("Serialisation failed: {e}")))?;

    #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
    let len = bytes.len() as u32;

    stream.writable().await?;

    // Write in a single call using try_write for the header
    let mut buf = Vec::with_capacity(4 + bytes.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&bytes);

    let mut written = 0;
    while written < buf.len() {
        stream.writable().await?;
        match stream.try_write(&buf[written..]) {
            Ok(n) => written += n,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e.into()),
        }
    }

    Ok(())
}

/// Receive build output and final result.
async fn receive_build_output(
    stream: UnixStream,
    status_tx: Option<&tokio::sync::mpsc::Sender<BuildStatus>>,
    cancel: CancellationToken,
) -> BuildResult<ProtocolBuildResult> {
    let mut reader = tokio::io::BufReader::new(stream);

    loop {
        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: crate::error::CancelReason::UserRequested,
            });
        }

        // Read length header
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(BuildError::SandboxSetup(
                    "Connection closed unexpectedly".to_owned(),
                ));
            }
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_le_bytes(len_buf) as usize;

        // Read message
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        // Deserialise
        let message: BuildMessage = rkyv::from_bytes::<BuildMessage, RkyvError>(&buf)
            .map_err(|e| BuildError::SandboxSetup(format!("Deserialisation failed: {e}")))?;

        match message {
            BuildMessage::Request(_) => {
                warn!("Received unexpected request message from builder");
            }
            BuildMessage::Output(output) => match &output {
                BuildOutput::Stdout(line) => {
                    debug!("[cargo stdout] {line}");
                    if let Some(tx) = status_tx {
                        let _ = tx
                            .send(BuildStatus::Compiling {
                                progress: Some(line.clone()),
                            })
                            .await;
                    }
                }
                BuildOutput::Stderr(line) => {
                    debug!("[cargo stderr] {line}");
                }
                BuildOutput::Progress { stage, progress } => {
                    debug!("[progress] {stage}: {:.1}%", progress * 100.0);
                }
            },
            BuildMessage::Result(result) => {
                return Ok(result);
            }
        }
    }
}

/// Create an ext4 filesystem image from a directory.
fn create_ext4_from_dir(
    source: &Path,
    output: &Path,
    label: &str,
    size_mb: u32,
) -> BuildResult<()> {
    // Create a sparse file of the target size
    let size = format!("{}M", size_mb);
    let status = Command::new("truncate")
        .args(["-s", &size])
        .arg(output)
        .status()?;

    if !status.success() {
        return Err(BuildError::SandboxSetup(
            "truncate failed to create sparse file".to_owned(),
        ));
    }

    // Create ext4 filesystem and populate with source directory contents
    let status = Command::new("mkfs.ext4")
        .args(["-d", &source.to_string_lossy()])
        .args(["-L", label])
        .arg(output)
        .status()?;

    if !status.success() {
        return Err(BuildError::SandboxSetup(
            "mkfs.ext4 failed to create filesystem".to_owned(),
        ));
    }

    Ok(())
}

/// Create an empty ext4 filesystem image.
fn create_empty_ext4(output: &Path, size_mb: u32, label: &str) -> BuildResult<()> {
    // Create a sparse file
    let size = format!("{}M", size_mb);
    let status = Command::new("truncate")
        .args(["-s", &size])
        .arg(output)
        .status()?;

    if !status.success() {
        return Err(BuildError::SandboxSetup(
            "truncate failed to create sparse file".to_owned(),
        ));
    }

    // Format as ext4
    let status = Command::new("mkfs.ext4")
        .args(["-L", label])
        .arg(output)
        .status()?;

    if !status.success() {
        return Err(BuildError::SandboxSetup(
            "mkfs.ext4 failed to format filesystem".to_owned(),
        ));
    }

    Ok(())
}
