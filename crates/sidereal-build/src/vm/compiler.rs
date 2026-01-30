//! Firecracker-based compiler implementation.
//!
//! Provides a complete build pipeline inside Firecracker microVMs. The VM handles
//! everything: git clone, caching, compilation, project discovery, artifact creation,
//! and upload to S3. The host only manages VM lifecycle and credential passing.

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
    BuildMessage, BuildOutput as ProtocolBuildOutput, BuildRequest,
    BuildResult as ProtocolBuildResult, CacheConfig as ProtocolCacheConfig, GitConfig,
    RuntimeConfig, S3Config, BUILD_PORT,
};
use crate::proxy::{
    CargoProxy, ProxyConfig, ProxyServer, TcpForwarder, PROXY_PORT, S3_FORWARD_PORT,
};
use crate::types::{BuildStatus, FunctionMetadata, ProjectId};

/// Configuration for the VM-based compiler.
#[derive(Debug, Clone)]
pub struct VmCompilerConfig {
    /// Path to the Linux kernel for VMs.
    pub kernel_path: PathBuf,

    /// Path to the builder rootfs image.
    pub builder_rootfs: PathBuf,

    /// Number of vCPUs for builder VMs.
    pub vcpu_count: u8,

    /// Memory size in MB for builder VMs.
    pub mem_size_mib: u32,

    /// Rust target triple.
    pub target: String,

    /// Build timeout.
    pub timeout: Duration,

    /// S3 configuration for artifacts and caches.
    pub s3: S3Config,

    /// S3 key for the runtime binary.
    pub runtime_s3_key: String,

    /// Additional domains to allow through the proxy.
    pub additional_proxy_domains: Vec<String>,
}

impl VmCompilerConfig {
    /// Create a new configuration with required S3 settings.
    #[must_use]
    pub fn new(s3: S3Config, runtime_s3_key: impl Into<String>) -> Self {
        Self {
            kernel_path: PathBuf::from("/var/lib/sidereal/kernel/vmlinux"),
            builder_rootfs: PathBuf::from("/var/lib/sidereal/rootfs/builder.ext4"),
            vcpu_count: 2,
            mem_size_mib: 4096,
            target: "x86_64-unknown-linux-musl".to_owned(),
            timeout: Duration::from_secs(600),
            s3,
            runtime_s3_key: runtime_s3_key.into(),
            additional_proxy_domains: Vec::new(),
        }
    }

    /// Add additional domains to the proxy allowlist.
    #[must_use]
    pub fn with_proxy_domains(
        mut self,
        domains: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.additional_proxy_domains = domains.into_iter().map(Into::into).collect();
        self
    }
}

/// Input parameters for a build operation.
#[derive(Debug, Clone)]
pub struct BuildInput {
    /// Unique build identifier.
    pub build_id: String,
    /// Project identifier.
    pub project_id: ProjectId,
    /// Repository URL.
    pub repo_url: String,
    /// Branch name.
    pub branch: String,
    /// Commit SHA.
    pub commit_sha: String,
    /// SSH key for private repos (optional).
    pub ssh_key: Option<String>,
    /// Subdirectory within the repo (optional).
    pub subpath: Option<String>,
    /// Registry cache S3 key (optional).
    pub registry_cache_key: Option<String>,
    /// Target cache S3 key (optional).
    pub target_cache_key: Option<String>,
}

/// Output from a successful build operation.
#[derive(Debug, Clone)]
pub struct BuildOutput {
    /// S3 URL of the uploaded artifact.
    pub artifact_url: String,
    /// SHA-256 hash of the artifact.
    pub artifact_hash: String,
    /// Discovered functions.
    pub functions: Vec<FunctionMetadata>,
    /// Build duration in seconds.
    pub duration_secs: f64,
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

    /// Execute a complete build in a Firecracker VM.
    ///
    /// The VM handles all phases: clone, cache pull, compile, discovery, artifact
    /// creation, upload, and cache push.
    pub async fn build(
        &self,
        input: &BuildInput,
        cancel: CancellationToken,
        status_tx: Option<&tokio::sync::mpsc::Sender<BuildStatus>>,
    ) -> BuildResult<BuildOutput> {
        info!(
            project = %input.project_id,
            build_id = %input.build_id,
            "Starting VM-based build pipeline"
        );

        // Create empty target drive for build output (6GB)
        let build_dir = self.vm_manager.work_dir().join(&input.build_id);
        std::fs::create_dir_all(&build_dir)?;

        let target_img = build_dir.join("target.ext4");
        create_empty_ext4(&target_img, 6144, "target")?;

        // Configure VM with only the target drive (source will be cloned inside)
        let vm_config = VmConfig {
            kernel_path: self.config.kernel_path.clone(),
            rootfs_path: self.config.builder_rootfs.clone(),
            vcpu_count: self.config.vcpu_count,
            mem_size_mib: self.config.mem_size_mib,
            vsock_cid: 3,
            boot_args: "console=ttyS0 reboot=k panic=1 pci=off rw".to_owned(),
            additional_drives: vec![DriveConfig {
                drive_id: "target".to_owned(),
                path: target_img.clone(),
                is_root_device: false,
                is_read_only: false,
            }],
        };

        // Start VM
        let mut vm = self
            .vm_manager
            .start(vm_config)
            .await
            .map_err(|e| BuildError::SandboxSetup(format!("Failed to start VM: {e}")))?;

        let vsock_path = vm.vsock_uds_path().to_path_buf();
        debug!(path = %vsock_path.display(), "Connecting to builder VM via vsock");

        // Start proxy server with S3 endpoint added to allowed domains
        let proxy_path = PathBuf::from(format!("{}_{PROXY_PORT}", vsock_path.display()));
        let proxy_config = self.create_proxy_config();
        let proxy_server = ProxyServer::new(CargoProxy::new(proxy_config));
        let proxy_cancel = cancel.child_token();
        let proxy_cancel_trigger = proxy_cancel.clone();

        let proxy_handle = tokio::spawn(async move {
            if let Err(e) = proxy_server.run(&proxy_path, proxy_cancel).await {
                warn!(error = %e, "Cargo proxy server error");
            }
        });
        debug!(port = PROXY_PORT, "Cargo proxy server started");

        // Start S3/Garage forwarder
        let s3_path = PathBuf::from(format!("{}_{S3_FORWARD_PORT}", vsock_path.display()));
        let s3_target = self.get_s3_forward_target();
        let s3_forwarder = TcpForwarder::new(s3_target);
        let s3_cancel = cancel.child_token();

        let s3_cancel_trigger = s3_cancel.clone();
        let s3_handle = tokio::spawn(async move {
            if let Err(e) = s3_forwarder.run(&s3_path, s3_cancel).await {
                warn!(error = %e, "S3 forwarder error");
            }
        });
        debug!(port = S3_FORWARD_PORT, target = %s3_target, "S3 forwarder started");

        let console_log_path = vm.console_log_path().to_path_buf();
        let read_console_tail = || -> String {
            std::fs::read_to_string(&console_log_path)
                .map(|s| {
                    let lines: Vec<&str> = s.lines().collect();
                    let start = lines.len().saturating_sub(50);
                    lines[start..].join("\n")
                })
                .unwrap_or_else(|_| "Unable to read console log".to_owned())
        };

        // Poll VM health while waiting for vsock socket to appear
        let ready_timeout = Duration::from_secs(5);
        let poll_interval = Duration::from_millis(100);
        let start = tokio::time::Instant::now();

        while start.elapsed() < ready_timeout {
            if let Some(status) = vm.check_exited() {
                let console_tail = read_console_tail();
                // Clean up
                proxy_cancel_trigger.cancel();
                s3_cancel_trigger.cancel();
                let _ = proxy_handle.await;
                let _ = s3_handle.await;
                let _ = std::fs::remove_dir_all(&build_dir);

                return Err(BuildError::SandboxSetup(format!(
                    "VM exited with status {status} during startup\n\nConsole log:\n{console_tail}"
                )));
            }
            if vsock_path.exists() {
                break;
            }
            tokio::time::sleep(poll_interval).await;
        }

        // Build the protocol request
        let request = self.create_build_request(input);

        // Perform build communication with timeout
        let timeout = self.config.timeout;
        let build_result: BuildResult<ProtocolBuildResult> = match tokio::time::timeout(
            timeout,
            perform_build(
                &vsock_path,
                request,
                &mut vm,
                &read_console_tail,
                status_tx,
                cancel.clone(),
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(BuildError::Timeout { limit: timeout }),
        };

        // Shutdown VM
        if let Err(e) = vm.shutdown().await {
            warn!("Failed to shutdown VM cleanly: {e}");
        }

        proxy_cancel_trigger.cancel();
        s3_cancel_trigger.cancel();
        let _ = proxy_handle.await;
        let _ = s3_handle.await;

        // Clean up build directory
        let _ = std::fs::remove_dir_all(&build_dir);

        // Handle build result
        let result = match build_result {
            Ok(r) => r,
            Err(e) => {
                let console_tail = read_console_tail();
                return Err(BuildError::SandboxSetup(format!(
                    "{e}\n\nVM console log (last 50 lines):\n{console_tail}"
                )));
            }
        };

        // Convert protocol result to build output
        if result.success {
            let artifact_url = result.artifact_url.ok_or_else(|| {
                BuildError::Internal("Build succeeded but no artifact URL returned".to_owned())
            })?;
            let artifact_hash = result.artifact_hash.ok_or_else(|| {
                BuildError::Internal("Build succeeded but no artifact hash returned".to_owned())
            })?;

            let functions: Vec<FunctionMetadata> = result
                .functions
                .into_iter()
                .map(|f| FunctionMetadata {
                    name: f.name,
                    route: f.route,
                    method: f.method,
                    queue: f.queue,
                })
                .collect();

            info!(
                artifact_url = %artifact_url,
                function_count = functions.len(),
                duration_secs = result.duration_secs,
                "VM build complete"
            );

            Ok(BuildOutput {
                artifact_url,
                artifact_hash,
                functions,
                duration_secs: result.duration_secs,
            })
        } else {
            let console_tail = read_console_tail();
            let error_message = result
                .error_message
                .unwrap_or_else(|| result.stderr.clone());
            Err(BuildError::CompileFailed {
                stderr: format!(
                    "{}\n\nVM console log (last 50 lines):\n{console_tail}",
                    error_message
                ),
                exit_code: result.exit_code,
            })
        }
    }

    fn create_proxy_config(&self) -> ProxyConfig {
        let mut config = ProxyConfig::default();

        // Add S3 endpoint domain to allowlist
        if let Some(domain) = extract_domain(&self.config.s3.endpoint) {
            config.allow_domain(domain);
        }

        // Add any additional configured domains
        for domain in &self.config.additional_proxy_domains {
            config.allow_domain(domain.clone());
        }

        config
    }

    /// Get the target address for S3 forwarding.
    ///
    /// Extracts host and port from the S3 endpoint URL.
    fn get_s3_forward_target(&self) -> std::net::SocketAddr {
        let endpoint = &self.config.s3.endpoint;

        // Parse the endpoint URL to extract host and port
        if let Some(rest) = endpoint.strip_prefix("http://") {
            if let Some((host, port)) = rest.split_once(':') {
                let port = port.trim_end_matches('/').parse().unwrap_or(80);
                if let Ok(addr) = format!("{host}:{port}").parse() {
                    return addr;
                }
            }
            // No port specified, use default
            if let Ok(addr) = format!("{rest}:80").parse() {
                return addr;
            }
        } else if let Some(rest) = endpoint.strip_prefix("https://") {
            if let Some((host, port)) = rest.split_once(':') {
                let port = port.trim_end_matches('/').parse().unwrap_or(443);
                if let Ok(addr) = format!("{host}:{port}").parse() {
                    return addr;
                }
            }
            // No port specified, use default
            if let Ok(addr) = format!("{rest}:443").parse() {
                return addr;
            }
        }

        // Default fallback
        "127.0.0.1:3900".parse().unwrap()
    }

    fn create_build_request(&self, input: &BuildInput) -> BuildRequest {
        let cache = if input.registry_cache_key.is_some() || input.target_cache_key.is_some() {
            Some(ProtocolCacheConfig {
                registry_key: input.registry_cache_key.clone(),
                target_key: input.target_cache_key.clone(),
            })
        } else {
            None
        };

        BuildRequest {
            build_id: input.build_id.clone(),
            project_id: input.project_id.to_string(),
            git: GitConfig {
                repo_url: input.repo_url.clone(),
                branch: input.branch.clone(),
                commit_sha: input.commit_sha.clone(),
                ssh_key: input.ssh_key.clone(),
                subpath: input.subpath.clone(),
            },
            s3: self.config.s3.clone(),
            cache,
            runtime: RuntimeConfig {
                s3_key: self.config.runtime_s3_key.clone(),
            },
            target_triple: self.config.target.clone(),
            release: true,
            locked: true,
            artifact_key_prefix: format!("artifacts/{}", input.project_id),
        }
    }
}

/// Extract domain from a URL.
fn extract_domain(url: &str) -> Option<String> {
    url.trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .map(|s| s.split(':').next().unwrap_or(s).to_owned())
}

/// Connect to the builder runtime via vsock using Firecracker's CONNECT protocol.
async fn connect_to_builder<F>(
    vsock_path: &Path,
    port: u32,
    vm: &mut sidereal_firecracker::VmInstance,
    read_console_tail: &F,
) -> BuildResult<UnixStream>
where
    F: Fn() -> String,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let max_attempts = 30;
    let mut delay = Duration::from_millis(50);
    let max_delay = Duration::from_millis(500);

    for attempt in 0..max_attempts {
        if let Some(status) = vm.check_exited() {
            return Err(BuildError::SandboxSetup(format!(
                "VM exited with status {status} (attempt {attempt})\n\nConsole log:\n{}",
                read_console_tail()
            )));
        }

        match UnixStream::connect(vsock_path).await {
            Ok(mut stream) => {
                let connect_cmd = format!("CONNECT {port}\n");
                if let Err(e) = stream.write_all(connect_cmd.as_bytes()).await {
                    debug!(attempt, error = %e, "Failed to send CONNECT command");
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, max_delay);
                    continue;
                }

                let mut reader = BufReader::new(&mut stream);
                let mut response = String::new();
                if let Err(e) = reader.read_line(&mut response).await {
                    debug!(attempt, error = %e, "Failed to read CONNECT response");
                    tokio::time::sleep(delay).await;
                    delay = std::cmp::min(delay * 2, max_delay);
                    continue;
                }

                if response.starts_with("OK ") {
                    debug!(port, "vsock connection established");
                    return Ok(stream);
                }

                debug!(attempt, response = %response.trim(), "CONNECT failed");
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, max_delay);
            }
            Err(e) => {
                debug!(attempt, error = %e, "Failed to connect to vsock UDS, retrying...");
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(delay * 2, max_delay);
            }
        }
    }

    Err(BuildError::SandboxSetup(
        "Failed to connect to builder VM after 30 attempts".to_owned(),
    ))
}

/// Perform the build communication with the VM.
async fn perform_build<F>(
    vsock_path: &Path,
    request: BuildRequest,
    vm: &mut sidereal_firecracker::VmInstance,
    read_console_tail: &F,
    status_tx: Option<&tokio::sync::mpsc::Sender<BuildStatus>>,
    cancel: CancellationToken,
) -> BuildResult<ProtocolBuildResult>
where
    F: Fn() -> String,
{
    let stream = connect_to_builder(vsock_path, BUILD_PORT, vm, read_console_tail).await?;

    send_message(&stream, &BuildMessage::Request(request)).await?;

    receive_build_output(stream, status_tx, cancel, vm, read_console_tail).await
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

/// Receive build output and final result with periodic VM health checks.
async fn receive_build_output<F>(
    stream: UnixStream,
    status_tx: Option<&tokio::sync::mpsc::Sender<BuildStatus>>,
    cancel: CancellationToken,
    vm: &mut sidereal_firecracker::VmInstance,
    read_console_tail: &F,
) -> BuildResult<ProtocolBuildResult>
where
    F: Fn() -> String,
{
    let mut reader = tokio::io::BufReader::new(stream);
    let health_interval = Duration::from_millis(500);
    let mut last_health_check = tokio::time::Instant::now();

    loop {
        if cancel.is_cancelled() {
            return Err(BuildError::Cancelled {
                reason: crate::error::CancelReason::UserRequested,
            });
        }

        if last_health_check.elapsed() > health_interval {
            if let Some(status) = vm.check_exited() {
                return Err(BuildError::SandboxSetup(format!(
                    "VM exited unexpectedly with status {status}\n\nConsole log:\n{}",
                    read_console_tail()
                )));
            }
            last_health_check = tokio::time::Instant::now();
        }

        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                if let Some(status) = vm.check_exited() {
                    return Err(BuildError::SandboxSetup(format!(
                        "VM exited with status {status} during build\n\nConsole log:\n{}",
                        read_console_tail()
                    )));
                }
                return Err(BuildError::SandboxSetup(
                    "Connection closed unexpectedly".to_owned(),
                ));
            }
            Err(e) => return Err(e.into()),
        }

        #[allow(clippy::cast_possible_truncation)]
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        let message: BuildMessage = rkyv::from_bytes::<BuildMessage, RkyvError>(&buf)
            .map_err(|e| BuildError::SandboxSetup(format!("Deserialisation failed: {e}")))?;

        match message {
            BuildMessage::Request(_) => {
                warn!("Received unexpected request message from builder");
            }
            BuildMessage::Output(output) => {
                handle_build_output(&output, status_tx).await;
            }
            BuildMessage::Result(result) => {
                return Ok(result);
            }
        }
    }
}

/// Handle a build output message, updating status as appropriate.
async fn handle_build_output(
    output: &ProtocolBuildOutput,
    status_tx: Option<&tokio::sync::mpsc::Sender<BuildStatus>>,
) {
    match output {
        ProtocolBuildOutput::Stdout(line) => {
            debug!("[cargo stdout] {line}");
            if let Some(tx) = status_tx {
                let _ = tx
                    .send(BuildStatus::Compiling {
                        progress: Some(line.clone()),
                    })
                    .await;
            }
        }
        ProtocolBuildOutput::Stderr(line) => {
            debug!("[cargo stderr] {line}");
        }
        ProtocolBuildOutput::Progress { stage, progress } => {
            debug!("[progress] {stage}: {:.1}%", progress * 100.0);
        }
        ProtocolBuildOutput::Cloning { repo } => {
            info!("[clone] Cloning {repo}");
            if let Some(tx) = status_tx {
                let _ = tx.send(BuildStatus::CheckingOut).await;
            }
        }
        ProtocolBuildOutput::PullingCache => {
            info!("[cache] Pulling build caches");
            if let Some(tx) = status_tx {
                let _ = tx.send(BuildStatus::PullingCache).await;
            }
        }
        ProtocolBuildOutput::PushingCache => {
            info!("[cache] Pushing build caches");
            if let Some(tx) = status_tx {
                let _ = tx.send(BuildStatus::PushingCache).await;
            }
        }
        ProtocolBuildOutput::CreatingArtifact => {
            info!("[artifact] Creating rootfs artifact");
            if let Some(tx) = status_tx {
                let _ = tx.send(BuildStatus::GeneratingArtifact).await;
            }
        }
        ProtocolBuildOutput::UploadingArtifact { progress } => {
            debug!("[upload] Uploading artifact: {:.1}%", progress * 100.0);
        }
        ProtocolBuildOutput::DiscoveringProjects => {
            info!("[discovery] Discovering deployable projects");
            if let Some(tx) = status_tx {
                let _ = tx.send(BuildStatus::DiscoveringProjects).await;
            }
        }
        ProtocolBuildOutput::DownloadingRuntime => {
            info!("[runtime] Downloading runtime binary");
        }
    }
}

/// Create an empty ext4 filesystem image.
fn create_empty_ext4(output: &Path, size_mb: u32, label: &str) -> BuildResult<()> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_domain_https() {
        assert_eq!(
            extract_domain("https://s3.amazonaws.com/bucket"),
            Some("s3.amazonaws.com".to_owned())
        );
    }

    #[test]
    fn extract_domain_with_port() {
        assert_eq!(
            extract_domain("http://localhost:9000"),
            Some("localhost".to_owned())
        );
    }

    #[test]
    fn extract_domain_empty() {
        assert_eq!(extract_domain(""), Some(String::new()));
    }
}
