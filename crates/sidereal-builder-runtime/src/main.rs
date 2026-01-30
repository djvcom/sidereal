//! Sidereal builder runtime - runs as /sbin/init inside builder VMs.
//!
//! This binary:
//! 1. Mounts required filesystems (proc, sys, tmp, dev)
//! 2. Mounts tmpfs over working directories for build isolation
//! 3. Sets up signal handlers (PID 1 responsibilities)
//! 4. Sets up the build environment (symlinks, certificates)
//! 5. Remounts rootfs read-only to prevent state leakage
//! 6. Listens on vsock port 1028 for a build request
//! 7. Clones the git repository
//! 8. Pulls caches from S3 (if configured)
//! 9. Downloads the runtime binary from S3
//! 10. Executes cargo zigbuild with streaming output
//! 11. Discovers deployable projects
//! 12. Creates artifact rootfs images
//! 13. Uploads artifacts to S3
//! 14. Pushes caches to S3 (if configured)
//! 15. Sends the build result back to the host
//! 16. Shuts down the VM

use std::path::Path;
use std::time::Instant;

use tracing::{error, info, warn};

mod artifact;
mod build;
mod cache;
mod discovery;
mod filesystem;
mod git;
mod network;
mod protocol;
mod s3;
mod signals;
mod vsock;

use protocol::{BuildOutput, BuildRequest, BuildResult};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("PATH", "/usr/bin:/usr/local/bin:/sbin:/bin:/usr/sbin");
    }

    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .init();

    info!("Sidereal builder runtime starting (PID 1)");

    // Mount core filesystems (proc, sys, tmp, dev)
    if let Err(e) = filesystem::mount_filesystems() {
        error!(error = %e, "Failed to mount filesystems");
    }

    // Mount build drives and tmpfs working directories
    if let Err(e) = filesystem::mount_build_drives() {
        error!(error = %e, "Failed to mount build drives");
    }

    signals::setup_signal_handlers();

    // Set up build environment (symlinks for tools)
    if let Err(e) = filesystem::setup_build_environment() {
        error!(error = %e, "Failed to set up build environment");
    }

    // Set up SSL certificates for HTTPS
    if let Err(e) = filesystem::setup_ssl_certs() {
        error!(error = %e, "Failed to set up SSL certificates");
    }

    // Remount rootfs read-only now that setup is complete
    // This prevents any state leakage between builds
    if let Err(e) = filesystem::remount_rootfs_readonly() {
        error!(error = %e, "Failed to remount rootfs read-only");
    }

    if let Err(e) = network::setup_loopback() {
        error!(error = %e, "Failed to set up loopback interface");
    }

    let _proxy_bridge = network::start_proxy_bridge(
        "127.0.0.1:1080".parse().expect("valid socket address"),
        1080,
    )
    .await;
    if let Err(ref e) = _proxy_bridge {
        error!(error = %e, "Failed to start proxy bridge");
    }

    let _s3_bridge = network::start_s3_bridge(3900).await;
    if let Err(ref e) = _s3_bridge {
        error!(error = %e, "Failed to start S3 bridge");
    }

    if let Err(e) = run_build().await {
        error!(error = %e, "Build workflow failed");
    }

    info!("Build complete, shutting down VM");
    shutdown_vm();
}

/// Main build workflow: accept request, execute full pipeline, send result.
async fn run_build() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (mut stream, request) = vsock::accept_build_request().await?;

    info!(
        build_id = %request.build_id,
        target = %request.target_triple,
        release = request.release,
        "Received build request"
    );

    let start_time = Instant::now();
    let result = execute_full_pipeline(&mut stream, &request).await;

    let duration_secs = start_time.elapsed().as_secs_f64();

    let build_result = match result {
        Ok(r) => r,
        Err(e) => {
            error!(error = %e, "Build pipeline failed");
            BuildResult::failure(e.to_string(), 1, duration_secs)
        }
    };

    vsock::send_result(&mut stream, build_result).await?;

    if let Err(e) = stream.shutdown(std::net::Shutdown::Both) {
        error!(error = %e, "Failed to shutdown vsock stream");
    }

    Ok(())
}

/// Execute the full build pipeline.
async fn execute_full_pipeline(
    stream: &mut tokio_vsock::VsockStream,
    request: &BuildRequest,
) -> Result<BuildResult, PipelineError> {
    let start_time = Instant::now();

    // Step 1: Clone repository
    vsock::send_output(
        stream,
        BuildOutput::Cloning {
            repo: request.git.repo_url.clone(),
        },
    )
    .await?;

    git::clone_repository(&request.git, &request.source_path()).await?;
    info!("Repository cloned successfully");

    // Step 2: Pull caches (if configured)
    if let Some(ref cache_config) = request.cache {
        vsock::send_output(stream, BuildOutput::PullingCache).await?;

        let s3_client = s3::S3Client::new(&request.s3)?;
        if let Err(e) = cache::pull_caches(&s3_client, cache_config).await {
            warn!(error = %e, "Failed to pull caches, continuing with fresh build");
        }
    }

    // Step 3: Download runtime binary
    vsock::send_output(stream, BuildOutput::DownloadingRuntime).await?;

    let runtime_path = Path::new("/tmp/runtime");
    let s3_client = s3::S3Client::new(&request.s3)?;
    s3_client
        .download(&request.runtime.s3_key, runtime_path)
        .await?;
    info!("Runtime binary downloaded");

    // Step 4: Compile
    vsock::send_output(
        stream,
        BuildOutput::Progress {
            stage: "compiling".to_owned(),
            progress: 0.0,
        },
    )
    .await?;

    let compile_result = build::compile(stream, request).await?;

    if !compile_result.success {
        return Ok(BuildResult::failure(
            compile_result.stderr.clone(),
            compile_result.exit_code,
            compile_result.duration_secs,
        )
        .with_stdout(compile_result.stdout)
        .with_stderr(compile_result.stderr));
    }

    // Step 5: Discover binaries
    let binaries = build::discover_binaries(request).await;
    if binaries.is_empty() {
        return Ok(BuildResult::failure(
            "No binaries found after compilation",
            0,
            compile_result.duration_secs,
        ));
    }
    info!(count = binaries.len(), "Discovered binaries");

    // Step 6: Discover projects
    vsock::send_output(stream, BuildOutput::DiscoveringProjects).await?;

    let projects = discovery::discover_projects(&binaries)?;
    if projects.is_empty() {
        return Ok(BuildResult::failure(
            "No deployable projects found (no sidereal.toml)",
            0,
            compile_result.duration_secs,
        ));
    }
    info!(count = projects.len(), "Discovered deployable projects");

    // Step 7: Create artifact (use first project for now)
    vsock::send_output(stream, BuildOutput::CreatingArtifact).await?;

    let project = &projects[0];
    let output_dir = Path::new("/output");
    std::fs::create_dir_all(output_dir)?;

    let artifact_info = artifact::create_artifact(project, runtime_path, output_dir)?;
    info!(
        hash = %artifact_info.hash,
        size = artifact_info.size,
        "Artifact created"
    );

    // Step 8: Upload artifact to S3
    vsock::send_output(stream, BuildOutput::UploadingArtifact { progress: 0.0 }).await?;

    let artifact_key = format!("{}/{}.ext4", request.artifact_key_prefix, project.name);
    let artifact_url = s3_client.upload(&artifact_info.path, &artifact_key).await?;
    info!(url = %artifact_url, "Artifact uploaded");

    // Step 9: Push caches (if configured)
    if let Some(ref cache_config) = request.cache {
        vsock::send_output(stream, BuildOutput::PushingCache).await?;

        if let Err(e) = cache::push_caches(&s3_client, cache_config).await {
            warn!(error = %e, "Failed to push caches");
        }
    }

    let total_duration = start_time.elapsed().as_secs_f64();

    Ok(BuildResult::success(
        artifact_url,
        artifact_info.hash,
        project.functions.clone(),
        total_duration,
    ))
}

/// Errors that can occur during the build pipeline.
#[derive(Debug, thiserror::Error)]
enum PipelineError {
    #[error("vsock error: {0}")]
    Vsock(#[from] vsock::VsockError),

    #[error("git error: {0}")]
    Git(#[from] git::GitError),

    #[error("S3 error: {0}")]
    S3(#[from] s3::S3Error),

    #[error("cache error: {0}")]
    Cache(#[from] cache::CacheError),

    #[error("discovery error: {0}")]
    Discovery(#[from] discovery::DiscoveryError),

    #[error("artifact error: {0}")]
    Artifact(#[from] artifact::ArtifactError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Shutdown the VM.
///
/// As PID 1, we need to properly shut down the system.
fn shutdown_vm() {
    std::thread::sleep(std::time::Duration::from_millis(500));

    #[allow(unsafe_code)]
    unsafe {
        libc::sync();
    }

    info!("Requesting VM poweroff");

    #[allow(unsafe_code)]
    unsafe {
        libc::reboot(0x4321_fedc);
    }

    std::process::exit(0);
}
