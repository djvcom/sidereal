//! Sidereal builder runtime - runs as /sbin/init inside builder VMs.
//!
//! This binary:
//! 1. Mounts required filesystems (proc, sys, tmp, dev)
//! 2. Sets up signal handlers (PID 1 responsibilities)
//! 3. Sets up the build environment (symlinks, certificates)
//! 4. Listens on vsock port 1028 for a build request
//! 5. Executes cargo zigbuild with streaming output
//! 6. Sends the build result back to the host
//! 7. Shuts down the VM

use tracing::{error, info};

mod build;
mod filesystem;
mod network;
mod protocol;
mod signals;
mod vsock;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Set PATH before any tool lookups. The kernel boots PID 1 with
    // PATH=/sbin:/bin which misses /usr/bin where cargo and rustc live.
    #[allow(unsafe_code)]
    unsafe {
        std::env::set_var("PATH", "/usr/bin:/usr/local/bin:/sbin:/bin:/usr/sbin");
    }

    // Initialise tracing (no ANSI colours for VM console)
    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .init();

    info!("Sidereal builder runtime starting (PID 1)");

    // Mount required filesystems
    if let Err(e) = filesystem::mount_filesystems() {
        error!(error = %e, "Failed to mount filesystems");
        // Continue anyway - some mounts may have succeeded
    }

    // Create mount points for virtio-blk drives
    if let Err(e) = filesystem::create_mount_points() {
        error!(error = %e, "Failed to create mount points");
    }

    // Mount virtio-blk drives (source and target)
    if let Err(e) = filesystem::mount_build_drives() {
        error!(error = %e, "Failed to mount build drives");
    }

    // Set up signal handlers
    signals::setup_signal_handlers();

    // Set up the build environment
    if let Err(e) = filesystem::setup_build_environment() {
        error!(error = %e, "Failed to set up build environment");
        // Continue - build may still work
    }

    // Set up SSL certificates for HTTPS
    if let Err(e) = filesystem::setup_ssl_certs() {
        error!(error = %e, "Failed to set up SSL certificates");
        // Continue - HTTP may still work
    }

    // Bring up loopback interface for local proxy bridge
    if let Err(e) = network::setup_loopback() {
        error!(error = %e, "Failed to set up loopback interface");
    }

    // Start vsock-to-TCP proxy bridge for cargo registry access
    let _proxy_bridge = network::start_proxy_bridge(
        "127.0.0.1:1080".parse().expect("valid socket address"),
        1080,
    )
    .await;
    if let Err(ref e) = _proxy_bridge {
        error!(error = %e, "Failed to start proxy bridge");
    }

    // Run the build workflow
    if let Err(e) = run_build().await {
        error!(error = %e, "Build workflow failed");
    }

    // Shutdown the VM
    info!("Build complete, shutting down VM");
    shutdown_vm();
}

/// Main build workflow: accept request, execute build, send result.
async fn run_build() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Wait for a build request from the host
    let (mut stream, request) = vsock::accept_build_request().await?;

    info!(
        build_id = %request.build_id,
        target = %request.target_triple,
        release = request.release,
        "Received build request"
    );

    // Execute the build (streams output back via vsock)
    build::execute_build(&mut stream, &request).await?;

    // Explicitly shutdown the stream to ensure all data is flushed
    // before we shutdown the VM. This is critical because the VM shutdown
    // will kill the vsock device, potentially losing buffered messages.
    if let Err(e) = stream.shutdown(std::net::Shutdown::Both) {
        error!(error = %e, "Failed to shutdown vsock stream");
    }

    Ok(())
}

/// Shutdown the VM.
///
/// As PID 1, we need to properly shut down the system.
fn shutdown_vm() {
    // Give time for any final I/O (especially vsock messages) to be delivered.
    // The kernel may buffer vsock data, and we need this to reach the host
    // before the VM dies. 500ms provides a reasonable margin.
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Sync filesystems
    #[allow(unsafe_code)]
    unsafe {
        libc::sync();
    }

    // Request VM poweroff
    // Note: In Firecracker, we can also just exit(0) and the VM will stop
    info!("Requesting VM poweroff");

    #[allow(unsafe_code)]
    unsafe {
        // LINUX_REBOOT_CMD_POWER_OFF = 0x4321fedc
        libc::reboot(0x4321_fedc);
    }

    // If reboot syscall failed, just exit
    std::process::exit(0);
}
