//! Builder runtime for Sidereal compilation VMs.
//!
//! This binary runs as `/sbin/init` inside Firecracker builder VMs. It:
//! 1. Initialises the VM (mounts filesystems)
//! 2. Listens on vsock port 1028 for build requests
//! 3. Executes cargo zigbuild for the workspace
//! 4. Streams stdout/stderr back to the host
//! 5. Sends the build result with binary locations
//! 6. Shuts down the VM

mod build;
mod vsock;

use std::process::ExitCode;

use nix::mount::{mount, MsFlags};
use nix::sys::reboot::{reboot, RebootMode};

use crate::build::execute_build;
use crate::vsock::run_vsock_server;

/// Mount pseudo-filesystems required for a functional Linux environment.
fn mount_filesystems() -> anyhow::Result<()> {
    // Mount /proc
    mount(
        Some("proc"),
        "/proc",
        Some("proc"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    // Mount /sys
    mount(
        Some("sysfs"),
        "/sys",
        Some("sysfs"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    // Mount /dev
    mount(
        Some("devtmpfs"),
        "/dev",
        Some("devtmpfs"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    // Mount /dev/pts for pseudo-terminals
    std::fs::create_dir_all("/dev/pts").ok();
    mount(
        Some("devpts"),
        "/dev/pts",
        Some("devpts"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    // Mount /tmp as tmpfs
    mount(
        Some("tmpfs"),
        "/tmp",
        Some("tmpfs"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    // Mount /run as tmpfs
    std::fs::create_dir_all("/run").ok();
    mount(
        Some("tmpfs"),
        "/run",
        Some("tmpfs"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    Ok(())
}

/// Shutdown the VM.
fn shutdown() {
    eprintln!("[init] Shutting down...");

    // Sync filesystems
    unsafe { libc::sync() };

    // Power off - this uses libc directly for the reboot syscall
    // Note: reboot() is infallible on success (never returns), so we only handle error case
    match reboot(RebootMode::RB_POWER_OFF) {
        Ok(infallible) => match infallible {},
        Err(e) => {
            eprintln!("[init] Failed to power off: {e}");
            // If reboot fails, try halting
            let _ = reboot(RebootMode::RB_HALT_SYSTEM);
        }
    }
}

fn main() -> ExitCode {
    eprintln!("[init] Sidereal builder runtime starting...");

    // Mount essential filesystems
    if let Err(e) = mount_filesystems() {
        eprintln!("[init] Failed to mount filesystems: {e}");
        shutdown();
        return ExitCode::FAILURE;
    }
    eprintln!("[init] Filesystems mounted");

    // Set up environment for cargo
    std::env::set_var("HOME", "/tmp");
    std::env::set_var("USER", "build");

    // Set up PATH to include tools from /opt
    let path = "/opt/rust/bin:\
                /opt/cargo-zigbuild/bin:\
                /opt/zig/bin:\
                /opt/gcc/bin:\
                /opt/busybox/bin:\
                /opt/git/bin:\
                /opt/pkg-config/bin:\
                /usr/bin:/bin:/sbin";
    std::env::set_var("PATH", path);

    // Set SSL certificates path
    std::env::set_var("SSL_CERT_FILE", "/opt/cacert/etc/ssl/certs/ca-bundle.crt");
    std::env::set_var("SSL_CERT_DIR", "/opt/cacert/etc/ssl/certs");

    // Set up pkg-config for OpenSSL
    std::env::set_var("PKG_CONFIG_PATH", "/opt/openssl-dev/lib/pkgconfig");
    std::env::set_var("OPENSSL_DIR", "/opt/openssl");

    // Run the tokio runtime for vsock server
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("[init] Failed to create tokio runtime: {e}");
            shutdown();
            return ExitCode::FAILURE;
        }
    };

    // Run the vsock server (handles a single build, then exits)
    let result = runtime.block_on(async {
        match run_vsock_server(execute_build).await {
            Ok(()) => {
                eprintln!("[init] Build completed successfully");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("[init] Build failed: {e}");
                ExitCode::FAILURE
            }
        }
    });

    // Shutdown the VM
    shutdown();

    result
}
