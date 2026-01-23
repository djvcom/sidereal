//! Filesystem setup for the builder VM.
//!
//! Mounts required filesystems and prepares mount points for
//! virtio-blk drives containing source code, target directory, and cargo cache.

use nix::mount::{mount, MsFlags};
use std::fs;
use std::path::Path;
use tracing::{debug, info, warn};

/// Mount all required filesystems for the builder VM.
pub fn mount_filesystems() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Mounting filesystems");

    mount_proc()?;
    mount_sys()?;
    mount_tmp()?;
    mount_dev()?;

    Ok(())
}

/// Create mount points for virtio-blk drives.
///
/// These directories will be used by the host to mount:
/// - /source: User's git checkout (read-only)
/// - /target: Compilation output (read-write)
/// - /cargo: Cargo registry cache (read-write)
pub fn create_mount_points() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let dirs = ["/source", "/target", "/cargo"];

    for dir in dirs {
        let path = Path::new(dir);
        if !path.exists() {
            fs::create_dir_all(path)?;
            debug!(path = %dir, "created mount point");
        }
    }

    Ok(())
}

fn mount_proc() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let target = Path::new("/proc");
    if !target.exists() {
        fs::create_dir_all(target)?;
    }

    debug!("Mounting /proc");
    mount(
        Some("proc"),
        target,
        Some("proc"),
        MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC | MsFlags::MS_NODEV,
        None::<&str>,
    )?;

    Ok(())
}

fn mount_sys() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let target = Path::new("/sys");
    if !target.exists() {
        fs::create_dir_all(target)?;
    }

    debug!("Mounting /sys");
    mount(
        Some("sysfs"),
        target,
        Some("sysfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC | MsFlags::MS_NODEV,
        None::<&str>,
    )?;

    Ok(())
}

fn mount_tmp() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let target = Path::new("/tmp");
    if !target.exists() {
        fs::create_dir_all(target)?;
    }

    debug!("Mounting /tmp (512M tmpfs)");
    mount(
        Some("tmpfs"),
        target,
        Some("tmpfs"),
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
        Some("size=512M"),
    )?;

    Ok(())
}

fn mount_dev() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let target = Path::new("/dev");
    if !target.exists() {
        fs::create_dir_all(target)?;
    }

    debug!("Mounting /dev");
    mount(
        Some("devtmpfs"),
        target,
        Some("devtmpfs"),
        MsFlags::MS_NOSUID,
        None::<&str>,
    )?;

    Ok(())
}

/// Set up the environment for cargo builds.
///
/// Creates necessary directories and sets up symlinks for tools.
pub fn setup_build_environment() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Setting up build environment");

    // Ensure /bin/sh exists (required by cargo for build scripts)
    let bin_sh = Path::new("/bin/sh");
    if !bin_sh.exists() {
        // Try to find busybox or bash
        let shell_candidates = [
            "/opt/busybox/bin/busybox",
            "/opt/busybox/bin/sh",
            "/usr/bin/bash",
            "/bin/bash",
        ];

        for candidate in shell_candidates {
            let candidate_path = Path::new(candidate);
            if candidate_path.exists() {
                if let Some(parent) = bin_sh.parent() {
                    fs::create_dir_all(parent)?;
                }

                // Create symlink to the shell
                std::os::unix::fs::symlink(candidate_path, bin_sh)?;

                debug!(shell = %candidate, "linked /bin/sh");
                break;
            }
        }

        if !bin_sh.exists() {
            warn!("/bin/sh not available - build scripts may fail");
        }
    }

    // Set up PATH components
    setup_path_symlinks()?;

    Ok(())
}

/// Create symlinks in /usr/bin for tools installed in /opt.
fn setup_path_symlinks() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let usr_bin = Path::new("/usr/bin");
    if !usr_bin.exists() {
        fs::create_dir_all(usr_bin)?;
    }

    // Tools we need accessible in PATH
    let tools = [
        ("/opt/rust/bin/cargo", "/usr/bin/cargo"),
        ("/opt/rust/bin/rustc", "/usr/bin/rustc"),
        (
            "/opt/cargo-zigbuild/bin/cargo-zigbuild",
            "/usr/bin/cargo-zigbuild",
        ),
        ("/opt/zig/bin/zig", "/usr/bin/zig"),
        ("/opt/gcc/bin/gcc", "/usr/bin/gcc"),
        ("/opt/gcc/bin/cc", "/usr/bin/cc"),
        ("/opt/git/bin/git", "/usr/bin/git"),
        ("/opt/pkg-config/bin/pkg-config", "/usr/bin/pkg-config"),
    ];

    for (source, target) in tools {
        let source_path = Path::new(source);
        let target_path = Path::new(target);

        if source_path.exists() && !target_path.exists() {
            std::os::unix::fs::symlink(source_path, target_path)?;
            debug!(source = %source, target = %target, "created symlink");
        }
    }

    Ok(())
}

/// Set up SSL certificate paths for cargo to use HTTPS.
pub fn setup_ssl_certs() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let etc_ssl = Path::new("/etc/ssl");
    if !etc_ssl.exists() {
        fs::create_dir_all(etc_ssl)?;
    }

    // Link CA certificates
    let cacert_source = Path::new("/opt/cacert/etc/ssl/certs");
    let certs_target = Path::new("/etc/ssl/certs");

    if cacert_source.exists() && !certs_target.exists() {
        std::os::unix::fs::symlink(cacert_source, certs_target)?;
        debug!("linked SSL certificates");
    }

    Ok(())
}
