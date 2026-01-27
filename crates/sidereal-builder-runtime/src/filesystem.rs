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

/// Mount virtio-blk drives for source code and build output.
///
/// Mounts:
/// - `/dev/vdb` to `/source` (read-only) - user's git checkout
/// - `/dev/vdc` to `/target` (read-write) - compilation output
pub fn mount_build_drives() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Mounting build drives");

    // Ensure mount points exist
    create_mount_points()?;

    // Mount source drive (read-only)
    let source_dev = Path::new("/dev/vdb");
    let source_mount = Path::new("/source");

    if source_dev.exists() {
        debug!("Mounting /dev/vdb to /source (read-only)");
        mount(
            Some(source_dev),
            source_mount,
            Some("ext4"),
            MsFlags::MS_RDONLY,
            None::<&str>,
        )?;
    } else {
        warn!("/dev/vdb not present, skipping source mount");
    }

    // Mount target drive (read-write)
    let target_dev = Path::new("/dev/vdc");
    let target_mount = Path::new("/target");

    if target_dev.exists() {
        debug!("Mounting /dev/vdc to /target (read-write)");
        mount(
            Some(target_dev),
            target_mount,
            Some("ext4"),
            MsFlags::empty(),
            None::<&str>,
        )?;
    } else {
        warn!("/dev/vdc not present, skipping target mount");
    }

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
    setup_build_environment_in(Path::new("/"))
}

fn setup_build_environment_in(root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Setting up build environment");

    let bin_sh = root.join("bin/sh");
    if !bin_sh.exists() {
        let shell_candidates = [
            "bin/bash",
            "usr/bin/bash",
            "bin/dash",
            "opt/busybox/bin/sh",
            "opt/busybox/bin/busybox",
        ];

        for candidate in shell_candidates {
            let candidate_path = root.join(candidate);
            if candidate_path.exists() {
                if let Some(parent) = bin_sh.parent() {
                    fs::create_dir_all(parent)?;
                }

                std::os::unix::fs::symlink(&candidate_path, &bin_sh)?;
                debug!(shell = %candidate, "linked /bin/sh");
                break;
            }
        }

        if !bin_sh.exists() {
            warn!("/bin/sh not available - build scripts may fail");
        }
    }

    setup_path_symlinks_in(root)?;

    Ok(())
}

/// Create symlinks in /usr/bin for tools, checking Docker and Nix locations.
fn setup_path_symlinks_in(root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let usr_bin = root.join("usr/bin");
    if !usr_bin.exists() {
        fs::create_dir_all(&usr_bin)?;
    }

    // Each entry: (possible source locations checked in order, target under usr/bin)
    let tools: &[(&[&str], &str)] = &[
        (
            &[
                "usr/local/rustup/toolchains/1.92-x86_64-unknown-linux-gnu/bin/cargo",
                "opt/rust/bin/cargo",
            ],
            "cargo",
        ),
        (
            &[
                "usr/local/rustup/toolchains/1.92-x86_64-unknown-linux-gnu/bin/rustc",
                "opt/rust/bin/rustc",
            ],
            "rustc",
        ),
        (
            &[
                "usr/local/cargo/bin/cargo-zigbuild",
                "opt/cargo-zigbuild/bin/cargo-zigbuild",
            ],
            "cargo-zigbuild",
        ),
        (&["opt/zig/bin/zig"], "zig"),
        (&["usr/bin/gcc", "opt/gcc/bin/gcc"], "gcc"),
        (&["usr/bin/cc", "opt/gcc/bin/cc"], "cc"),
        (&["usr/bin/git", "opt/git/bin/git"], "git"),
        (
            &["usr/bin/pkg-config", "opt/pkg-config/bin/pkg-config"],
            "pkg-config",
        ),
    ];

    for (sources, name) in tools {
        let target_path = usr_bin.join(name);
        if target_path.exists() {
            continue;
        }

        for source in *sources {
            let source_path = root.join(source);
            if source_path.exists() {
                std::os::unix::fs::symlink(&source_path, &target_path)?;
                debug!(source = %source, target = %name, "created symlink");
                break;
            }
        }
    }

    setup_dynamic_linker_in(root)?;

    Ok(())
}

/// Set up the dynamic linker symlink so dynamically linked binaries can run.
fn setup_dynamic_linker_in(root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let lib64 = root.join("lib64");
    let ld_target = lib64.join("ld-linux-x86-64.so.2");

    if ld_target.exists() {
        return Ok(());
    }

    let ld_source = root.join("opt/glibc/lib/ld-linux-x86-64.so.2");
    if ld_source.exists() {
        fs::create_dir_all(&lib64)?;
        std::os::unix::fs::symlink(&ld_source, &ld_target)?;
        debug!("linked dynamic linker");
    }

    Ok(())
}

/// Set up SSL certificate paths for cargo to use HTTPS.
pub fn setup_ssl_certs() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    setup_ssl_certs_in(Path::new("/"))
}

fn setup_ssl_certs_in(root: &Path) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let certs_target = root.join("etc/ssl/certs");
    if certs_target.exists() {
        return Ok(());
    }

    let cacert_source = root.join("opt/cacert/etc/ssl/certs");
    if cacert_source.exists() {
        let etc_ssl = root.join("etc/ssl");
        fs::create_dir_all(&etc_ssl)?;
        std::os::unix::fs::symlink(&cacert_source, &certs_target)?;
        debug!("linked SSL certificates");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;
    use tempfile::TempDir;

    fn create_executable(path: &Path) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, "#!/bin/sh\n").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    #[test]
    fn docker_paths_already_present_are_skipped() {
        let root = TempDir::new().unwrap();
        create_executable(&root.path().join("usr/bin/cargo"));

        setup_path_symlinks_in(root.path()).unwrap();

        assert!(!root.path().join("usr/bin/cargo").is_symlink());
    }

    #[test]
    fn nix_paths_get_symlinked_to_usr_bin() {
        let root = TempDir::new().unwrap();
        create_executable(&root.path().join("opt/rust/bin/cargo"));

        setup_path_symlinks_in(root.path()).unwrap();

        let link = root.path().join("usr/bin/cargo");
        assert!(link.is_symlink());
        assert_eq!(
            fs::read_link(&link).unwrap(),
            root.path().join("opt/rust/bin/cargo")
        );
    }

    #[test]
    fn rustup_toolchain_preferred_over_opt() {
        let root = TempDir::new().unwrap();
        let rustup_cargo = root
            .path()
            .join("usr/local/rustup/toolchains/1.92-x86_64-unknown-linux-gnu/bin/cargo");
        create_executable(&rustup_cargo);
        create_executable(&root.path().join("opt/rust/bin/cargo"));

        setup_path_symlinks_in(root.path()).unwrap();

        let link = root.path().join("usr/bin/cargo");
        assert!(link.is_symlink());
        assert_eq!(fs::read_link(&link).unwrap(), rustup_cargo);
    }

    #[test]
    fn shell_detection_finds_bash() {
        let root = TempDir::new().unwrap();
        create_executable(&root.path().join("bin/bash"));

        setup_build_environment_in(root.path()).unwrap();

        let sh = root.path().join("bin/sh");
        assert!(sh.exists());
        assert!(sh.is_symlink());
    }

    #[test]
    fn shell_detection_skips_when_sh_exists() {
        let root = TempDir::new().unwrap();
        create_executable(&root.path().join("bin/sh"));
        create_executable(&root.path().join("bin/bash"));

        setup_build_environment_in(root.path()).unwrap();

        assert!(!root.path().join("bin/sh").is_symlink());
    }

    #[test]
    fn ssl_certs_skipped_when_present() {
        let root = TempDir::new().unwrap();
        fs::create_dir_all(root.path().join("etc/ssl/certs")).unwrap();

        setup_ssl_certs_in(root.path()).unwrap();

        assert!(!root.path().join("etc/ssl/certs").is_symlink());
    }

    #[test]
    fn ssl_certs_linked_from_nix_cacert() {
        let root = TempDir::new().unwrap();
        fs::create_dir_all(root.path().join("opt/cacert/etc/ssl/certs")).unwrap();

        setup_ssl_certs_in(root.path()).unwrap();

        let link = root.path().join("etc/ssl/certs");
        assert!(link.is_symlink());
    }

    #[test]
    fn dynamic_linker_skipped_when_present() {
        let root = TempDir::new().unwrap();
        let ld = root.path().join("lib64/ld-linux-x86-64.so.2");
        create_executable(&ld);

        setup_dynamic_linker_in(root.path()).unwrap();

        assert!(!ld.is_symlink());
    }

    #[test]
    fn dynamic_linker_linked_from_nix_glibc() {
        let root = TempDir::new().unwrap();
        create_executable(&root.path().join("opt/glibc/lib/ld-linux-x86-64.so.2"));

        setup_dynamic_linker_in(root.path()).unwrap();

        let link = root.path().join("lib64/ld-linux-x86-64.so.2");
        assert!(link.is_symlink());
    }
}
