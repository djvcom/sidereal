//! Rootfs preparation for Firecracker VMs.
//!
//! Creates minimal ext4 filesystems containing the runtime binary.

use crate::error::{FirecrackerError, Result};
use std::path::{Path, PathBuf};
use std::process::Command;
use tracing::{debug, info};

/// Builder for creating rootfs images.
pub struct RootfsBuilder {
    work_dir: PathBuf,
    size_mb: u32,
}

impl RootfsBuilder {
    /// Create a new rootfs builder.
    pub fn new(work_dir: impl Into<PathBuf>) -> Self {
        Self {
            work_dir: work_dir.into(),
            size_mb: 64,
        }
    }

    /// Set the rootfs size in megabytes.
    pub fn with_size(mut self, mb: u32) -> Self {
        self.size_mb = mb;
        self
    }

    /// Build a rootfs image containing the given binary as /sbin/init.
    pub fn build(&self, runtime_binary: &Path, output_path: &Path) -> Result<PathBuf> {
        if !runtime_binary.exists() {
            return Err(FirecrackerError::RootfsFailed(format!(
                "Runtime binary not found: {}",
                runtime_binary.display()
            )));
        }

        std::fs::create_dir_all(&self.work_dir)?;
        std::fs::create_dir_all(output_path.parent().unwrap_or(Path::new(".")))?;

        info!(
            size_mb = self.size_mb,
            output = %output_path.display(),
            "Building rootfs"
        );

        let image_path = output_path.to_path_buf();
        self.create_empty_image(&image_path)?;
        self.format_ext4(&image_path)?;
        self.populate_rootfs(&image_path, runtime_binary)?;

        info!("Rootfs created successfully");
        Ok(image_path)
    }

    /// Create an empty image file.
    fn create_empty_image(&self, path: &Path) -> Result<()> {
        debug!(path = %path.display(), size_mb = self.size_mb, "Creating empty image");

        let status = Command::new("dd")
            .args([
                "if=/dev/zero",
                &format!("of={}", path.display()),
                "bs=1M",
                &format!("count={}", self.size_mb),
            ])
            .status()
            .map_err(|e| FirecrackerError::RootfsFailed(format!("dd failed: {}", e)))?;

        if !status.success() {
            return Err(FirecrackerError::RootfsFailed(
                "dd failed to create image".to_string(),
            ));
        }

        Ok(())
    }

    /// Format the image as ext4.
    fn format_ext4(&self, path: &Path) -> Result<()> {
        debug!(path = %path.display(), "Formatting as ext4");

        let status = Command::new("mkfs.ext4")
            .args(["-F", &path.display().to_string()])
            .status()
            .map_err(|e| FirecrackerError::RootfsFailed(format!("mkfs.ext4 failed: {}", e)))?;

        if !status.success() {
            return Err(FirecrackerError::RootfsFailed(
                "mkfs.ext4 failed".to_string(),
            ));
        }

        Ok(())
    }

    /// Populate the rootfs with required files.
    fn populate_rootfs(&self, image_path: &Path, runtime_binary: &Path) -> Result<()> {
        let mount_dir = self.work_dir.join("mnt");
        std::fs::create_dir_all(&mount_dir)?;

        debug!(mount_dir = %mount_dir.display(), "Mounting rootfs");

        let status = Command::new("sudo")
            .args([
                "mount",
                "-o",
                "loop",
                &image_path.display().to_string(),
                &mount_dir.display().to_string(),
            ])
            .status()
            .map_err(|e| FirecrackerError::RootfsFailed(format!("mount failed: {}", e)))?;

        if !status.success() {
            return Err(FirecrackerError::RootfsFailed("mount failed".to_string()));
        }

        let result = self.populate_mounted_rootfs(&mount_dir, runtime_binary);

        debug!("Unmounting rootfs");
        let _ = Command::new("sudo")
            .args(["umount", &mount_dir.display().to_string()])
            .status();

        result
    }

    /// Populate the mounted rootfs.
    fn populate_mounted_rootfs(&self, mount_dir: &Path, runtime_binary: &Path) -> Result<()> {
        let sbin_dir = mount_dir.join("sbin");
        let tmp_dir = mount_dir.join("tmp");
        let dev_dir = mount_dir.join("dev");
        let proc_dir = mount_dir.join("proc");
        let sys_dir = mount_dir.join("sys");

        for dir in [&sbin_dir, &tmp_dir, &dev_dir, &proc_dir, &sys_dir] {
            let status = Command::new("sudo")
                .args(["mkdir", "-p", &dir.display().to_string()])
                .status()
                .map_err(|e| FirecrackerError::RootfsFailed(format!("mkdir failed: {}", e)))?;

            if !status.success() {
                return Err(FirecrackerError::RootfsFailed(format!(
                    "Failed to create directory: {}",
                    dir.display()
                )));
            }
        }

        debug!(
            src = %runtime_binary.display(),
            dest = %sbin_dir.join("init").display(),
            "Copying runtime binary"
        );

        let init_path = sbin_dir.join("init");
        let status = Command::new("sudo")
            .args([
                "cp",
                &runtime_binary.display().to_string(),
                &init_path.display().to_string(),
            ])
            .status()
            .map_err(|e| FirecrackerError::RootfsFailed(format!("cp failed: {}", e)))?;

        if !status.success() {
            return Err(FirecrackerError::RootfsFailed(
                "Failed to copy runtime binary".to_string(),
            ));
        }

        let status = Command::new("sudo")
            .args(["chmod", "+x", &init_path.display().to_string()])
            .status()
            .map_err(|e| FirecrackerError::RootfsFailed(format!("chmod failed: {}", e)))?;

        if !status.success() {
            return Err(FirecrackerError::RootfsFailed(
                "Failed to chmod init binary".to_string(),
            ));
        }

        Ok(())
    }
}

/// Download a pre-built kernel for Firecracker.
pub async fn download_kernel(dest: &Path) -> Result<()> {
    const KERNEL_URL: &str =
        "https://s3.amazonaws.com/spec.ccfc.min/img/quickstart_guide/x86_64/kernels/vmlinux.bin";

    info!(url = KERNEL_URL, dest = %dest.display(), "Downloading kernel");

    std::fs::create_dir_all(dest.parent().unwrap_or(Path::new(".")))?;

    let status = Command::new("curl")
        .args(["-fsSL", "-o", &dest.display().to_string(), KERNEL_URL])
        .status()
        .map_err(|e| FirecrackerError::RootfsFailed(format!("curl failed: {}", e)))?;

    if !status.success() {
        return Err(FirecrackerError::RootfsFailed(
            "Failed to download kernel".to_string(),
        ));
    }

    info!("Kernel downloaded successfully");
    Ok(())
}
