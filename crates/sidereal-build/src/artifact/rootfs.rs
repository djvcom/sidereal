//! Rootfs generation for Firecracker VMs.
//!
//! Creates minimal ext4 filesystems containing the compiled binary and runtime.

use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::error::{BuildError, BuildResult};

/// Builder for creating rootfs images.
#[derive(Debug)]
pub struct RootfsBuilder {
    work_dir: PathBuf,
    size_mb: u32,
}

impl RootfsBuilder {
    /// Create a new rootfs builder.
    #[must_use]
    pub fn new(work_dir: impl Into<PathBuf>) -> Self {
        Self {
            work_dir: work_dir.into(),
            size_mb: 64,
        }
    }

    /// Set the rootfs size in megabytes.
    #[must_use]
    pub const fn with_size(mut self, mb: u32) -> Self {
        self.size_mb = mb;
        self
    }

    /// Build a rootfs image containing the runtime and user binary.
    ///
    /// The rootfs will have:
    /// - `/sbin/init` - the sidereal runtime
    /// - `/app/binary` - the compiled user code
    /// - `/etc/functions.json` - function metadata
    pub fn build(
        &self,
        runtime_binary: &Path,
        user_binary: &Path,
        functions_json: Option<&Path>,
        output_path: &Path,
    ) -> BuildResult<RootfsOutput> {
        self.validate_inputs(runtime_binary, user_binary)?;

        std::fs::create_dir_all(&self.work_dir)?;
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        info!(
            size_mb = self.size_mb,
            output = %output_path.display(),
            "building rootfs"
        );

        self.create_empty_image(output_path)?;
        self.format_ext4(output_path)?;
        self.populate_rootfs(output_path, runtime_binary, user_binary, functions_json)?;

        let hash = calculate_sha256(output_path)?;
        let size = std::fs::metadata(output_path)?.len();

        info!(
            hash = %hash,
            size_bytes = size,
            "rootfs created successfully"
        );

        Ok(RootfsOutput {
            path: output_path.to_owned(),
            hash,
            size,
        })
    }

    fn validate_inputs(&self, runtime: &Path, user_binary: &Path) -> BuildResult<()> {
        if !runtime.exists() {
            return Err(BuildError::RootfsGeneration(format!(
                "runtime binary not found: {}",
                runtime.display()
            )));
        }

        if !user_binary.exists() {
            return Err(BuildError::RootfsGeneration(format!(
                "user binary not found: {}",
                user_binary.display()
            )));
        }

        Ok(())
    }

    fn create_empty_image(&self, path: &Path) -> BuildResult<()> {
        debug!(path = %path.display(), size_mb = self.size_mb, "creating empty image");

        let status = Command::new("dd")
            .args([
                "if=/dev/zero",
                &format!("of={}", path.display()),
                "bs=1M",
                &format!("count={}", self.size_mb),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| BuildError::RootfsGeneration(format!("dd failed: {e}")))?;

        if !status.success() {
            return Err(BuildError::RootfsGeneration(
                "dd failed to create image".to_owned(),
            ));
        }

        Ok(())
    }

    fn format_ext4(&self, path: &Path) -> BuildResult<()> {
        debug!(path = %path.display(), "formatting as ext4");

        let status = Command::new("mkfs.ext4")
            .args(["-F", "-q", &path.display().to_string()])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map_err(|e| BuildError::RootfsGeneration(format!("mkfs.ext4 failed: {e}")))?;

        if !status.success() {
            return Err(BuildError::RootfsGeneration("mkfs.ext4 failed".to_owned()));
        }

        Ok(())
    }

    fn populate_rootfs(
        &self,
        image_path: &Path,
        runtime: &Path,
        user_binary: &Path,
        functions_json: Option<&Path>,
    ) -> BuildResult<()> {
        let mount_dir = self.work_dir.join("mnt");
        std::fs::create_dir_all(&mount_dir)?;

        debug!(mount_dir = %mount_dir.display(), "mounting rootfs");

        // Mount the image (requires elevated privileges)
        let status = Command::new("sudo")
            .args([
                "mount",
                "-o",
                "loop",
                &image_path.display().to_string(),
                &mount_dir.display().to_string(),
            ])
            .status()
            .map_err(|e| BuildError::RootfsGeneration(format!("mount failed: {e}")))?;

        if !status.success() {
            return Err(BuildError::RootfsGeneration("mount failed".to_owned()));
        }

        // Populate the mounted filesystem
        let result = self.populate_mounted_rootfs(&mount_dir, runtime, user_binary, functions_json);

        // Always unmount
        debug!("unmounting rootfs");
        let _ = Command::new("sudo")
            .args(["umount", &mount_dir.display().to_string()])
            .status();

        result
    }

    fn populate_mounted_rootfs(
        &self,
        mount_dir: &Path,
        runtime: &Path,
        user_binary: &Path,
        functions_json: Option<&Path>,
    ) -> BuildResult<()> {
        // Create required directories
        let dirs = ["sbin", "app", "etc", "tmp", "dev", "proc", "sys"];

        for dir in dirs {
            let dir_path = mount_dir.join(dir);
            run_sudo(&["mkdir", "-p", &dir_path.display().to_string()])?;
        }

        // Copy runtime as /sbin/init
        let init_path = mount_dir.join("sbin/init");
        run_sudo(&[
            "cp",
            &runtime.display().to_string(),
            &init_path.display().to_string(),
        ])?;
        run_sudo(&["chmod", "+x", &init_path.display().to_string()])?;

        // Copy user binary as /app/binary
        let binary_path = mount_dir.join("app/binary");
        run_sudo(&[
            "cp",
            &user_binary.display().to_string(),
            &binary_path.display().to_string(),
        ])?;
        run_sudo(&["chmod", "+x", &binary_path.display().to_string()])?;

        // Copy functions.json if provided
        if let Some(functions) = functions_json {
            if functions.exists() {
                let etc_path = mount_dir.join("etc/functions.json");
                run_sudo(&[
                    "cp",
                    &functions.display().to_string(),
                    &etc_path.display().to_string(),
                ])?;
            }
        }

        debug!("rootfs populated successfully");
        Ok(())
    }
}

/// Output from rootfs generation.
#[derive(Debug, Clone)]
pub struct RootfsOutput {
    /// Path to the rootfs image.
    pub path: PathBuf,
    /// SHA-256 hash of the rootfs.
    pub hash: String,
    /// Size in bytes.
    pub size: u64,
}

/// Calculate SHA-256 hash of a file.
fn calculate_sha256(path: &Path) -> BuildResult<String> {
    let data = std::fs::read(path)?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let result = hasher.finalize();
    Ok(hex::encode(result))
}

/// Run a command with sudo.
fn run_sudo(args: &[&str]) -> BuildResult<()> {
    let status = Command::new("sudo")
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| BuildError::RootfsGeneration(format!("sudo {} failed: {e}", args[0])))?;

    if !status.success() {
        return Err(BuildError::RootfsGeneration(format!(
            "sudo {} failed",
            args[0]
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rootfs_builder_defaults() {
        let builder = RootfsBuilder::new("/tmp/work");
        assert_eq!(builder.size_mb, 64);
    }

    #[test]
    fn rootfs_builder_with_size() {
        let builder = RootfsBuilder::new("/tmp/work").with_size(128);
        assert_eq!(builder.size_mb, 128);
    }
}
