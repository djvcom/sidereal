//! Rootfs generation for Firecracker VMs.
//!
//! Creates minimal ext4 filesystems containing the compiled binary and runtime.
//! Uses `mkfs.ext4 -d` for rootless image creation (no mount required).

use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
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
    ///
    /// Uses `mkfs.ext4 -d` for rootless image creation - no mount or sudo required.
    pub fn build(
        &self,
        runtime_binary: &Path,
        user_binary: &Path,
        functions_json: Option<&Path>,
        output_path: &Path,
    ) -> BuildResult<RootfsOutput> {
        Self::validate_inputs(runtime_binary, user_binary)?;

        std::fs::create_dir_all(&self.work_dir)?;
        if let Some(parent) = output_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        info!(
            size_mb = self.size_mb,
            output = %output_path.display(),
            "building rootfs"
        );

        // Create staging directory with rootfs contents
        let staging_dir = self.work_dir.join("rootfs-staging");
        Self::create_staging_directory(&staging_dir, runtime_binary, user_binary, functions_json)?;

        // Create ext4 image directly from staging directory (rootless!)
        Self::create_ext4_from_directory(&staging_dir, output_path, self.size_mb)?;

        // Clean up staging directory
        if staging_dir.exists() {
            std::fs::remove_dir_all(&staging_dir)?;
        }

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

    fn validate_inputs(runtime: &Path, user_binary: &Path) -> BuildResult<()> {
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

    /// Create staging directory with all rootfs contents.
    fn create_staging_directory(
        staging_dir: &Path,
        runtime: &Path,
        user_binary: &Path,
        functions_json: Option<&Path>,
    ) -> BuildResult<()> {
        // Remove existing staging directory if present
        if staging_dir.exists() {
            std::fs::remove_dir_all(staging_dir)?;
        }

        // Create directory structure
        let dirs = ["sbin", "app", "etc", "tmp", "dev", "proc", "sys"];
        for dir in dirs {
            let dir_path = staging_dir.join(dir);
            std::fs::create_dir_all(&dir_path)?;
            debug!(dir = %dir_path.display(), "created directory");
        }

        // Copy runtime as /sbin/init
        let init_path = staging_dir.join("sbin/init");
        std::fs::copy(runtime, &init_path)?;
        std::fs::set_permissions(&init_path, Permissions::from_mode(0o755))?;
        debug!(path = %init_path.display(), "copied runtime binary");

        // Copy user binary as /app/binary
        let binary_path = staging_dir.join("app/binary");
        std::fs::copy(user_binary, &binary_path)?;
        std::fs::set_permissions(&binary_path, Permissions::from_mode(0o755))?;
        debug!(path = %binary_path.display(), "copied user binary");

        // Copy functions.json if provided
        if let Some(functions) = functions_json {
            if functions.exists() {
                let etc_path = staging_dir.join("etc/functions.json");
                std::fs::copy(functions, &etc_path)?;
                debug!(path = %etc_path.display(), "copied functions.json");
            }
        }

        debug!(staging = %staging_dir.display(), "staging directory prepared");
        Ok(())
    }

    /// Create ext4 image from a directory using mkfs.ext4 -d.
    ///
    /// This approach requires no mounting or elevated privileges.
    fn create_ext4_from_directory(
        source_dir: &Path,
        output_path: &Path,
        size_mb: u32,
    ) -> BuildResult<()> {
        // Remove existing output file if present
        if output_path.exists() {
            std::fs::remove_file(output_path)?;
        }

        // Calculate size in bytes (with some padding for filesystem metadata)
        let size_bytes = u64::from(size_mb) * 1024 * 1024;

        debug!(
            source = %source_dir.display(),
            output = %output_path.display(),
            size_mb,
            "creating ext4 image from directory"
        );

        // mkfs.ext4 -d <source_dir> -L <label> <output_path> <size>
        // The size is specified in filesystem blocks (1KB by default)
        let size_blocks = size_bytes / 1024;

        let output = Command::new("mkfs.ext4")
            .args([
                "-d",
                &source_dir.display().to_string(),
                "-L",
                "sidereal",
                "-q", // Quiet mode
                &output_path.display().to_string(),
                &size_blocks.to_string(),
            ])
            .output()
            .map_err(|e| {
                BuildError::RootfsGeneration(format!("mkfs.ext4 failed to execute: {e}"))
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(BuildError::RootfsGeneration(format!(
                "mkfs.ext4 failed: {}",
                stderr.trim()
            )));
        }

        debug!(output = %output_path.display(), "ext4 image created");
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
