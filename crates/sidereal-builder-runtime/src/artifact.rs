//! Artifact creation for the builder runtime.
//!
//! Creates minimal ext4 filesystems (rootfs) containing the compiled binary and runtime.
//! Uses `mkfs.ext4 -d` for rootless image creation (no mount required).

use std::fs::Permissions;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

use sha2::{Digest, Sha256};
use tracing::{debug, info};

use crate::discovery::DeployableProject;
use crate::protocol::FunctionInfo;

/// Default rootfs size in megabytes.
const DEFAULT_ROOTFS_SIZE_MB: u32 = 64;

/// Error type for artifact operations.
#[derive(Debug, thiserror::Error)]
pub enum ArtifactError {
    #[error("runtime binary not found: {0}")]
    RuntimeNotFound(PathBuf),

    #[error("user binary not found: {0}")]
    UserBinaryNotFound(PathBuf),

    #[error("mkfs.ext4 failed: {0}")]
    MkfsFailedToExecute(std::io::Error),

    #[error("mkfs.ext4 returned error: {0}")]
    MkfsFailed(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Information about a created artifact.
#[derive(Debug, Clone)]
pub struct ArtifactInfo {
    /// Path to the rootfs ext4 image.
    pub path: PathBuf,
    /// SHA-256 hash of the rootfs.
    pub hash: String,
    /// Size of the rootfs in bytes.
    pub size: u64,
}

/// Create an artifact (rootfs image) for a deployable project.
///
/// The rootfs will contain:
/// - `/sbin/init` - the sidereal runtime
/// - `/app/binary` - the compiled user code
/// - `/etc/functions.json` - function metadata
pub fn create_artifact(
    project: &DeployableProject,
    runtime_path: &Path,
    output_dir: &Path,
) -> Result<ArtifactInfo, ArtifactError> {
    create_artifact_with_size(project, runtime_path, output_dir, DEFAULT_ROOTFS_SIZE_MB)
}

/// Create an artifact with a custom rootfs size.
pub fn create_artifact_with_size(
    project: &DeployableProject,
    runtime_path: &Path,
    output_dir: &Path,
    size_mb: u32,
) -> Result<ArtifactInfo, ArtifactError> {
    validate_inputs(runtime_path, &project.binary_path)?;

    std::fs::create_dir_all(output_dir)?;

    let rootfs_path = output_dir.join(format!("{}.ext4", project.name));

    info!(
        project = %project.name,
        size_mb = size_mb,
        output = %rootfs_path.display(),
        "building rootfs artifact"
    );

    let staging_dir = output_dir.join("staging");
    create_staging_directory(
        &staging_dir,
        runtime_path,
        &project.binary_path,
        &project.functions,
    )?;
    create_ext4_from_directory(&staging_dir, &rootfs_path, size_mb)?;

    if staging_dir.exists() {
        std::fs::remove_dir_all(&staging_dir)?;
    }

    let hash = calculate_sha256(&rootfs_path)?;
    let size = std::fs::metadata(&rootfs_path)?.len();

    info!(
        hash = %hash,
        size_bytes = size,
        "rootfs artifact created successfully"
    );

    Ok(ArtifactInfo {
        path: rootfs_path,
        hash,
        size,
    })
}

fn validate_inputs(runtime: &Path, user_binary: &Path) -> Result<(), ArtifactError> {
    if !runtime.exists() {
        return Err(ArtifactError::RuntimeNotFound(runtime.to_owned()));
    }

    if !user_binary.exists() {
        return Err(ArtifactError::UserBinaryNotFound(user_binary.to_owned()));
    }

    Ok(())
}

fn create_staging_directory(
    staging_dir: &Path,
    runtime: &Path,
    user_binary: &Path,
    functions: &[FunctionInfo],
) -> Result<(), ArtifactError> {
    if staging_dir.exists() {
        std::fs::remove_dir_all(staging_dir)?;
    }

    let dirs = ["sbin", "app", "etc", "tmp", "dev", "proc", "sys"];
    for dir in dirs {
        let dir_path = staging_dir.join(dir);
        std::fs::create_dir_all(&dir_path)?;
        debug!(dir = %dir_path.display(), "created directory");
    }

    let init_path = staging_dir.join("sbin/init");
    std::fs::copy(runtime, &init_path)?;
    std::fs::set_permissions(&init_path, Permissions::from_mode(0o755))?;
    debug!(path = %init_path.display(), "copied runtime binary");

    let binary_path = staging_dir.join("app/binary");
    std::fs::copy(user_binary, &binary_path)?;
    std::fs::set_permissions(&binary_path, Permissions::from_mode(0o755))?;
    debug!(path = %binary_path.display(), "copied user binary");

    if !functions.is_empty() {
        let functions_json = serde_json::to_string_pretty(functions)?;
        let functions_path = staging_dir.join("etc/functions.json");
        std::fs::write(&functions_path, functions_json)?;
        debug!(
            path = %functions_path.display(),
            count = functions.len(),
            "wrote functions.json"
        );
    }

    debug!(staging = %staging_dir.display(), "staging directory prepared");
    Ok(())
}

fn create_ext4_from_directory(
    source_dir: &Path,
    output_path: &Path,
    size_mb: u32,
) -> Result<(), ArtifactError> {
    if output_path.exists() {
        std::fs::remove_file(output_path)?;
    }

    let size_bytes = u64::from(size_mb) * 1024 * 1024;
    let size_blocks = size_bytes / 1024;

    debug!(
        source = %source_dir.display(),
        output = %output_path.display(),
        size_mb,
        "creating ext4 image from directory"
    );

    let output = Command::new("mkfs.ext4")
        .args([
            "-d",
            &source_dir.display().to_string(),
            "-L",
            "sidereal",
            "-q",
            &output_path.display().to_string(),
            &size_blocks.to_string(),
        ])
        .output()
        .map_err(ArtifactError::MkfsFailedToExecute)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(ArtifactError::MkfsFailed(stderr.trim().to_owned()));
    }

    debug!(output = %output_path.display(), "ext4 image created");
    Ok(())
}

fn calculate_sha256(path: &Path) -> Result<String, ArtifactError> {
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
    fn artifact_error_display() {
        let err = ArtifactError::RuntimeNotFound(PathBuf::from("/tmp/missing"));
        assert!(err.to_string().contains("runtime binary not found"));
        assert!(err.to_string().contains("/tmp/missing"));
    }

    #[test]
    fn artifact_error_mkfs() {
        let err = ArtifactError::MkfsFailed("no space left".to_owned());
        assert!(err.to_string().contains("mkfs.ext4"));
        assert!(err.to_string().contains("no space left"));
    }

    #[test]
    fn default_rootfs_size() {
        assert_eq!(DEFAULT_ROOTFS_SIZE_MB, 64);
    }
}
