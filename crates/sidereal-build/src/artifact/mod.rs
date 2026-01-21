//! Artifact generation and management.
//!
//! Handles:
//! - Rootfs image generation
//! - Artifact metadata
//! - Manifest creation
//! - Object storage upload/download

mod rootfs;
mod storage;

pub use rootfs::{RootfsBuilder, RootfsOutput};
pub use storage::{ArtifactStore, StorageConfig};

use std::path::Path;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use tracing::info;

use crate::error::{BuildError, BuildResult};
use crate::types::{ArtifactId, FunctionMetadata, ProjectId};

/// A built artifact ready for deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artifact {
    /// Unique artifact identifier.
    pub id: ArtifactId,
    /// Project that owns this artifact.
    pub project_id: ProjectId,
    /// Branch this artifact was built from.
    pub branch: String,
    /// Commit SHA this artifact was built from.
    pub commit_sha: String,
    /// Path to the rootfs image (local or URL).
    pub rootfs_path: String,
    /// SHA-256 hash of the rootfs.
    pub rootfs_hash: String,
    /// Size of the rootfs in bytes.
    pub rootfs_size: u64,
    /// Functions discovered in this artifact.
    pub functions: Vec<FunctionMetadata>,
    /// When the artifact was built.
    pub built_at: SystemTime,
}

impl Artifact {
    /// Create a new artifact.
    #[must_use]
    pub fn new(
        project_id: ProjectId,
        branch: String,
        commit_sha: String,
        rootfs_output: RootfsOutput,
        functions: Vec<FunctionMetadata>,
    ) -> Self {
        Self {
            id: ArtifactId::generate(),
            project_id,
            branch,
            commit_sha,
            rootfs_path: rootfs_output.path.display().to_string(),
            rootfs_hash: rootfs_output.hash,
            rootfs_size: rootfs_output.size,
            functions,
            built_at: SystemTime::now(),
        }
    }
}

/// Artifact manifest for storage alongside the rootfs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactManifest {
    /// Artifact metadata.
    pub artifact: Artifact,
    /// Schema version for forward compatibility.
    pub schema_version: u32,
}

impl ArtifactManifest {
    /// Current schema version.
    pub const SCHEMA_VERSION: u32 = 1;

    /// Create a new manifest for an artifact.
    #[must_use]
    pub const fn new(artifact: Artifact) -> Self {
        Self {
            artifact,
            schema_version: Self::SCHEMA_VERSION,
        }
    }

    /// Write the manifest to a file.
    pub fn write_to(&self, path: &Path) -> BuildResult<()> {
        let content = serde_json::to_string_pretty(self)
            .map_err(|e| BuildError::Internal(format!("failed to serialise manifest: {e}")))?;

        std::fs::write(path, content)?;
        info!(path = %path.display(), "wrote artifact manifest");
        Ok(())
    }

    /// Read a manifest from a file.
    pub fn read_from(path: &Path) -> BuildResult<Self> {
        let content = std::fs::read_to_string(path)?;
        let manifest: Self = serde_json::from_str(&content)
            .map_err(|e| BuildError::ConfigParse(format!("failed to parse manifest: {e}")))?;
        Ok(manifest)
    }
}

/// Input parameters for building an artifact.
pub struct BuildInput<'a> {
    /// Project that owns this artifact.
    pub project_id: &'a ProjectId,
    /// Branch this artifact is built from.
    pub branch: &'a str,
    /// Commit SHA this artifact is built from.
    pub commit_sha: &'a str,
    /// Path to the runtime binary.
    pub runtime_binary: &'a Path,
    /// Path to the compiled user binary.
    pub user_binary: &'a Path,
    /// Functions discovered in the user code.
    pub functions: Vec<FunctionMetadata>,
    /// Directory to write the artifact files to.
    pub output_dir: &'a Path,
}

/// Builder for creating complete artifacts.
pub struct ArtifactBuilder {
    work_dir: std::path::PathBuf,
    rootfs_size_mb: u32,
}

impl ArtifactBuilder {
    /// Create a new artifact builder.
    #[must_use]
    pub fn new(work_dir: impl Into<std::path::PathBuf>) -> Self {
        Self {
            work_dir: work_dir.into(),
            rootfs_size_mb: 64,
        }
    }

    /// Set the rootfs size.
    #[must_use]
    pub const fn with_rootfs_size(mut self, mb: u32) -> Self {
        self.rootfs_size_mb = mb;
        self
    }

    /// Build a complete artifact.
    ///
    /// # Errors
    ///
    /// Returns an error if the artifact cannot be built.
    pub fn build(&self, input: BuildInput<'_>) -> BuildResult<Artifact> {
        std::fs::create_dir_all(input.output_dir)?;

        // Write functions manifest
        let functions_path = input.output_dir.join("functions.json");
        crate::discovery::write_manifest(&functions_path, &input.functions)?;

        // Build rootfs
        let rootfs_path = input.output_dir.join("rootfs.ext4");
        let rootfs_builder = RootfsBuilder::new(&self.work_dir).with_size(self.rootfs_size_mb);

        let rootfs_output = rootfs_builder.build(
            input.runtime_binary,
            input.user_binary,
            Some(&functions_path),
            &rootfs_path,
        )?;

        // Create artifact
        let artifact = Artifact::new(
            input.project_id.clone(),
            input.branch.to_owned(),
            input.commit_sha.to_owned(),
            rootfs_output,
            input.functions,
        );

        // Write manifest
        let manifest = ArtifactManifest::new(artifact.clone());
        manifest.write_to(&input.output_dir.join("manifest.json"))?;

        info!(
            artifact_id = %artifact.id,
            project = %input.project_id,
            branch = %input.branch,
            "artifact built successfully"
        );

        Ok(artifact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_manifest_schema_version() {
        assert_eq!(ArtifactManifest::SCHEMA_VERSION, 1);
    }
}
