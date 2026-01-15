//! Artifact storage using object_store.
//!
//! Supports local filesystem, S3, and GCS backends.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use tracing::{debug, info};

use crate::error::{BuildError, BuildResult};
use crate::types::ProjectId;

use super::{Artifact, ArtifactManifest};

/// Configuration for artifact storage.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct StorageConfig {
    /// Storage type: "local", "s3", or "gcs".
    pub storage_type: String,
    /// Base path or bucket name.
    pub path: String,
    /// S3 region (for S3 only).
    pub region: Option<String>,
    /// S3 endpoint (for S3-compatible stores).
    pub endpoint: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            storage_type: "local".to_owned(),
            path: "/var/lib/sidereal/artifacts".to_owned(),
            region: None,
            endpoint: None,
        }
    }
}

/// Artifact storage for uploading and downloading build artifacts.
pub struct ArtifactStore {
    store: Arc<dyn ObjectStore>,
    base_path: String,
}

impl ArtifactStore {
    /// Create a new artifact store with the given configuration.
    pub fn new(config: &StorageConfig) -> BuildResult<Self> {
        let store = create_object_store(config)?;
        Ok(Self {
            store,
            base_path: config.path.clone(),
        })
    }

    /// Create an artifact store with a pre-configured object store.
    #[must_use]
    pub fn with_store(store: Arc<dyn ObjectStore>, base_path: String) -> Self {
        Self { store, base_path }
    }

    /// Upload an artifact to storage.
    pub async fn upload(&self, artifact: &Artifact, rootfs_path: &Path) -> BuildResult<String> {
        let storage_path = self.artifact_path(&artifact.project_id, &artifact.branch);

        // Upload rootfs
        let rootfs_data = tokio::fs::read(rootfs_path).await?;
        let rootfs_object_path = ObjectPath::from(format!("{storage_path}/rootfs.ext4"));

        debug!(path = %rootfs_object_path, size = rootfs_data.len(), "uploading rootfs");
        self.store
            .put(&rootfs_object_path, Bytes::from(rootfs_data).into())
            .await
            .map_err(|e| BuildError::ArtifactStorage(format!("failed to upload rootfs: {e}")))?;

        // Upload manifest
        let manifest = ArtifactManifest::new(artifact.clone());
        let manifest_data = serde_json::to_vec_pretty(&manifest)
            .map_err(|e| BuildError::Internal(format!("failed to serialise manifest: {e}")))?;
        let manifest_object_path = ObjectPath::from(format!("{storage_path}/manifest.json"));

        debug!(path = %manifest_object_path, "uploading manifest");
        self.store
            .put(&manifest_object_path, Bytes::from(manifest_data).into())
            .await
            .map_err(|e| BuildError::ArtifactStorage(format!("failed to upload manifest: {e}")))?;

        info!(
            project = %artifact.project_id,
            branch = %artifact.branch,
            path = %storage_path,
            "artifact uploaded"
        );

        Ok(storage_path)
    }

    /// Download an artifact manifest from storage.
    pub async fn download_manifest(
        &self,
        project_id: &ProjectId,
        branch: &str,
    ) -> BuildResult<ArtifactManifest> {
        let storage_path = self.artifact_path(project_id, branch);
        let manifest_path = ObjectPath::from(format!("{storage_path}/manifest.json"));

        let result = self.store.get(&manifest_path).await.map_err(|e| {
            BuildError::ArtifactStorage(format!("failed to download manifest: {e}"))
        })?;

        let data = result
            .bytes()
            .await
            .map_err(|e| BuildError::ArtifactStorage(format!("failed to read manifest: {e}")))?;

        let manifest: ArtifactManifest = serde_json::from_slice(&data)
            .map_err(|e| BuildError::ConfigParse(format!("failed to parse manifest: {e}")))?;

        Ok(manifest)
    }

    /// Download a rootfs to a local path.
    pub async fn download_rootfs(
        &self,
        project_id: &ProjectId,
        branch: &str,
        dest: &Path,
    ) -> BuildResult<()> {
        let storage_path = self.artifact_path(project_id, branch);
        let rootfs_path = ObjectPath::from(format!("{storage_path}/rootfs.ext4"));

        let result =
            self.store.get(&rootfs_path).await.map_err(|e| {
                BuildError::ArtifactStorage(format!("failed to download rootfs: {e}"))
            })?;

        let data = result
            .bytes()
            .await
            .map_err(|e| BuildError::ArtifactStorage(format!("failed to read rootfs: {e}")))?;

        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(dest, &data).await?;

        info!(path = %dest.display(), size = data.len(), "rootfs downloaded");
        Ok(())
    }

    /// Delete an artifact from storage.
    pub async fn delete(&self, project_id: &ProjectId, branch: &str) -> BuildResult<()> {
        let storage_path = self.artifact_path(project_id, branch);

        // Delete rootfs
        let rootfs_path = ObjectPath::from(format!("{storage_path}/rootfs.ext4"));
        self.store.delete(&rootfs_path).await.ok();

        // Delete manifest
        let manifest_path = ObjectPath::from(format!("{storage_path}/manifest.json"));
        self.store.delete(&manifest_path).await.ok();

        info!(project = %project_id, branch = %branch, "artifact deleted");
        Ok(())
    }

    /// List all artifacts for a project.
    pub async fn list(&self, project_id: &ProjectId) -> BuildResult<Vec<String>> {
        let prefix = ObjectPath::from(format!("{}/{}", self.base_path, project_id));

        let mut branches = Vec::new();
        let mut stream = self.store.list(Some(&prefix));

        use futures::StreamExt;
        while let Some(result) = stream.next().await {
            if let Ok(meta) = result {
                let path_str = meta.location.to_string();
                if path_str.ends_with("/manifest.json") {
                    if let Some(branch) = extract_branch(&path_str, project_id.as_str()) {
                        branches.push(branch);
                    }
                }
            }
        }

        Ok(branches)
    }

    /// Check if an artifact exists.
    pub async fn exists(&self, project_id: &ProjectId, branch: &str) -> BuildResult<bool> {
        let storage_path = self.artifact_path(project_id, branch);
        let manifest_path = ObjectPath::from(format!("{storage_path}/manifest.json"));

        match self.store.head(&manifest_path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(BuildError::ArtifactStorage(format!(
                "failed to check artifact: {e}"
            ))),
        }
    }

    fn artifact_path(&self, project_id: &ProjectId, branch: &str) -> String {
        format!(
            "{}/{}/{}",
            self.base_path,
            project_id,
            sanitise_branch(branch)
        )
    }
}

/// Create an object store from configuration.
fn create_object_store(config: &StorageConfig) -> BuildResult<Arc<dyn ObjectStore>> {
    match config.storage_type.as_str() {
        "local" => {
            let store = object_store::local::LocalFileSystem::new_with_prefix(&config.path)
                .map_err(|e| {
                    BuildError::ArtifactStorage(format!("failed to create local store: {e}"))
                })?;
            Ok(Arc::new(store))
        }
        "memory" => Ok(Arc::new(object_store::memory::InMemory::new())),
        #[cfg(feature = "aws")]
        "s3" => {
            use object_store::aws::AmazonS3Builder;
            let mut builder = AmazonS3Builder::new().with_bucket_name(&config.path);

            if let Some(region) = &config.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &config.endpoint {
                builder = builder.with_endpoint(endpoint);
            }

            let store = builder.build().map_err(|e| {
                BuildError::ArtifactStorage(format!("failed to create S3 store: {e}"))
            })?;
            Ok(Arc::new(store))
        }
        #[cfg(feature = "gcp")]
        "gcs" => {
            use object_store::gcp::GoogleCloudStorageBuilder;
            let store = GoogleCloudStorageBuilder::new()
                .with_bucket_name(&config.path)
                .build()
                .map_err(|e| {
                    BuildError::ArtifactStorage(format!("failed to create GCS store: {e}"))
                })?;
            Ok(Arc::new(store))
        }
        _ => Err(BuildError::ArtifactStorage(format!(
            "unsupported storage type: {}",
            config.storage_type
        ))),
    }
}

/// Sanitise a branch name for use in storage paths.
fn sanitise_branch(branch: &str) -> String {
    branch
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Extract branch name from a storage path.
fn extract_branch(path: &str, project_id: &str) -> Option<String> {
    let prefix = format!("{project_id}/");
    let suffix = "/manifest.json";

    if let Some(start) = path.find(&prefix) {
        let after_prefix = &path[start + prefix.len()..];
        if let Some(end) = after_prefix.find(suffix) {
            return Some(after_prefix[..end].to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitise_branch_names() {
        assert_eq!(sanitise_branch("main"), "main");
        assert_eq!(sanitise_branch("feature/auth"), "feature_auth");
        assert_eq!(sanitise_branch("fix-bug-123"), "fix-bug-123");
        assert_eq!(sanitise_branch("release/v1.0"), "release_v1_0");
    }

    #[test]
    fn extract_branch_from_path() {
        let path = "artifacts/my-project/main/manifest.json";
        assert_eq!(extract_branch(path, "my-project"), Some("main".to_owned()));

        let path2 = "artifacts/my-project/feature_auth/manifest.json";
        assert_eq!(
            extract_branch(path2, "my-project"),
            Some("feature_auth".to_owned())
        );
    }

    #[test]
    fn storage_config_defaults() {
        let config = StorageConfig::default();
        assert_eq!(config.storage_type, "local");
    }

    #[tokio::test]
    async fn memory_store_operations() {
        let config = StorageConfig {
            storage_type: "memory".to_owned(),
            path: "artifacts".to_owned(),
            region: None,
            endpoint: None,
        };

        let store = ArtifactStore::new(&config).unwrap();
        let project_id = ProjectId::new("test-project");

        // Check non-existent artifact
        let exists = store.exists(&project_id, "main").await.unwrap();
        assert!(!exists);
    }
}
