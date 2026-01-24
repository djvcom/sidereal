//! Build cache management for S3-backed caching.
//!
//! Provides caching of:
//! - Registry cache (cargo home - global, shared across all projects)
//! - Target cache (per-project, per-branch build artifacts)

mod compression;

use std::path::Path;
use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use serde::Deserialize;
use tracing::{debug, info};

use crate::artifact::StorageConfig;
use crate::error::{BuildError, BuildResult};
use crate::types::ProjectId;

pub use compression::{compress_directory, decompress_to_directory};

/// Result of a cache operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheResult {
    /// Cache was successfully retrieved.
    Hit,
    /// Cache was not found (first build or expired).
    Miss,
}

impl CacheResult {
    /// Check if this is a cache hit.
    #[must_use]
    pub const fn is_hit(&self) -> bool {
        matches!(self, Self::Hit)
    }
}

/// Configuration for build caching.
#[derive(Debug, Clone, Deserialize)]
pub struct CacheConfig {
    /// Whether caching is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Storage configuration for cache.
    #[serde(default)]
    pub storage: StorageConfig,

    /// Prefix for registry cache in storage.
    #[serde(default = "default_registry_prefix")]
    pub registry_prefix: String,

    /// Prefix for target cache in storage.
    #[serde(default = "default_target_prefix")]
    pub target_prefix: String,

    /// Zstd compression level (1-22, default 3).
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            storage: StorageConfig::default(),
            registry_prefix: default_registry_prefix(),
            target_prefix: default_target_prefix(),
            compression_level: default_compression_level(),
        }
    }
}

const fn default_enabled() -> bool {
    false
}

fn default_registry_prefix() -> String {
    "cache/registry".to_owned()
}

fn default_target_prefix() -> String {
    "cache/targets".to_owned()
}

const fn default_compression_level() -> i32 {
    3
}

/// Manager for build cache operations.
pub struct CacheManager {
    store: Arc<dyn ObjectStore>,
    config: CacheConfig,
}

impl CacheManager {
    /// Create a new cache manager with the given configuration.
    pub fn new(config: CacheConfig) -> BuildResult<Self> {
        let store = create_cache_store(&config.storage)?;
        Ok(Self { store, config })
    }

    /// Create a cache manager with a pre-configured object store.
    #[must_use]
    pub fn with_store(store: Arc<dyn ObjectStore>, config: CacheConfig) -> Self {
        Self { store, config }
    }

    /// Check if caching is enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Pull the global registry cache to the destination directory.
    ///
    /// The registry cache contains cargo's downloaded crates and git dependencies.
    /// This is shared across all projects to avoid re-downloading dependencies.
    pub async fn pull_registry(&self, dest: &Path) -> BuildResult<CacheResult> {
        if !self.config.enabled {
            return Ok(CacheResult::Miss);
        }

        let cache_path =
            ObjectPath::from(format!("{}/registry.tar.zst", self.config.registry_prefix));

        match self.pull_archive(&cache_path, dest).await {
            Ok(()) => {
                info!(path = %dest.display(), "registry cache restored");
                Ok(CacheResult::Hit)
            }
            Err(e) => {
                debug!(error = %e, "registry cache miss");
                Ok(CacheResult::Miss)
            }
        }
    }

    /// Push the registry cache from the source directory to storage.
    pub async fn push_registry(&self, src: &Path) -> BuildResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if !src.exists() {
            debug!(path = %src.display(), "registry directory does not exist, skipping push");
            return Ok(());
        }

        let cache_path =
            ObjectPath::from(format!("{}/registry.tar.zst", self.config.registry_prefix));
        self.push_archive(src, &cache_path).await?;
        info!(path = %src.display(), "registry cache pushed");
        Ok(())
    }

    /// Pull the target cache for a specific project and branch.
    ///
    /// The target cache contains compiled dependencies and incremental compilation data.
    /// This is per-branch to support cargo's incremental compilation.
    pub async fn pull_target(
        &self,
        project_id: &ProjectId,
        branch: &str,
        dest: &Path,
    ) -> BuildResult<CacheResult> {
        if !self.config.enabled {
            return Ok(CacheResult::Miss);
        }

        let sanitised_branch = sanitise_branch(branch);
        let cache_path = ObjectPath::from(format!(
            "{}/{}/{}.tar.zst",
            self.config.target_prefix, project_id, sanitised_branch
        ));

        match self.pull_archive(&cache_path, dest).await {
            Ok(()) => {
                info!(
                    project = %project_id,
                    branch = %branch,
                    path = %dest.display(),
                    "target cache restored"
                );
                Ok(CacheResult::Hit)
            }
            Err(e) => {
                debug!(
                    project = %project_id,
                    branch = %branch,
                    error = %e,
                    "target cache miss"
                );
                Ok(CacheResult::Miss)
            }
        }
    }

    /// Push the target cache for a specific project and branch.
    pub async fn push_target(
        &self,
        project_id: &ProjectId,
        branch: &str,
        src: &Path,
    ) -> BuildResult<()> {
        if !self.config.enabled {
            return Ok(());
        }

        if !src.exists() {
            debug!(path = %src.display(), "target directory does not exist, skipping push");
            return Ok(());
        }

        let sanitised_branch = sanitise_branch(branch);
        let cache_path = ObjectPath::from(format!(
            "{}/{}/{}.tar.zst",
            self.config.target_prefix, project_id, sanitised_branch
        ));

        self.push_archive(src, &cache_path).await?;
        info!(
            project = %project_id,
            branch = %branch,
            path = %src.display(),
            "target cache pushed"
        );
        Ok(())
    }

    async fn pull_archive(&self, cache_path: &ObjectPath, dest: &Path) -> BuildResult<()> {
        let result = self.store.get(cache_path).await.map_err(|e| {
            BuildError::Cache(format!("failed to get cache object {}: {}", cache_path, e))
        })?;

        let bytes = result
            .bytes()
            .await
            .map_err(|e| BuildError::Cache(format!("failed to read cache data: {}", e)))?;

        decompress_to_directory(&bytes, dest)
            .await
            .map_err(|e| BuildError::Cache(format!("failed to decompress cache: {}", e)))?;

        Ok(())
    }

    async fn push_archive(&self, src: &Path, cache_path: &ObjectPath) -> BuildResult<()> {
        let compressed = compress_directory(src, self.config.compression_level)
            .await
            .map_err(|e| BuildError::Cache(format!("failed to compress cache: {}", e)))?;

        self.store
            .put(cache_path, compressed.into())
            .await
            .map_err(|e| {
                BuildError::Cache(format!("failed to upload cache to {}: {}", cache_path, e))
            })?;

        Ok(())
    }
}

/// Create an object store for caching from configuration.
fn create_cache_store(config: &StorageConfig) -> BuildResult<Arc<dyn ObjectStore>> {
    match config.storage_type.as_str() {
        "local" => {
            std::fs::create_dir_all(&config.path).map_err(|e| {
                BuildError::Cache(format!("failed to create cache directory: {}", e))
            })?;
            let store = object_store::local::LocalFileSystem::new_with_prefix(&config.path)
                .map_err(|e| BuildError::Cache(format!("failed to create local store: {}", e)))?;
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

            let store = builder
                .build()
                .map_err(|e| BuildError::Cache(format!("failed to create S3 store: {}", e)))?;
            Ok(Arc::new(store))
        }
        #[cfg(feature = "gcp")]
        "gcs" => {
            use object_store::gcp::GoogleCloudStorageBuilder;
            let store = GoogleCloudStorageBuilder::new()
                .with_bucket_name(&config.path)
                .build()
                .map_err(|e| BuildError::Cache(format!("failed to create GCS store: {}", e)))?;
            Ok(Arc::new(store))
        }
        _ => Err(BuildError::Cache(format!(
            "unsupported storage type for cache: {}",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_config_defaults() {
        let config = CacheConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.compression_level, 3);
        assert_eq!(config.registry_prefix, "cache/registry");
        assert_eq!(config.target_prefix, "cache/targets");
    }

    #[test]
    fn sanitise_branch_names() {
        assert_eq!(sanitise_branch("main"), "main");
        assert_eq!(sanitise_branch("feature/auth"), "feature_auth");
        assert_eq!(sanitise_branch("fix-bug-123"), "fix-bug-123");
        assert_eq!(sanitise_branch("release/v1.0"), "release_v1_0");
    }

    #[test]
    fn cache_result_is_hit() {
        assert!(CacheResult::Hit.is_hit());
        assert!(!CacheResult::Miss.is_hit());
    }

    #[tokio::test]
    async fn cache_manager_disabled() {
        let config = CacheConfig {
            enabled: false,
            storage: StorageConfig {
                storage_type: "memory".to_owned(),
                path: "test".to_owned(),
                region: None,
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
            },
            ..Default::default()
        };

        let manager = CacheManager::new(config).unwrap();
        assert!(!manager.is_enabled());

        let result = manager.pull_registry(Path::new("/tmp/test")).await.unwrap();
        assert_eq!(result, CacheResult::Miss);
    }
}
