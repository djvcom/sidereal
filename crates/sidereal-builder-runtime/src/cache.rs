//! Build cache management for the builder runtime.
//!
//! Handles pulling and pushing build caches from/to S3.
//! Uses tar.zstd archives compatible with the host-side caching.

use std::io::Cursor;
use std::path::Path;

use bytes::Bytes;
use tokio::task::spawn_blocking;
use tracing::{debug, info, warn};

use crate::protocol::CacheConfig;
use crate::s3::{S3Client, S3Error};

/// Default zstd compression level.
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;

/// Error type for cache operations.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("S3 operation failed: {0}")]
    S3(#[from] S3Error),

    #[error("compression/decompression failed: {0}")]
    Compression(std::io::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// Pull caches from S3.
///
/// Downloads and extracts registry cache to `/cargo` and target cache to `/target`.
pub async fn pull_caches(s3: &S3Client, config: &CacheConfig) -> Result<(), CacheError> {
    let mut pulled_any = false;

    // Pull registry cache if configured
    if let Some(ref key) = config.registry_key {
        match pull_cache(s3, key, Path::new("/cargo")).await {
            Ok(true) => {
                info!(key = %key, "Registry cache pulled");
                pulled_any = true;
            }
            Ok(false) => {
                debug!(key = %key, "Registry cache not found");
            }
            Err(e) => {
                warn!(key = %key, error = %e, "Failed to pull registry cache");
            }
        }
    }

    // Pull target cache if configured
    if let Some(ref key) = config.target_key {
        match pull_cache(s3, key, Path::new("/target")).await {
            Ok(true) => {
                info!(key = %key, "Target cache pulled");
                pulled_any = true;
            }
            Ok(false) => {
                debug!(key = %key, "Target cache not found");
            }
            Err(e) => {
                warn!(key = %key, error = %e, "Failed to pull target cache");
            }
        }
    }

    if pulled_any {
        info!("Cache pull complete");
    } else {
        info!("No caches found (clean build)");
    }

    Ok(())
}

/// Push caches to S3.
///
/// Compresses and uploads registry cache from `/cargo` and target cache from `/target`.
pub async fn push_caches(s3: &S3Client, config: &CacheConfig) -> Result<(), CacheError> {
    // Push registry cache if configured
    if let Some(ref key) = config.registry_key {
        let cargo_registry = Path::new("/cargo/registry");
        if cargo_registry.exists() {
            match push_cache(s3, cargo_registry, key).await {
                Ok(()) => {
                    info!(key = %key, "Registry cache pushed");
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "Failed to push registry cache");
                }
            }
        } else {
            debug!("No registry cache to push (directory doesn't exist)");
        }
    }

    // Push target cache if configured
    if let Some(ref key) = config.target_key {
        let target_dir = Path::new("/target");
        if target_dir.exists() {
            match push_cache(s3, target_dir, key).await {
                Ok(()) => {
                    info!(key = %key, "Target cache pushed");
                }
                Err(e) => {
                    warn!(key = %key, error = %e, "Failed to push target cache");
                }
            }
        } else {
            debug!("No target cache to push (directory doesn't exist)");
        }
    }

    info!("Cache push complete");
    Ok(())
}

/// Pull a single cache archive from S3 and extract it.
///
/// Returns `true` if the cache was found and extracted, `false` if not found.
async fn pull_cache(s3: &S3Client, key: &str, dest: &Path) -> Result<bool, CacheError> {
    // Download to a temporary file
    let temp_path = format!("/tmp/cache-{}.tar.zst", uuid_simple());
    let temp_file = Path::new(&temp_path);

    let exists = s3.download_if_exists(key, temp_file).await?;
    if !exists {
        return Ok(false);
    }

    // Read and decompress
    let data = tokio::fs::read(temp_file).await?;
    let _ = tokio::fs::remove_file(temp_file).await;

    decompress_to_directory(&Bytes::from(data), dest).await?;

    Ok(true)
}

/// Compress a directory and push it to S3.
async fn push_cache(s3: &S3Client, src: &Path, key: &str) -> Result<(), CacheError> {
    // Compress the directory
    let data = compress_directory(src, DEFAULT_COMPRESSION_LEVEL).await?;

    // Write to a temporary file
    let temp_path = format!("/tmp/cache-{}.tar.zst", uuid_simple());
    let temp_file = Path::new(&temp_path);

    tokio::fs::write(temp_file, &data).await?;

    // Upload to S3
    s3.upload(temp_file, key).await?;

    // Clean up temp file
    let _ = tokio::fs::remove_file(temp_file).await;

    Ok(())
}

/// Generate a simple unique identifier.
fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:x}{:x}", now.as_secs(), now.subsec_nanos())
}

/// Compress a directory into a tar.zst archive.
async fn compress_directory(src: &Path, compression_level: i32) -> Result<Bytes, CacheError> {
    let src = src.to_owned();
    spawn_blocking(move || compress_directory_sync(&src, compression_level))
        .await
        .map_err(|e| CacheError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
        .map_err(CacheError::Compression)
}

/// Decompress a tar.zst archive to a destination directory.
async fn decompress_to_directory(data: &Bytes, dest: &Path) -> Result<(), CacheError> {
    let data = data.clone();
    let dest = dest.to_owned();
    spawn_blocking(move || decompress_to_directory_sync(&data, &dest))
        .await
        .map_err(|e| CacheError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
        .map_err(CacheError::Compression)
}

fn compress_directory_sync(src: &Path, compression_level: i32) -> std::io::Result<Bytes> {
    let mut tar_data = Vec::new();

    {
        let mut tar_builder = tar::Builder::new(&mut tar_data);
        tar_builder.follow_symlinks(false);

        if src.is_dir() {
            for entry in walkdir(src)? {
                let path = entry?;
                if path == src {
                    continue;
                }

                let relative_path = path.strip_prefix(src).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
                })?;

                if path.is_file() {
                    tar_builder.append_path_with_name(&path, relative_path)?;
                } else if path.is_dir() {
                    tar_builder.append_dir(relative_path, &path)?;
                }
            }
        }

        tar_builder.finish()?;
    }

    debug!(uncompressed_size = tar_data.len(), "created tar archive");

    let compressed = zstd::encode_all(Cursor::new(&tar_data), compression_level)?;

    debug!(
        compressed_size = compressed.len(),
        ratio = format!(
            "{:.1}%",
            if tar_data.is_empty() {
                0.0
            } else {
                (compressed.len() as f64 / tar_data.len() as f64) * 100.0
            }
        ),
        "compressed tar archive"
    );

    Ok(Bytes::from(compressed))
}

fn decompress_to_directory_sync(data: &[u8], dest: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dest)?;

    let decompressed = zstd::decode_all(Cursor::new(data))?;

    debug!(
        compressed_size = data.len(),
        decompressed_size = decompressed.len(),
        "decompressed archive"
    );

    let mut archive = tar::Archive::new(Cursor::new(decompressed));
    archive.set_preserve_permissions(true);
    archive.set_preserve_mtime(true);
    archive.unpack(dest)?;

    Ok(())
}

fn walkdir(
    path: &Path,
) -> std::io::Result<impl Iterator<Item = std::io::Result<std::path::PathBuf>>> {
    let entries = std::fs::read_dir(path)?;
    let mut paths = Vec::new();

    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        paths.push(Ok(path.clone()));

        if path.is_dir() {
            for subpath in walkdir(&path)? {
                paths.push(subpath);
            }
        }
    }

    Ok(paths.into_iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_error_display() {
        let err =
            CacheError::Compression(std::io::Error::new(std::io::ErrorKind::Other, "test error"));
        assert!(err.to_string().contains("compression"));
    }

    #[test]
    fn uuid_simple_generates_unique() {
        let id1 = uuid_simple();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = uuid_simple();
        assert_ne!(id1, id2);
    }
}
