//! Tar and zstd compression utilities for cache archives.
//!
//! Provides streaming compression and decompression of directory contents
//! using tar archives with zstd compression.

use std::io::Cursor;
use std::path::Path;

use bytes::Bytes;
use tokio::task::spawn_blocking;
use tracing::debug;

/// Compress a directory into a tar.zst archive.
///
/// Returns the compressed bytes that can be uploaded to storage.
pub async fn compress_directory(src: &Path, compression_level: i32) -> std::io::Result<Bytes> {
    let src = src.to_owned();
    spawn_blocking(move || compress_directory_sync(&src, compression_level)).await?
}

/// Decompress a tar.zst archive to a destination directory.
///
/// Creates the destination directory if it does not exist.
pub async fn decompress_to_directory(data: &Bytes, dest: &Path) -> std::io::Result<()> {
    let data = data.clone();
    let dest = dest.to_owned();
    spawn_blocking(move || decompress_to_directory_sync(&data, &dest)).await?
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
            (compressed.len() as f64 / tar_data.len() as f64) * 100.0
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
    use tempfile::TempDir;

    #[tokio::test]
    async fn compress_and_decompress_directory() {
        let src_dir = TempDir::new().unwrap();
        let dest_dir = TempDir::new().unwrap();

        std::fs::write(src_dir.path().join("file1.txt"), "hello world").unwrap();
        std::fs::create_dir(src_dir.path().join("subdir")).unwrap();
        std::fs::write(src_dir.path().join("subdir/file2.txt"), "nested content").unwrap();

        let compressed = compress_directory(src_dir.path(), 3).await.unwrap();
        assert!(!compressed.is_empty());

        decompress_to_directory(&compressed, dest_dir.path())
            .await
            .unwrap();

        let content1 = std::fs::read_to_string(dest_dir.path().join("file1.txt")).unwrap();
        assert_eq!(content1, "hello world");

        let content2 = std::fs::read_to_string(dest_dir.path().join("subdir/file2.txt")).unwrap();
        assert_eq!(content2, "nested content");
    }

    #[tokio::test]
    async fn compress_empty_directory() {
        let src_dir = TempDir::new().unwrap();
        let dest_dir = TempDir::new().unwrap();

        let compressed = compress_directory(src_dir.path(), 3).await.unwrap();
        decompress_to_directory(&compressed, dest_dir.path())
            .await
            .unwrap();

        assert!(dest_dir.path().exists());
    }

    #[tokio::test]
    async fn compression_level_affects_size() {
        let src_dir = TempDir::new().unwrap();

        let data = "a".repeat(10000);
        std::fs::write(src_dir.path().join("large.txt"), &data).unwrap();

        let low_compression = compress_directory(src_dir.path(), 1).await.unwrap();
        let high_compression = compress_directory(src_dir.path(), 19).await.unwrap();

        assert!(high_compression.len() <= low_compression.len());
    }
}
