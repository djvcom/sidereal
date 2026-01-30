//! S3 client for the builder runtime.
//!
//! Handles downloading and uploading files to S3-compatible storage.

use std::path::Path;

use bytes::Bytes;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use object_store::ObjectStoreExt;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};

use crate::protocol::S3Config;

/// Error type for S3 operations.
#[derive(Debug, thiserror::Error)]
pub enum S3Error {
    #[error("failed to create S3 client: {0}")]
    ClientCreation(String),

    #[error("download failed for key '{key}': {source}")]
    Download {
        key: String,
        source: object_store::Error,
    },

    #[error("upload failed for key '{key}': {source}")]
    Upload {
        key: String,
        source: object_store::Error,
    },

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid object path: {0}")]
    InvalidPath(String),
}

/// S3 client wrapper for builder runtime operations.
pub struct S3Client {
    store: AmazonS3,
    endpoint: String,
    bucket: String,
}

impl S3Client {
    /// Create a new S3 client from configuration.
    pub fn new(config: &S3Config) -> Result<Self, S3Error> {
        let store = AmazonS3Builder::new()
            .with_endpoint(&config.endpoint)
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(&config.access_key_id)
            .with_secret_access_key(&config.secret_access_key)
            .with_allow_http(config.endpoint.starts_with("http://"))
            .build()
            .map_err(|e| S3Error::ClientCreation(e.to_string()))?;

        info!(
            endpoint = %config.endpoint,
            bucket = %config.bucket,
            region = %config.region,
            "S3 client created"
        );

        Ok(Self {
            store,
            endpoint: config.endpoint.clone(),
            bucket: config.bucket.clone(),
        })
    }

    /// Download an object to a local file.
    pub async fn download(&self, key: &str, dest: &Path) -> Result<(), S3Error> {
        debug!(key = %key, dest = %dest.display(), "Downloading from S3");

        let path = ObjectPath::parse(key).map_err(|e| S3Error::InvalidPath(e.to_string()))?;

        let result = self.store.get(&path).await.map_err(|e| S3Error::Download {
            key: key.to_owned(),
            source: e,
        })?;

        let bytes = result.bytes().await.map_err(|e| S3Error::Download {
            key: key.to_owned(),
            source: e,
        })?;

        // Ensure parent directory exists
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // Write to file
        let mut file = tokio::fs::File::create(dest).await?;
        file.write_all(&bytes).await?;
        file.flush().await?;

        info!(
            key = %key,
            size = bytes.len(),
            dest = %dest.display(),
            "Downloaded from S3"
        );

        Ok(())
    }

    /// Upload a local file to S3.
    ///
    /// Returns the full URL of the uploaded object.
    pub async fn upload(&self, src: &Path, key: &str) -> Result<String, S3Error> {
        debug!(src = %src.display(), key = %key, "Uploading to S3");

        let path = ObjectPath::parse(key).map_err(|e| S3Error::InvalidPath(e.to_string()))?;

        let data = tokio::fs::read(src).await?;
        let size = data.len();
        let payload: object_store::PutPayload = Bytes::from(data).into();

        self.store
            .put(&path, payload)
            .await
            .map_err(|e| S3Error::Upload {
                key: key.to_owned(),
                source: e,
            })?;

        let url = format!("{}/{}/{}", self.endpoint, self.bucket, key);

        info!(
            key = %key,
            size = size,
            url = %url,
            "Uploaded to S3"
        );

        Ok(url)
    }

    /// Download an object if it exists.
    ///
    /// Returns `true` if the object was downloaded, `false` if it doesn't exist.
    pub async fn download_if_exists(&self, key: &str, dest: &Path) -> Result<bool, S3Error> {
        debug!(key = %key, "Checking if object exists in S3");

        let path = ObjectPath::parse(key).map_err(|e| S3Error::InvalidPath(e.to_string()))?;

        match self.store.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|e| S3Error::Download {
                    key: key.to_owned(),
                    source: e,
                })?;

                // Ensure parent directory exists
                if let Some(parent) = dest.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }

                // Write to file
                let mut file = tokio::fs::File::create(dest).await?;
                file.write_all(&bytes).await?;
                file.flush().await?;

                info!(
                    key = %key,
                    size = bytes.len(),
                    dest = %dest.display(),
                    "Downloaded from S3"
                );

                Ok(true)
            }
            Err(object_store::Error::NotFound { .. }) => {
                debug!(key = %key, "Object not found in S3");
                Ok(false)
            }
            Err(e) => Err(S3Error::Download {
                key: key.to_owned(),
                source: e,
            }),
        }
    }

    /// Upload with progress callback.
    ///
    /// Returns the full URL of the uploaded object.
    pub async fn upload_with_progress<F>(
        &self,
        src: &Path,
        key: &str,
        progress_fn: F,
    ) -> Result<String, S3Error>
    where
        F: Fn(f32),
    {
        debug!(src = %src.display(), key = %key, "Uploading to S3 with progress");

        let path = ObjectPath::parse(key).map_err(|e| S3Error::InvalidPath(e.to_string()))?;

        let data = tokio::fs::read(src).await?;
        let size = data.len();
        let payload: object_store::PutPayload = Bytes::from(data).into();

        // For now, just report 0% and 100% - object_store doesn't support streaming progress
        progress_fn(0.0);

        self.store
            .put(&path, payload)
            .await
            .map_err(|e| S3Error::Upload {
                key: key.to_owned(),
                source: e,
            })?;

        progress_fn(1.0);

        let url = format!("{}/{}/{}", self.endpoint, self.bucket, key);

        info!(
            key = %key,
            size = size,
            url = %url,
            "Uploaded to S3"
        );

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_error_display() {
        let err = S3Error::ClientCreation("connection refused".to_owned());
        assert!(err.to_string().contains("failed to create S3 client"));
        assert!(err.to_string().contains("connection refused"));
    }

    #[test]
    fn invalid_path_error() {
        let err = S3Error::InvalidPath("bad path".to_owned());
        assert!(err.to_string().contains("invalid object path"));
    }
}
