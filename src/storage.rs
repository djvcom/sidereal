//! Object store factory and partition path builder.
//!
//! This module provides a factory function to create the appropriate `ObjectStore`
//! implementation based on configuration, and utilities for generating Hive-style
//! partition paths for telemetry data.
//!
//! # Data Retention
//!
//! Telemetry data is stored using Hive-style partitioning:
//!
//! ```text
//! {signal}/date={YYYY-MM-DD}/hour={HH}/{ulid}.parquet
//! ```
//!
//! This structure enables efficient time-based retention policies using native
//! cloud storage lifecycle rules. **The recommended approach is to configure
//! retention directly on the storage backend** rather than in the application,
//! as this is more reliable and doesn't require the telemetry service to be running.
//!
//! ## AWS S3 Lifecycle Rules
//!
//! Create a lifecycle rule to delete objects older than a specified number of days:
//!
//! ```json
//! {
//!   "Rules": [
//!     {
//!       "ID": "delete-old-telemetry",
//!       "Status": "Enabled",
//!       "Filter": {
//!         "Prefix": ""
//!       },
//!       "Expiration": {
//!         "Days": 30
//!       }
//!     }
//!   ]
//! }
//! ```
//!
//! Apply with the AWS CLI:
//!
//! ```bash
//! aws s3api put-bucket-lifecycle-configuration \
//!   --bucket my-telemetry-bucket \
//!   --lifecycle-configuration file://lifecycle.json
//! ```
//!
//! For different retention per signal type:
//!
//! ```json
//! {
//!   "Rules": [
//!     {
//!       "ID": "delete-old-traces",
//!       "Status": "Enabled",
//!       "Filter": { "Prefix": "traces/" },
//!       "Expiration": { "Days": 7 }
//!     },
//!     {
//!       "ID": "delete-old-metrics",
//!       "Status": "Enabled",
//!       "Filter": { "Prefix": "metrics/" },
//!       "Expiration": { "Days": 30 }
//!     },
//!     {
//!       "ID": "delete-old-logs",
//!       "Status": "Enabled",
//!       "Filter": { "Prefix": "logs/" },
//!       "Expiration": { "Days": 14 }
//!     }
//!   ]
//! }
//! ```
//!
//! ## Google Cloud Storage Lifecycle Rules
//!
//! ```json
//! {
//!   "lifecycle": {
//!     "rule": [
//!       {
//!         "action": { "type": "Delete" },
//!         "condition": {
//!           "age": 30,
//!           "matchesPrefix": ["traces/", "metrics/", "logs/"]
//!         }
//!       }
//!     ]
//!   }
//! }
//! ```
//!
//! Apply with gsutil:
//!
//! ```bash
//! gsutil lifecycle set lifecycle.json gs://my-telemetry-bucket
//! ```
//!
//! ## Azure Blob Storage Lifecycle Management
//!
//! ```json
//! {
//!   "rules": [
//!     {
//!       "enabled": true,
//!       "name": "delete-old-telemetry",
//!       "type": "Lifecycle",
//!       "definition": {
//!         "actions": {
//!           "baseBlob": {
//!             "delete": { "daysAfterModificationGreaterThan": 30 }
//!           }
//!         },
//!         "filters": {
//!           "blobTypes": ["blockBlob"],
//!           "prefixMatch": ["telemetry/"]
//!         }
//!       }
//!     }
//!   ]
//! }
//! ```
//!
//! Apply with Azure CLI:
//!
//! ```bash
//! az storage account management-policy create \
//!   --account-name myaccount \
//!   --policy @lifecycle.json
//! ```
//!
//! ## Local Filesystem Retention
//!
//! For local storage, use a cron job with find:
//!
//! ```bash
//! # Delete traces older than 7 days
//! find /data/telemetry/traces -name "*.parquet" -mtime +7 -delete
//!
//! # Delete metrics older than 30 days
//! find /data/telemetry/metrics -name "*.parquet" -mtime +30 -delete
//!
//! # Delete logs older than 14 days
//! find /data/telemetry/logs -name "*.parquet" -mtime +14 -delete
//!
//! # Clean up empty date/hour directories
//! find /data/telemetry -type d -empty -delete
//! ```
//!
//! ## S3-Compatible Services (MinIO, Garage, etc.)
//!
//! Most S3-compatible services support the same lifecycle API. Check your
//! service's documentation for specific configuration options.
//!
//! For Garage:
//!
//! ```bash
//! garage bucket lifecycle set my-telemetry-bucket \
//!   --expiration-days 30 \
//!   --prefix ""
//! ```

use chrono::{DateTime, Timelike, Utc};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::ObjectStore;
use std::sync::Arc;

use crate::config::StorageConfig;
use crate::TelemetryError;

/// Telemetry signal type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// Trace spans.
    Traces,
    /// Metrics (gauges, counters, histograms).
    Metrics,
    /// Log records.
    Logs,
}

impl Signal {
    /// Get the string representation for use in paths.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Metrics => "metrics",
            Self::Logs => "logs",
        }
    }
}

impl std::fmt::Display for Signal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Create an object store from configuration.
///
/// # Errors
///
/// Returns an error if the object store cannot be created (e.g., invalid path,
/// missing credentials for cloud storage).
pub fn create_object_store(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>, TelemetryError> {
    match config {
        StorageConfig::Local { path } => {
            std::fs::create_dir_all(path)?;
            let store = LocalFileSystem::new_with_prefix(path)?;
            Ok(Arc::new(store))
        }
        StorageConfig::Memory => Ok(Arc::new(InMemory::new())),
        #[cfg(feature = "s3")]
        StorageConfig::S3 {
            bucket,
            prefix: _,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
            force_path_style,
            allow_http,
        } => {
            use object_store::aws::AmazonS3Builder;

            let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

            if let Some(r) = region {
                builder = builder.with_region(r);
            }
            if let Some(ref ep) = endpoint {
                builder = builder.with_endpoint(ep);
            }
            if let Some(ref key) = access_key_id {
                builder = builder.with_access_key_id(key);
            }
            if let Some(ref secret) = secret_access_key {
                builder = builder.with_secret_access_key(secret);
            }
            if *force_path_style {
                builder = builder.with_virtual_hosted_style_request(false);
            }
            if *allow_http {
                builder = builder.with_allow_http(true);
            }

            let store = builder.build()?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "s3"))]
        StorageConfig::S3 { .. } => Err(TelemetryError::Config(
            "S3 storage requires the 's3' feature to be enabled".to_owned(),
        )),
        #[cfg(feature = "gcs")]
        StorageConfig::Gcs {
            bucket,
            prefix: _,
            service_account_path,
        } => {
            use object_store::gcp::GoogleCloudStorageBuilder;

            let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);

            if let Some(ref path) = service_account_path {
                builder = builder.with_service_account_path(path);
            }

            let store = builder.build()?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "gcs"))]
        StorageConfig::Gcs { .. } => Err(TelemetryError::Config(
            "GCS storage requires the 'gcs' feature to be enabled".to_owned(),
        )),
        #[cfg(feature = "azure")]
        StorageConfig::Azure {
            account,
            container,
            prefix: _,
            access_key,
        } => {
            use object_store::azure::MicrosoftAzureBuilder;

            let mut builder = MicrosoftAzureBuilder::from_env()
                .with_account(account)
                .with_container_name(container);

            if let Some(ref key) = access_key {
                builder = builder.with_access_key(key);
            }

            let store = builder.build()?;
            Ok(Arc::new(store))
        }
        #[cfg(not(feature = "azure"))]
        StorageConfig::Azure { .. } => Err(TelemetryError::Config(
            "Azure storage requires the 'azure' feature to be enabled".to_owned(),
        )),
    }
}

/// Generate a Hive-style partition path for a telemetry signal.
///
/// The path format is:
/// `{signal}/date={YYYY-MM-DD}/hour={HH}/{ulid}.parquet`
///
/// Or with project ID:
/// `{signal}/date={YYYY-MM-DD}/hour={HH}/project={project}/{ulid}.parquet`
///
/// This format enables DataFusion to automatically prune partitions based on
/// WHERE clauses filtering by date, hour, or project.
pub fn partition_path(signal: Signal, timestamp: DateTime<Utc>, project_id: Option<&str>) -> Path {
    let date = timestamp.format("%Y-%m-%d");
    let hour = timestamp.hour();
    let ulid = ulid::Ulid::new();

    let path_str = match project_id {
        Some(proj) => format!(
            "{}/date={}/hour={:02}/project={}/{}.parquet",
            signal.as_str(),
            date,
            hour,
            proj,
            ulid
        ),
        None => format!(
            "{}/date={}/hour={:02}/{}.parquet",
            signal.as_str(),
            date,
            hour,
            ulid
        ),
    };

    Path::from(path_str)
}

/// Get the base URL for DataFusion table registration.
///
/// Returns the appropriate URL scheme for the storage backend:
/// - Local: `file://`
/// - Memory: `memory://`
/// - S3: `s3://{bucket}/{prefix}`
/// - GCS: `gs://{bucket}/{prefix}`
pub fn base_url(config: &StorageConfig) -> String {
    match config {
        StorageConfig::Local { path } => {
            format!("file://{}", path.display())
        }
        StorageConfig::Memory => "memory://".to_owned(),
        StorageConfig::S3 { bucket, prefix, .. } => {
            if prefix.is_empty() {
                format!("s3://{bucket}")
            } else {
                format!("s3://{bucket}/{prefix}")
            }
        }
        StorageConfig::Gcs { bucket, prefix, .. } => {
            if prefix.is_empty() {
                format!("gs://{bucket}")
            } else {
                format!("gs://{bucket}/{prefix}")
            }
        }
        StorageConfig::Azure {
            account,
            container,
            prefix,
            ..
        } => {
            if prefix.is_empty() {
                format!("az://{account}/{container}")
            } else {
                format!("az://{account}/{container}/{prefix}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn signal_as_str() {
        assert_eq!(Signal::Traces.as_str(), "traces");
        assert_eq!(Signal::Metrics.as_str(), "metrics");
        assert_eq!(Signal::Logs.as_str(), "logs");
    }

    #[test]
    fn partition_path_without_project() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
        let path = partition_path(Signal::Traces, timestamp, None);
        let path_str = path.to_string();

        assert!(path_str.starts_with("traces/date=2024-01-15/hour=14/"));
        assert!(path_str.ends_with(".parquet"));
    }

    #[test]
    fn partition_path_with_project() {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
        let path = partition_path(Signal::Metrics, timestamp, Some("my-app"));
        let path_str = path.to_string();

        assert!(path_str.starts_with("metrics/date=2024-01-15/hour=14/project=my-app/"));
        assert!(path_str.ends_with(".parquet"));
    }

    #[test]
    fn base_url_local() {
        let config = StorageConfig::Local {
            path: "/tmp/telemetry".into(),
        };
        assert_eq!(base_url(&config), "file:///tmp/telemetry");
    }

    #[test]
    fn base_url_memory() {
        let config = StorageConfig::Memory;
        assert_eq!(base_url(&config), "memory://");
    }

    #[test]
    fn base_url_s3_no_prefix() {
        let config = StorageConfig::S3 {
            bucket: "my-bucket".to_string(),
            prefix: String::new(),
            region: None,
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            force_path_style: false,
            allow_http: false,
        };
        assert_eq!(base_url(&config), "s3://my-bucket");
    }

    #[test]
    fn base_url_s3_with_prefix() {
        let config = StorageConfig::S3 {
            bucket: "my-bucket".to_string(),
            prefix: "telemetry".to_string(),
            region: Some("eu-west-1".to_string()),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            force_path_style: false,
            allow_http: false,
        };
        assert_eq!(base_url(&config), "s3://my-bucket/telemetry");
    }

    #[test]
    fn base_url_s3_custom_endpoint() {
        let config = StorageConfig::S3 {
            bucket: "telemetry".to_string(),
            prefix: String::new(),
            region: Some("garage".to_string()),
            endpoint: Some("http://localhost:3900".to_string()),
            access_key_id: Some("test-key".to_string()),
            secret_access_key: Some("test-secret".to_string()),
            force_path_style: true,
            allow_http: true,
        };
        assert_eq!(base_url(&config), "s3://telemetry");
    }

    #[test]
    fn base_url_gcs() {
        let config = StorageConfig::Gcs {
            bucket: "my-bucket".to_string(),
            prefix: "telemetry".to_string(),
            service_account_path: None,
        };
        assert_eq!(base_url(&config), "gs://my-bucket/telemetry");
    }

    #[test]
    fn base_url_azure() {
        let config = StorageConfig::Azure {
            account: "myaccount".to_string(),
            container: "telemetry".to_string(),
            prefix: String::new(),
            access_key: None,
        };
        assert_eq!(base_url(&config), "az://myaccount/telemetry");
    }

    #[test]
    fn base_url_azure_with_prefix() {
        let config = StorageConfig::Azure {
            account: "myaccount".to_string(),
            container: "telemetry".to_string(),
            prefix: "data".to_string(),
            access_key: None,
        };
        assert_eq!(base_url(&config), "az://myaccount/telemetry/data");
    }

    #[tokio::test]
    async fn create_memory_store() {
        let config = StorageConfig::Memory;
        let store = create_object_store(&config).unwrap();

        // Verify we can write and read
        let path = Path::from("test.txt");
        store.put(&path, "hello".into()).await.unwrap();
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(&bytes[..], b"hello");
    }

    #[tokio::test]
    async fn create_local_store() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = StorageConfig::Local {
            path: temp_dir.path().to_path_buf(),
        };
        let store = create_object_store(&config).unwrap();

        // Verify we can write and read
        let path = Path::from("test.txt");
        store.put(&path, "hello".into()).await.unwrap();
        let result = store.get(&path).await.unwrap();
        let bytes = result.bytes().await.unwrap();
        assert_eq!(&bytes[..], b"hello");
    }
}
