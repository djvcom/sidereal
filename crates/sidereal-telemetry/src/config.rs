//! Configuration types for the telemetry service.

use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;

use crate::redact::RedactionConfig;
use crate::TelemetryError;

// ============================================================================
// Default configuration constants
// ============================================================================

/// Default OTLP gRPC port (OpenTelemetry standard).
pub const DEFAULT_GRPC_PORT: u16 = 4317;

/// Default OTLP HTTP port (OpenTelemetry standard).
pub const DEFAULT_HTTP_PORT: u16 = 4318;

/// Default query API port.
pub const DEFAULT_QUERY_PORT: u16 = 3100;

/// Default maximum records per batch before flush.
pub const DEFAULT_MAX_BATCH_SIZE: usize = 10_000;

/// Default flush interval in seconds.
pub const DEFAULT_FLUSH_INTERVAL_SECS: u64 = 30;

/// Default maximum buffer size in bytes (100 MiB).
pub const DEFAULT_MAX_BUFFER_BYTES: usize = 100 * 1024 * 1024;

/// Default maximum records per ingestion request.
///
/// Limits memory usage from a single large request. Requests exceeding
/// this limit will receive a 413 Payload Too Large response.
pub const DEFAULT_MAX_RECORDS_PER_REQUEST: usize = 100_000;

/// Default maximum flush retry attempts.
pub const DEFAULT_FLUSH_MAX_RETRIES: u32 = 3;

/// Default initial retry delay in milliseconds.
pub const DEFAULT_FLUSH_INITIAL_DELAY_MS: u64 = 100;

/// Default maximum retry delay in milliseconds.
pub const DEFAULT_FLUSH_MAX_DELAY_MS: u64 = 10_000;

/// Default maximum rows per Parquet row group.
pub const DEFAULT_ROW_GROUP_SIZE: usize = 1_000_000;

/// Default Parquet compression algorithm.
pub const DEFAULT_COMPRESSION: &str = "zstd";

/// Default local storage path.
pub const DEFAULT_STORAGE_PATH: &str = "./telemetry-data";

/// Telemetry service configuration.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// Server configuration.
    pub server: ServerConfig,
    /// Buffer configuration.
    pub buffer: BufferConfig,
    /// Storage configuration.
    pub storage: StorageConfig,
    /// Parquet configuration.
    pub parquet: ParquetConfig,
    /// Redaction configuration.
    pub redaction: RedactionConfig,
}

impl TelemetryConfig {
    /// Load configuration from files and environment.
    ///
    /// Configuration is loaded in order (later sources override earlier):
    /// 1. Default values
    /// 2. `telemetry.toml` in current directory
    /// 3. Environment variables prefixed with `TELEMETRY_`
    pub fn load() -> Result<Self, TelemetryError> {
        Figment::new()
            .merge(Toml::file("telemetry.toml"))
            .merge(Env::prefixed("TELEMETRY_").split("_"))
            .extract()
            .map_err(|e| TelemetryError::Config(e.to_string()))
    }

    /// Load configuration from a specific file path.
    pub fn load_from(path: &str) -> Result<Self, TelemetryError> {
        Figment::new()
            .merge(Toml::file(path))
            .merge(Env::prefixed("TELEMETRY_").split("_"))
            .extract()
            .map_err(|e| TelemetryError::Config(e.to_string()))
    }
}

/// Server address configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// OTLP gRPC address (default: 0.0.0.0:4317).
    pub grpc_addr: SocketAddr,
    /// OTLP HTTP address (default: 0.0.0.0:4318).
    pub http_addr: SocketAddr,
    /// Query API address (default: 0.0.0.0:3100).
    pub query_addr: SocketAddr,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_addr: SocketAddr::from(([0, 0, 0, 0], DEFAULT_GRPC_PORT)),
            http_addr: SocketAddr::from(([0, 0, 0, 0], DEFAULT_HTTP_PORT)),
            query_addr: SocketAddr::from(([0, 0, 0, 0], DEFAULT_QUERY_PORT)),
        }
    }
}

/// Buffer configuration for in-memory buffering before flush.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct BufferConfig {
    /// Maximum records per batch before flush.
    pub max_batch_size: usize,
    /// Flush interval in seconds.
    pub flush_interval_secs: u64,
    /// Maximum buffer size in bytes before backpressure.
    pub max_buffer_bytes: usize,
    /// Maximum records per ingestion request.
    ///
    /// Requests exceeding this limit will receive a 413 Payload Too Large response.
    pub max_records_per_request: usize,
    /// Maximum flush retry attempts before giving up.
    ///
    /// When a flush fails (Parquet write or object store upload), the ingester
    /// retries with exponential backoff. Set to 0 to disable retries.
    pub flush_max_retries: u32,
    /// Initial retry delay in milliseconds.
    ///
    /// The delay doubles after each failed attempt, up to `flush_max_delay_ms`.
    pub flush_initial_delay_ms: u64,
    /// Maximum retry delay in milliseconds.
    ///
    /// Caps the exponential backoff to prevent excessively long delays.
    pub flush_max_delay_ms: u64,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            flush_interval_secs: DEFAULT_FLUSH_INTERVAL_SECS,
            max_buffer_bytes: DEFAULT_MAX_BUFFER_BYTES,
            max_records_per_request: DEFAULT_MAX_RECORDS_PER_REQUEST,
            flush_max_retries: DEFAULT_FLUSH_MAX_RETRIES,
            flush_initial_delay_ms: DEFAULT_FLUSH_INITIAL_DELAY_MS,
            flush_max_delay_ms: DEFAULT_FLUSH_MAX_DELAY_MS,
        }
    }
}

/// Storage backend configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageConfig {
    /// Local filesystem storage.
    Local {
        /// Path to storage directory.
        path: PathBuf,
    },
    /// AWS S3 or S3-compatible storage (MinIO, Garage, etc.).
    S3 {
        /// S3 bucket name.
        bucket: String,
        /// Optional prefix within the bucket.
        #[serde(default)]
        prefix: String,
        /// AWS region or custom region for S3-compatible services.
        region: Option<String>,
        /// Custom endpoint URL for S3-compatible services (e.g., "http://localhost:3900").
        endpoint: Option<String>,
        /// Access key ID (can also be set via AWS_ACCESS_KEY_ID env var).
        access_key_id: Option<String>,
        /// Secret access key (can also be set via AWS_SECRET_ACCESS_KEY env var).
        secret_access_key: Option<String>,
        /// Force path-style URLs (required for some S3-compatible services).
        #[serde(default)]
        force_path_style: bool,
        /// Allow HTTP (non-TLS) connections (for local development).
        #[serde(default)]
        allow_http: bool,
    },
    /// Google Cloud Storage.
    Gcs {
        /// GCS bucket name.
        bucket: String,
        /// Optional prefix within the bucket.
        #[serde(default)]
        prefix: String,
        /// Path to service account JSON key file (can also use GOOGLE_APPLICATION_CREDENTIALS env var).
        service_account_path: Option<String>,
    },
    /// Azure Blob Storage.
    Azure {
        /// Azure storage account name.
        account: String,
        /// Azure container name.
        container: String,
        /// Optional prefix within the container.
        #[serde(default)]
        prefix: String,
        /// Access key (can also use AZURE_STORAGE_ACCESS_KEY env var).
        access_key: Option<String>,
    },
    /// In-memory storage (for testing).
    Memory,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::Local {
            path: PathBuf::from(DEFAULT_STORAGE_PATH),
        }
    }
}

/// Parquet file configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ParquetConfig {
    /// Maximum rows per row group.
    pub row_group_size: usize,
    /// Compression algorithm (zstd, snappy, lz4, none).
    pub compression: String,
}

impl Default for ParquetConfig {
    fn default() -> Self {
        Self {
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            compression: DEFAULT_COMPRESSION.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = TelemetryConfig::default();
        assert_eq!(config.server.grpc_addr.port(), DEFAULT_GRPC_PORT);
        assert_eq!(config.server.http_addr.port(), DEFAULT_HTTP_PORT);
        assert_eq!(config.server.query_addr.port(), DEFAULT_QUERY_PORT);
    }

    #[test]
    fn buffer_defaults() {
        let config = BufferConfig::default();
        assert_eq!(config.max_batch_size, DEFAULT_MAX_BATCH_SIZE);
        assert_eq!(config.flush_interval_secs, DEFAULT_FLUSH_INTERVAL_SECS);
        assert_eq!(config.max_buffer_bytes, DEFAULT_MAX_BUFFER_BYTES);
    }

    #[test]
    fn storage_defaults_to_local() {
        let config = StorageConfig::default();
        match config {
            StorageConfig::Local { path } => {
                assert_eq!(path, PathBuf::from(DEFAULT_STORAGE_PATH));
            }
            _ => panic!("Expected local storage as default"),
        }
    }

    #[test]
    fn parquet_defaults() {
        let config = ParquetConfig::default();
        assert_eq!(config.row_group_size, DEFAULT_ROW_GROUP_SIZE);
        assert_eq!(config.compression, DEFAULT_COMPRESSION);
    }
}
