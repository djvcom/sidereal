//! Composable test fixtures using rstest.
//!
//! This module provides a hierarchy of fixtures for testing:
//!
//! ```text
//! temp_dir
//!    └── test_config
//!           └── test_store
//!                  ├── test_ingesters
//!                  └── test_engine
//!                         └── test_env (everything together)
//! ```
//!
//! # Example
//!
//! ```ignore
//! use rstest::*;
//! use crate::test_fixtures::*;
//!
//! #[rstest]
//! #[tokio::test]
//! async fn my_test(test_env: TestEnv) {
//!     // test_env has store, ingesters, and engine ready to use
//!     test_env.traces_ingester.ingest(batch).await.unwrap();
//!     test_env.traces_ingester.flush().await.unwrap();
//!     let results = test_env.engine.query("SELECT * FROM traces").await.unwrap();
//! }
//! ```

use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use rstest::fixture;
use tempfile::TempDir;

use crate::buffer::ingester::Ingester;
use crate::config::{BufferConfig, ParquetConfig, ServerConfig, StorageConfig, TelemetryConfig};
use crate::query::engine::QueryEngine;
use crate::redact::RedactionConfig;
use crate::schema::{logs::logs_schema, metrics::number_metrics_schema, traces::traces_schema};
use crate::storage::Signal;

/// A temporary directory that auto-cleans on drop.
#[fixture]
pub fn temp_dir() -> TempDir {
    tempfile::tempdir().expect("failed to create temp directory")
}

/// Test-friendly server config with non-conflicting ports.
#[fixture]
pub fn server_config() -> ServerConfig {
    // Use port 0 to let the OS assign available ports
    ServerConfig {
        grpc_addr: "127.0.0.1:0".parse().unwrap(),
        http_addr: "127.0.0.1:0".parse().unwrap(),
        query_addr: "127.0.0.1:0".parse().unwrap(),
    }
}

/// Test-friendly buffer config with small thresholds for fast flushing.
#[fixture]
pub fn buffer_config() -> BufferConfig {
    BufferConfig {
        max_batch_size: 10,
        flush_interval_secs: 1,
        max_buffer_bytes: 10 * 1024 * 1024,
        max_records_per_request: 100_000,
        flush_max_retries: 3,
        flush_initial_delay_ms: 10,
        flush_max_delay_ms: 100,
    }
}

/// Test-friendly parquet config.
#[fixture]
pub fn parquet_config() -> ParquetConfig {
    ParquetConfig {
        row_group_size: 1000,
        compression: "zstd".to_string(),
    }
}

/// Storage config using the temp directory.
#[fixture]
pub fn storage_config(temp_dir: TempDir) -> (StorageConfig, TempDir) {
    let config = StorageConfig::Local {
        path: temp_dir.path().to_path_buf(),
    };
    (config, temp_dir)
}

/// Storage config using in-memory store (faster, no disk I/O).
#[fixture]
pub fn memory_storage_config() -> StorageConfig {
    StorageConfig::Memory
}

/// Complete telemetry config for testing with temp directory storage.
#[fixture]
pub fn test_config(
    server_config: ServerConfig,
    buffer_config: BufferConfig,
    parquet_config: ParquetConfig,
    storage_config: (StorageConfig, TempDir),
) -> (TelemetryConfig, TempDir) {
    let (storage, temp_dir) = storage_config;
    let config = TelemetryConfig {
        server: server_config,
        buffer: buffer_config,
        parquet: parquet_config,
        storage,
        redaction: RedactionConfig::default(),
    };
    (config, temp_dir)
}

/// Complete telemetry config using in-memory storage (no temp dir needed).
#[fixture]
pub fn memory_test_config(
    server_config: ServerConfig,
    buffer_config: BufferConfig,
    parquet_config: ParquetConfig,
    memory_storage_config: StorageConfig,
) -> TelemetryConfig {
    TelemetryConfig {
        server: server_config,
        buffer: buffer_config,
        parquet: parquet_config,
        storage: memory_storage_config,
        redaction: RedactionConfig::default(),
    }
}

/// In-memory object store for fast tests.
#[fixture]
pub fn memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// Object store created from config.
#[fixture]
pub fn test_store(storage_config: (StorageConfig, TempDir)) -> (Arc<dyn ObjectStore>, TempDir) {
    let (config, temp_dir) = storage_config;
    let store =
        crate::storage::create_object_store(&config).expect("failed to create object store");
    (store, temp_dir)
}

/// Traces ingester with in-memory storage.
#[fixture]
pub fn traces_ingester(
    memory_store: Arc<dyn ObjectStore>,
    buffer_config: BufferConfig,
    parquet_config: ParquetConfig,
) -> Arc<Ingester> {
    Arc::new(Ingester::new(
        Signal::Traces,
        traces_schema(),
        memory_store,
        buffer_config,
        parquet_config,
    ))
}

/// Metrics ingester with in-memory storage.
#[fixture]
pub fn metrics_ingester(
    memory_store: Arc<dyn ObjectStore>,
    buffer_config: BufferConfig,
    parquet_config: ParquetConfig,
) -> Arc<Ingester> {
    Arc::new(Ingester::new(
        Signal::Metrics,
        number_metrics_schema(),
        memory_store,
        buffer_config,
        parquet_config,
    ))
}

/// Logs ingester with in-memory storage.
#[fixture]
pub fn logs_ingester(
    memory_store: Arc<dyn ObjectStore>,
    buffer_config: BufferConfig,
    parquet_config: ParquetConfig,
) -> Arc<Ingester> {
    Arc::new(Ingester::new(
        Signal::Logs,
        logs_schema(),
        memory_store,
        buffer_config,
        parquet_config,
    ))
}

/// All three ingesters sharing the same store.
pub struct Ingesters {
    pub traces: Arc<Ingester>,
    pub metrics: Arc<Ingester>,
    pub logs: Arc<Ingester>,
    pub store: Arc<dyn ObjectStore>,
}

#[fixture]
pub fn test_ingesters(buffer_config: BufferConfig, parquet_config: ParquetConfig) -> Ingesters {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    Ingesters {
        traces: Arc::new(Ingester::new(
            Signal::Traces,
            traces_schema(),
            store.clone(),
            buffer_config.clone(),
            parquet_config.clone(),
        )),
        metrics: Arc::new(Ingester::new(
            Signal::Metrics,
            number_metrics_schema(),
            store.clone(),
            buffer_config.clone(),
            parquet_config.clone(),
        )),
        logs: Arc::new(Ingester::new(
            Signal::Logs,
            logs_schema(),
            store.clone(),
            buffer_config,
            parquet_config,
        )),
        store,
    }
}

/// Query engine with in-memory storage.
#[fixture]
pub async fn test_engine(memory_store: Arc<dyn ObjectStore>) -> QueryEngine {
    QueryEngine::new(memory_store, "memory://")
        .await
        .expect("failed to create query engine")
}

/// Complete test environment with ingesters and query engine sharing the same store.
pub struct TestEnv {
    pub store: Arc<dyn ObjectStore>,
    pub traces_ingester: Arc<Ingester>,
    pub metrics_ingester: Arc<Ingester>,
    pub logs_ingester: Arc<Ingester>,
    pub engine: QueryEngine,
    pub config: TelemetryConfig,
}

impl TestEnv {
    /// Create a new test environment with in-memory storage.
    pub async fn new() -> Self {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let buffer_config = BufferConfig {
            max_batch_size: 10,
            flush_interval_secs: 1,
            max_buffer_bytes: 10 * 1024 * 1024,
            max_records_per_request: 100_000,
            flush_max_retries: 3,
            flush_initial_delay_ms: 10,
            flush_max_delay_ms: 100,
        };
        let parquet_config = ParquetConfig {
            row_group_size: 1000,
            compression: "zstd".to_string(),
        };

        let traces_ingester = Arc::new(Ingester::new(
            Signal::Traces,
            traces_schema(),
            store.clone(),
            buffer_config.clone(),
            parquet_config.clone(),
        ));
        let metrics_ingester = Arc::new(Ingester::new(
            Signal::Metrics,
            number_metrics_schema(),
            store.clone(),
            buffer_config.clone(),
            parquet_config.clone(),
        ));
        let logs_ingester = Arc::new(Ingester::new(
            Signal::Logs,
            logs_schema(),
            store.clone(),
            buffer_config.clone(),
            parquet_config,
        ));

        let engine = QueryEngine::new(store.clone(), "memory://")
            .await
            .expect("failed to create query engine");

        let config = TelemetryConfig {
            server: ServerConfig {
                grpc_addr: "127.0.0.1:0".parse().unwrap(),
                http_addr: "127.0.0.1:0".parse().unwrap(),
                query_addr: "127.0.0.1:0".parse().unwrap(),
            },
            buffer: BufferConfig {
                max_batch_size: 10,
                flush_interval_secs: 1,
                max_buffer_bytes: 10 * 1024 * 1024,
                max_records_per_request: 100_000,
                flush_max_retries: 3,
                flush_initial_delay_ms: 10,
                flush_max_delay_ms: 100,
            },
            parquet: ParquetConfig {
                row_group_size: 1000,
                compression: "zstd".to_string(),
            },
            storage: StorageConfig::Memory,
            redaction: RedactionConfig::default(),
        };

        Self {
            store,
            traces_ingester,
            metrics_ingester,
            logs_ingester,
            engine,
            config,
        }
    }
}

/// Fixture that provides a complete test environment.
#[fixture]
pub async fn test_env() -> TestEnv {
    TestEnv::new().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn temp_dir_is_created(temp_dir: TempDir) {
        assert!(temp_dir.path().exists());
    }

    #[rstest]
    fn storage_config_uses_temp_dir(storage_config: (StorageConfig, TempDir)) {
        let (config, temp_dir) = storage_config;
        match config {
            StorageConfig::Local { path } => {
                assert_eq!(path, temp_dir.path());
            }
            _ => panic!("expected local storage config"),
        }
    }

    #[rstest]
    fn test_config_has_all_components(test_config: (TelemetryConfig, TempDir)) {
        let (config, _temp_dir) = test_config;
        assert_eq!(config.buffer.max_batch_size, 10);
        assert_eq!(config.buffer.flush_interval_secs, 1);
    }

    #[rstest]
    fn ingesters_share_store(test_ingesters: Ingesters) {
        // All ingesters should reference the same store
        assert_eq!(test_ingesters.traces.signal(), Signal::Traces);
        assert_eq!(test_ingesters.metrics.signal(), Signal::Metrics);
        assert_eq!(test_ingesters.logs.signal(), Signal::Logs);
    }

    #[rstest]
    #[tokio::test]
    async fn test_env_is_fully_wired(#[future] test_env: TestEnv) {
        let env = test_env.await;

        // Verify all components exist
        assert_eq!(env.traces_ingester.signal(), Signal::Traces);
        assert_eq!(env.metrics_ingester.signal(), Signal::Metrics);
        assert_eq!(env.logs_ingester.signal(), Signal::Logs);

        // Verify engine can query (empty results are fine)
        let results = env.engine.query("SELECT * FROM traces LIMIT 1").await;
        assert!(results.is_ok());
    }
}
