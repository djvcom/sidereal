//! S3 integration tests.
//!
//! These tests require an S3-compatible service (AWS S3, MinIO, Garage) and credentials.
//! They are ignored by default and can be enabled by setting environment variables:
//!
//! ```sh
//! export TELEMETRY_S3_TEST_BUCKET=telemetry-test
//! export TELEMETRY_S3_TEST_ENDPOINT=http://localhost:3900
//! export TELEMETRY_S3_TEST_REGION=garage
//! export AWS_ACCESS_KEY_ID=your-key
//! export AWS_SECRET_ACCESS_KEY=your-secret
//! cargo test -p sidereal-telemetry --features s3 --test s3_integration -- --ignored
//! ```

#![cfg(feature = "s3")]

use std::env;

use futures::StreamExt;
use object_store::ObjectStore;

use sidereal_telemetry::buffer::Ingester;
use sidereal_telemetry::config::{BufferConfig, ParquetConfig, StorageConfig};
use sidereal_telemetry::ingest::convert_traces_to_arrow;
use sidereal_telemetry::query::QueryEngine;
use sidereal_telemetry::schema::traces::traces_storage_schema;
use sidereal_telemetry::storage::{base_url, create_object_store, Signal};

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

fn get_s3_config() -> Option<StorageConfig> {
    let bucket = env::var("TELEMETRY_S3_TEST_BUCKET").ok()?;
    let endpoint = env::var("TELEMETRY_S3_TEST_ENDPOINT").ok();
    let region = env::var("TELEMETRY_S3_TEST_REGION").ok();
    let access_key_id = env::var("AWS_ACCESS_KEY_ID").ok();
    let secret_access_key = env::var("AWS_SECRET_ACCESS_KEY").ok();

    Some(StorageConfig::S3 {
        bucket,
        prefix: format!("test-{}", ulid::Ulid::new()),
        region,
        endpoint,
        access_key_id,
        secret_access_key,
        force_path_style: true,
        allow_http: true,
    })
}

fn test_buffer_config() -> BufferConfig {
    BufferConfig {
        max_batch_size: 100,
        flush_interval_secs: 30,
        max_buffer_bytes: 10 * 1024 * 1024,
    }
}

fn test_parquet_config() -> ParquetConfig {
    ParquetConfig {
        row_group_size: 1000,
        compression: "zstd".to_string(),
    }
}

fn sample_trace_request(service_name: &str, span_name: &str) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(service_name.to_string())),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![Span {
                    trace_id: vec![0xAA; 16],
                    span_id: vec![0xBB; 8],
                    name: span_name.to_string(),
                    start_time_unix_nano: 1_704_067_200_000_000_000,
                    end_time_unix_nano: 1_704_067_201_000_000_000,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

#[tokio::test]
#[ignore = "requires S3-compatible service with credentials"]
async fn s3_write_and_list() {
    let config = get_s3_config().expect("S3 config not set - see test docs");
    let store = create_object_store(&config).expect("failed to create S3 store");

    // Write a test file
    let path = object_store::path::Path::from("test-file.txt");
    store
        .put(&path, "hello from S3 test".into())
        .await
        .expect("failed to put object");

    // List files
    let files: Vec<_> = store.list(None).collect().await;
    assert!(!files.is_empty(), "should have at least one file");

    // Read back
    let result = store.get(&path).await.expect("failed to get object");
    let bytes = result.bytes().await.expect("failed to read bytes");
    assert_eq!(&bytes[..], b"hello from S3 test");

    // Clean up
    store.delete(&path).await.expect("failed to delete object");
}

#[tokio::test]
#[ignore = "requires S3-compatible service with credentials"]
async fn s3_ingest_flush_query() {
    let config = get_s3_config().expect("S3 config not set - see test docs");
    let base = base_url(&config);
    let store = create_object_store(&config).expect("failed to create S3 store");

    // Create ingester with storage schema (excludes partition columns)
    let ingester = Ingester::new(
        Signal::Traces,
        traces_storage_schema(),
        store.clone(),
        test_buffer_config(),
        test_parquet_config(),
    );

    // Ingest traces
    for i in 0..3 {
        let request = sample_trace_request("s3-test-service", &format!("span-{}", i));
        let batch = convert_traces_to_arrow(&request)
            .expect("failed to convert")
            .batch;
        ingester.ingest(batch).await.expect("failed to ingest");
    }

    // Flush to S3
    ingester.flush().await.expect("failed to flush");

    // List files to verify
    let files: Vec<_> = store.list(None).collect().await;
    let parquet_files: Vec<_> = files
        .iter()
        .filter_map(|f| f.as_ref().ok())
        .filter(|f| f.location.to_string().ends_with(".parquet"))
        .collect();
    assert!(!parquet_files.is_empty(), "should have parquet files");

    // Query via DataFusion
    let engine = QueryEngine::new(store.clone(), &base)
        .await
        .expect("failed to create query engine");

    let results = engine
        .query("SELECT name, \"service.name\" FROM traces ORDER BY name")
        .await
        .expect("failed to query");

    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "should have 3 trace spans");

    // Clean up - delete all files with our test prefix
    let prefix = match &config {
        StorageConfig::S3 { prefix, .. } => prefix.clone(),
        _ => String::new(),
    };
    let prefix_path = object_store::path::Path::from(prefix);
    let files_to_delete: Vec<_> = store
        .list(Some(&prefix_path))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .filter_map(|f| f.ok())
        .collect();

    for file in files_to_delete {
        let _ = store.delete(&file.location).await;
    }
}
