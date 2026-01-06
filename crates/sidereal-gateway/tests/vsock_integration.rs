//! Integration tests for vsock backend.
//!
//! These tests require a running Firecracker VM with the Sidereal guest runtime.
//! Run with: cargo test --features firecracker -- --ignored

#![cfg(feature = "firecracker")]

use sidereal_gateway::backend::{DispatchRequest, VsockBackend, WorkerBackend};
use std::path::PathBuf;

fn test_uds_path() -> PathBuf {
    std::env::var("SIDEREAL_TEST_VSOCK_UDS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp/sidereal/test_vsock.sock"))
}

fn test_vsock_port() -> u32 {
    std::env::var("SIDEREAL_TEST_VSOCK_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1024)
}

#[tokio::test]
#[ignore = "requires running Firecracker VM"]
async fn vsock_health_check() {
    let backend = VsockBackend::new(test_uds_path(), test_vsock_port());

    backend
        .health_check()
        .await
        .expect("Health check should succeed");
}

#[tokio::test]
#[ignore = "requires running Firecracker VM"]
async fn vsock_dispatch_request() {
    let backend = VsockBackend::new(test_uds_path(), test_vsock_port());

    let request = DispatchRequest {
        function_name: "echo".to_string(),
        payload: br#"{"message": "hello"}"#.to_vec(),
        trace_id: "test-trace-id".to_string(),
        headers: http::HeaderMap::new(),
    };

    let response = backend
        .dispatch(request)
        .await
        .expect("Dispatch should succeed");

    assert_eq!(response.status, 200);
}

#[tokio::test]
#[ignore = "requires running Firecracker VM"]
async fn vsock_connection_failure() {
    let backend = VsockBackend::new("/nonexistent/path.sock".into(), 1024);

    let result = backend.health_check().await;
    assert!(result.is_err(), "Should fail with nonexistent socket");
}
