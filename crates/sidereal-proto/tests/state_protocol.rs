//! Integration tests for the state protocol.
//!
//! These tests verify that the state client and server can communicate
//! correctly over a stream. We use TCP for testing since vsock requires
//! running inside a VM.

use std::sync::Arc;
use std::time::Duration;

use sidereal_proto::codec::{Codec, FrameHeader, MessageType, FRAME_HEADER_SIZE};
use sidereal_proto::{Envelope, StateErrorCode, StateMessage, StateRequest, StateResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

/// Send a state request and receive a response over TCP.
async fn send_request(
    stream: &mut TcpStream,
    codec: &Mutex<Codec>,
    request: StateRequest,
) -> StateResponse {
    let envelope = Envelope::new(StateMessage::Request(request));

    let bytes = {
        let mut c = codec.lock().await;
        c.encode(&envelope, MessageType::State).unwrap().to_vec()
    };

    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();

    let mut header_buf = [0u8; FRAME_HEADER_SIZE];
    stream.read_exact(&mut header_buf).await.unwrap();

    let header = FrameHeader::decode(&header_buf).unwrap();
    let mut payload = vec![0u8; header.payload_len as usize];
    stream.read_exact(&mut payload).await.unwrap();

    let response: Envelope<StateMessage> = Codec::decode(&payload).unwrap();

    match response.payload {
        StateMessage::Response(r) => r,
        _ => panic!("Expected response"),
    }
}

/// Simple in-memory KV handler for testing.
async fn handle_request(
    kv: &Mutex<std::collections::HashMap<String, Vec<u8>>>,
    request: StateRequest,
) -> StateResponse {
    match request {
        StateRequest::KvGet { key } => {
            let store = kv.lock().await;
            StateResponse::KvValue(store.get(&key).cloned())
        }
        StateRequest::KvPut { key, value, .. } => {
            let mut store = kv.lock().await;
            store.insert(key, value);
            StateResponse::KvSuccess(true)
        }
        StateRequest::KvDelete { key } => {
            let mut store = kv.lock().await;
            let existed = store.remove(&key).is_some();
            StateResponse::KvSuccess(existed)
        }
        StateRequest::KvExists { key } => {
            let store = kv.lock().await;
            StateResponse::KvSuccess(store.contains_key(&key))
        }
        StateRequest::KvList { prefix, limit, .. } => {
            let store = kv.lock().await;
            let keys: Vec<String> = store
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .take(limit as usize)
                .cloned()
                .collect();
            StateResponse::KvList { keys, cursor: None }
        }
        _ => StateResponse::error(StateErrorCode::NotConfigured, "Not implemented"),
    }
}

/// Run a simple state server for testing.
async fn run_test_server(
    listener: TcpListener,
    kv: Arc<Mutex<std::collections::HashMap<String, Vec<u8>>>>,
) {
    while let Ok((mut stream, _)) = listener.accept().await {
        let kv = kv.clone();
        tokio::spawn(async move {
            let codec = Mutex::new(Codec::with_capacity(8192));

            loop {
                let mut header_buf = [0u8; FRAME_HEADER_SIZE];
                if stream.read_exact(&mut header_buf).await.is_err() {
                    break;
                }

                let header = match FrameHeader::decode(&header_buf) {
                    Ok(h) => h,
                    Err(_) => break,
                };

                let mut payload = vec![0u8; header.payload_len as usize];
                if stream.read_exact(&mut payload).await.is_err() {
                    break;
                }

                let envelope: Envelope<StateMessage> = match Codec::decode(&payload) {
                    Ok(e) => e,
                    Err(_) => break,
                };

                let response = match envelope.payload {
                    StateMessage::Request(req) => {
                        let resp = handle_request(&kv, req).await;
                        Envelope::response_to(&envelope.header, StateMessage::Response(resp))
                    }
                    _ => continue,
                };

                let bytes = {
                    let mut c = codec.lock().await;
                    c.encode(&response, MessageType::State).unwrap().to_vec()
                };

                if stream.write_all(&bytes).await.is_err() {
                    break;
                }
                let _ = stream.flush().await;
            }
        });
    }
}

#[tokio::test]
async fn test_kv_get_put() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let kv = Arc::new(Mutex::new(std::collections::HashMap::new()));

    // Start server in background
    let kv_server = kv.clone();
    tokio::spawn(async move {
        run_test_server(listener, kv_server).await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Connect client
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let codec = Mutex::new(Codec::with_capacity(8192));

    // Test put
    let response = send_request(
        &mut stream,
        &codec,
        StateRequest::KvPut {
            key: "test-key".into(),
            value: b"test-value".to_vec(),
            ttl_secs: None,
        },
    )
    .await;
    assert!(matches!(response, StateResponse::KvSuccess(true)));

    // Test get
    let response = send_request(
        &mut stream,
        &codec,
        StateRequest::KvGet {
            key: "test-key".into(),
        },
    )
    .await;
    assert!(matches!(response, StateResponse::KvValue(Some(v)) if v == b"test-value"));

    // Test get non-existent
    let response = send_request(
        &mut stream,
        &codec,
        StateRequest::KvGet {
            key: "missing".into(),
        },
    )
    .await;
    assert!(matches!(response, StateResponse::KvValue(None)));
}

#[tokio::test]
async fn test_kv_exists_delete() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let kv = Arc::new(Mutex::new(std::collections::HashMap::new()));

    let kv_server = kv.clone();
    tokio::spawn(async move {
        run_test_server(listener, kv_server).await;
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let codec = Mutex::new(Codec::with_capacity(8192));

    // Put a key
    send_request(
        &mut stream,
        &codec,
        StateRequest::KvPut {
            key: "to-delete".into(),
            value: b"data".to_vec(),
            ttl_secs: None,
        },
    )
    .await;

    // Check exists
    let response = send_request(
        &mut stream,
        &codec,
        StateRequest::KvExists {
            key: "to-delete".into(),
        },
    )
    .await;
    assert!(matches!(response, StateResponse::KvSuccess(true)));

    // Delete
    let response = send_request(
        &mut stream,
        &codec,
        StateRequest::KvDelete {
            key: "to-delete".into(),
        },
    )
    .await;
    assert!(matches!(response, StateResponse::KvSuccess(true)));

    // Check not exists
    let response = send_request(
        &mut stream,
        &codec,
        StateRequest::KvExists {
            key: "to-delete".into(),
        },
    )
    .await;
    assert!(matches!(response, StateResponse::KvSuccess(false)));
}

#[tokio::test]
async fn test_kv_list() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let kv = Arc::new(Mutex::new(std::collections::HashMap::new()));

    let kv_server = kv.clone();
    tokio::spawn(async move {
        run_test_server(listener, kv_server).await;
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let codec = Mutex::new(Codec::with_capacity(8192));

    // Put multiple keys
    for i in 0..5 {
        send_request(
            &mut stream,
            &codec,
            StateRequest::KvPut {
                key: format!("prefix:{i}"),
                value: vec![i as u8],
                ttl_secs: None,
            },
        )
        .await;
    }

    send_request(
        &mut stream,
        &codec,
        StateRequest::KvPut {
            key: "other:key".into(),
            value: b"x".to_vec(),
            ttl_secs: None,
        },
    )
    .await;

    // List with prefix
    let response = send_request(
        &mut stream,
        &codec,
        StateRequest::KvList {
            prefix: "prefix:".into(),
            limit: 100,
            cursor: None,
        },
    )
    .await;

    match response {
        StateResponse::KvList { keys, .. } => {
            assert_eq!(keys.len(), 5);
            assert!(keys.iter().all(|k| k.starts_with("prefix:")));
        }
        _ => panic!("Expected KvList response"),
    }
}

#[tokio::test]
async fn test_correlation_id_preserved() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let kv = Arc::new(Mutex::new(std::collections::HashMap::new()));

    let kv_server = kv.clone();
    tokio::spawn(async move {
        run_test_server(listener, kv_server).await;
    });

    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let codec = Mutex::new(Codec::with_capacity(8192));

    // Create request with specific correlation ID
    let envelope = Envelope::new(StateMessage::Request(StateRequest::KvGet {
        key: "test".into(),
    }));
    let original_correlation_id = envelope.header.correlation_id;

    let bytes = {
        let mut c = codec.lock().await;
        c.encode(&envelope, MessageType::State).unwrap().to_vec()
    };

    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();

    let mut header_buf = [0u8; FRAME_HEADER_SIZE];
    stream.read_exact(&mut header_buf).await.unwrap();

    let header = FrameHeader::decode(&header_buf).unwrap();
    let mut payload = vec![0u8; header.payload_len as usize];
    stream.read_exact(&mut payload).await.unwrap();

    let response: Envelope<StateMessage> = Codec::decode(&payload).unwrap();

    // Verify correlation ID is echoed back
    assert_eq!(response.header.correlation_id, original_correlation_id);
}
