//! HTTP proxy for forwarding requests to internal services via Unix sockets.

use axum::{
    body::Body,
    extract::{Path, Request, State},
    http::header,
    response::Response,
};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::UnixStream;

use crate::error::GatewayError;

/// State for the proxy handlers.
#[derive(Clone)]
pub struct ProxyState {
    /// Path to the build service Unix socket.
    pub build_socket: PathBuf,
    /// Path to the control service Unix socket.
    pub control_socket: PathBuf,
}

/// Proxy a request to the build service.
pub async fn proxy_build(
    State(state): State<Arc<ProxyState>>,
    Path(path): Path<String>,
    request: Request,
) -> Result<Response, GatewayError> {
    proxy_to_socket(&state.build_socket, &format!("/builds/{path}"), request).await
}

/// Proxy a request to the build service root.
pub async fn proxy_build_root(
    State(state): State<Arc<ProxyState>>,
    request: Request,
) -> Result<Response, GatewayError> {
    let uri_path = request.uri().path().to_owned();
    let suffix = uri_path.strip_prefix("/api/builds").unwrap_or(&uri_path);
    let path = format!("/builds{suffix}");
    proxy_to_socket(&state.build_socket, &path, request).await
}

/// Proxy a request to the control service.
pub async fn proxy_control(
    State(state): State<Arc<ProxyState>>,
    Path(path): Path<String>,
    request: Request,
) -> Result<Response, GatewayError> {
    proxy_to_socket(
        &state.control_socket,
        &format!("/deployments/{path}"),
        request,
    )
    .await
}

/// Proxy a request to the control service root.
pub async fn proxy_control_root(
    State(state): State<Arc<ProxyState>>,
    request: Request,
) -> Result<Response, GatewayError> {
    let uri_path = request.uri().path().to_owned();
    let path = uri_path.strip_prefix("/api").unwrap_or(&uri_path);
    proxy_to_socket(&state.control_socket, path, request).await
}

/// Forward an HTTP request to a Unix socket.
async fn proxy_to_socket(
    socket_path: &std::path::Path,
    path: &str,
    request: Request,
) -> Result<Response, GatewayError> {
    let stream = UnixStream::connect(socket_path).await.map_err(|e| {
        GatewayError::BackendError(format!(
            "Failed to connect to {}: {e}",
            socket_path.display()
        ))
    })?;

    let io = TokioIo::new(stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .map_err(|e| GatewayError::BackendError(format!("Handshake failed: {e}")))?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::error!(error = %e, "Connection error");
        }
    });

    let (parts, body) = request.into_parts();

    let query = parts
        .uri
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let uri = format!("{path}{query}");

    let mut builder = hyper::Request::builder().method(parts.method).uri(&uri);

    for (key, value) in &parts.headers {
        if key != header::HOST {
            builder = builder.header(key, value);
        }
    }
    builder = builder.header(header::HOST, "localhost");

    let hyper_request = builder
        .body(body)
        .map_err(|e| GatewayError::BackendError(format!("Failed to build request: {e}")))?;

    let response = sender
        .send_request(hyper_request)
        .await
        .map_err(|e| GatewayError::BackendError(format!("Request failed: {e}")))?;

    Ok(response_to_axum(response))
}

/// Convert a hyper response to an axum response.
fn response_to_axum(response: hyper::Response<Incoming>) -> Response {
    let (parts, body) = response.into_parts();
    Response::from_parts(parts, Body::new(body))
}
