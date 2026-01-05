//! Development server for running Sidereal functions locally.
//!
//! This module provides the HTTP server that routes requests to registered functions.

use crate::context::Context;
use crate::registry::{get_http_functions, FunctionResult};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use std::net::SocketAddr;
use std::sync::Arc;

/// Configuration for the development server.
#[derive(Clone)]
pub struct ServerConfig {
    /// The port to listen on.
    pub port: u16,
    /// The host to bind to.
    pub host: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 7850,
            host: "127.0.0.1".to_string(),
        }
    }
}

impl ServerConfig {
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }
}

struct AppState {}

/// Run the development server.
///
/// This discovers all registered functions and serves them via HTTP.
///
/// # Example
///
/// ```ignore
/// // In your main.rs
/// #[tokio::main]
/// async fn main() {
///     sidereal_sdk::run(sidereal_sdk::ServerConfig::default()).await;
/// }
/// ```
pub async fn run(config: ServerConfig) {
    // Discover all registered HTTP functions
    let functions: Vec<_> = get_http_functions().collect();

    if functions.is_empty() {
        eprintln!("Warning: No functions registered. Make sure your functions are annotated with #[sidereal_sdk::function]");
    }

    println!("Sidereal development server starting...");
    println!();
    println!("Functions available:");
    for func in &functions {
        println!("  POST /{}", func.name);
    }
    println!();

    // Build the router
    let app = Router::new()
        .route("/{function}", post(handle_function))
        .with_state(Arc::new(AppState {}));

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("Invalid address");

    println!("Listening on http://{}", addr);
    println!();

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind to address");

    axum::serve(listener, app)
        .await
        .expect("Server error");
}

async fn handle_function(
    State(_state): State<Arc<AppState>>,
    Path(function_name): Path<String>,
    body: Bytes,
) -> impl IntoResponse {
    // Find the function
    let func = match crate::registry::find_function(&function_name) {
        Some(f) => f,
        None => {
            return (
                StatusCode::NOT_FOUND,
                format!("Function '{}' not found", function_name),
            )
                .into_response();
        }
    };

    // Create a context for this invocation
    let ctx = Context::new_dev(&function_name);

    // Call the handler
    let result: FunctionResult = (func.handler)(&body, ctx).await;

    // Return the response
    let status = StatusCode::from_u16(result.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    (
        status,
        [("content-type", "application/json")],
        result.body,
    )
        .into_response()
}
