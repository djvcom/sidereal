//! Development server for running Sidereal functions locally.
//!
//! This module provides the HTTP server that routes requests to registered functions.

use crate::config::SiderealConfig;
use crate::context::Context;
use crate::registry::{get_http_functions, get_queue_functions, FunctionResult};
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use std::collections::HashSet;
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
    // Discover all registered functions
    let http_functions: Vec<_> = get_http_functions().collect();
    let queue_functions: Vec<_> = get_queue_functions().collect();

    if http_functions.is_empty() && queue_functions.is_empty() {
        eprintln!("Warning: No functions registered. Make sure your functions are annotated with #[sidereal_sdk::function]");
    }

    // Load and validate configuration
    let sidereal_config = SiderealConfig::load();
    if let Some(ref cfg) = sidereal_config {
        validate_queue_configuration(cfg, &queue_functions);
    }

    println!("Sidereal development server starting...");
    println!();

    if !http_functions.is_empty() {
        println!("HTTP functions:");
        for func in &http_functions {
            println!("  POST /{}", func.name);
        }
        println!();
    }

    if !queue_functions.is_empty() {
        println!("Queue functions:");
        for func in &queue_functions {
            println!("  {} (queue consumer)", func.name);
        }
        println!();
    }

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

/// Validate that queue functions match declared queue resources.
fn validate_queue_configuration(
    config: &SiderealConfig,
    queue_functions: &[&crate::registry::FunctionMetadata],
) {
    let declared_queues: HashSet<&str> = config.declared_queues().into_iter().collect();

    // Collect queue names from function metadata
    let mut consumer_queues: HashSet<&str> = HashSet::new();

    for func in queue_functions {
        if let Some(queue_name) = func.queue_name {
            consumer_queues.insert(queue_name);
        }
    }

    // Warn about queue consumers without declared queues
    for queue_name in &consumer_queues {
        if !declared_queues.contains(queue_name) {
            eprintln!(
                "Warning: Queue consumer for '{}' has no matching queue in sidereal.toml",
                queue_name
            );
            eprintln!(
                "  Add [resources.queue.{}] to your sidereal.toml",
                queue_name
            );
        }
    }

    // Warn about declared queues without consumers
    for queue_name in &declared_queues {
        if !consumer_queues.contains(queue_name) {
            eprintln!(
                "Warning: Queue '{}' is declared but has no consumer function",
                queue_name
            );
        }
    }
}
