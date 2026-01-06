//! Development server for running Sidereal functions and services locally.
//!
//! This module provides the HTTP server that routes requests to registered functions
//! and manages the lifecycle of background and router services.

use crate::config::{ConfigManager, SiderealConfig};
use crate::extractors::AppState;
use crate::registry::{get_http_functions, get_queue_functions, FunctionMetadata, FunctionResult};
use crate::service_registry::{
    get_background_services, get_router_services, ServiceFactory, ServiceMetadata,
};
use crate::tracing_layer::OtelTraceLayer;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    Router,
};
use sidereal_state::{StateConfig, StateProvider};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Configuration for the development server.
#[derive(Clone)]
pub struct ServerConfig {
    /// The port to listen on.
    pub port: u16,
    /// The host to bind to.
    pub host: String,
    /// Timeout for graceful shutdown of background services.
    pub shutdown_timeout: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: 7850,
            host: "127.0.0.1".to_string(),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

impl ServerConfig {
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }
}

/// Run the development server.
///
/// This discovers all registered functions and services, then:
/// - Spawns background services as tokio tasks
/// - Mounts router services at their path prefixes
/// - Serves HTTP functions via POST /{function_name}
/// - Handles graceful shutdown on Ctrl+C
///
/// # Example
///
/// ```no_run
/// // In your main.rs
/// #[tokio::main]
/// async fn main() {
///     sidereal_sdk::run(sidereal_sdk::ServerConfig::default()).await;
/// }
/// ```
pub async fn run(config: ServerConfig) {
    // Create a cancellation token for graceful shutdown
    let cancel_token = CancellationToken::new();
    let cancel_for_signal = cancel_token.clone();

    // Discover all registered functions and services
    let http_functions: Vec<_> = get_http_functions().collect();
    let queue_functions: Vec<_> = get_queue_functions().collect();
    let background_services: Vec<_> = get_background_services().collect();
    let router_services: Vec<_> = get_router_services().collect();

    let has_functions = !http_functions.is_empty() || !queue_functions.is_empty();
    let has_services = !background_services.is_empty() || !router_services.is_empty();

    if !has_functions && !has_services {
        eprintln!("Warning: No functions or services registered.");
        eprintln!("  Use #[sidereal_sdk::function] for functions");
        eprintln!("  Use #[sidereal_sdk::service] for services");
    }

    // Load configuration
    let config_manager = match ConfigManager::load() {
        Ok(cm) => {
            eprintln!("Loaded configuration (env: {})", cm.active_environment());
            Some(cm)
        }
        Err(e) => {
            eprintln!("Warning: Could not load configuration: {}", e);
            eprintln!("  Continuing without config support");
            None
        }
    };

    // Validate queue configuration if config is loaded
    if config_manager.is_some() {
        let sidereal_config = SiderealConfig::load();
        if let Some(ref cfg) = sidereal_config {
            validate_queue_configuration(cfg, &queue_functions);
        }
    }

    // Initialise state provider from configuration
    let state_config = config_manager
        .as_ref()
        .and_then(|cm| cm.section::<StateConfig>("state").ok())
        .unwrap_or_default();

    let state_provider = match StateProvider::from_config(&state_config).await {
        Ok(sp) => {
            eprintln!("State provider initialised");
            sp
        }
        Err(e) => {
            eprintln!("Warning: Could not initialise state provider: {}", e);
            eprintln!("  State extractors (Kv, Queue, Lock) will not be available");
            StateProvider::default()
        }
    };

    // Print startup info
    print_startup_info(
        &http_functions,
        &queue_functions,
        &background_services,
        &router_services,
    );

    // Build shared state
    let state = Arc::new(AppState::new(config_manager.clone(), state_provider));

    // Spawn background services
    let mut background_tasks = JoinSet::new();
    for service in &background_services {
        let state_clone = state.clone();
        let cancel = cancel_token.clone();

        if let ServiceFactory::Background(factory) = service.factory {
            let name = service.name.to_string();
            background_tasks.spawn(async move {
                eprintln!("Starting background service: {}", name);
                let result = factory(state_clone, cancel).await;
                match &result {
                    Ok(()) => eprintln!("Background service '{}' completed", name),
                    Err(e) => eprintln!("Background service '{}' failed: {}", name, e),
                }
                (name, result)
            });
        }
    }

    // Build the router with function routes
    let mut app = Router::new()
        .route("/{function}", post(handle_function))
        .layer(OtelTraceLayer::new())
        .with_state(state);

    // Mount router services
    for service in &router_services {
        if let ServiceFactory::Router(factory) = service.factory {
            let router = factory();
            let prefix = service.path_prefix.unwrap_or("/");
            eprintln!("Mounting router service '{}' at {}", service.name, prefix);
            app = app.nest(prefix, router);
        }
    }

    // Setup graceful shutdown
    let shutdown_signal = async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C signal handler");
        eprintln!("\nShutdown signal received, stopping services...");
        cancel_for_signal.cancel();
    };

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .expect("Invalid address");

    println!("Listening on http://{}", addr);
    println!();

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("Failed to bind to address");

    // Run server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal)
        .await
        .expect("Server error");

    // Wait for background services to finish (with timeout)
    if !background_tasks.is_empty() {
        eprintln!("Waiting for background services to complete...");
        let shutdown_deadline = tokio::time::Instant::now() + config.shutdown_timeout;

        while let Some(result) =
            tokio::time::timeout_at(shutdown_deadline, background_tasks.join_next())
                .await
                .ok()
                .flatten()
        {
            match result {
                Ok((name, Ok(()))) => eprintln!("Service '{}' shut down cleanly", name),
                Ok((name, Err(e))) => eprintln!("Service '{}' error during shutdown: {}", name, e),
                Err(e) => eprintln!("Service task panicked: {}", e),
            }
        }

        if !background_tasks.is_empty() {
            eprintln!(
                "Warning: {} background services did not shut down in time",
                background_tasks.len()
            );
            background_tasks.abort_all();
        }
    }

    eprintln!("Shutdown complete");
}

fn print_startup_info(
    http_functions: &[&FunctionMetadata],
    queue_functions: &[&FunctionMetadata],
    background_services: &[&ServiceMetadata],
    router_services: &[&ServiceMetadata],
) {
    println!("Sidereal development server starting...");
    println!();

    if !http_functions.is_empty() {
        println!("HTTP functions:");
        for func in http_functions {
            println!("  POST /{}", func.name);
        }
        println!();
    }

    if !queue_functions.is_empty() {
        println!("Queue functions:");
        for func in queue_functions {
            println!("  {} (queue consumer)", func.name);
        }
        println!();
    }

    if !background_services.is_empty() {
        println!("Background services:");
        for svc in background_services {
            println!("  {} (spawned task)", svc.name);
        }
        println!();
    }

    if !router_services.is_empty() {
        println!("Router services:");
        for svc in router_services {
            let path = svc.path_prefix.unwrap_or("/<name>");
            println!("  {}/* -> {}", path, svc.name);
        }
        println!();
    }
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

    // Call the legacy handler (eventually this will be replaced with axum handler mounting)
    let result: FunctionResult = (func.handler)(&body, ()).await;

    // Return the response
    let status = StatusCode::from_u16(result.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    (status, [("content-type", "application/json")], result.body).into_response()
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
