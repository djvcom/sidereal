//! Gateway server implementation.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::backend::{BackendRegistry, DispatchRequest};
use crate::config::{GatewayConfig, RoutingConfig};
use crate::error::GatewayError;
use crate::middleware::{OtelTraceLayer, SecurityLayer};
use crate::resolver::{validate_function_name, FunctionResolver, StaticResolver};

/// Shared gateway state.
pub struct GatewayState {
    resolver: Arc<dyn FunctionResolver>,
    backends: Arc<BackendRegistry>,
}

impl std::fmt::Debug for GatewayState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayState")
            .field("resolver", &self.resolver)
            .field("backends", &self.backends)
            .finish()
    }
}

/// Run the gateway server.
pub async fn run(config: GatewayConfig, cancel: CancellationToken) -> Result<(), GatewayError> {
    // Create resolver
    let resolver: Arc<dyn FunctionResolver> = match &config.routing {
        RoutingConfig::Static { .. } => Arc::new(
            StaticResolver::from_config(&config.routing)
                .map_err(|e| GatewayError::Config(e.to_string()))?,
        ),
        RoutingConfig::Discovery { .. } => {
            return Err(GatewayError::Config(
                "Discovery routing not yet implemented".into(),
            ));
        }
    };

    // Create backend registry
    let backends = Arc::new(BackendRegistry::new());

    // Create state
    let state = Arc::new(GatewayState { resolver, backends });

    // Build router
    let app = Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/{function}", post(handle_function))
        .route("/{function}/*path", post(handle_function_with_path))
        .layer(OtelTraceLayer::new())
        .layer(SecurityLayer::new())
        .with_state(state);

    // Bind and serve
    let addr = config.server.bind_address;
    tracing::info!(address = %addr, "Gateway listening");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(GatewayError::Io)?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await
        .map_err(GatewayError::Io)?;

    tracing::info!("Gateway shutdown complete");
    Ok(())
}

/// Health check endpoint.
async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "healthy"
    }))
}

/// Readiness check endpoint.
async fn readiness_check(State(state): State<Arc<GatewayState>>) -> impl IntoResponse {
    match state.resolver.list_functions().await {
        Ok(functions) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "ready",
                "functions": functions.len()
            })),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({
                "status": "not_ready",
                "error": e.to_string()
            })),
        ),
    }
}

/// Handle function invocation.
async fn handle_function(
    State(state): State<Arc<GatewayState>>,
    Path(function_name): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, GatewayError> {
    dispatch_to_function(state, &function_name, headers, body.to_vec()).await
}

/// Handle function invocation with sub-path.
async fn handle_function_with_path(
    State(state): State<Arc<GatewayState>>,
    Path((function_name, _path)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, GatewayError> {
    dispatch_to_function(state, &function_name, headers, body.to_vec()).await
}

/// Dispatch a request to a function.
async fn dispatch_to_function(
    state: Arc<GatewayState>,
    function_name: &str,
    headers: HeaderMap,
    payload: Vec<u8>,
) -> Result<impl IntoResponse, GatewayError> {
    // Validate function name
    validate_function_name(function_name)?;

    // Resolve the function
    let info = state
        .resolver
        .resolve(function_name)
        .await?
        .ok_or_else(|| GatewayError::FunctionNotFound(function_name.to_string()))?;

    // Record backend address in span
    opentelemetry_configuration::tracing::Span::current()
        .record("gateway.backend.address", info.backend_address.to_string());

    // Get backend
    let backend = state.backends.get_backend(&info.backend_address)?;

    // Extract or generate trace ID
    let trace_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    // Dispatch request
    let req = DispatchRequest {
        function_name: function_name.to_string(),
        payload,
        trace_id,
        headers,
    };

    let response = backend.dispatch(req).await?;

    // Build response
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    Ok((status, response.body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackendAddress;
    use std::collections::HashMap;

    fn test_config() -> GatewayConfig {
        let mut functions = HashMap::new();
        functions.insert(
            "hello".to_string(),
            BackendAddress::Http {
                url: "http://127.0.0.1:7850".to_string(),
            },
        );

        GatewayConfig {
            server: Default::default(),
            routing: RoutingConfig::Static { functions },
            middleware: Default::default(),
            limits: Default::default(),
        }
    }

    #[tokio::test]
    async fn resolver_creation() {
        let config = test_config();
        let resolver = StaticResolver::from_config(&config.routing).unwrap();
        let info = resolver.resolve("hello").await.unwrap();
        assert!(info.is_some());
    }
}
