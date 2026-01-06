//! Gateway server implementation.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::backend::{BackendRegistry, CircuitBreakerRegistry, DispatchRequest};
use crate::config::{GatewayConfig, RoutingConfig};
use crate::error::GatewayError;
use crate::middleware::{create_rate_limit_layer, OtelTraceLayer, SecurityLayer};
use crate::resolver::{validate_function_name, FunctionResolver, StaticResolver};

/// Shared gateway state.
pub struct GatewayState {
    resolver: Arc<dyn FunctionResolver>,
    backends: Arc<BackendRegistry>,
    circuit_breakers: Option<Arc<CircuitBreakerRegistry>>,
}

impl std::fmt::Debug for GatewayState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayState")
            .field("resolver", &self.resolver)
            .field("backends", &self.backends)
            .field("circuit_breakers", &self.circuit_breakers.is_some())
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

    // Create circuit breaker registry if configured
    let circuit_breakers = config.middleware.circuit_breaker.as_ref().map(|cb_config| {
        tracing::info!(
            failure_threshold = cb_config.failure_threshold,
            success_threshold = cb_config.success_threshold,
            reset_timeout_ms = cb_config.reset_timeout_ms,
            "Circuit breaker enabled"
        );
        Arc::new(CircuitBreakerRegistry::new(cb_config.clone()))
    });

    // Create state
    let state = Arc::new(GatewayState {
        resolver,
        backends,
        circuit_breakers,
    });

    // Build base router
    let base_router = Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/{function}", post(handle_function))
        .route("/{function}/*path", post(handle_function_with_path))
        .layer(OtelTraceLayer::new())
        .layer(SecurityLayer::new())
        .with_state(state);

    // Bind and serve
    let addr = config.server.bind_address;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(GatewayError::Io)?;

    // Apply rate limiting if configured, then serve
    // Note: into_make_service_with_connect_info is required for IP-based rate limiting
    if let Some(rate_limit_config) = &config.middleware.rate_limit {
        tracing::info!(
            requests_per_second = rate_limit_config.requests_per_second,
            burst_size = rate_limit_config.burst_size,
            "Rate limiting enabled"
        );
        let app = base_router.layer(create_rate_limit_layer(rate_limit_config));
        tracing::info!(address = %addr, "Gateway listening");
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await
        .map_err(GatewayError::Io)?;
    } else {
        tracing::info!(address = %addr, "Gateway listening (rate limiting disabled)");
        axum::serve(
            listener,
            base_router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await
        .map_err(GatewayError::Io)?;
    }

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
    let backend_key = info.backend_address.to_string();
    opentelemetry_configuration::tracing::Span::current()
        .record("gateway.backend.address", &backend_key);

    // Check circuit breaker if enabled
    let circuit_breaker = state
        .circuit_breakers
        .as_ref()
        .map(|registry| registry.get_or_create(&backend_key));

    if let Some(ref cb) = circuit_breaker {
        cb.allow_request().await?;
    }

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

    let response = match backend.dispatch(req).await {
        Ok(resp) => {
            // Record success
            if let Some(cb) = circuit_breaker {
                cb.record_success().await;
            }
            resp
        }
        Err(e) => {
            // Record failure (only for connection/backend errors, not validation errors)
            if let Some(cb) = circuit_breaker {
                if is_backend_failure(&e) {
                    cb.record_failure().await;
                }
            }
            return Err(e);
        }
    };

    // Build response
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    Ok((status, response.body))
}

/// Check if an error represents a backend failure that should trip the circuit breaker.
fn is_backend_failure(error: &GatewayError) -> bool {
    matches!(
        error,
        GatewayError::ConnectionFailed(_)
            | GatewayError::Timeout
            | GatewayError::BackendError(_)
            | GatewayError::VsockError(_)
    )
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
