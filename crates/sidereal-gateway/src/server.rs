//! Gateway server implementation.

use axum::{
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use crate::backend::{
    BackendRegistry, CircuitBreakerRegistry, DispatchRequest, LoadBalanceStrategy, LoadBalancer,
};
use crate::config::{GatewayConfig, LoadBalanceStrategyConfig, RoutingConfig};
use crate::error::GatewayError;
use crate::middleware::{
    create_rate_limit_layer, AuthLayer, MetricsLayer, OtelTraceLayer, SecurityLayer,
};
use crate::resolver::{
    validate_function_name, FunctionResolver, SchedulerResolver, StaticResolver,
};

/// Shared gateway state.
pub struct GatewayState {
    resolver: Arc<dyn FunctionResolver>,
    backends: Arc<BackendRegistry>,
    circuit_breakers: Option<Arc<CircuitBreakerRegistry>>,
    load_balancer: LoadBalancer,
}

impl std::fmt::Debug for GatewayState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayState")
            .field("resolver", &self.resolver)
            .field("backends", &self.backends)
            .field("circuit_breakers", &self.circuit_breakers.is_some())
            .field("load_balancer", &self.load_balancer)
            .finish()
    }
}

/// Run the gateway server.
pub async fn run(config: GatewayConfig, cancel: CancellationToken) -> Result<(), GatewayError> {
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
        RoutingConfig::Scheduler(scheduler_config) => {
            tracing::info!(
                valkey_url = %scheduler_config.valkey_url,
                cache_enabled = scheduler_config.enable_cache,
                cache_ttl_secs = scheduler_config.cache_ttl_secs,
                "Using scheduler-based function resolution"
            );
            Arc::new(SchedulerResolver::from_config(scheduler_config).await?)
        }
    };

    let backends = Arc::new(BackendRegistry::new());

    let circuit_breakers = config.middleware.circuit_breaker.as_ref().map(|cb_config| {
        tracing::info!(
            failure_threshold = cb_config.failure_threshold,
            success_threshold = cb_config.success_threshold,
            reset_timeout_ms = cb_config.reset_timeout_ms,
            "Circuit breaker enabled"
        );
        Arc::new(CircuitBreakerRegistry::new(cb_config.clone()))
    });

    let load_balance_strategy = match &config.routing {
        RoutingConfig::Static { load_balance, .. } => match load_balance {
            LoadBalanceStrategyConfig::RoundRobin => LoadBalanceStrategy::RoundRobin,
            LoadBalanceStrategyConfig::Random => LoadBalanceStrategy::Random,
        },
        RoutingConfig::Discovery { .. } => LoadBalanceStrategy::RoundRobin,
        RoutingConfig::Scheduler(scheduler_config) => match scheduler_config.load_balance {
            LoadBalanceStrategyConfig::RoundRobin => LoadBalanceStrategy::RoundRobin,
            LoadBalanceStrategyConfig::Random => LoadBalanceStrategy::Random,
        },
    };
    let load_balancer = LoadBalancer::new(load_balance_strategy);
    tracing::info!(strategy = ?load_balance_strategy, "Load balancer configured");

    let state = Arc::new(GatewayState {
        resolver,
        backends,
        circuit_breakers,
        load_balancer,
    });

    let _metrics_handle = if let Some(ref metrics_config) = config.metrics {
        let (handle, _task) = crate::middleware::metrics::spawn_metrics_server(
            metrics_config.clone(),
            cancel.clone(),
        )?;
        tracing::info!(
            address = %metrics_config.bind_address,
            path = %metrics_config.path,
            "Metrics server enabled"
        );
        Some(handle)
    } else {
        None
    };

    let mut router = Router::new()
        .route("/health", get(health_check))
        .route("/ready", get(readiness_check))
        .route("/{function}", post(handle_function))
        .route("/{function}/{*path}", post(handle_function_with_path))
        .layer(MetricsLayer::new())
        .layer(OtelTraceLayer::new())
        .layer(SecurityLayer::new())
        .with_state(state);

    if let Some(ref api_config) = config.api {
        use crate::proxy::{
            proxy_build, proxy_build_root, proxy_callbacks, proxy_control, proxy_control_root,
            proxy_keys, ProxyState,
        };
        use axum::routing::any;

        let proxy_state = Arc::new(ProxyState {
            build_socket: api_config.build_socket.clone(),
            control_socket: api_config.control_socket.clone(),
        });

        let api_router = Router::new()
            .route("/api/builds", any(proxy_build_root))
            .route("/api/builds/{*path}", any(proxy_build))
            .route("/api/deployments", any(proxy_control_root))
            .route("/api/deployments/{*path}", any(proxy_control))
            .route("/api/callbacks/{*path}", any(proxy_callbacks))
            .route("/api/keys/{*path}", any(proxy_keys))
            .with_state(proxy_state);

        router = api_router.merge(router);

        tracing::info!(
            build_socket = %api_config.build_socket.display(),
            control_socket = %api_config.control_socket.display(),
            "API proxy enabled"
        );
    }

    if let Some(ref auth_config) = config.middleware.auth {
        tracing::info!(algorithm = ?auth_config.algorithm, "JWT authentication enabled");
        router = router.layer(AuthLayer::new(auth_config));
    }

    let app = if let Some(ref rate_limit_config) = config.middleware.rate_limit {
        tracing::info!(
            requests_per_second = rate_limit_config.requests_per_second,
            burst_size = rate_limit_config.burst_size,
            "Rate limiting enabled"
        );
        router.layer(create_rate_limit_layer(rate_limit_config)?)
    } else {
        router
    };

    let addr = config.server.bind_address;

    if let Some(ref tls_config) = config.server.tls {
        let rustls_config =
            RustlsConfig::from_pem_file(&tls_config.cert_path, &tls_config.key_path)
                .await
                .map_err(|e| GatewayError::Config(format!("TLS configuration error: {e}")))?;

        tracing::info!(
            address = %addr,
            cert = %tls_config.cert_path.display(),
            "Gateway listening (TLS enabled)"
        );

        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();

        tokio::spawn(async move {
            cancel.cancelled().await;
            shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(30)));
        });

        axum_server::bind_rustls(addr, rustls_config)
            .handle(handle)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .map_err(GatewayError::Io)?;
    } else {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(GatewayError::Io)?;

        tracing::info!(address = %addr, "Gateway listening");

        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
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
    validate_function_name(function_name)?;

    let info = state
        .resolver
        .resolve(function_name)
        .await?
        .ok_or_else(|| GatewayError::FunctionNotFound(function_name.to_owned()))?;

    let selected_address = state
        .load_balancer
        .select(&info.backend_addresses)
        .ok_or_else(|| {
            GatewayError::BackendError(format!(
                "No backends available for function '{function_name}'"
            ))
        })?;

    let backend_key = selected_address.to_string();
    opentelemetry_configuration::tracing::Span::current()
        .record("gateway.backend.address", &backend_key);

    let circuit_breaker = state
        .circuit_breakers
        .as_ref()
        .map(|registry| registry.get_or_create(&backend_key));

    if let Some(ref cb) = circuit_breaker {
        cb.allow_request().await?;
    }

    let backend = state.backends.get_backend(selected_address)?;

    let trace_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map_or_else(
            || uuid::Uuid::new_v4().to_string(),
            std::borrow::ToOwned::to_owned,
        );

    let req = DispatchRequest {
        function_name: function_name.to_owned(),
        payload,
        trace_id,
        headers,
    };

    let response = match backend.dispatch(req).await {
        Ok(resp) => {
            if let Some(cb) = circuit_breaker {
                cb.record_success().await;
            }
            resp
        }
        Err(e) => {
            if let Some(cb) = circuit_breaker {
                if is_backend_failure(&e) {
                    cb.record_failure().await;
                }
            }
            return Err(e);
        }
    };

    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);

    Ok((status, response.body))
}

/// Check if an error represents a backend failure that should trip the circuit breaker.
const fn is_backend_failure(error: &GatewayError) -> bool {
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
    use crate::config::{BackendAddress, FunctionBackendConfig};
    use std::collections::HashMap;

    fn test_config() -> GatewayConfig {
        let mut functions = HashMap::new();
        functions.insert(
            "hello".to_string(),
            FunctionBackendConfig {
                addresses: vec![BackendAddress::Http {
                    url: "http://127.0.0.1:7850".to_string(),
                }],
                load_balance: None,
            },
        );

        GatewayConfig {
            server: Default::default(),
            routing: RoutingConfig::Static {
                functions,
                load_balance: Default::default(),
            },
            middleware: Default::default(),
            limits: Default::default(),
            metrics: None,
            api: None,
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
