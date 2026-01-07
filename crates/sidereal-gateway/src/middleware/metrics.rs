//! Prometheus metrics middleware and server.

use axum::{routing::get, Router};
use http::{Request, Response};
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use tower::{Layer, Service};

use crate::config::MetricsConfig;
use crate::error::GatewayError;

/// Tower layer that records request metrics.
#[derive(Clone)]
pub struct MetricsLayer;

impl MetricsLayer {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MetricsLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for MetricsLayer {
    type Service = MetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsService { inner }
    }
}

/// The service wrapper that records metrics.
#[derive(Clone)]
pub struct MetricsService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for MetricsService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send,
    ReqBody: Send + 'static,
    ResBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let method = req.method().to_string();
        let path = req.uri().path().to_string();

        // Extract function name from path (first segment after /)
        let function = extract_function_name(&path);

        // Record active requests
        gauge!("platform.gateway.active_requests").increment(1.0);

        let start = Instant::now();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let result = inner.call(req).await;

            let duration = start.elapsed();
            let duration_secs = duration.as_secs_f64();

            // Decrement active requests
            gauge!("platform.gateway.active_requests").decrement(1.0);

            match &result {
                Ok(response) => {
                    let status = response.status().as_u16().to_string();

                    // Record request count
                    counter!(
                        "platform.gateway.requests",
                        "function" => function.clone(),
                        "method" => method.clone(),
                        "status" => status.clone()
                    )
                    .increment(1);

                    // Record request duration
                    histogram!(
                        "platform.gateway.request.duration",
                        "function" => function.clone(),
                        "method" => method
                    )
                    .record(duration_secs);

                    // Record errors (4xx and 5xx)
                    if response.status().is_client_error() || response.status().is_server_error() {
                        let error_type = if response.status().is_client_error() {
                            "client_error"
                        } else {
                            "server_error"
                        };
                        counter!(
                            "platform.gateway.errors",
                            "function" => function,
                            "error_type" => error_type
                        )
                        .increment(1);
                    }
                }
                Err(_) => {
                    // Record internal error
                    counter!(
                        "platform.gateway.errors",
                        "function" => function,
                        "error_type" => "internal"
                    )
                    .increment(1);
                }
            }

            result
        })
    }
}

fn extract_function_name(path: &str) -> String {
    path.trim_start_matches('/')
        .split('/')
        .next()
        .filter(|s| !s.is_empty() && *s != "health" && *s != "ready")
        .unwrap_or("unknown")
        .to_string()
}

/// Initialise the Prometheus metrics recorder.
pub fn init_metrics_recorder() -> PrometheusHandle {
    PrometheusBuilder::new()
        .install_recorder()
        .expect("Failed to install Prometheus recorder")
}

/// Run the metrics server on a separate address.
pub async fn run_metrics_server(
    config: &MetricsConfig,
    handle: PrometheusHandle,
    cancel: CancellationToken,
) -> Result<(), GatewayError> {
    let metrics_handle = Arc::new(handle);
    let path = config.path.clone();

    let app = Router::new().route(
        &path,
        get({
            let handle = metrics_handle.clone();
            move || {
                let handle = handle.clone();
                async move { handle.render() }
            }
        }),
    );

    let addr = config.bind_address;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(GatewayError::Io)?;

    tracing::info!(address = %addr, path = %path, "Metrics server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await
        .map_err(GatewayError::Io)?;

    Ok(())
}

/// Metrics server handle for managing the background server.
pub struct MetricsServer {
    handle: PrometheusHandle,
}

impl MetricsServer {
    pub fn new() -> Self {
        Self {
            handle: init_metrics_recorder(),
        }
    }

    pub fn handle(&self) -> PrometheusHandle {
        self.handle.clone()
    }

    pub async fn run(
        self,
        config: &MetricsConfig,
        cancel: CancellationToken,
    ) -> Result<(), GatewayError> {
        run_metrics_server(config, self.handle, cancel).await
    }
}

impl Default for MetricsServer {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the metrics server in a background task.
pub fn spawn_metrics_server(
    config: MetricsConfig,
    cancel: CancellationToken,
) -> (
    PrometheusHandle,
    tokio::task::JoinHandle<Result<(), GatewayError>>,
) {
    let server = MetricsServer::new();
    let handle = server.handle();

    let task = tokio::spawn(async move { server.run(&config, cancel).await });

    (handle, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use http::StatusCode;
    use tower::ServiceExt;

    async fn test_service(_req: Request<Body>) -> Result<Response<Body>, std::convert::Infallible> {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(Body::empty())
            .unwrap())
    }

    #[test]
    fn extract_function_from_path() {
        assert_eq!(extract_function_name("/hello"), "hello");
        assert_eq!(extract_function_name("/hello/world"), "hello");
        assert_eq!(extract_function_name("/process_data"), "process_data");
        assert_eq!(extract_function_name("/health"), "unknown");
        assert_eq!(extract_function_name("/ready"), "unknown");
        assert_eq!(extract_function_name("/"), "unknown");
    }

    #[tokio::test]
    async fn metrics_layer_passes_through() {
        let layer = MetricsLayer::new();
        let service = layer.layer(tower::service_fn(test_service));

        let req = Request::builder()
            .uri("/test_function")
            .body(Body::empty())
            .unwrap();

        let response = service.oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
