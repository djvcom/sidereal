//! OpenTelemetry tracing layer for Sidereal functions.
//!
//! This module provides a Tower middleware layer that extracts trace context
//! from incoming requests and creates spans that are linked to the parent context.

use http::{Request, Response};
use opentelemetry_configuration::opentelemetry::propagation::TextMapPropagator;
use opentelemetry_configuration::opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_configuration::tracing::Instrument;
use opentelemetry_configuration::tracing_opentelemetry::OpenTelemetrySpanExt;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// Tower layer that extracts OpenTelemetry trace context from incoming requests.
///
/// This layer creates a span for each request and links it to any incoming
/// trace context from the `traceparent` and `tracestate` headers.
#[derive(Clone)]
pub struct OtelTraceLayer {
    propagator: TraceContextPropagator,
}

impl OtelTraceLayer {
    /// Create a new tracing layer with the default W3C trace context propagator.
    pub fn new() -> Self {
        Self {
            propagator: TraceContextPropagator::new(),
        }
    }
}

impl Default for OtelTraceLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for OtelTraceLayer {
    type Service = OtelTraceService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        OtelTraceService {
            inner,
            propagator: self.propagator.clone(),
        }
    }
}

/// The service wrapper that performs trace context extraction.
#[derive(Clone)]
pub struct OtelTraceService<S> {
    inner: S,
    propagator: TraceContextPropagator,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for OtelTraceService<S>
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
        // Extract parent context from headers
        let parent_context = self.propagator.extract(&HeaderExtractor(req.headers()));

        // Extract route/function name from URI
        let path = req.uri().path();
        let function_name = path
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("unknown");

        // Create a span for this request
        let span = opentelemetry_configuration::tracing::info_span!(
            "function",
            otel.name = %function_name,
            http.method = %req.method(),
            http.route = %path,
            http.status_code = tracing::field::Empty,
        );

        // Link span to parent context if present
        let _ = span.set_parent(parent_context);

        // Clone inner service for the async block
        let mut inner = self.inner.clone();

        Box::pin(
            async move {
                let response = inner.call(req).await;

                // Record status code on the span
                if let Ok(ref resp) = response {
                    opentelemetry_configuration::tracing::Span::current()
                        .record("http.status_code", resp.status().as_u16());
                }

                response
            }
            .instrument(span),
        )
    }
}

/// Extractor for trace context from HTTP headers.
struct HeaderExtractor<'a>(&'a http::HeaderMap);

impl opentelemetry_configuration::opentelemetry::propagation::Extractor for HeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(http::HeaderName::as_str).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layer_creation() {
        let _layer = OtelTraceLayer::new();
    }
}
