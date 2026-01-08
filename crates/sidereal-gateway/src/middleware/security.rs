//! Security middleware for header sanitisation and response headers.

use axum::http::header::{
    HeaderName, HeaderValue, CONTENT_SECURITY_POLICY, STRICT_TRANSPORT_SECURITY,
    X_CONTENT_TYPE_OPTIONS, X_FRAME_OPTIONS,
};
use http::{Request, Response};
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// Headers that should be stripped from client requests.
const STRIP_HEADERS: &[&str] = &[
    "x-forwarded-for",
    "x-forwarded-proto",
    "x-forwarded-host",
    "x-real-ip",
    "forwarded",
    "transfer-encoding",
    "connection",
    "upgrade",
];

/// Tower layer that adds security headers to responses and sanitises request headers.
#[derive(Clone, Default)]
pub struct SecurityLayer;

impl SecurityLayer {
    pub const fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for SecurityLayer {
    type Service = SecurityService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        SecurityService { inner }
    }
}

/// The service wrapper that performs header security.
#[derive(Clone)]
pub struct SecurityService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for SecurityService<S>
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

    fn call(&mut self, mut req: Request<ReqBody>) -> Self::Future {
        // Strip dangerous headers from request
        for header_name in STRIP_HEADERS {
            if let Ok(name) = HeaderName::try_from(*header_name) {
                req.headers_mut().remove(&name);
            }
        }

        let mut inner = self.inner.clone();

        Box::pin(async move {
            let mut response = inner.call(req).await?;

            // Add security headers to response
            let headers = response.headers_mut();

            // Strict-Transport-Security (HSTS)
            headers.insert(
                STRICT_TRANSPORT_SECURITY,
                HeaderValue::from_static("max-age=31536000; includeSubDomains"),
            );

            // Prevent MIME sniffing
            headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));

            // Prevent clickjacking
            headers.insert(X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));

            // Content Security Policy
            headers.insert(
                CONTENT_SECURITY_POLICY,
                HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'"),
            );

            Ok(response)
        })
    }
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

    #[tokio::test]
    async fn adds_security_headers() {
        let layer = SecurityLayer::new();
        let service = layer.layer(tower::service_fn(test_service));

        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let response = service.oneshot(req).await.unwrap();

        assert!(response.headers().contains_key(STRICT_TRANSPORT_SECURITY));
        assert!(response.headers().contains_key(X_CONTENT_TYPE_OPTIONS));
        assert!(response.headers().contains_key(X_FRAME_OPTIONS));
        assert!(response.headers().contains_key(CONTENT_SECURITY_POLICY));
    }

    #[tokio::test]
    async fn strips_dangerous_headers() {
        let layer = SecurityLayer::new();

        // Create a service that checks headers were stripped
        let check_service = tower::service_fn(|req: Request<Body>| async move {
            // Verify X-Forwarded-For was stripped
            assert!(req.headers().get("x-forwarded-for").is_none());
            Ok::<_, std::convert::Infallible>(
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::empty())
                    .unwrap(),
            )
        });

        let service = layer.layer(check_service);

        let req = Request::builder()
            .uri("/test")
            .header("x-forwarded-for", "192.168.1.1")
            .header("x-real-ip", "10.0.0.1")
            .body(Body::empty())
            .unwrap();

        let _response = service.oneshot(req).await.unwrap();
    }
}
