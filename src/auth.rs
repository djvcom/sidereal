//! Authentication middleware for HTTP and gRPC servers.
//!
//! When an API key is configured, all data endpoints require a valid
//! `Authorization: Bearer <key>` or `X-API-Key: <key>` header. Health
//! and readiness probes remain unauthenticated.

use std::fmt;

use axum::{
    extract::{Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// Authentication configuration.
///
/// When an API key is set, all data endpoints require this key as a
/// `Bearer` token or `X-API-Key` header value. Health and readiness
/// endpoints remain open regardless of this setting.
#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    api_key: Option<String>,
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthConfig")
            .field(
                "api_key",
                if self.api_key.is_some() {
                    &"[REDACTED]"
                } else {
                    &"None"
                },
            )
            .finish()
    }
}

impl AuthConfig {
    /// Whether authentication is enabled.
    pub const fn is_enabled(&self) -> bool {
        self.api_key.is_some()
    }

    /// Return the configured API key, if any.
    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }
}

/// Shared state for the authentication middleware.
#[derive(Clone)]
pub struct AuthState {
    expected_hash: [u8; 32],
}

impl AuthState {
    /// Create auth state from the configured API key.
    pub fn new(api_key: &str) -> Self {
        Self {
            expected_hash: Sha256::digest(api_key.as_bytes()).into(),
        }
    }
}

/// Axum middleware that validates bearer token or API key headers.
///
/// Compares the provided key against the expected key using SHA-256
/// hashing to avoid timing side-channels.
pub async fn auth_middleware(
    State(state): State<AuthState>,
    request: Request,
    next: Next,
) -> Response {
    let provided = extract_api_key(&request);

    match provided {
        Some(key) if verify_key(key, &state.expected_hash) => next.run(request).await,
        Some(_) => (StatusCode::UNAUTHORIZED, "invalid API key").into_response(),
        None => (StatusCode::UNAUTHORIZED, "missing API key").into_response(),
    }
}

/// Extract an API key from `Authorization: Bearer <key>` or `X-API-Key: <key>`.
fn extract_api_key(request: &Request) -> Option<&str> {
    if let Some(auth) = request.headers().get(AUTHORIZATION) {
        if let Ok(value) = auth.to_str() {
            if let Some(token) = value.strip_prefix("Bearer ") {
                return Some(token);
            }
        }
    }

    if let Some(key) = request.headers().get("x-api-key") {
        if let Ok(value) = key.to_str() {
            return Some(value);
        }
    }

    None
}

/// Compare a provided key against the expected hash using SHA-256 to
/// avoid timing side-channels on string comparison.
fn verify_key(provided: &str, expected_hash: &[u8; 32]) -> bool {
    let provided_hash: [u8; 32] = Sha256::digest(provided.as_bytes()).into();
    provided_hash == *expected_hash
}

/// Create a tonic interceptor that validates gRPC `authorization` metadata.
pub fn grpc_auth_interceptor(
    api_key: &str,
) -> impl Fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> + Clone {
    let expected_hash: [u8; 32] = Sha256::digest(api_key.as_bytes()).into();

    move |request: tonic::Request<()>| {
        let metadata = request.metadata();

        let provided = metadata
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .or_else(|| metadata.get("x-api-key").and_then(|v| v.to_str().ok()));

        match provided {
            Some(key) if verify_key(key, &expected_hash) => Ok(request),
            _ => Err(tonic::Status::unauthenticated("invalid or missing API key")),
        }
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::indexing_slicing
)]
mod tests {
    use axum::{body::Body, middleware, routing::get, Router};
    use tower::ServiceExt;

    use super::*;

    fn test_router() -> Router {
        let state = AuthState::new("test-secret");
        Router::new()
            .route("/data", get(|| async { "ok" }))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn valid_bearer_token_passes() {
        let router = test_router();
        let request = axum::http::Request::builder()
            .uri("/data")
            .header("Authorization", "Bearer test-secret")
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn valid_x_api_key_passes() {
        let router = test_router();
        let request = axum::http::Request::builder()
            .uri("/data")
            .header("x-api-key", "test-secret")
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn missing_auth_returns_401() {
        let router = test_router();
        let request = axum::http::Request::builder()
            .uri("/data")
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_key_returns_401() {
        let router = test_router();
        let request = axum::http::Request::builder()
            .uri("/data")
            .header("Authorization", "Bearer wrong-key")
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn bearer_prefix_is_case_sensitive() {
        let router = test_router();
        let request = axum::http::Request::builder()
            .uri("/data")
            .header("Authorization", "bearer test-secret")
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
