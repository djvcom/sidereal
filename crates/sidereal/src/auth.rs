//! Authentication middleware for HTTP and gRPC servers.
//!
//! Provides API key validation for OTLP ingest endpoints and OIDC JWT
//! validation for the query API.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Request, State},
    http::{header::AUTHORIZATION, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use jsonwebtoken::{decode, decode_header, jwk::JwkSet, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::RwLock;

/// OIDC provider configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct OidcConfig {
    /// The issuer URL of the OIDC provider.
    pub issuer: String,
    /// The expected audience claim value.
    pub audience: String,
    /// How often to refresh the JWKS cache, in seconds.
    #[serde(default = "default_jwks_refresh_secs")]
    pub jwks_refresh_secs: u64,
}

fn default_jwks_refresh_secs() -> u64 {
    3600
}

/// Authentication configuration.
///
/// When an API key is set, OTLP data endpoints require this key as a
/// `Bearer` token or `X-API-Key` header value. Health and readiness
/// endpoints remain open regardless of this setting.
#[derive(Clone, Default, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// API key for OTLP ingest endpoints.
    api_key: Option<String>,
    /// OIDC configuration for the query API.
    pub oidc: Option<OidcConfig>,
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
            .field("oidc", &self.oidc)
            .finish()
    }
}

impl AuthConfig {
    /// Whether API key authentication is enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.api_key.is_some()
    }

    /// Return the configured API key, if any.
    #[must_use]
    pub fn api_key(&self) -> Option<&str> {
        self.api_key.as_deref()
    }
}

/// Shared state for the API key authentication middleware.
#[derive(Clone)]
pub struct AuthState {
    expected_hash: [u8; 32],
}

impl AuthState {
    /// Create auth state from the configured API key.
    #[must_use]
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
    let provided = extract_bearer_or_api_key(&request);

    match provided {
        Some(key) if verify_key(key, &state.expected_hash) => next.run(request).await,
        Some(_) => (StatusCode::UNAUTHORIZED, "invalid API key").into_response(),
        None => (StatusCode::UNAUTHORIZED, "missing API key").into_response(),
    }
}

/// Extract an API key from `Authorization: Bearer <key>` or `X-API-Key: <key>`.
pub(crate) fn extract_bearer_or_api_key(request: &Request) -> Option<&str> {
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

// ============================================================================
// OIDC JWT validation
// ============================================================================

/// Errors that can occur during authentication.
#[derive(Debug, Error)]
pub enum AuthError {
    /// No token was present in the request.
    #[error("missing bearer token")]
    MissingToken,
    /// The provided token was rejected during validation.
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// The JWKS endpoint could not be reached or parsed.
    #[error("JWKS unavailable: {0}")]
    JwksUnavailable(String),
}

/// Validated JWT claims.
#[derive(Debug, Deserialize)]
pub struct Claims {
    /// Subject identifier.
    pub sub: String,
    /// Expiry timestamp (seconds since Unix epoch).
    pub exp: u64,
}

/// Validates OIDC JWTs against a cached JWKS fetched from the provider.
pub struct OidcValidator {
    audience: String,
    issuer: String,
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
}

impl OidcValidator {
    /// Initialise the validator by fetching OIDC discovery metadata and
    /// performing an initial JWKS fetch.
    ///
    /// Spawns a background task that refreshes the key cache on the interval
    /// specified by `config.jwks_refresh_secs`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::JwksUnavailable`] if the OIDC discovery document
    /// or initial JWKS cannot be fetched or parsed.
    pub async fn new(config: &OidcConfig) -> Result<Arc<Self>, AuthError> {
        let http = reqwest::Client::new();
        let jwks_uri = discover_jwks_uri(&http, &config.issuer).await?;
        let initial_keys = Self::refresh_keys(&http, &jwks_uri).await?;

        let keys = Arc::new(RwLock::new(initial_keys));
        let validator = Arc::new(Self {
            audience: config.audience.clone(),
            issuer: config.issuer.clone(),
            keys: keys.clone(),
        });

        let refresh_interval = Duration::from_secs(config.jwks_refresh_secs);
        tokio::spawn({
            let jwks_uri = jwks_uri.clone();
            async move {
                loop {
                    tokio::time::sleep(refresh_interval).await;
                    match Self::refresh_keys(&reqwest::Client::new(), &jwks_uri).await {
                        Ok(fresh_keys) => {
                            *keys.write().await = fresh_keys;
                            tracing::debug!("JWKS cache refreshed");
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "Failed to refresh JWKS cache");
                        }
                    }
                }
            }
        });

        Ok(validator)
    }

    /// Validate a raw JWT string, returning the decoded claims on success.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::InvalidToken`] if the token is malformed, has
    /// expired, uses an unknown key ID, or fails audience/issuer validation.
    /// Returns [`AuthError::JwksUnavailable`] if a key refresh fails.
    pub async fn validate(&self, token: &str) -> Result<Claims, AuthError> {
        let header = decode_header(token).map_err(|e| AuthError::InvalidToken(e.to_string()))?;

        let kid = header
            .kid
            .ok_or_else(|| AuthError::InvalidToken("token header missing kid".to_owned()))?;

        let key = self.find_or_refresh_key(&kid).await?;
        self.decode_token(token, &key)
    }

    async fn find_or_refresh_key(&self, kid: &str) -> Result<DecodingKey, AuthError> {
        {
            let cache = self.keys.read().await;
            if let Some(key) = cache.get(kid) {
                return Ok(key.clone());
            }
        }

        let http = reqwest::Client::new();
        let jwks_uri = discover_jwks_uri(&http, &self.issuer).await?;
        let fresh = Self::refresh_keys(&http, &jwks_uri).await?;
        let key = fresh
            .get(kid)
            .ok_or_else(|| {
                AuthError::InvalidToken(format!("no key found for kid '{kid}' after refresh"))
            })?
            .clone();
        *self.keys.write().await = fresh;
        Ok(key)
    }

    fn decode_token(&self, token: &str, key: &DecodingKey) -> Result<Claims, AuthError> {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[&self.audience]);
        validation.set_issuer(&[&self.issuer]);

        decode::<Claims>(token, key, &validation)
            .map(|data| data.claims)
            .map_err(|e| AuthError::InvalidToken(e.to_string()))
    }

    async fn refresh_keys(
        http: &reqwest::Client,
        jwks_uri: &str,
    ) -> Result<HashMap<String, DecodingKey>, AuthError> {
        let jwks: JwkSet = http
            .get(jwks_uri)
            .send()
            .await
            .map_err(|e| AuthError::JwksUnavailable(e.to_string()))?
            .json()
            .await
            .map_err(|e| AuthError::JwksUnavailable(format!("invalid JWKS response: {e}")))?;

        let mut keys = HashMap::new();
        for jwk in &jwks.keys {
            if let Some(kid) = &jwk.common.key_id {
                match DecodingKey::from_jwk(jwk) {
                    Ok(decoding_key) => {
                        keys.insert(kid.clone(), decoding_key);
                    }
                    Err(err) => {
                        tracing::warn!(kid = %kid, error = %err, "Skipping unsupported JWK");
                    }
                }
            }
        }
        Ok(keys)
    }
}

/// Fetch the `jwks_uri` from the OIDC discovery document.
async fn discover_jwks_uri(http: &reqwest::Client, issuer: &str) -> Result<String, AuthError> {
    let discovery_url = format!("{issuer}/.well-known/openid-configuration");

    let doc: serde_json::Value = http
        .get(&discovery_url)
        .send()
        .await
        .map_err(|e| AuthError::JwksUnavailable(format!("discovery request failed: {e}")))?
        .json()
        .await
        .map_err(|e| AuthError::JwksUnavailable(format!("invalid discovery document: {e}")))?;

    doc.get("jwks_uri")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .ok_or_else(|| AuthError::JwksUnavailable("discovery document missing jwks_uri".to_owned()))
}

/// Axum middleware that validates OIDC JWT bearer tokens.
pub async fn oidc_auth_middleware(
    State(validator): State<Arc<OidcValidator>>,
    request: Request,
    next: Next,
) -> Response {
    let token = match extract_bearer_or_api_key(&request) {
        Some(t) => t.to_owned(),
        None => {
            return (StatusCode::UNAUTHORIZED, "missing bearer token").into_response();
        }
    };

    match validator.validate(&token).await {
        Ok(_claims) => next.run(request).await,
        Err(AuthError::MissingToken) => {
            (StatusCode::UNAUTHORIZED, "missing bearer token").into_response()
        }
        Err(AuthError::InvalidToken(msg)) => {
            tracing::debug!(reason = %msg, "JWT validation failed");
            (StatusCode::UNAUTHORIZED, "invalid token").into_response()
        }
        Err(AuthError::JwksUnavailable(msg)) => {
            tracing::error!(reason = %msg, "JWKS unavailable during request validation");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication service unavailable",
            )
                .into_response()
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
