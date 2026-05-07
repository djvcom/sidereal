//! Authentication via `OAuth2` Device Code Flow (RFC 8628).
//!
//! The flow:
//! 1. Load a cached access token; return it if still valid.
//! 2. If a refresh token is cached and the access token has expired, attempt a
//!    token refresh grant.
//! 3. If no usable cached credentials exist, run the Device Code Flow:
//!    - Discover the OIDC provider configuration.
//!    - Request a device code with the required scopes.
//!    - Print the user code and verification URI to stderr.
//!    - Poll the token endpoint until the user authorises or the code expires.
//!    - Cache and return the resulting access token.

pub mod token_cache;

use chrono::Utc;
use serde::Deserialize;
use std::time::Duration;
use token_cache::CachedTokens;

use crate::config::{token_cache_path, OidcConfig};

/// Obtain a valid access token, using the cache where possible.
pub async fn authenticate(config: &OidcConfig) -> Result<String, AuthError> {
    let cache_path = token_cache_path();

    if let Some(cached) = token_cache::load(&cache_path)? {
        if cached.access_token_valid() {
            return Ok(cached.access_token);
        }
        if let Some(ref refresh_token) = cached.refresh_token {
            match try_refresh(config, refresh_token).await {
                Ok(tokens) => {
                    token_cache::save(&cache_path, &tokens)?;
                    return Ok(tokens.access_token);
                }
                Err(AuthError::Refresh(_)) => {}
                Err(e) => return Err(e),
            }
        }
    }

    let tokens = device_code_flow(config).await?;
    token_cache::save(&cache_path, &tokens)?;
    Ok(tokens.access_token)
}

async fn try_refresh(config: &OidcConfig, refresh_token: &str) -> Result<CachedTokens, AuthError> {
    let discovery = discover_oidc(config).await?;
    let client = reqwest::Client::new();

    let params = [
        ("grant_type", "refresh_token"),
        ("client_id", &config.client_id),
        ("refresh_token", refresh_token),
    ];

    let response = client
        .post(&discovery.token_endpoint)
        .form(&params)
        .send()
        .await?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AuthError::Refresh(body));
    }

    let token_response: TokenResponse = response.json().await?;
    Ok(tokens_from_response(token_response))
}

async fn device_code_flow(config: &OidcConfig) -> Result<CachedTokens, AuthError> {
    let discovery = discover_oidc(config).await?;

    let device_authorization_endpoint = config
        .device_endpoint
        .as_deref()
        .or(discovery.device_authorization_endpoint.as_deref())
        .ok_or_else(|| {
            AuthError::DeviceCode(
                "OIDC provider does not advertise a device authorisation endpoint; \
                 set oidc.device_endpoint in config.toml"
                    .to_owned(),
            )
        })?;

    let client = reqwest::Client::new();

    let device_params = [
        ("client_id", config.client_id.as_str()),
        ("scope", "openid offline_access"),
    ];

    let device_response = client
        .post(device_authorization_endpoint)
        .form(&device_params)
        .send()
        .await?;

    if !device_response.status().is_success() {
        let body = device_response.text().await.unwrap_or_default();
        return Err(AuthError::DeviceCode(body));
    }

    let device_auth: DeviceAuthResponse = device_response.json().await?;

    eprintln!();
    eprintln!("To authorise, visit:  {}", device_auth.verification_uri);
    eprintln!("Enter code:           {}", device_auth.user_code);
    eprintln!();

    let interval = Duration::from_secs(device_auth.interval.unwrap_or(5));
    poll_for_token(
        &client,
        &discovery.token_endpoint,
        &config.client_id,
        &device_auth,
        interval,
    )
    .await
}

async fn poll_for_token(
    client: &reqwest::Client,
    token_endpoint: &str,
    client_id: &str,
    device_auth: &DeviceAuthResponse,
    mut interval: Duration,
) -> Result<CachedTokens, AuthError> {
    let deadline = Utc::now() + chrono::Duration::seconds(i64::from(device_auth.expires_in));

    loop {
        if Utc::now() > deadline {
            return Err(AuthError::DeviceCode(
                "device code expired before authorisation was completed".to_owned(),
            ));
        }

        tokio::time::sleep(interval).await;

        let params = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
            ("client_id", client_id),
            ("device_code", &device_auth.device_code),
        ];

        let response = client.post(token_endpoint).form(&params).send().await?;
        let status = response.status();
        let body: serde_json::Value = response.json().await?;

        if status.is_success() {
            let token_response: TokenResponse = serde_json::from_value(body)?;
            return Ok(tokens_from_response(token_response));
        }

        let error = body
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown_error");

        match error {
            "authorization_pending" => {}
            "slow_down" => {
                interval += Duration::from_secs(5);
            }
            "access_denied" => {
                return Err(AuthError::TokenFetch(
                    "access denied by the user".to_owned(),
                ));
            }
            other => {
                return Err(AuthError::TokenFetch(format!(
                    "token endpoint returned error: {other}"
                )));
            }
        }
    }
}

fn tokens_from_response(response: TokenResponse) -> CachedTokens {
    let expires_at =
        Utc::now() + chrono::Duration::seconds(i64::from(response.expires_in.unwrap_or(3600)));

    CachedTokens {
        access_token: response.access_token,
        refresh_token: response.refresh_token,
        expires_at,
    }
}

async fn discover_oidc(config: &OidcConfig) -> Result<OidcDiscovery, AuthError> {
    let discovery_url = format!(
        "{}/.well-known/openid-configuration",
        config.issuer.trim_end_matches('/')
    );

    let client = reqwest::Client::new();
    let response = client
        .get(&discovery_url)
        .send()
        .await
        .map_err(|e| AuthError::Discover(e.to_string()))?;

    if !response.status().is_success() {
        let status = response.status();
        return Err(AuthError::Discover(format!(
            "discovery endpoint returned {status}"
        )));
    }

    response
        .json::<OidcDiscovery>()
        .await
        .map_err(|e| AuthError::Discover(e.to_string()))
}

/// Subset of the OIDC discovery document relevant to this client.
#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    token_endpoint: String,
    #[serde(rename = "device_authorization_endpoint")]
    device_authorization_endpoint: Option<String>,
}

/// `OAuth2` device authorisation response (RFC 8628 §3.2).
#[derive(Debug, Deserialize)]
struct DeviceAuthResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    expires_in: u32,
    interval: Option<u64>,
}

/// `OAuth2` token endpoint success response.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<u32>,
}

/// Errors that can occur during authentication.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// OIDC provider discovery failed.
    #[error("OIDC discovery failed: {0}")]
    Discover(String),
    /// Device code request failed.
    #[error("device code request failed: {0}")]
    DeviceCode(String),
    /// Token fetch during polling failed.
    #[error("token fetch failed: {0}")]
    TokenFetch(String),
    /// Token refresh grant failed.
    #[error("token refresh failed: {0}")]
    Refresh(String),
    /// Token cache I/O or parse error.
    #[error("token cache error: {0}")]
    Cache(#[from] token_cache::CacheError),
    /// HTTP request error.
    #[error("HTTP error: {0}")]
    Request(#[from] reqwest::Error),
    /// JSON deserialisation error.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
