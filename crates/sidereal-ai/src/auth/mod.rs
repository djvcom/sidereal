//! Authentication via `OAuth2` Authorization Code Flow with PKCE (RFC 7636).
//!
//! The flow:
//! 1. Load a cached access token; return it if still valid.
//! 2. If a refresh token is cached and the access token has expired, attempt a
//!    token refresh grant.
//! 3. Otherwise, run the Authorization Code Flow:
//!    - Discover the OIDC provider configuration.
//!    - Generate a PKCE code verifier and challenge.
//!    - Bind a local HTTP server on a random port to receive the callback.
//!    - Print the authorization URL for the user to open in their browser.
//!    - Wait for the redirect callback, exchange the code for tokens.
//!    - Cache and return the resulting access token.

pub mod token_cache;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::Utc;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::TcpListener;
use token_cache::CachedTokens;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener as AsyncTcpListener;

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

    let tokens = authorization_code_flow(config).await?;
    token_cache::save(&cache_path, &tokens)?;
    Ok(tokens.access_token)
}

async fn try_refresh(config: &OidcConfig, refresh_token: &str) -> Result<CachedTokens, AuthError> {
    let discovery = discover_oidc(config).await?;
    let client = reqwest::Client::new();

    let params = [
        ("grant_type", "refresh_token"),
        ("client_id", config.client_id.as_str()),
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

async fn authorization_code_flow(config: &OidcConfig) -> Result<CachedTokens, AuthError> {
    let discovery = discover_oidc(config).await?;

    let port = free_port()?;
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");

    let (verifier, challenge) = pkce_pair();
    let state = random_state();

    let auth_url = format!(
        "{}?response_type=code&client_id={}&redirect_uri={}&scope=openid+offline_access&state={}&code_challenge={}&code_challenge_method=S256",
        discovery.authorization_endpoint,
        urlencoded(&config.client_id),
        urlencoded(&redirect_uri),
        urlencoded(&state),
        urlencoded(&challenge),
    );

    eprintln!();
    eprintln!("Opening browser to authenticate. If it does not open automatically, visit:");
    eprintln!("{auth_url}");
    eprintln!();

    let _ = open::that(&auth_url);

    let code = wait_for_callback(port, &state).await?;

    let client = reqwest::Client::new();
    let params = [
        ("grant_type", "authorization_code"),
        ("client_id", config.client_id.as_str()),
        ("code", code.as_str()),
        ("redirect_uri", redirect_uri.as_str()),
        ("code_verifier", verifier.as_str()),
    ];

    let response = client
        .post(&discovery.token_endpoint)
        .form(&params)
        .send()
        .await?;

    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(AuthError::TokenFetch(body));
    }

    let token_response: TokenResponse = response.json().await?;
    Ok(tokens_from_response(token_response))
}

async fn wait_for_callback(port: u16, expected_state: &str) -> Result<String, AuthError> {
    let listener = AsyncTcpListener::bind(format!("127.0.0.1:{port}"))
        .await
        .map_err(|e| AuthError::Callback(e.to_string()))?;

    let (mut stream, _) = listener
        .accept()
        .await
        .map_err(|e| AuthError::Callback(e.to_string()))?;

    let mut buf = vec![0u8; 4096];
    let n = stream
        .read(&mut buf)
        .await
        .map_err(|e| AuthError::Callback(e.to_string()))?;

    let request = String::from_utf8_lossy(buf.get(..n).unwrap_or_default());
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("");

    let query = path.split_once('?').map_or("", |x| x.1);
    let params: HashMap<&str, &str> = query
        .split('&')
        .filter_map(|pair| {
            let mut it = pair.splitn(2, '=');
            Some((it.next()?, it.next()?))
        })
        .collect();

    let response_body = "Authentication successful. You may close this tab.";
    let http_response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    let _ = stream.write_all(http_response.as_bytes()).await;

    let state = params
        .get("state")
        .ok_or_else(|| AuthError::Callback("missing state parameter".to_owned()))?;

    if *state != expected_state {
        return Err(AuthError::Callback("state mismatch".to_owned()));
    }

    if let Some(error) = params.get("error") {
        let description = params.get("error_description").copied().unwrap_or("");
        return Err(AuthError::TokenFetch(format!("{error}: {description}")));
    }

    params
        .get("code")
        .map(|s| (*s).to_owned())
        .ok_or_else(|| AuthError::Callback("missing code parameter in callback".to_owned()))
}

fn pkce_pair() -> (String, String) {
    let verifier = random_bytes(32);
    let verifier_b64 = URL_SAFE_NO_PAD.encode(&verifier);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier_b64.as_bytes()));
    (verifier_b64, challenge)
}

fn random_state() -> String {
    URL_SAFE_NO_PAD.encode(random_bytes(16))
}

fn random_bytes(len: usize) -> Vec<u8> {
    (0..len).map(|_| rand::random::<u8>()).collect()
}

fn free_port() -> Result<u16, AuthError> {
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|e| AuthError::Callback(format!("could not bind local port: {e}")))?;
    listener
        .local_addr()
        .map(|a| a.port())
        .map_err(|e| AuthError::Callback(format!("could not read local port: {e}")))
}

fn urlencoded(s: &str) -> String {
    s.chars()
        .flat_map(|c| {
            if c.is_ascii_alphanumeric() || "-._~".contains(c) {
                vec![c]
            } else {
                format!("%{:02X}", c as u32).chars().collect()
            }
        })
        .collect()
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

#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    authorization_endpoint: String,
    token_endpoint: String,
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: Option<String>,
    expires_in: Option<u32>,
}

/// Errors that can occur during authentication.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("OIDC discovery failed: {0}")]
    Discover(String),
    #[error("token fetch failed: {0}")]
    TokenFetch(String),
    #[error("token refresh failed: {0}")]
    Refresh(String),
    #[error("local callback failed: {0}")]
    Callback(String),
    #[error("token cache error: {0}")]
    Cache(#[from] token_cache::CacheError),
    #[error("HTTP error: {0}")]
    Request(#[from] reqwest::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}
