//! Axum-style extractors for Sidereal functions.
//!
//! Extractors provide a composable way to access platform services
//! without a monolithic Context object.
//!
//! # Example
//!
//! ```ignore
//! use sidereal_sdk::prelude::*;
//!
//! #[sidereal_sdk::function]
//! async fn create_order(
//!     req: HttpRequest<CreateOrderPayload>,
//!     Config(stripe): Config<StripeConfig>,
//!     secret: Secret,
//!     Kv(store): Kv,
//! ) -> HttpResponse<Order> {
//!     let api_key = secret.get("STRIPE_API_KEY")?;
//!     // stripe, api_key, store are directly available
//! }
//! ```

use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
};
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::config::{ConfigError, ConfigManager};

/// Shared application state for extractors.
#[derive(Clone)]
pub struct AppState {
    pub(crate) config: Option<ConfigManager>,
    pub(crate) kv: KvClient,
}

impl AppState {
    /// Create a new AppState with optional configuration.
    pub fn new(config: Option<ConfigManager>) -> Self {
        Self {
            config,
            kv: KvClient::new_in_memory(),
        }
    }
}

/// Extract typed configuration from `[app.*]` sections in sidereal.toml.
///
/// The section name is derived from the type name by convention (lowercase).
/// For example, `Config<StripeConfig>` extracts from `[app.stripe]`.
///
/// # Example
///
/// ```ignore
/// #[derive(Deserialize)]
/// struct StripeConfig {
///     api_key: String,
///     webhook_secret: String,
/// }
///
/// #[sidereal_sdk::function]
/// async fn process_payment(
///     req: HttpRequest<PaymentRequest>,
///     Config(stripe): Config<StripeConfig>,
/// ) -> HttpResponse<PaymentResponse> {
///     // Use stripe.api_key...
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Config<T>(pub T);

/// Rejection type for Config extractor failures.
#[derive(Debug)]
pub enum ConfigRejection {
    /// No configuration manager available.
    NotLoaded,
    /// The requested section was not found.
    SectionNotFound(String),
    /// Failed to deserialise the section.
    DeserialiseError(String),
}

impl IntoResponse for ConfigRejection {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ConfigRejection::NotLoaded => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Configuration not loaded".to_string(),
            ),
            ConfigRejection::SectionNotFound(section) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Configuration section '{}' not found", section),
            ),
            ConfigRejection::DeserialiseError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Configuration error: {}", msg),
            ),
        };
        (status, message).into_response()
    }
}

impl<S, T> FromRequestParts<S> for Config<T>
where
    S: Send + Sync,
    T: DeserializeOwned + Send + 'static,
    Arc<AppState>: FromRequestParts<S>,
{
    type Rejection = ConfigRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = Arc::<AppState>::from_request_parts(parts, state)
            .await
            .map_err(|_| ConfigRejection::NotLoaded)?;

        let config_manager = app_state
            .config
            .as_ref()
            .ok_or(ConfigRejection::NotLoaded)?;

        // Derive section name from type name (lowercase, strip "Config" suffix)
        let type_name = std::any::type_name::<T>();
        let section = derive_section_name(type_name);

        config_manager
            .section::<T>(&section)
            .map(Config)
            .map_err(|e| match e {
                ConfigError::SectionNotFound(s) => ConfigRejection::SectionNotFound(s),
                ConfigError::Deserialisation { source, .. } => {
                    ConfigRejection::DeserialiseError(source.to_string())
                }
                ConfigError::Figment(e) => ConfigRejection::DeserialiseError(e.to_string()),
                ConfigError::FileNotFound(path) => {
                    ConfigRejection::DeserialiseError(format!("Config file not found: {}", path))
                }
            })
    }
}

/// Derive a section name from a type name.
///
/// Examples:
/// - `my_app::config::StripeConfig` -> `stripe`
/// - `StripeConfig` -> `stripe`
/// - `Stripe` -> `stripe`
fn derive_section_name(type_name: &str) -> String {
    // Get the last segment (after ::)
    let name = type_name.rsplit("::").next().unwrap_or(type_name);

    // Strip "Config" suffix if present
    let name = name.strip_suffix("Config").unwrap_or(name);

    // Convert to lowercase
    name.to_lowercase()
}

/// Secrets accessor for retrieving sensitive configuration.
///
/// # Example
///
/// ```ignore
/// #[sidereal_sdk::function]
/// async fn process_payment(
///     req: HttpRequest<PaymentRequest>,
///     secrets: Secrets,
/// ) -> HttpResponse<PaymentResponse> {
///     let api_key = secrets.get("STRIPE_API_KEY")?;
///     // Use api_key...
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Secrets;

impl Secrets {
    /// Get a secret by name.
    ///
    /// Returns an error if the secret is not found.
    pub fn get(&self, name: &str) -> Result<String, SecretError> {
        std::env::var(name).map_err(|_| SecretError::NotFound(name.to_string()))
    }

    /// Get a secret by name, returning None if not found.
    pub fn get_optional(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// Errors from secret retrieval.
#[derive(Debug, thiserror::Error)]
pub enum SecretError {
    #[error("secret not found: {0}")]
    NotFound(String),
}

/// Rejection type for Secrets extractor failures.
#[derive(Debug)]
pub struct SecretsRejection;

impl IntoResponse for SecretsRejection {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Secrets accessor not available",
        )
            .into_response()
    }
}

impl<S> FromRequestParts<S> for Secrets
where
    S: Send + Sync,
{
    type Rejection = SecretsRejection;

    async fn from_request_parts(_parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Secrets)
    }
}

/// Key-value store client.
#[derive(Clone)]
pub struct KvClient {
    inner: Arc<KvClientInner>,
}

enum KvClientInner {
    InMemory(RwLock<HashMap<String, Vec<u8>>>),
}

impl KvClient {
    /// Create a new in-memory KV client.
    pub fn new_in_memory() -> Self {
        Self {
            inner: Arc::new(KvClientInner::InMemory(RwLock::new(HashMap::new()))),
        }
    }

    /// Get a value from the KV store.
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, KvError> {
        match &*self.inner {
            KvClientInner::InMemory(map) => {
                let map = map.read().map_err(|_| KvError::Internal)?;
                match map.get(key) {
                    Some(bytes) => {
                        let value = serde_json::from_slice(bytes)
                            .map_err(|e| KvError::Deserialisation(e.to_string()))?;
                        Ok(Some(value))
                    }
                    None => Ok(None),
                }
            }
        }
    }

    /// Put a value into the KV store.
    pub async fn put<T: serde::Serialize>(&self, key: &str, value: &T) -> Result<(), KvError> {
        let bytes = serde_json::to_vec(value).map_err(|e| KvError::Serialisation(e.to_string()))?;

        match &*self.inner {
            KvClientInner::InMemory(map) => {
                let mut map = map.write().map_err(|_| KvError::Internal)?;
                map.insert(key.to_string(), bytes);
                Ok(())
            }
        }
    }

    /// Delete a key from the KV store.
    pub async fn delete(&self, key: &str) -> Result<bool, KvError> {
        match &*self.inner {
            KvClientInner::InMemory(map) => {
                let mut map = map.write().map_err(|_| KvError::Internal)?;
                Ok(map.remove(key).is_some())
            }
        }
    }
}

/// Errors from KV operations.
#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("serialisation error: {0}")]
    Serialisation(String),

    #[error("deserialisation error: {0}")]
    Deserialisation(String),

    #[error("internal error")]
    Internal,
}

/// Extract the KV store client.
///
/// # Example
///
/// ```ignore
/// #[sidereal_sdk::function]
/// async fn get_user(
///     req: HttpRequest<GetUserRequest>,
///     Kv(store): Kv,
/// ) -> HttpResponse<User> {
///     let user: Option<User> = store.get(&req.body.user_id).await?;
///     // ...
/// }
/// ```
#[derive(Clone)]
pub struct Kv(pub KvClient);

/// Rejection type for Kv extractor failures.
#[derive(Debug)]
pub struct KvRejection;

impl IntoResponse for KvRejection {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, "KV store not available").into_response()
    }
}

impl<S> FromRequestParts<S> for Kv
where
    S: Send + Sync,
    Arc<AppState>: FromRequestParts<S>,
{
    type Rejection = KvRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = Arc::<AppState>::from_request_parts(parts, state)
            .await
            .map_err(|_| KvRejection)?;

        Ok(Kv(app_state.kv.clone()))
    }
}

/// Invocation metadata for the current request.
///
/// Provides information about the function invocation context.
#[derive(Debug, Clone)]
pub struct InvocationMeta {
    /// Unique identifier for this request.
    pub request_id: String,
    /// The name of the function being invoked.
    pub function_name: String,
}

/// Rejection type for InvocationMeta extractor failures.
#[derive(Debug)]
pub struct InvocationMetaRejection;

impl IntoResponse for InvocationMetaRejection {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Invocation metadata not available",
        )
            .into_response()
    }
}

impl<S> FromRequestParts<S> for InvocationMeta
where
    S: Send + Sync,
{
    type Rejection = InvocationMetaRejection;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        // Extract request ID from header or generate one
        let request_id = parts
            .headers
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
            .unwrap_or_else(generate_request_id);

        // Extract function name from path
        let function_name = parts
            .uri
            .path()
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("unknown")
            .to_string();

        Ok(InvocationMeta {
            request_id,
            function_name,
        })
    }
}

fn generate_request_id() -> String {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::time::{SystemTime, UNIX_EPOCH};

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let random: u32 = RandomState::new().build_hasher().finish() as u32;
    format!("{:x}-{:08x}", timestamp, random)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_section_name() {
        assert_eq!(derive_section_name("StripeConfig"), "stripe");
        assert_eq!(
            derive_section_name("my_app::config::StripeConfig"),
            "stripe"
        );
        assert_eq!(derive_section_name("Stripe"), "stripe");
        assert_eq!(derive_section_name("DatabaseConfig"), "database");
    }
}
