//! Axum-style extractors for Sidereal functions.
//!
//! Extractors provide a composable way to access platform services
//! without a monolithic Context object.
//!
//! # Example
//!
//! ```no_run
//! use sidereal_sdk::prelude::*;
//!
//! #[derive(Deserialize)]
//! struct StripeConfig { api_key: String }
//!
//! #[derive(Serialize, Deserialize)]
//! struct CreateOrderPayload { item_id: String }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Order { id: String }
//!
//! #[sidereal_sdk::function]
//! async fn create_order(
//!     req: HttpRequest<CreateOrderPayload>,
//!     Config(stripe): Config<StripeConfig>,
//!     secrets: Secrets,
//!     Kv(store): Kv,
//! ) -> HttpResponse<Order> {
//!     let _api_key = secrets.get("STRIPE_API_KEY");
//!     HttpResponse::ok(Order { id: "123".into() })
//! }
//! ```

use axum::{
    extract::FromRequestParts,
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
};
use serde::de::DeserializeOwned;
use sidereal_state::{KvBackend, LockBackend, LockGuard, QueueBackend, StateProvider};
use std::sync::Arc;
use std::time::Duration;

use crate::config::{ConfigError, ConfigManager};

/// Shared application state for extractors.
#[derive(Clone)]
pub struct AppState {
    pub(crate) config: Option<ConfigManager>,
    pub(crate) state: StateProvider,
}

impl AppState {
    /// Create a new AppState with optional configuration and state provider.
    pub const fn new(config: Option<ConfigManager>, state: StateProvider) -> Self {
        Self { config, state }
    }
}

/// Extract typed configuration from `[app.*]` sections in sidereal.toml.
///
/// The section name is derived from the type name by convention (lowercase).
/// For example, `Config<StripeConfig>` extracts from `[app.stripe]`.
///
/// # Example
///
/// ```no_run
/// use sidereal_sdk::prelude::*;
///
/// #[derive(Deserialize)]
/// struct StripeConfig {
///     api_key: String,
///     webhook_secret: String,
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct PaymentRequest { amount: u64 }
///
/// #[derive(Serialize, Deserialize)]
/// struct PaymentResponse { success: bool }
///
/// #[sidereal_sdk::function]
/// async fn process_payment(
///     req: HttpRequest<PaymentRequest>,
///     Config(stripe): Config<StripeConfig>,
/// ) -> HttpResponse<PaymentResponse> {
///     // Use stripe.api_key...
///     HttpResponse::ok(PaymentResponse { success: true })
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
            Self::NotLoaded => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Configuration not loaded".to_owned(),
            ),
            Self::SectionNotFound(section) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Configuration section '{section}' not found"),
            ),
            Self::DeserialiseError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Configuration error: {msg}"),
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
                    ConfigRejection::DeserialiseError(format!("Config file not found: {path}"))
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
/// ```no_run
/// use sidereal_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// struct PaymentRequest { amount: u64 }
///
/// #[derive(Serialize, Deserialize)]
/// struct PaymentResponse { success: bool }
///
/// #[sidereal_sdk::function]
/// async fn process_payment(
///     req: HttpRequest<PaymentRequest>,
///     secrets: Secrets,
/// ) -> HttpResponse<PaymentResponse> {
///     let _api_key = secrets.get("STRIPE_API_KEY");
///     HttpResponse::ok(PaymentResponse { success: true })
/// }
/// ```
#[derive(Debug, Clone)]
pub struct Secrets;

impl Secrets {
    /// Get a secret by name.
    ///
    /// Returns an error if the secret is not found.
    pub fn get(&self, name: &str) -> Result<String, SecretError> {
        std::env::var(name).map_err(|_| SecretError::NotFound(name.to_owned()))
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
        Ok(Self)
    }
}

/// Key-value store client.
#[derive(Clone)]
pub struct KvClient {
    backend: Arc<dyn KvBackend>,
}

impl KvClient {
    /// Create a new KV client from a backend.
    pub fn new(backend: Arc<dyn KvBackend>) -> Self {
        Self { backend }
    }

    /// Get a value from the KV store.
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, KvError> {
        match self.backend.get(key).await {
            Ok(Some(bytes)) => {
                let value = serde_json::from_slice(&bytes)
                    .map_err(|e| KvError::Deserialisation(e.to_string()))?;
                Ok(Some(value))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(KvError::Backend(e.to_string())),
        }
    }

    /// Get a raw byte value from the KV store.
    pub async fn get_raw(&self, key: &str) -> Result<Option<Vec<u8>>, KvError> {
        self.backend
            .get(key)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))
    }

    /// Put a value into the KV store.
    pub async fn put<T: serde::Serialize + Sync>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), KvError> {
        let bytes = serde_json::to_vec(value).map_err(|e| KvError::Serialisation(e.to_string()))?;
        self.backend
            .put(key, &bytes, None)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))
    }

    /// Put a value into the KV store with a TTL.
    pub async fn put_with_ttl<T: serde::Serialize + Sync>(
        &self,
        key: &str,
        value: &T,
        ttl: Duration,
    ) -> Result<(), KvError> {
        let bytes = serde_json::to_vec(value).map_err(|e| KvError::Serialisation(e.to_string()))?;
        self.backend
            .put(key, &bytes, Some(ttl))
            .await
            .map_err(|e| KvError::Backend(e.to_string()))
    }

    /// Put raw bytes into the KV store.
    pub async fn put_raw(&self, key: &str, value: &[u8]) -> Result<(), KvError> {
        self.backend
            .put(key, value, None)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))
    }

    /// Delete a key from the KV store.
    pub async fn delete(&self, key: &str) -> Result<bool, KvError> {
        self.backend
            .delete(key)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))
    }

    /// Check if a key exists in the KV store.
    pub async fn exists(&self, key: &str) -> Result<bool, KvError> {
        self.backend
            .exists(key)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))
    }

    /// List keys with a given prefix.
    pub async fn list(
        &self,
        prefix: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<(Vec<String>, Option<String>), KvError> {
        self.backend
            .list(prefix, limit, cursor)
            .await
            .map_err(|e| KvError::Backend(e.to_string()))
    }
}

/// Errors from KV operations.
#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("serialisation error: {0}")]
    Serialisation(String),

    #[error("deserialisation error: {0}")]
    Deserialisation(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("kv not configured")]
    NotConfigured,
}

/// Extract the KV store client.
///
/// # Example
///
/// ```no_run
/// use sidereal_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// struct GetUserRequest { user_id: String }
///
/// #[derive(Serialize, Deserialize)]
/// struct User { name: String }
///
/// #[sidereal_sdk::function]
/// async fn get_user(
///     req: HttpRequest<GetUserRequest>,
///     Kv(store): Kv,
/// ) -> HttpResponse<User> {
///     let _user: Option<User> = store.get(&req.body.user_id).await.ok().flatten();
///     HttpResponse::ok(User { name: "Alice".into() })
/// }
/// ```
#[derive(Clone)]
pub struct Kv(pub KvClient);

/// Rejection type for Kv extractor failures.
#[derive(Debug)]
pub enum KvRejection {
    NotAvailable,
    NotConfigured,
}

impl IntoResponse for KvRejection {
    fn into_response(self) -> Response {
        let message = match self {
            Self::NotAvailable => "KV store not available",
            Self::NotConfigured => "KV store not configured",
        };
        (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
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
            .map_err(|_| KvRejection::NotAvailable)?;

        let backend = app_state
            .state
            .kv()
            .map_err(|_| KvRejection::NotConfigured)?;

        Ok(Self(KvClient::new(backend)))
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
            .map_or_else(generate_request_id, std::borrow::ToOwned::to_owned);

        // Extract function name from path
        let function_name = parts
            .uri
            .path()
            .trim_start_matches('/')
            .split('/')
            .next()
            .unwrap_or("unknown")
            .to_owned();

        Ok(Self {
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
    #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
    let random: u32 = RandomState::new().build_hasher().finish() as u32;
    format!("{timestamp:x}-{random:08x}")
}

// ============================================================================
// Queue extractor
// ============================================================================

/// Queue client for publishing and receiving messages.
#[derive(Clone)]
pub struct QueueClient {
    backend: Arc<dyn QueueBackend>,
}

impl QueueClient {
    /// Create a new queue client from a backend.
    pub fn new(backend: Arc<dyn QueueBackend>) -> Self {
        Self { backend }
    }

    /// Publish a message to a queue.
    pub async fn publish<T: serde::Serialize + Sync>(
        &self,
        queue: &str,
        message: &T,
    ) -> Result<sidereal_state::MessageId, QueueError> {
        let bytes =
            serde_json::to_vec(message).map_err(|e| QueueError::Serialisation(e.to_string()))?;
        self.backend
            .publish(queue, &bytes)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))
    }

    /// Publish raw bytes to a queue.
    pub async fn publish_raw(
        &self,
        queue: &str,
        message: &[u8],
    ) -> Result<sidereal_state::MessageId, QueueError> {
        self.backend
            .publish(queue, message)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))
    }

    /// Receive a message from a queue with a visibility timeout.
    pub async fn receive(
        &self,
        queue: &str,
        visibility_timeout: Duration,
    ) -> Result<Option<sidereal_state::Message>, QueueError> {
        self.backend
            .receive(queue, visibility_timeout)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))
    }

    /// Acknowledge a message as processed.
    pub async fn ack(
        &self,
        queue: &str,
        message_id: &sidereal_state::MessageId,
    ) -> Result<(), QueueError> {
        self.backend
            .ack(queue, message_id)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))
    }

    /// Negative-acknowledge a message (return to queue immediately).
    pub async fn nack(
        &self,
        queue: &str,
        message_id: &sidereal_state::MessageId,
    ) -> Result<(), QueueError> {
        self.backend
            .nack(queue, message_id)
            .await
            .map_err(|e| QueueError::Backend(e.to_string()))
    }
}

/// Errors from queue operations.
#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("serialisation error: {0}")]
    Serialisation(String),

    #[error("deserialisation error: {0}")]
    Deserialisation(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("queue not configured")]
    NotConfigured,
}

/// Extract the queue client.
///
/// # Example
///
/// ```no_run
/// use sidereal_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// struct JobPayload { task: String }
///
/// #[sidereal_sdk::function]
/// async fn enqueue_job(
///     req: HttpRequest<JobPayload>,
///     Queue(queue): Queue,
/// ) -> HttpResponse<()> {
///     queue.publish("jobs", &req.body).await.unwrap();
///     HttpResponse::ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Queue(pub QueueClient);

/// Rejection type for Queue extractor failures.
#[derive(Debug)]
pub enum QueueRejection {
    NotAvailable,
    NotConfigured,
}

impl IntoResponse for QueueRejection {
    fn into_response(self) -> Response {
        let message = match self {
            Self::NotAvailable => "Queue not available",
            Self::NotConfigured => "Queue not configured",
        };
        (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
    }
}

impl<S> FromRequestParts<S> for Queue
where
    S: Send + Sync,
    Arc<AppState>: FromRequestParts<S>,
{
    type Rejection = QueueRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = Arc::<AppState>::from_request_parts(parts, state)
            .await
            .map_err(|_| QueueRejection::NotAvailable)?;

        let backend = app_state
            .state
            .queue()
            .map_err(|_| QueueRejection::NotConfigured)?;

        Ok(Self(QueueClient::new(backend)))
    }
}

// ============================================================================
// Lock extractor
// ============================================================================

/// Lock client for distributed locking.
#[derive(Clone)]
pub struct LockClient {
    backend: Arc<dyn LockBackend>,
}

impl LockClient {
    /// Create a new lock client from a backend.
    pub fn new(backend: Arc<dyn LockBackend>) -> Self {
        Self { backend }
    }

    /// Acquire a lock on a resource (blocks until acquired).
    pub async fn acquire(&self, resource: &str, ttl: Duration) -> Result<LockGuard, LockError> {
        self.backend
            .acquire(resource, ttl)
            .await
            .map_err(|e| LockError::Backend(e.to_string()))
    }

    /// Try to acquire a lock on a resource (returns immediately).
    pub async fn try_acquire(
        &self,
        resource: &str,
        ttl: Duration,
    ) -> Result<Option<LockGuard>, LockError> {
        self.backend
            .try_acquire(resource, ttl)
            .await
            .map_err(|e| LockError::Backend(e.to_string()))
    }
}

/// Errors from lock operations.
#[derive(Debug, thiserror::Error)]
pub enum LockError {
    #[error("backend error: {0}")]
    Backend(String),

    #[error("lock not configured")]
    NotConfigured,
}

/// Extract the lock client.
///
/// # Example
///
/// ```no_run
/// use sidereal_sdk::prelude::*;
/// use std::time::Duration;
///
/// #[derive(Serialize, Deserialize)]
/// struct ProcessRequest { resource_id: String }
///
/// #[sidereal_sdk::function]
/// async fn process_resource(
///     req: HttpRequest<ProcessRequest>,
///     Lock(lock): Lock,
/// ) -> HttpResponse<()> {
///     let guard = lock.acquire(&req.body.resource_id, Duration::from_secs(30)).await.unwrap();
///     // Process the resource...
///     guard.release().await.unwrap();
///     HttpResponse::ok(())
/// }
/// ```
#[derive(Clone)]
pub struct Lock(pub LockClient);

/// Rejection type for Lock extractor failures.
#[derive(Debug)]
pub enum LockRejection {
    NotAvailable,
    NotConfigured,
}

impl IntoResponse for LockRejection {
    fn into_response(self) -> Response {
        let message = match self {
            Self::NotAvailable => "Lock service not available",
            Self::NotConfigured => "Lock service not configured",
        };
        (StatusCode::INTERNAL_SERVER_ERROR, message).into_response()
    }
}

impl<S> FromRequestParts<S> for Lock
where
    S: Send + Sync,
    Arc<AppState>: FromRequestParts<S>,
{
    type Rejection = LockRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = Arc::<AppState>::from_request_parts(parts, state)
            .await
            .map_err(|_| LockRejection::NotAvailable)?;

        let backend = app_state
            .state
            .lock()
            .map_err(|_| LockRejection::NotConfigured)?;

        Ok(Self(LockClient::new(backend)))
    }
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
