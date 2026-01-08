//! Function registry for discovering registered functions at runtime.
//!
//! Functions annotated with `#[sidereal_sdk::function]` are automatically
//! registered using the `inventory` crate's distributed slice pattern.

use crate::triggers::TriggerKind;
use std::future::Future;
use std::pin::Pin;

/// Metadata about a registered function.
pub struct FunctionMetadata {
    /// The function name (derived from the Rust function name).
    pub name: &'static str,

    /// The kind of trigger for this function.
    pub trigger_kind: TriggerKind,

    /// For queue triggers, the name of the queue this function consumes from.
    /// Derived from the message type name (e.g., `OrderCreated` → `order-created`).
    pub queue_name: Option<&'static str>,

    /// The legacy handler function (deprecated - use axum handlers instead).
    pub handler: FunctionHandler,
}

/// Type alias for the legacy async function handler.
///
/// Takes JSON bytes as input, returns JSON bytes as output.
/// Note: This is deprecated in favour of axum handlers with extractors.
pub type FunctionHandler =
    fn(bytes: &[u8], _ctx: ()) -> Pin<Box<dyn Future<Output = FunctionResult> + Send + '_>>;

/// Result of invoking a function.
pub struct FunctionResult {
    /// The serialised response body.
    pub body: Vec<u8>,
    /// HTTP status code (for HTTP triggers).
    pub status: u16,
}

impl FunctionResult {
    pub const fn ok(body: Vec<u8>) -> Self {
        Self { body, status: 200 }
    }

    pub const fn with_status(status: u16, body: Vec<u8>) -> Self {
        Self { body, status }
    }

    pub fn error(status: u16, message: &str) -> Self {
        let body = serde_json::to_vec(&serde_json::json!({ "error": message }))
            .unwrap_or_else(|_| b"{}".to_vec());
        Self { body, status }
    }
}

// Register the inventory collection for function metadata
inventory::collect!(FunctionMetadata);

/// Get all registered functions.
pub fn get_functions() -> impl Iterator<Item = &'static FunctionMetadata> {
    inventory::iter::<FunctionMetadata>()
}

/// Find a function by name.
pub fn find_function(name: &str) -> Option<&'static FunctionMetadata> {
    get_functions().find(|f| f.name == name)
}

/// Get all HTTP-triggered functions.
pub fn get_http_functions() -> impl Iterator<Item = &'static FunctionMetadata> {
    get_functions().filter(|f| f.trigger_kind == TriggerKind::Http)
}

/// Get all queue-triggered functions.
pub fn get_queue_functions() -> impl Iterator<Item = &'static FunctionMetadata> {
    get_functions().filter(|f| f.trigger_kind == TriggerKind::Queue)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_result() {
        let result = FunctionResult::ok(b"{}".to_vec());
        assert_eq!(result.status, 200);

        let result = FunctionResult::error(400, "bad request");
        assert_eq!(result.status, 400);
    }
}
