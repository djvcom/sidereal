//! Trigger types that determine how functions are invoked.
//!
//! The type of the first parameter to a `#[sidereal_sdk::function]` determines
//! how the function is triggered:
//!
//! - `HttpRequest<T>` - Triggered by HTTP POST requests
//! - `QueueMessage<T>` - Triggered by messages on a queue (future)
//! - `Schedule<S>` - Triggered on a cron schedule (future)

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;

/// HTTP request trigger.
///
/// Functions with this as their first parameter are exposed as HTTP endpoints.
///
/// # Example
///
/// ```no_run
/// use sidereal_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// struct CreateOrderPayload { item_id: String }
///
/// #[derive(Serialize, Deserialize)]
/// struct Order { id: String }
///
/// #[sidereal_sdk::function]
/// async fn create_order(
///     req: HttpRequest<CreateOrderPayload>,
/// ) -> HttpResponse<Order> {
///     HttpResponse::ok(Order { id: "123".into() })
/// }
/// ```
#[derive(Debug, Clone)]
pub struct HttpRequest<T> {
    /// The deserialised request body.
    pub body: T,
    /// HTTP headers from the request.
    pub headers: HashMap<String, String>,
    /// Query parameters.
    pub query: HashMap<String, String>,
    /// The request path (e.g., "/orders/123").
    pub path: String,
    /// HTTP method (typically POST for functions).
    pub method: String,
}

impl<T: DeserializeOwned> HttpRequest<T> {
    /// Create a new HTTP request with the given body.
    pub fn new(body: T) -> Self {
        Self {
            body,
            headers: HashMap::new(),
            query: HashMap::new(),
            path: String::new(),
            method: "POST".to_string(),
        }
    }

    /// Create a request with headers.
    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }

    /// Create a request with query parameters.
    pub fn with_query(mut self, query: HashMap<String, String>) -> Self {
        self.query = query;
        self
    }

    /// Create a request with a path.
    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }
}

/// HTTP response from a function.
///
/// # Example
///
/// ```no_run
/// use sidereal_sdk::prelude::*;
///
/// #[derive(Serialize)]
/// struct Order { id: String }
///
/// let order = Order { id: "123".into() };
/// let _ = HttpResponse::ok(order);
/// let _ = HttpResponse::bad_request("Invalid input");
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpResponse<T> {
    /// The response body.
    pub body: T,
    /// HTTP status code.
    #[serde(skip)]
    pub status: u16,
    /// Response headers.
    #[serde(skip)]
    pub headers: HashMap<String, String>,
}

impl<T: Serialize> HttpResponse<T> {
    /// Create a response with a custom status code.
    pub fn new(status: u16, body: T) -> Self {
        Self {
            body,
            status,
            headers: HashMap::new(),
        }
    }

    /// Create a 200 OK response.
    pub fn ok(body: T) -> Self {
        Self::new(200, body)
    }

    /// Create a 201 Created response.
    pub fn created(body: T) -> Self {
        Self::new(201, body)
    }

    /// Create a 204 No Content response (body will be ignored).
    pub fn no_content(body: T) -> Self {
        Self::new(204, body)
    }

    /// Add a header to the response.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// Error response helper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }
}

impl HttpResponse<ErrorResponse> {
    /// Create a 400 Bad Request response.
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(400, ErrorResponse::new(message))
    }

    /// Create a 404 Not Found response.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(404, ErrorResponse::new(message))
    }

    /// Create a 500 Internal Server Error response.
    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new(500, ErrorResponse::new(message))
    }
}

/// Marker trait for trigger types.
///
/// This is used by the proc macro to determine the trigger kind.
pub trait Trigger {
    /// The kind of trigger (http, queue, schedule).
    const KIND: TriggerKind;
}

/// The kind of trigger for a function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerKind {
    Http,
    Queue,
    Schedule,
}

impl<T> Trigger for HttpRequest<T> {
    const KIND: TriggerKind = TriggerKind::Http;
}

/// Queue message trigger.
///
/// Functions with this as their first parameter are triggered when messages
/// arrive on a queue. The queue name is derived from the message type name
/// by convention (e.g., `OrderCreated` → `order-created`).
///
/// # Example
///
/// ```no_run
/// use sidereal_sdk::prelude::*;
///
/// #[derive(Serialize, Deserialize)]
/// struct OrderCreated { order_id: String }
///
/// #[derive(Serialize, Deserialize)]
/// struct ProcessResult { success: bool }
///
/// #[sidereal_sdk::function]
/// async fn process_order(
///     msg: QueueMessage<OrderCreated>,
/// ) -> HttpResponse<ProcessResult> {
///     // Process the message...
///     HttpResponse::ok(ProcessResult { success: true })
/// }
/// ```
#[derive(Debug, Clone)]
pub struct QueueMessage<T> {
    /// The deserialised message body.
    pub body: T,
    /// The name of the queue this message came from.
    pub queue_name: String,
    /// Unique identifier for this message.
    pub message_id: String,
    /// Unix timestamp (milliseconds) when the message was published.
    pub timestamp: u64,
    /// Number of times this message has been delivered (starts at 1).
    pub delivery_count: u32,
}

impl<T: DeserializeOwned> QueueMessage<T> {
    /// Create a new queue message with the given body.
    pub fn new(body: T, queue_name: impl Into<String>) -> Self {
        Self {
            body,
            queue_name: queue_name.into(),
            message_id: String::new(),
            timestamp: 0,
            delivery_count: 1,
        }
    }

    /// Create a message with full metadata.
    pub fn with_metadata(
        body: T,
        queue_name: impl Into<String>,
        message_id: impl Into<String>,
        timestamp: u64,
        delivery_count: u32,
    ) -> Self {
        Self {
            body,
            queue_name: queue_name.into(),
            message_id: message_id.into(),
            timestamp,
            delivery_count,
        }
    }
}

impl<T> Trigger for QueueMessage<T> {
    const KIND: TriggerKind = TriggerKind::Queue;
}

/// Convert a type name to a queue name using kebab-case convention.
///
/// Examples:
/// - `OrderCreated` → `order-created`
/// - `UserNotification` → `user-notification`
/// - `HTTPRequest` → `http-request`
pub fn type_name_to_queue_name(type_name: &str) -> String {
    let mut result = String::with_capacity(type_name.len() + 4);
    let mut chars = type_name.chars().peekable();

    while let Some(c) = chars.next() {
        if c.is_uppercase() {
            if !result.is_empty() {
                // Check if next char is lowercase (start of new word)
                // or if previous was lowercase (end of acronym)
                let next_is_lower = chars.peek().is_some_and(|n| n.is_lowercase());
                if next_is_lower || result.chars().last().is_some_and(|p| p.is_lowercase()) {
                    result.push('-');
                }
            }
            result.push(c.to_ascii_lowercase());
        } else {
            result.push(c);
        }
    }

    result
}

// Future: Schedule trigger
//
// pub struct Schedule<const CRON: &'static str> { ... }
// impl<const CRON: &'static str> Trigger for Schedule<CRON> { const KIND: TriggerKind = TriggerKind::Schedule; }
