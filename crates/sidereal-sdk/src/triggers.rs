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
/// ```ignore
/// #[sidereal_sdk::function]
/// async fn create_order(
///     req: HttpRequest<CreateOrderPayload>,
///     ctx: Context,
/// ) -> HttpResponse<Order> {
///     // ...
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
/// ```ignore
/// HttpResponse::ok(order)
/// HttpResponse::created(order)
/// HttpResponse::bad_request("Invalid input")
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

// Future: QueueMessage and Schedule triggers
//
// pub struct QueueMessage<T> { ... }
// impl<T> Trigger for QueueMessage<T> { const KIND: TriggerKind = TriggerKind::Queue; }
//
// pub struct Schedule<const CRON: &'static str> { ... }
// impl<const CRON: &'static str> Trigger for Schedule<CRON> { const KIND: TriggerKind = TriggerKind::Schedule; }
