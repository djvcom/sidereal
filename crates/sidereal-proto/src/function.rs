//! Function invocation message types.

use rkyv::{Archive, Deserialize, Serialize};

/// Function invocation messages.
///
/// Used for gateway → worker and worker → worker communication.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum FunctionMessage {
    /// Function invocation request.
    Invoke(InvokeRequest),

    /// Function invocation response.
    Response(InvokeResponse),
}

/// Request to invoke a function.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InvokeRequest {
    /// Target function name.
    ///
    /// Must match `^[a-z0-9_]+$` pattern, max 64 characters.
    pub function_name: String,

    /// Serialised request payload.
    ///
    /// The SDK handles JSON encoding/decoding - the protocol carries raw bytes.
    pub payload: Vec<u8>,

    /// HTTP method if this is an HTTP-triggered function.
    pub http_method: Option<String>,

    /// HTTP headers if this is an HTTP-triggered function.
    ///
    /// Header values are bytes to support non-UTF8 values.
    pub http_headers: Vec<(String, Vec<u8>)>,
}

impl InvokeRequest {
    /// Creates a new invoke request with just function name and payload.
    #[must_use]
    pub fn new(function_name: impl Into<String>, payload: Vec<u8>) -> Self {
        Self {
            function_name: function_name.into(),
            payload,
            http_method: None,
            http_headers: Vec::new(),
        }
    }

    /// Creates an HTTP invoke request.
    #[must_use]
    pub fn http(
        function_name: impl Into<String>,
        method: impl Into<String>,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            function_name: function_name.into(),
            payload,
            http_method: Some(method.into()),
            http_headers: Vec::new(),
        }
    }

    /// Adds an HTTP header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.http_headers.push((name.into(), value.into()));
        self
    }

    /// Gets an HTTP header value by name (case-insensitive).
    #[must_use]
    pub fn get_header(&self, name: &str) -> Option<&[u8]> {
        let name_lower = name.to_lowercase();
        self.http_headers
            .iter()
            .find(|(k, _)| k.to_lowercase() == name_lower)
            .map(|(_, v)| v.as_slice())
    }
}

/// Response from a function invocation.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct InvokeResponse {
    /// HTTP status code.
    ///
    /// Standard codes: 200 success, 4xx client error, 5xx server error.
    pub status: u16,

    /// Response body.
    pub body: Vec<u8>,

    /// Response headers.
    pub headers: Vec<(String, Vec<u8>)>,
}

impl InvokeResponse {
    /// Creates a successful response (200 OK).
    #[must_use]
    pub const fn ok(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            body,
            headers: Vec::new(),
        }
    }

    /// Creates a response with the given status code.
    #[must_use]
    pub const fn with_status(status: u16, body: Vec<u8>) -> Self {
        Self {
            status,
            body,
            headers: Vec::new(),
        }
    }

    /// Creates an error response (500 Internal Server Error).
    #[must_use]
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            status: 500,
            body: message.into().into_bytes(),
            headers: Vec::new(),
        }
    }

    /// Creates a not found response (404).
    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: 404,
            body: message.into().into_bytes(),
            headers: Vec::new(),
        }
    }

    /// Creates a bad request response (400).
    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: 400,
            body: message.into().into_bytes(),
            headers: Vec::new(),
        }
    }

    /// Adds a response header.
    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Checks if this is a successful response (2xx).
    #[must_use]
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// Checks if this is a client error (4xx).
    #[must_use]
    pub fn is_client_error(&self) -> bool {
        (400..500).contains(&self.status)
    }

    /// Checks if this is a server error (5xx).
    #[must_use]
    pub fn is_server_error(&self) -> bool {
        (500..600).contains(&self.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invoke_request_new() {
        let req = InvokeRequest::new("greet", b"hello".to_vec());
        assert_eq!(req.function_name, "greet");
        assert_eq!(req.payload, b"hello");
        assert!(req.http_method.is_none());
        assert!(req.http_headers.is_empty());
    }

    #[test]
    fn invoke_request_http() {
        let req = InvokeRequest::http("greet", "POST", b"hello".to_vec())
            .with_header("Content-Type", b"application/json".to_vec());

        assert_eq!(req.http_method, Some("POST".to_string()));
        assert_eq!(
            req.get_header("content-type"),
            Some(b"application/json".as_slice())
        );
    }

    #[test]
    fn invoke_response_ok() {
        let resp = InvokeResponse::ok(b"success".to_vec());
        assert_eq!(resp.status, 200);
        assert!(resp.is_success());
        assert!(!resp.is_client_error());
        assert!(!resp.is_server_error());
    }

    #[test]
    fn invoke_response_error() {
        let resp = InvokeResponse::error("something went wrong");
        assert_eq!(resp.status, 500);
        assert!(resp.is_server_error());
    }

    #[test]
    fn invoke_response_not_found() {
        let resp = InvokeResponse::not_found("function not found");
        assert_eq!(resp.status, 404);
        assert!(resp.is_client_error());
    }

    #[test]
    fn function_message_variants() {
        let invoke = FunctionMessage::Invoke(InvokeRequest::new("test", vec![]));
        let response = FunctionMessage::Response(InvokeResponse::ok(vec![]));

        match invoke {
            FunctionMessage::Invoke(req) => assert_eq!(req.function_name, "test"),
            _ => panic!("expected Invoke"),
        }

        match response {
            FunctionMessage::Response(resp) => assert!(resp.is_success()),
            _ => panic!("expected Response"),
        }
    }
}
