//! Gateway error types.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum GatewayError {
    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Function not found: {0}")]
    FunctionNotFound(String),

    #[error("Invalid function name: {0}")]
    InvalidFunctionName(String),

    #[error("Invalid backend URL: {0}")]
    InvalidBackendUrl(String),

    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Request build failed: {0}")]
    RequestBuildFailed(String),

    #[error("Backend error: {0}")]
    BackendError(String),

    #[error("Request timeout")]
    Timeout,

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Circuit breaker open")]
    CircuitOpen,

    #[error("Request body too large")]
    PayloadTooLarge,

    #[error("URI too long")]
    UriTooLong,

    #[error("Too many headers")]
    TooManyHeaders,

    #[error("vsock error: {0}")]
    VsockError(String),

    #[error("Protocol error: {0}")]
    ProtocolError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Service provisioning: {0}")]
    ServiceProvisioning(String),

    #[error("All workers unhealthy: {0}")]
    AllWorkersUnhealthy(String),

    #[error("Placement store error: {0}")]
    PlacementStore(String),
}

impl GatewayError {
    pub fn error_type(&self) -> &'static str {
        match self {
            GatewayError::Config(_) => "config_error",
            GatewayError::FunctionNotFound(_) => "function_not_found",
            GatewayError::InvalidFunctionName(_) => "invalid_function_name",
            GatewayError::InvalidBackendUrl(_) => "invalid_backend_url",
            GatewayError::ConnectionFailed(_) => "connection_failed",
            GatewayError::RequestBuildFailed(_) => "request_build_failed",
            GatewayError::BackendError(_) => "backend_error",
            GatewayError::Timeout => "timeout",
            GatewayError::RateLimitExceeded => "rate_limit_exceeded",
            GatewayError::CircuitOpen => "circuit_open",
            GatewayError::PayloadTooLarge => "payload_too_large",
            GatewayError::UriTooLong => "uri_too_long",
            GatewayError::TooManyHeaders => "too_many_headers",
            GatewayError::VsockError(_) => "vsock_error",
            GatewayError::ProtocolError(_) => "protocol_error",
            GatewayError::Io(_) => "io_error",
            GatewayError::ServiceProvisioning(_) => "service_provisioning",
            GatewayError::AllWorkersUnhealthy(_) => "all_workers_unhealthy",
            GatewayError::PlacementStore(_) => "placement_store_error",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            GatewayError::FunctionNotFound(_) => StatusCode::NOT_FOUND,
            GatewayError::InvalidFunctionName(_) => StatusCode::BAD_REQUEST,
            GatewayError::RateLimitExceeded => StatusCode::TOO_MANY_REQUESTS,
            GatewayError::Timeout => StatusCode::GATEWAY_TIMEOUT,
            GatewayError::CircuitOpen
            | GatewayError::ServiceProvisioning(_)
            | GatewayError::AllWorkersUnhealthy(_) => StatusCode::SERVICE_UNAVAILABLE,
            GatewayError::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            GatewayError::UriTooLong => StatusCode::URI_TOO_LONG,
            GatewayError::TooManyHeaders => StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            GatewayError::ConnectionFailed(_)
            | GatewayError::BackendError(_)
            | GatewayError::VsockError(_)
            | GatewayError::ProtocolError(_) => StatusCode::BAD_GATEWAY,
            GatewayError::Config(_)
            | GatewayError::InvalidBackendUrl(_)
            | GatewayError::RequestBuildFailed(_)
            | GatewayError::PlacementStore(_)
            | GatewayError::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.status_code();

        // Sanitise error messages for external responses
        let message = match &self {
            // Safe to expose these messages
            GatewayError::FunctionNotFound(name) => format!("Function not found: {}", name),
            GatewayError::InvalidFunctionName(reason) => {
                format!("Invalid function name: {}", reason)
            }
            GatewayError::RateLimitExceeded => "Rate limit exceeded".to_string(),
            GatewayError::Timeout => "Request timeout".to_string(),
            GatewayError::CircuitOpen => "Service temporarily unavailable".to_string(),
            GatewayError::PayloadTooLarge => "Request body too large".to_string(),
            GatewayError::UriTooLong => "URI too long".to_string(),
            GatewayError::TooManyHeaders => "Too many request headers".to_string(),
            GatewayError::ServiceProvisioning(func) => {
                format!("Service provisioning: {}", func)
            }
            GatewayError::AllWorkersUnhealthy(func) => {
                format!("All workers unhealthy for function: {}", func)
            }

            // Hide internal details for security
            GatewayError::Config(_)
            | GatewayError::InvalidBackendUrl(_)
            | GatewayError::ConnectionFailed(_)
            | GatewayError::RequestBuildFailed(_)
            | GatewayError::BackendError(_)
            | GatewayError::VsockError(_)
            | GatewayError::ProtocolError(_)
            | GatewayError::PlacementStore(_)
            | GatewayError::Io(_) => "Internal server error".to_string(),
        };

        (status, message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_status_codes() {
        assert_eq!(
            GatewayError::FunctionNotFound("test".into()).status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            GatewayError::RateLimitExceeded.status_code(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            GatewayError::Timeout.status_code(),
            StatusCode::GATEWAY_TIMEOUT
        );
        assert_eq!(
            GatewayError::PayloadTooLarge.status_code(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }

    #[test]
    fn error_types() {
        assert_eq!(
            GatewayError::FunctionNotFound("test".into()).error_type(),
            "function_not_found"
        );
        assert_eq!(GatewayError::Timeout.error_type(), "timeout");
    }
}
