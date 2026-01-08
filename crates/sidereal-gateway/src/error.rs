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
    pub const fn error_type(&self) -> &'static str {
        match self {
            Self::Config(_) => "config_error",
            Self::FunctionNotFound(_) => "function_not_found",
            Self::InvalidFunctionName(_) => "invalid_function_name",
            Self::InvalidBackendUrl(_) => "invalid_backend_url",
            Self::ConnectionFailed(_) => "connection_failed",
            Self::RequestBuildFailed(_) => "request_build_failed",
            Self::BackendError(_) => "backend_error",
            Self::Timeout => "timeout",
            Self::RateLimitExceeded => "rate_limit_exceeded",
            Self::CircuitOpen => "circuit_open",
            Self::PayloadTooLarge => "payload_too_large",
            Self::UriTooLong => "uri_too_long",
            Self::TooManyHeaders => "too_many_headers",
            Self::VsockError(_) => "vsock_error",
            Self::ProtocolError(_) => "protocol_error",
            Self::Io(_) => "io_error",
            Self::ServiceProvisioning(_) => "service_provisioning",
            Self::AllWorkersUnhealthy(_) => "all_workers_unhealthy",
            Self::PlacementStore(_) => "placement_store_error",
        }
    }

    pub const fn status_code(&self) -> StatusCode {
        match self {
            Self::FunctionNotFound(_) => StatusCode::NOT_FOUND,
            Self::InvalidFunctionName(_) => StatusCode::BAD_REQUEST,
            Self::RateLimitExceeded => StatusCode::TOO_MANY_REQUESTS,
            Self::Timeout => StatusCode::GATEWAY_TIMEOUT,
            Self::CircuitOpen | Self::ServiceProvisioning(_) | Self::AllWorkersUnhealthy(_) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::UriTooLong => StatusCode::URI_TOO_LONG,
            Self::TooManyHeaders => StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
            Self::ConnectionFailed(_)
            | Self::BackendError(_)
            | Self::VsockError(_)
            | Self::ProtocolError(_) => StatusCode::BAD_GATEWAY,
            Self::Config(_)
            | Self::InvalidBackendUrl(_)
            | Self::RequestBuildFailed(_)
            | Self::PlacementStore(_)
            | Self::Io(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.status_code();

        // Sanitise error messages for external responses
        let message = match &self {
            // Safe to expose these messages
            Self::FunctionNotFound(name) => format!("Function not found: {name}"),
            Self::InvalidFunctionName(reason) => {
                format!("Invalid function name: {reason}")
            }
            Self::RateLimitExceeded => "Rate limit exceeded".to_owned(),
            Self::Timeout => "Request timeout".to_owned(),
            Self::CircuitOpen => "Service temporarily unavailable".to_owned(),
            Self::PayloadTooLarge => "Request body too large".to_owned(),
            Self::UriTooLong => "URI too long".to_owned(),
            Self::TooManyHeaders => "Too many request headers".to_owned(),
            Self::ServiceProvisioning(func) => {
                format!("Service provisioning: {func}")
            }
            Self::AllWorkersUnhealthy(func) => {
                format!("All workers unhealthy for function: {func}")
            }

            // Hide internal details for security
            Self::Config(_)
            | Self::InvalidBackendUrl(_)
            | Self::ConnectionFailed(_)
            | Self::RequestBuildFailed(_)
            | Self::BackendError(_)
            | Self::VsockError(_)
            | Self::ProtocolError(_)
            | Self::PlacementStore(_)
            | Self::Io(_) => "Internal server error".to_owned(),
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
