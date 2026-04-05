//! Error fingerprinting for grouping similar errors.
//!
//! The fingerprint is a SHA-256 hash computed from normalised error components,
//! enabling grouping of errors that have the same root cause but different
//! variable data (timestamps, IDs, line numbers, etc.).

use sha2::{Digest, Sha256};

use super::normalise::{normalise_message, normalise_stacktrace};

/// Default maximum number of stack frames to include in fingerprint.
pub const DEFAULT_MAX_FRAMES: usize = 5;

/// Configuration for fingerprint computation.
#[derive(Debug, Clone)]
pub struct FingerprintConfig {
    /// Maximum stack frames to include (0 = unlimited).
    pub max_frames: usize,
    /// Include service name in fingerprint.
    pub include_service: bool,
    /// Include error message in fingerprint.
    pub include_message: bool,
}

impl Default for FingerprintConfig {
    fn default() -> Self {
        Self {
            max_frames: DEFAULT_MAX_FRAMES,
            include_service: true,
            include_message: true,
        }
    }
}

/// Computes error fingerprints for grouping similar errors.
#[derive(Debug, Clone)]
pub struct ErrorFingerprinter {
    config: FingerprintConfig,
}

impl ErrorFingerprinter {
    /// Create a new fingerprinter with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: FingerprintConfig::default(),
        }
    }

    /// Create a new fingerprinter with custom configuration.
    #[must_use]
    pub const fn with_config(config: FingerprintConfig) -> Self {
        Self { config }
    }

    /// Compute fingerprint for an error.
    ///
    /// The fingerprint is computed from:
    /// 1. Error type (exception class name)
    /// 2. Normalised error message (if configured)
    /// 3. Normalised stacktrace (top N frames)
    /// 4. Service name (if configured)
    ///
    /// # Arguments
    ///
    /// * `error_type` - Exception type (e.g., "NullPointerException")
    /// * `message` - Error message
    /// * `stacktrace` - Full stacktrace
    /// * `service_name` - Service that produced the error
    ///
    /// # Returns
    ///
    /// SHA-256 fingerprint as a hex string.
    #[must_use]
    pub fn compute(
        &self,
        error_type: Option<&str>,
        message: Option<&str>,
        stacktrace: Option<&str>,
        service_name: &str,
    ) -> String {
        const DELIMITER: &[u8] = b"\x00";

        let mut hasher = Sha256::new();

        // 1. Error type (or empty)
        if let Some(et) = error_type {
            hasher.update(et.as_bytes());
        }
        hasher.update(DELIMITER);

        // 2. Normalised message (if configured)
        if self.config.include_message {
            if let Some(msg) = message {
                let normalised = normalise_message(msg);
                hasher.update(normalised.as_bytes());
            }
        }
        hasher.update(DELIMITER);

        // 3. Normalised stacktrace
        if let Some(st) = stacktrace {
            let normalised = normalise_stacktrace(st, self.config.max_frames);
            hasher.update(normalised.as_bytes());
        }
        hasher.update(DELIMITER);

        // 4. Service name (if configured)
        if self.config.include_service {
            hasher.update(service_name.as_bytes());
        }

        // Finalise and convert to hex
        let result = hasher.finalize();
        hex::encode(result)
    }

    /// Compute fingerprint from individual components without service isolation.
    ///
    /// Use this when grouping errors across services.
    #[must_use]
    pub fn compute_cross_service(
        &self,
        error_type: Option<&str>,
        message: Option<&str>,
        stacktrace: Option<&str>,
    ) -> String {
        let config = FingerprintConfig {
            include_service: false,
            ..self.config.clone()
        };
        let fp = Self::with_config(config);
        fp.compute(error_type, message, stacktrace, "")
    }
}

impl Default for ErrorFingerprinter {
    fn default() -> Self {
        Self::new()
    }
}

/// Convenience function to compute fingerprint with default settings.
#[must_use]
pub fn compute_fingerprint(
    error_type: Option<&str>,
    message: Option<&str>,
    stacktrace: Option<&str>,
    service_name: &str,
) -> String {
    ErrorFingerprinter::new().compute(error_type, message, stacktrace, service_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_error_same_fingerprint() {
        let fp = ErrorFingerprinter::new();

        let f1 = fp.compute(
            Some("NullPointerException"),
            Some("Cannot call method on null"),
            Some("at com.example.Foo.bar(Foo.java:123)"),
            "api-service",
        );

        let f2 = fp.compute(
            Some("NullPointerException"),
            Some("Cannot call method on null"),
            Some("at com.example.Foo.bar(Foo.java:456)"), // Different line number
            "api-service",
        );

        assert_eq!(f1, f2);
    }

    #[test]
    fn different_error_types_different_fingerprint() {
        let fp = ErrorFingerprinter::new();

        let f1 = fp.compute(
            Some("NullPointerException"),
            Some("Error message"),
            None,
            "api-service",
        );

        let f2 = fp.compute(
            Some("IllegalArgumentException"),
            Some("Error message"),
            None,
            "api-service",
        );

        assert_ne!(f1, f2);
    }

    #[test]
    fn different_services_different_fingerprint() {
        let fp = ErrorFingerprinter::new();

        let f1 = fp.compute(
            Some("TimeoutException"),
            Some("Connection timed out"),
            None,
            "api-service",
        );

        let f2 = fp.compute(
            Some("TimeoutException"),
            Some("Connection timed out"),
            None,
            "worker-service",
        );

        assert_ne!(f1, f2);
    }

    #[test]
    fn cross_service_fingerprint_ignores_service() {
        let fp = ErrorFingerprinter::new();

        let f1 =
            fp.compute_cross_service(Some("TimeoutException"), Some("Connection timed out"), None);

        let f2 =
            fp.compute_cross_service(Some("TimeoutException"), Some("Connection timed out"), None);

        assert_eq!(f1, f2);
    }

    #[test]
    fn variable_data_normalised() {
        let fp = ErrorFingerprinter::new();

        let f1 = fp.compute(
            Some("RequestError"),
            Some("Request 550e8400-e29b-41d4-a716-446655440000 failed"),
            None,
            "api-service",
        );

        let f2 = fp.compute(
            Some("RequestError"),
            Some("Request 12345678-1234-1234-1234-123456789012 failed"),
            None,
            "api-service",
        );

        assert_eq!(f1, f2);
    }

    #[test]
    fn fingerprint_is_valid_hex() {
        let fp = ErrorFingerprinter::new();
        let result = fp.compute(Some("Error"), Some("message"), None, "service");

        // Should be 64 hex characters (SHA-256 = 256 bits = 32 bytes = 64 hex chars)
        assert_eq!(result.len(), 64);
        assert!(result.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn empty_components_handled() {
        let fp = ErrorFingerprinter::new();

        // Should not panic with all None/empty
        let result = fp.compute(None, None, None, "");
        assert_eq!(result.len(), 64);
    }

    #[test]
    fn convenience_function_works() {
        let result = compute_fingerprint(
            Some("TestError"),
            Some("Test message"),
            Some("at test.rs:1"),
            "test-service",
        );

        assert_eq!(result.len(), 64);
    }

    #[test]
    fn config_without_message() {
        let config = FingerprintConfig {
            include_message: false,
            ..Default::default()
        };
        let fp = ErrorFingerprinter::with_config(config);

        let f1 = fp.compute(
            Some("Error"),
            Some("Message A"),
            Some("stacktrace"),
            "service",
        );

        let f2 = fp.compute(
            Some("Error"),
            Some("Message B"), // Different message
            Some("stacktrace"),
            "service",
        );

        // Should be equal since message is not included
        assert_eq!(f1, f2);
    }
}
