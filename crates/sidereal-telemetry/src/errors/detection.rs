//! Error detection from telemetry data.
//!
//! Detects errors from:
//! - Spans with `status_code = ERROR` (2)
//! - Logs with `severity_number >= 17` (ERROR level)
//! - Logs with `exception.type` attribute present

use super::types::ErrorSource;

/// OpenTelemetry span status code for ERROR.
pub const SPAN_STATUS_ERROR: u8 = 2;

/// Minimum severity number for ERROR level logs.
/// Severity 17 = ERROR in OpenTelemetry semantic conventions.
pub const LOG_SEVERITY_ERROR: u8 = 17;

/// Rules for detecting errors in telemetry data.
#[derive(Debug, Clone)]
pub struct ErrorDetector {
    /// Minimum log severity to consider as error.
    pub min_log_severity: u8,
    /// Whether to detect errors from logs with exception.type attribute.
    pub detect_exception_logs: bool,
}

impl ErrorDetector {
    /// Create a new error detector with default settings.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            min_log_severity: LOG_SEVERITY_ERROR,
            detect_exception_logs: true,
        }
    }

    /// Check if a span status code indicates an error.
    #[must_use]
    pub const fn is_span_error(&self, status_code: u8) -> bool {
        status_code == SPAN_STATUS_ERROR
    }

    /// Check if a log entry should be treated as an error.
    ///
    /// A log is an error if:
    /// - severity_number >= min_log_severity (default 17 = ERROR), OR
    /// - exception_type is present (if detect_exception_logs is true)
    #[must_use]
    pub const fn is_log_error(&self, severity_number: u8, has_exception_type: bool) -> bool {
        severity_number >= self.min_log_severity
            || (self.detect_exception_logs && has_exception_type)
    }

    /// Determine the error source based on data availability.
    #[must_use]
    pub const fn classify_source(
        &self,
        span_status_code: Option<u8>,
        log_severity: Option<u8>,
    ) -> Option<ErrorSource> {
        if let Some(status) = span_status_code {
            if self.is_span_error(status) {
                return Some(ErrorSource::Span);
            }
        }
        if let Some(severity) = log_severity {
            if severity >= self.min_log_severity {
                return Some(ErrorSource::Log);
            }
        }
        None
    }
}

impl Default for ErrorDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// SQL WHERE clause fragment for detecting error spans.
pub const SPAN_ERROR_CONDITION: &str = "status_code = 2";

/// SQL WHERE clause fragment for detecting error logs.
pub const LOG_ERROR_CONDITION: &str = "(severity_number >= 17 OR \"exception.type\" IS NOT NULL)";

/// Build a SQL WHERE clause for error detection.
#[derive(Debug, Default)]
pub struct ErrorConditionBuilder {
    conditions: Vec<String>,
}

impl ErrorConditionBuilder {
    /// Create a new condition builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add span error condition.
    #[must_use]
    pub fn with_span_errors(mut self) -> Self {
        self.conditions.push(SPAN_ERROR_CONDITION.to_owned());
        self
    }

    /// Add log error condition.
    #[must_use]
    pub fn with_log_errors(mut self) -> Self {
        self.conditions.push(LOG_ERROR_CONDITION.to_owned());
        self
    }

    /// Add custom condition.
    #[must_use]
    pub fn with_condition(mut self, condition: impl Into<String>) -> Self {
        self.conditions.push(condition.into());
        self
    }

    /// Build the WHERE clause (conditions joined with OR).
    #[must_use]
    pub fn build_or(mut self) -> String {
        match self.conditions.len() {
            0 => "1=1".to_owned(),
            1 => self.conditions.pop().unwrap_or_default(),
            _ => format!("({})", self.conditions.join(" OR ")),
        }
    }

    /// Build the WHERE clause (conditions joined with AND).
    #[must_use]
    pub fn build_and(mut self) -> String {
        match self.conditions.len() {
            0 => "1=1".to_owned(),
            1 => self.conditions.pop().unwrap_or_default(),
            _ => format!("({})", self.conditions.join(" AND ")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_error_detection() {
        let detector = ErrorDetector::new();

        assert!(!detector.is_span_error(0)); // UNSET
        assert!(!detector.is_span_error(1)); // OK
        assert!(detector.is_span_error(2)); // ERROR
    }

    #[test]
    fn log_error_by_severity() {
        let detector = ErrorDetector::new();

        assert!(!detector.is_log_error(9, false)); // DEBUG
        assert!(!detector.is_log_error(13, false)); // WARN
        assert!(detector.is_log_error(17, false)); // ERROR
        assert!(detector.is_log_error(21, false)); // FATAL
    }

    #[test]
    fn log_error_by_exception_type() {
        let detector = ErrorDetector::new();

        // Low severity but has exception.type
        assert!(detector.is_log_error(9, true));
    }

    #[test]
    fn log_error_exception_detection_disabled() {
        let detector = ErrorDetector {
            detect_exception_logs: false,
            ..Default::default()
        };

        // Low severity with exception.type should not be error
        assert!(!detector.is_log_error(9, true));
        // High severity should still be error
        assert!(detector.is_log_error(17, false));
    }

    #[test]
    fn classify_source_span() {
        let detector = ErrorDetector::new();
        let source = detector.classify_source(Some(2), None);
        assert_eq!(source, Some(ErrorSource::Span));
    }

    #[test]
    fn classify_source_log() {
        let detector = ErrorDetector::new();
        let source = detector.classify_source(None, Some(17));
        assert_eq!(source, Some(ErrorSource::Log));
    }

    #[test]
    fn classify_source_none() {
        let detector = ErrorDetector::new();
        let source = detector.classify_source(Some(1), Some(9)); // OK span, DEBUG log
        assert_eq!(source, None);
    }

    #[test]
    fn condition_builder_span_only() {
        let condition = ErrorConditionBuilder::new().with_span_errors().build_or();
        assert_eq!(condition, "status_code = 2");
    }

    #[test]
    fn condition_builder_log_only() {
        let condition = ErrorConditionBuilder::new().with_log_errors().build_or();
        assert_eq!(
            condition,
            "(severity_number >= 17 OR \"exception.type\" IS NOT NULL)"
        );
    }

    #[test]
    fn condition_builder_combined() {
        let condition = ErrorConditionBuilder::new()
            .with_span_errors()
            .with_log_errors()
            .build_or();
        assert!(condition.starts_with('('));
        assert!(condition.contains(" OR "));
    }

    #[test]
    fn condition_builder_empty() {
        let condition = ErrorConditionBuilder::new().build_or();
        assert_eq!(condition, "1=1");
    }
}
