//! Envelope types for protocol messages.

use rkyv::{Archive, Deserialize, Serialize};

use crate::types::CorrelationId;

/// A protocol envelope containing header metadata and a typed payload.
///
/// The envelope wraps all messages with common metadata including:
/// - Protocol version for compatibility checking
/// - Correlation ID for request/response matching
/// - Generic metadata for OTEL context propagation
/// - Timestamps and deadlines
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Envelope<T> {
    /// Envelope header with metadata.
    pub header: EnvelopeHeader,

    /// The message payload.
    pub payload: T,
}

impl<T> Envelope<T> {
    /// Creates a new envelope with the given payload.
    ///
    /// Uses the current protocol version and generates a new correlation ID.
    #[must_use]
    pub fn new(payload: T) -> Self {
        Self {
            header: EnvelopeHeader::new(),
            payload,
        }
    }

    /// Creates a new envelope with the given payload and deadline.
    #[must_use]
    pub fn with_deadline(payload: T, deadline_ns: u64) -> Self {
        Self {
            header: EnvelopeHeader::with_deadline(deadline_ns),
            payload,
        }
    }

    /// Creates a response envelope echoing the request's correlation ID.
    #[must_use]
    pub fn response_to(request_header: &EnvelopeHeader, payload: T) -> Self {
        Self {
            header: EnvelopeHeader::response_to(request_header),
            payload,
        }
    }
}

/// Header metadata for all protocol envelopes.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeHeader {
    /// Protocol version.
    ///
    /// Receivers should reject messages with unsupported versions.
    pub version: u16,

    /// Correlation ID for request/response matching.
    ///
    /// Responses must echo the correlation ID from the request.
    pub correlation_id: CorrelationId,

    /// Generic key-value metadata for context propagation.
    ///
    /// Used by OTEL TextMapPropagator for trace context injection/extraction.
    /// The protocol is format-agnostic - propagation format is configured
    /// at SDK initialisation.
    pub metadata: Vec<(String, String)>,

    /// Message timestamp in nanoseconds since Unix epoch.
    pub timestamp_ns: u64,

    /// Optional deadline in nanoseconds since Unix epoch.
    ///
    /// Receivers should reject requests past their deadline.
    pub deadline_ns: Option<u64>,
}

impl EnvelopeHeader {
    /// Creates a new header with default values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            version: crate::version::CURRENT,
            correlation_id: CorrelationId::new(),
            metadata: Vec::new(),
            timestamp_ns: current_timestamp_ns(),
            deadline_ns: None,
        }
    }

    /// Creates a new header with a deadline.
    #[must_use]
    pub fn with_deadline(deadline_ns: u64) -> Self {
        Self {
            version: crate::version::CURRENT,
            correlation_id: CorrelationId::new(),
            metadata: Vec::new(),
            timestamp_ns: current_timestamp_ns(),
            deadline_ns: Some(deadline_ns),
        }
    }

    /// Creates a response header echoing the request's correlation ID.
    #[must_use]
    pub fn response_to(request: &Self) -> Self {
        Self {
            version: crate::version::CURRENT,
            correlation_id: request.correlation_id,
            metadata: Vec::new(),
            timestamp_ns: current_timestamp_ns(),
            deadline_ns: None,
        }
    }

    /// Checks if this message version is compatible.
    #[must_use]
    pub const fn is_compatible(&self) -> bool {
        self.version >= crate::version::MIN_SUPPORTED && self.version <= crate::version::CURRENT
    }

    /// Checks if this message has exceeded its deadline.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.deadline_ns
            .is_some_and(|deadline| current_timestamp_ns() > deadline)
    }

    /// Gets metadata value by key.
    #[must_use]
    pub fn get_metadata(&self, key: &str) -> Option<&str> {
        self.metadata
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    /// Sets or replaces a metadata value.
    pub fn set_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();

        if let Some(entry) = self.metadata.iter_mut().find(|(k, _)| k == &key) {
            entry.1 = value;
        } else {
            self.metadata.push((key, value));
        }
    }
}

impl Default for EnvelopeHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// Returns the current timestamp in nanoseconds since Unix epoch.
#[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
fn current_timestamp_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_new() {
        let envelope = Envelope::new("test payload");
        assert_eq!(envelope.header.version, crate::version::CURRENT);
        assert!(envelope.header.metadata.is_empty());
        assert!(envelope.header.deadline_ns.is_none());
    }

    #[test]
    fn envelope_with_deadline() {
        let deadline = current_timestamp_ns() + 1_000_000_000; // 1 second
        let envelope = Envelope::with_deadline("test", deadline);
        assert_eq!(envelope.header.deadline_ns, Some(deadline));
        assert!(!envelope.header.is_expired());
    }

    #[test]
    fn header_response_echoes_correlation_id() {
        let request = EnvelopeHeader::new();
        let response = EnvelopeHeader::response_to(&request);
        assert_eq!(request.correlation_id, response.correlation_id);
    }

    #[test]
    fn header_compatibility() {
        let header = EnvelopeHeader::new();
        assert!(header.is_compatible());

        let old_header = EnvelopeHeader {
            version: 0,
            ..Default::default()
        };
        assert!(!old_header.is_compatible());
    }

    #[test]
    fn header_metadata_operations() {
        let mut header = EnvelopeHeader::new();

        // Set metadata
        header.set_metadata("traceparent", "00-abc-def-01");
        assert_eq!(header.get_metadata("traceparent"), Some("00-abc-def-01"));

        // Replace metadata
        header.set_metadata("traceparent", "00-xyz-uvw-00");
        assert_eq!(header.get_metadata("traceparent"), Some("00-xyz-uvw-00"));

        // Non-existent key
        assert_eq!(header.get_metadata("nonexistent"), None);
    }

    #[test]
    fn header_expiry() {
        // Expired deadline (in the past)
        let past = current_timestamp_ns().saturating_sub(1_000_000_000);
        let header = EnvelopeHeader {
            deadline_ns: Some(past),
            ..Default::default()
        };
        assert!(header.is_expired());

        // No deadline = never expired
        let no_deadline = EnvelopeHeader::new();
        assert!(!no_deadline.is_expired());
    }
}
