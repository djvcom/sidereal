//! OpenTelemetry context propagation utilities.
//!
//! This module provides `Injector` and `Extractor` implementations for
//! protocol metadata, enabling trace context propagation across boundaries.
//!
//! # Design
//!
//! The protocol uses generic `Vec<(String, String)>` metadata rather than
//! trace-specific fields. This keeps the protocol decoupled from specific
//! propagation formats (W3C TraceContext, B3, Jaeger, etc.).
//!
//! Propagation format is configured at SDK initialisation via
//! `opentelemetry::global::set_text_map_propagator()`.
//!
//! # Example
//!
//! ```ignore
//! use opentelemetry::global;
//! use sidereal_proto::{EnvelopeHeader, MetadataCarrier, MetadataExtractor};
//!
//! // Inject trace context into outgoing message
//! let mut header = EnvelopeHeader::new();
//! let propagator = global::get_text_map_propagator(|p| p.clone());
//! let cx = tracing::Span::current().context();
//! propagator.inject_context(&cx, &mut MetadataCarrier(&mut header.metadata));
//!
//! // Extract trace context from incoming message
//! let parent_cx = propagator.extract(&MetadataExtractor(&header.metadata));
//! ```

use opentelemetry::propagation::{Extractor, Injector};

/// Carrier for injecting trace context into envelope metadata.
///
/// Implements `opentelemetry::propagation::Injector` for use with
/// `TextMapPropagator::inject_context()`.
pub struct MetadataCarrier<'a>(pub &'a mut Vec<(String, String)>);

impl Injector for MetadataCarrier<'_> {
    fn set(&mut self, key: &str, value: String) {
        // Replace if key exists, otherwise append
        if let Some(entry) = self.0.iter_mut().find(|(k, _)| k == key) {
            entry.1 = value;
        } else {
            self.0.push((key.to_string(), value));
        }
    }
}

/// Extractor for reading trace context from envelope metadata.
///
/// Implements `opentelemetry::propagation::Extractor` for use with
/// `TextMapPropagator::extract()`.
pub struct MetadataExtractor<'a>(pub &'a [(String, String)]);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.iter().map(|(k, _)| k.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carrier_set_new_key() {
        let mut metadata = Vec::new();
        let mut carrier = MetadataCarrier(&mut metadata);

        carrier.set("traceparent", "00-abc-def-01".to_string());

        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata[0],
            ("traceparent".to_string(), "00-abc-def-01".to_string())
        );
    }

    #[test]
    fn carrier_replace_existing_key() {
        let mut metadata = vec![("traceparent".to_string(), "old-value".to_string())];
        let mut carrier = MetadataCarrier(&mut metadata);

        carrier.set("traceparent", "new-value".to_string());

        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata[0].1, "new-value");
    }

    #[test]
    fn carrier_multiple_keys() {
        let mut metadata = Vec::new();
        let mut carrier = MetadataCarrier(&mut metadata);

        carrier.set("traceparent", "00-abc-def-01".to_string());
        carrier.set("tracestate", "vendor=value".to_string());
        carrier.set("baggage", "key=value".to_string());

        assert_eq!(metadata.len(), 3);
    }

    #[test]
    fn extractor_get_existing() {
        let metadata = vec![
            ("traceparent".to_string(), "00-abc-def-01".to_string()),
            ("tracestate".to_string(), "vendor=value".to_string()),
        ];
        let extractor = MetadataExtractor(&metadata);

        assert_eq!(extractor.get("traceparent"), Some("00-abc-def-01"));
        assert_eq!(extractor.get("tracestate"), Some("vendor=value"));
    }

    #[test]
    fn extractor_get_nonexistent() {
        let metadata = vec![("traceparent".to_string(), "value".to_string())];
        let extractor = MetadataExtractor(&metadata);

        assert_eq!(extractor.get("nonexistent"), None);
    }

    #[test]
    fn extractor_keys() {
        let metadata = vec![
            ("traceparent".to_string(), "value1".to_string()),
            ("tracestate".to_string(), "value2".to_string()),
        ];
        let extractor = MetadataExtractor(&metadata);

        let keys = extractor.keys();
        assert!(keys.contains(&"traceparent"));
        assert!(keys.contains(&"tracestate"));
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn extractor_empty_metadata() {
        let metadata: Vec<(String, String)> = Vec::new();
        let extractor = MetadataExtractor(&metadata);

        assert_eq!(extractor.get("anything"), None);
        assert!(extractor.keys().is_empty());
    }
}
