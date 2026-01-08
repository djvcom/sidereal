//! Error tracking module for sidereal-telemetry.
//!
//! Provides Datadog-style error tracking built on telemetry data:
//!
//! - **Detection**: Identify errors from spans (status_code = ERROR) and logs
//!   (severity >= ERROR or exception.type present)
//! - **Fingerprinting**: Group similar errors by normalising variable data
//!   (timestamps, IDs, line numbers) and computing SHA-256 hash
//! - **Aggregation**: Query-time grouping of errors with counts and statistics
//! - **API**: REST endpoints for listing, detail, samples, and timeline
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
//! │  ErrorDetector  │────>│ ErrorFingerprint │────>│ ErrorAggregator │
//! │  (detection.rs) │     │  (fingerprint.rs)│     │ (aggregation.rs)│
//! └─────────────────┘     └──────────────────┘     └─────────────────┘
//!         │                        │                        │
//!         v                        v                        v
//!   Identify errors         Normalise &            Query DataFusion
//!   from spans/logs         compute hash           aggregate results
//! ```
//!
//! # Example
//!
//! ```ignore
//! use sidereal_telemetry::errors::{
//!     ErrorDetector, ErrorFingerprinter, ErrorFilter,
//! };
//!
//! // Detect if a span is an error
//! let detector = ErrorDetector::new();
//! if detector.is_span_error(status_code) {
//!     // Compute fingerprint for grouping
//!     let fingerprinter = ErrorFingerprinter::new();
//!     let fingerprint = fingerprinter.compute(
//!         exception_type.as_deref(),
//!         message.as_deref(),
//!         stacktrace.as_deref(),
//!         &service_name,
//!     );
//! }
//! ```

mod aggregation;
pub mod api;
mod detection;
mod fingerprint;
mod normalise;
mod queries;
mod types;

// Re-export public API
pub use aggregation::{ErrorAggregator, ErrorComparison, ErrorDelta};
pub use api::{error_router, ErrorApiState};
pub use detection::{
    ErrorConditionBuilder, ErrorDetector, LOG_ERROR_CONDITION, LOG_SEVERITY_ERROR,
    SPAN_ERROR_CONDITION, SPAN_STATUS_ERROR,
};
pub use fingerprint::{
    compute_fingerprint, ErrorFingerprinter, FingerprintConfig, DEFAULT_MAX_FRAMES,
};
pub use normalise::{normalise_message, normalise_stacktrace};
pub use queries::{
    ErrorAggregationQueryBuilder, ErrorCountQueryBuilder, ErrorSamplesQueryBuilder,
    ErrorTimelineQueryBuilder, DEFAULT_ERROR_LIMIT, DEFAULT_SAMPLE_LIMIT,
};
pub use types::{
    span_id_from_hex, span_id_to_hex, trace_id_from_hex, trace_id_to_hex, ErrorFilter, ErrorGroup,
    ErrorSample, ErrorSortBy, ErrorSource, ErrorStats, ErrorTrend, SpanId, TraceId,
};
