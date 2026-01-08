//! Custom DataFusion UDFs for error tracking.
//!
//! Provides SQL-accessible functions for:
//! - `normalise_stacktrace(stacktrace)` - Normalise stacktrace for fingerprinting
//! - `error_fingerprint(error_type, message, stacktrace, service)` - Compute error fingerprint
//! - `cbor_extract(data, key)` - Extract value from CBOR-encoded attributes

mod cbor;
mod fingerprint;
mod normalise;

pub use cbor::create_cbor_extract_udf;
pub use fingerprint::create_error_fingerprint_udf;
pub use normalise::create_normalise_stacktrace_udf;

use datafusion::logical_expr::ScalarUDF;

/// Register all error tracking UDFs with a DataFusion context.
pub fn all_udfs() -> Vec<ScalarUDF> {
    vec![
        create_normalise_stacktrace_udf(),
        create_error_fingerprint_udf(),
        create_cbor_extract_udf(),
    ]
}
