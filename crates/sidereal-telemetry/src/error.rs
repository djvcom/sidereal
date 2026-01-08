//! Error types for the telemetry crate.

use std::io;

/// Errors that can occur in the telemetry pipeline.
///
/// Large error types are boxed to keep the enum size small, which improves
/// performance when passing `Result<T, TelemetryError>` on the stack.
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Arrow error (boxed - large type).
    #[error("Arrow error: {0}")]
    Arrow(Box<arrow::error::ArrowError>),

    /// Parquet error (boxed - large type).
    #[error("Parquet error: {0}")]
    Parquet(Box<parquet::errors::ParquetError>),

    /// DataFusion error (boxed - large type).
    #[error("DataFusion error: {0}")]
    DataFusion(Box<datafusion::error::DataFusionError>),

    /// Object store error (boxed - large type).
    #[error("object store error: {0}")]
    ObjectStore(Box<object_store::Error>),

    /// URL parse error.
    #[error("URL parse error: {0}")]
    UrlParse(#[from] url::ParseError),

    /// CBOR serialisation error.
    #[error("CBOR serialisation error: {0}")]
    Cbor(String),

    /// gRPC status error.
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),

    /// Buffer overflow - backpressure needed.
    #[error("buffer overflow: {message}")]
    BufferOverflow { message: String },

    /// Request too large - exceeds max_records_per_request.
    #[error("request too large: {records} records exceeds limit of {limit}")]
    RequestTooLarge {
        /// Number of records in the request.
        records: usize,
        /// Maximum allowed records.
        limit: usize,
    },

    /// Invalid OTLP data.
    #[error("invalid OTLP data: {0}")]
    InvalidOtlp(String),

    /// Invalid content type in HTTP request.
    #[error("unsupported content type: {content_type}")]
    InvalidContentType { content_type: String },

    /// Protobuf decoding error.
    #[error("protobuf decode error: {source}")]
    ProtoDecode { source: prost::DecodeError },

    /// JSON decoding error.
    #[error("JSON decode error: {source}")]
    JsonDecode { source: serde_json::Error },

    /// JSON encoding error.
    #[error("JSON encode error: {source}")]
    JsonEncode { source: serde_json::Error },

    /// Query timed out.
    #[error("query timed out after {duration:?}")]
    QueryTimeout {
        /// The duration after which the query timed out.
        duration: std::time::Duration,
    },
}

// Manual From implementations for boxed error types
impl From<arrow::error::ArrowError> for TelemetryError {
    fn from(err: arrow::error::ArrowError) -> Self {
        Self::Arrow(Box::new(err))
    }
}

impl From<parquet::errors::ParquetError> for TelemetryError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        Self::Parquet(Box::new(err))
    }
}

impl From<datafusion::error::DataFusionError> for TelemetryError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        Self::DataFusion(Box::new(err))
    }
}

impl From<object_store::Error> for TelemetryError {
    fn from(err: object_store::Error) -> Self {
        Self::ObjectStore(Box::new(err))
    }
}

impl<T> From<ciborium::ser::Error<T>> for TelemetryError
where
    T: std::fmt::Debug,
{
    fn from(err: ciborium::ser::Error<T>) -> Self {
        Self::Cbor(format!("{err:?}"))
    }
}

impl From<TelemetryError> for tonic::Status {
    fn from(err: TelemetryError) -> Self {
        match err {
            TelemetryError::BufferOverflow { message } => Self::unavailable(message),
            TelemetryError::RequestTooLarge { records, limit } => Self::resource_exhausted(
                format!("request too large: {records} records exceeds limit of {limit}"),
            ),
            TelemetryError::InvalidOtlp(msg) => Self::invalid_argument(msg),
            _ => Self::internal(err.to_string()),
        }
    }
}
