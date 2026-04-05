//! OTLP ingestion receivers.
//!
//! This module provides gRPC and HTTP receivers for OTLP telemetry data.

pub mod convert;
pub mod grpc;
pub mod http;

pub use convert::{
    convert_logs_to_arrow, convert_metrics_to_arrow, convert_traces_to_arrow, ConversionResult,
};
pub use grpc::{LogsServiceServer, MetricsServiceServer, OtlpGrpcReceiver, TraceServiceServer};
pub use http::{
    otlp_http_router, otlp_http_router_with_limit, OtlpHttpState, DEFAULT_MAX_BODY_SIZE,
};
