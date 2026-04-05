//! tonic gRPC OTLP receivers.
//!
//! Implements the TraceService, MetricsService, and LogsService gRPC services
//! from the OpenTelemetry collector protocol.

use std::sync::Arc;

use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        logs_service_server::LogsService, ExportLogsPartialSuccess, ExportLogsServiceRequest,
        ExportLogsServiceResponse,
    },
    metrics::v1::{
        metrics_service_server::MetricsService, ExportMetricsPartialSuccess,
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    },
    trace::v1::{
        trace_service_server::TraceService, ExportTracePartialSuccess, ExportTraceServiceRequest,
        ExportTraceServiceResponse,
    },
};
use tonic::{Request, Response, Status};

use super::convert::{
    convert_logs_to_arrow, convert_metrics_to_arrow, convert_traces_to_arrow, ConversionResult,
};
use crate::buffer::Ingester;
use crate::redact::RedactionEngine;

/// Trait for building partial_success responses from ConversionResult.
trait PartialSuccessBuilder: Default {
    fn with_rejected_count(count: i64) -> Self;
    fn with_error_message(self, message: String) -> Self;
}

impl PartialSuccessBuilder for ExportTracePartialSuccess {
    fn with_rejected_count(count: i64) -> Self {
        Self {
            rejected_spans: count,
            error_message: String::new(),
        }
    }
    fn with_error_message(mut self, message: String) -> Self {
        self.error_message = message;
        self
    }
}

impl PartialSuccessBuilder for ExportMetricsPartialSuccess {
    fn with_rejected_count(count: i64) -> Self {
        Self {
            rejected_data_points: count,
            error_message: String::new(),
        }
    }
    fn with_error_message(mut self, message: String) -> Self {
        self.error_message = message;
        self
    }
}

impl PartialSuccessBuilder for ExportLogsPartialSuccess {
    fn with_rejected_count(count: i64) -> Self {
        Self {
            rejected_log_records: count,
            error_message: String::new(),
        }
    }
    fn with_error_message(mut self, message: String) -> Self {
        self.error_message = message;
        self
    }
}

/// Build a partial_success response from a ConversionResult.
fn build_partial_success<T: PartialSuccessBuilder>(result: &ConversionResult) -> Option<T> {
    if result.rejected_count == 0 && result.error_message.is_none() {
        return None;
    }

    let partial = T::with_rejected_count(result.rejected_count);
    Some(match &result.error_message {
        Some(msg) => partial.with_error_message(msg.clone()),
        None => partial,
    })
}

/// gRPC receiver for all OTLP signals.
#[derive(Clone)]
pub struct OtlpGrpcReceiver {
    trace_ingester: Arc<Ingester>,
    metrics_ingester: Arc<Ingester>,
    logs_ingester: Arc<Ingester>,
    redaction: Arc<RedactionEngine>,
}

impl OtlpGrpcReceiver {
    /// Create a new OTLP gRPC receiver with the given ingesters.
    pub const fn new(
        trace_ingester: Arc<Ingester>,
        metrics_ingester: Arc<Ingester>,
        logs_ingester: Arc<Ingester>,
        redaction: Arc<RedactionEngine>,
    ) -> Self {
        Self {
            trace_ingester,
            metrics_ingester,
            logs_ingester,
            redaction,
        }
    }
}

#[tonic::async_trait]
impl TraceService for OtlpGrpcReceiver {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let request = request.into_inner();

        let result = convert_traces_to_arrow(&request, Some(&self.redaction))?;
        let partial_success = build_partial_success::<ExportTracePartialSuccess>(&result);
        if result.batch.num_rows() > 0 {
            self.trace_ingester.ingest(result.batch).await?;
        }

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcReceiver {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let request = request.into_inner();

        let result = convert_metrics_to_arrow(&request, Some(&self.redaction))?;
        let partial_success = build_partial_success::<ExportMetricsPartialSuccess>(&result);
        if result.batch.num_rows() > 0 {
            self.metrics_ingester.ingest(result.batch).await?;
        }

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success,
        }))
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcReceiver {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let request = request.into_inner();

        let result = convert_logs_to_arrow(&request, Some(&self.redaction))?;
        let partial_success = build_partial_success::<ExportLogsPartialSuccess>(&result);
        if result.batch.num_rows() > 0 {
            self.logs_ingester.ingest(result.batch).await?;
        }

        Ok(Response::new(ExportLogsServiceResponse { partial_success }))
    }
}

/// Re-export the service servers for convenience.
pub use opentelemetry_proto::tonic::collector::{
    logs::v1::logs_service_server::LogsServiceServer,
    metrics::v1::metrics_service_server::MetricsServiceServer,
    trace::v1::trace_service_server::TraceServiceServer,
};

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, number_data_point, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};

    use crate::config::{BufferConfig, ParquetConfig};
    use crate::redact::{RedactionConfig, RedactionEngine};
    use crate::schema::{
        logs::logs_storage_schema, metrics::number_metrics_storage_schema,
        traces::traces_storage_schema,
    };
    use crate::storage::Signal;

    fn test_config() -> (BufferConfig, ParquetConfig) {
        (
            BufferConfig {
                max_batch_size: 1000,
                flush_interval_secs: 30,
                max_buffer_bytes: 10 * 1024 * 1024,
                max_records_per_request: 100_000,
                flush_max_retries: 3,
                flush_initial_delay_ms: 10,
                flush_max_delay_ms: 100,
            },
            ParquetConfig {
                row_group_size: 1000,
                compression: "zstd".to_string(),
            },
        )
    }

    fn test_receiver() -> OtlpGrpcReceiver {
        let store = Arc::new(InMemory::new());
        let (buffer_config, parquet_config) = test_config();

        let trace_ingester = Arc::new(Ingester::new(
            Signal::Traces,
            traces_storage_schema(),
            store.clone(),
            buffer_config.clone(),
            parquet_config.clone(),
        ));
        let metrics_ingester = Arc::new(Ingester::new(
            Signal::Metrics,
            number_metrics_storage_schema(),
            store.clone(),
            buffer_config.clone(),
            parquet_config.clone(),
        ));
        let logs_ingester = Arc::new(Ingester::new(
            Signal::Logs,
            logs_storage_schema(),
            store,
            buffer_config,
            parquet_config,
        ));

        let redaction = Arc::new(
            RedactionEngine::new(&RedactionConfig::default()).expect("default config should work"),
        );

        OtlpGrpcReceiver::new(trace_ingester, metrics_ingester, logs_ingester, redaction)
    }

    fn service_name_kv(name: &str) -> KeyValue {
        KeyValue {
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(name.to_string())),
            }),
        }
    }

    #[tokio::test]
    async fn trace_service_export_succeeds() {
        let receiver = test_receiver();

        let request = Request::new(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![service_name_kv("grpc-test-service")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![0xAA; 16],
                        span_id: vec![0xBB; 8],
                        name: "grpc-test-span".to_string(),
                        start_time_unix_nano: 1_000_000_000,
                        end_time_unix_nano: 2_000_000_000,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let response = TraceService::export(&receiver, request).await;
        assert!(response.is_ok());

        // Verify the span was ingested
        assert_eq!(receiver.trace_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn trace_service_empty_request_succeeds() {
        let receiver = test_receiver();

        let request = Request::new(ExportTraceServiceRequest {
            resource_spans: vec![],
        });

        let response = TraceService::export(&receiver, request).await;
        assert!(response.is_ok());

        // No rows should be buffered
        assert_eq!(receiver.trace_ingester.buffered_rows(), 0);
    }

    #[tokio::test]
    async fn metrics_service_export_succeeds() {
        let receiver = test_receiver();

        let request = Request::new(ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![service_name_kv("grpc-metrics-service")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "grpc.test.gauge".to_string(),
                        description: "Test metric".to_string(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_000_000_000,
                                value: Some(number_data_point::Value::AsInt(42)),
                                ..Default::default()
                            }],
                        })),
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let response = MetricsService::export(&receiver, request).await;
        assert!(response.is_ok());

        // Verify the metric was ingested
        assert_eq!(receiver.metrics_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn logs_service_export_succeeds() {
        let receiver = test_receiver();

        let request = Request::new(ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![service_name_kv("grpc-logs-service")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: 1_000_000_000,
                        observed_time_unix_nano: 1_000_000_000,
                        severity_number: 9, // INFO
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("gRPC log test".to_string())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let response = LogsService::export(&receiver, request).await;
        assert!(response.is_ok());

        // Verify the log was ingested
        assert_eq!(receiver.logs_ingester.buffered_rows(), 1);
    }

    #[tokio::test]
    async fn multiple_spans_in_request() {
        let receiver = test_receiver();

        let request = Request::new(ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![service_name_kv("multi-span-service")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![
                        Span {
                            trace_id: vec![0x01; 16],
                            span_id: vec![0x01; 8],
                            name: "span-1".to_string(),
                            ..Default::default()
                        },
                        Span {
                            trace_id: vec![0x02; 16],
                            span_id: vec![0x02; 8],
                            name: "span-2".to_string(),
                            ..Default::default()
                        },
                        Span {
                            trace_id: vec![0x03; 16],
                            span_id: vec![0x03; 8],
                            name: "span-3".to_string(),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        });

        let response = TraceService::export(&receiver, request).await;
        assert!(response.is_ok());

        // All three spans should be buffered
        assert_eq!(receiver.trace_ingester.buffered_rows(), 3);
    }
}
