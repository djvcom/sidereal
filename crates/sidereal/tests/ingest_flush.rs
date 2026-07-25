//! Regression tests for the ingest flush path.
//!
//! Ingesters must be constructed with the storage schemas (without the
//! `date`/`hour` partition columns, which live in the object-store path).
//! Constructing them with the query schemas makes every parquet flush fail
//! with a row-count mismatch, because the partition column writers receive
//! no data.

use std::sync::Arc;

use object_store::memory::InMemory;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use sidereal::buffer::Ingester;
use sidereal::config::{BufferConfig, ParquetConfig};
use sidereal::ingest::{convert_logs_to_arrow, convert_metrics_to_arrow, convert_traces_to_arrow};
use sidereal::schema::logs::logs_storage_schema;
use sidereal::schema::metrics::number_metrics_storage_schema;
use sidereal::schema::traces::traces_storage_schema;
use sidereal::storage::Signal;

const LOG_JSON: &str = r#"{"resourceLogs":[{"resource":{"attributes":[
  {"key":"service.name","value":{"stringValue":"payments"}},
  {"key":"service.version","value":{"stringValue":"1.4.1"}},
  {"key":"deployment.environment.name","value":{"stringValue":"production"}}
]},"scopeLogs":[{"logRecords":[
  {"timeUnixNano":"1751400000000000000","severityNumber":9,
   "body":{"stringValue":"deployment of payments 1.4.1"},
   "attributes":[
     {"key":"event.name","value":{"stringValue":"deployment"}},
     {"key":"deployment.id","value":{"stringValue":"deploy-payments-1.4.1"}},
     {"key":"deployment.status","value":{"stringValue":"succeeded"}}
   ]}
]}]}]}"#;

const TRACE_JSON: &str = r#"{"resourceSpans":[{"resource":{"attributes":[
  {"key":"service.name","value":{"stringValue":"payments"}},
  {"key":"service.version","value":{"stringValue":"1.4.1"}}
]},"scopeSpans":[{"spans":[
  {"traceId":"5b8efff798038103d269b633813fc60c",
   "spanId":"eee19b7ec3c1b174",
   "name":"PaymentsService/Charge","kind":2,
   "startTimeUnixNano":"1751400000000000000",
   "endTimeUnixNano":"1751400000300000000",
   "status":{"code":1}}
]}]}]}"#;

const METRICS_JSON: &str = r#"{"resourceMetrics":[{"resource":{"attributes":[
  {"key":"service.name","value":{"stringValue":"payments"}},
  {"key":"service.version","value":{"stringValue":"1.4.1"}}
]},"scopeMetrics":[{"metrics":[
  {"name":"queue.depth","unit":"1","gauge":{"dataPoints":[
    {"timeUnixNano":"1751400000000000000","asInt":"42"}
  ]}}
]}]}]}"#;

#[test]
fn converted_metrics_match_the_storage_schema() {
    let request: ExportMetricsServiceRequest =
        serde_json::from_str(METRICS_JSON).expect("OTLP JSON should decode");
    let batch = convert_metrics_to_arrow(&request, None)
        .expect("conversion should succeed")
        .batch;
    assert_eq!(batch.schema(), number_metrics_storage_schema());
}

#[test]
fn converted_logs_match_the_storage_schema() {
    let request: ExportLogsServiceRequest =
        serde_json::from_str(LOG_JSON).expect("OTLP JSON should decode");
    let batch = convert_logs_to_arrow(&request, None)
        .expect("conversion should succeed")
        .batch;
    assert_eq!(batch.schema(), logs_storage_schema());
}

#[test]
fn converted_traces_match_the_storage_schema() {
    let request: ExportTraceServiceRequest =
        serde_json::from_str(TRACE_JSON).expect("OTLP JSON should decode");
    let batch = convert_traces_to_arrow(&request, None)
        .expect("conversion should succeed")
        .batch;
    assert_eq!(batch.schema(), traces_storage_schema());
}

#[tokio::test]
async fn ingested_json_logs_flush_to_parquet() {
    let request: ExportLogsServiceRequest =
        serde_json::from_str(LOG_JSON).expect("OTLP JSON should decode");
    let batch = convert_logs_to_arrow(&request, None)
        .expect("conversion should succeed")
        .batch;

    let ingester = Ingester::new(
        Signal::Logs,
        logs_storage_schema(),
        Arc::new(InMemory::new()),
        BufferConfig::default(),
        ParquetConfig::default(),
    );

    ingester.ingest(batch).await.expect("ingest should buffer");
    ingester.flush().await.expect("flush should write parquet");
}
