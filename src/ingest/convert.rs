//! OTLP proto to Arrow conversion.
//!
//! Converts OpenTelemetry protocol buffer messages to Arrow RecordBatches
//! with denormalised resource attributes for fast querying.

#![allow(clippy::branches_sharing_code)]

use std::collections::HashMap;
use std::sync::Arc;

/// Result of converting OTLP data to Arrow format.
///
/// Tracks both successfully converted records and any that were rejected
/// during conversion, allowing OTLP partial_success responses.
#[derive(Debug)]
pub struct ConversionResult {
    /// The converted Arrow RecordBatch.
    pub batch: RecordBatch,
    /// Number of records rejected during conversion.
    pub rejected_count: i64,
    /// Human-readable error message if any records were rejected.
    pub error_message: Option<String>,
}

impl ConversionResult {
    /// Create a result with no rejections.
    pub const fn success(batch: RecordBatch) -> Self {
        Self {
            batch,
            rejected_count: 0,
            error_message: None,
        }
    }

    /// Create a result with some rejections.
    #[allow(dead_code)]
    pub const fn partial(batch: RecordBatch, rejected_count: i64, error_message: String) -> Self {
        Self {
            batch,
            rejected_count,
            error_message: Some(error_message),
        }
    }
}

// ============================================================================
// Builder capacity hint constants
// ============================================================================
//
// These constants are used as per-row byte capacity hints when constructing
// Arrow StringBuilder and BinaryBuilder instances. They represent estimated
// average sizes for different field types to minimise reallocations during
// batch construction.

/// Estimated average length for short strings (method names, schemes, SDK names).
const CAPACITY_SHORT_STRING: usize = 8;

/// Estimated average length for typical identifiers (service names, namespaces).
const CAPACITY_IDENTIFIER: usize = 16;

/// Estimated average length for names and paths (span names, routes, hostnames).
const CAPACITY_NAME: usize = 32;

/// Estimated average length for URLs and longer paths.
const CAPACITY_PATH: usize = 64;

/// Estimated average length for attribute blobs and user agents.
const CAPACITY_ATTRIBUTES: usize = 128;

/// Estimated average length for log message bodies.
const CAPACITY_LOG_BODY: usize = 256;

/// Estimated average length for stack traces.
const CAPACITY_STACKTRACE: usize = 512;

/// Default service name when not specified in telemetry data.
const DEFAULT_SERVICE_NAME: &str = "unknown";

use arrow::array::{
    ArrayRef, BinaryBuilder, FixedSizeBinaryBuilder, Int32Builder, RecordBatch, StringBuilder,
    UInt64Builder, UInt8Builder,
};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{any_value::Value as AnyValueKind, AnyValue, KeyValue},
};

use crate::redact::{AttributeScope, RedactionEngine};
use crate::schema::{
    logs::logs_storage_schema, metrics::number_metrics_storage_schema,
    traces::traces_storage_schema,
};
use crate::storage::Signal;
use crate::TelemetryError;

/// Semantic convention attribute keys for extraction.
mod semconv {
    // Resource attributes
    pub const SERVICE_NAME: &str = "service.name";
    pub const SERVICE_VERSION: &str = "service.version";
    pub const SERVICE_NAMESPACE: &str = "service.namespace";
    pub const DEPLOYMENT_ENVIRONMENT_NAME: &str = "deployment.environment.name";
    pub const TELEMETRY_SDK_NAME: &str = "telemetry.sdk.name";
    pub const TELEMETRY_SDK_VERSION: &str = "telemetry.sdk.version";
    pub const TELEMETRY_SDK_LANGUAGE: &str = "telemetry.sdk.language";

    // HTTP attributes
    pub const HTTP_REQUEST_METHOD: &str = "http.request.method";
    pub const HTTP_RESPONSE_STATUS_CODE: &str = "http.response.status_code";
    pub const HTTP_ROUTE: &str = "http.route";
    pub const URL_SCHEME: &str = "url.scheme";
    pub const URL_PATH: &str = "url.path";
    pub const SERVER_ADDRESS: &str = "server.address";
    pub const SERVER_PORT: &str = "server.port";
    pub const ERROR_TYPE: &str = "error.type";

    // Database attributes
    pub const DB_SYSTEM_NAME: &str = "db.system.name";
    pub const DB_OPERATION_NAME: &str = "db.operation.name";
    pub const DB_COLLECTION_NAME: &str = "db.collection.name";

    // RPC attributes
    pub const RPC_SYSTEM: &str = "rpc.system";
    pub const RPC_SERVICE: &str = "rpc.service";
    pub const RPC_METHOD: &str = "rpc.method";

    // Messaging attributes
    pub const MESSAGING_SYSTEM: &str = "messaging.system";
    pub const MESSAGING_OPERATION_TYPE: &str = "messaging.operation.type";
    pub const MESSAGING_DESTINATION_NAME: &str = "messaging.destination.name";

    // Network attributes
    pub const NETWORK_PROTOCOL_NAME: &str = "network.protocol.name";
    pub const NETWORK_PROTOCOL_VERSION: &str = "network.protocol.version";
    pub const USER_AGENT_ORIGINAL: &str = "user_agent.original";

    // Client attributes
    pub const CLIENT_ADDRESS: &str = "client.address";
    pub const CLIENT_PORT: &str = "client.port";

    // Metrics host/infrastructure
    pub const HOST_NAME: &str = "host.name";
    pub const HOST_ID: &str = "host.id";
    pub const CONTAINER_ID: &str = "container.id";
    pub const K8S_POD_NAME: &str = "k8s.pod.name";
    pub const K8S_NAMESPACE_NAME: &str = "k8s.namespace.name";

    // Log attributes
    pub const LOG_FILE_PATH: &str = "log.file.path";
    pub const LOG_FILE_NAME: &str = "log.file.name";
    pub const LOG_IOSTREAM: &str = "log.iostream";
    pub const EXCEPTION_TYPE: &str = "exception.type";
    pub const EXCEPTION_MESSAGE: &str = "exception.message";
    pub const EXCEPTION_STACKTRACE: &str = "exception.stacktrace";
    pub const CODE_FUNCTION: &str = "code.function";
    pub const CODE_FILEPATH: &str = "code.filepath";
    pub const CODE_LINENO: &str = "code.lineno";

    /// Event attributes for structured events emitted as logs.
    /// See <https://opentelemetry.io/docs/specs/semconv/general/events/>
    pub const EVENT_NAME: &str = "event.name";
    pub const EVENT_DOMAIN: &str = "event.domain";
}

/// Extract a string value from an AnyValue.
fn any_value_to_string(value: &AnyValue) -> Option<String> {
    match &value.value {
        Some(AnyValueKind::StringValue(s)) => Some(s.clone()),
        Some(AnyValueKind::IntValue(i)) => Some(i.to_string()),
        Some(AnyValueKind::DoubleValue(d)) => Some(d.to_string()),
        Some(AnyValueKind::BoolValue(b)) => Some(b.to_string()),
        _ => None,
    }
}

/// Extract an i64 value from an AnyValue.
fn any_value_to_i64(value: &AnyValue) -> Option<i64> {
    match &value.value {
        Some(AnyValueKind::IntValue(i)) => Some(*i),
        Some(AnyValueKind::StringValue(s)) => s.parse().ok(),
        _ => None,
    }
}

/// Convert attributes to a map for easy lookup.
fn attributes_to_map(attrs: &[KeyValue]) -> HashMap<&str, &AnyValue> {
    let mut map = HashMap::with_capacity(attrs.len());
    for kv in attrs {
        if let Some(v) = &kv.value {
            map.insert(kv.key.as_str(), v);
        }
    }
    map
}

/// Get a string attribute from a map, removing it from remaining.
fn extract_string_attr(map: &HashMap<&str, &AnyValue>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| any_value_to_string(v))
}

/// Get an i32 attribute from a map.
fn extract_i32_attr(map: &HashMap<&str, &AnyValue>, key: &str) -> Option<i32> {
    map.get(key)
        .and_then(|v| any_value_to_i64(v))
        .and_then(|i| i32::try_from(i).ok())
}

/// Encode remaining attributes as CBOR, excluding extracted keys.
fn encode_remaining_attrs(attrs: &[KeyValue], extracted_keys: &[&str]) -> Option<Vec<u8>> {
    let remaining: Vec<(&str, serde_json::Value)> = attrs
        .iter()
        .filter(|kv| !extracted_keys.contains(&kv.key.as_str()))
        .filter_map(|kv| {
            kv.value.as_ref().map(|v| {
                let json_val = any_value_to_json(v);
                (kv.key.as_str(), json_val)
            })
        })
        .collect();

    if remaining.is_empty() {
        return None;
    }

    let map: HashMap<&str, serde_json::Value> = remaining.into_iter().collect();
    let mut buf = Vec::new();
    ciborium::into_writer(&map, &mut buf).ok()?;
    Some(buf)
}

/// Convert AnyValue to serde_json::Value for CBOR encoding.
fn any_value_to_json(value: &AnyValue) -> serde_json::Value {
    match &value.value {
        Some(AnyValueKind::StringValue(s)) => serde_json::Value::String(s.clone()),
        Some(AnyValueKind::IntValue(i)) => serde_json::json!(*i),
        Some(AnyValueKind::DoubleValue(d)) => serde_json::json!(*d),
        Some(AnyValueKind::BoolValue(b)) => serde_json::Value::Bool(*b),
        Some(AnyValueKind::ArrayValue(arr)) => {
            let items: Vec<serde_json::Value> = arr.values.iter().map(any_value_to_json).collect();
            serde_json::Value::Array(items)
        }
        Some(AnyValueKind::KvlistValue(kvlist)) => {
            let map: serde_json::Map<String, serde_json::Value> = kvlist
                .values
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|v| (kv.key.clone(), any_value_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(map)
        }
        Some(AnyValueKind::BytesValue(b)) => {
            use base64::Engine;
            serde_json::Value::String(base64::engine::general_purpose::STANDARD.encode(b))
        }
        None => serde_json::Value::Null,
    }
}

/// Conditionally redact attributes, returning borrowed or owned data.
///
/// Only clones attributes if redaction is enabled AND at least one attribute
/// matches a redaction rule. This avoids unnecessary allocations when no
/// attributes would actually be modified.
fn maybe_redact_attributes<'a>(
    attrs: &'a [KeyValue],
    redaction: Option<&RedactionEngine>,
    signal: Signal,
    scope: AttributeScope,
) -> std::borrow::Cow<'a, [KeyValue]> {
    match redaction {
        Some(engine) if engine.would_redact_any(attrs, signal, scope) => {
            let mut owned = attrs.to_vec();
            engine.redact_attributes(&mut owned, signal, scope);
            std::borrow::Cow::Owned(owned)
        }
        _ => std::borrow::Cow::Borrowed(attrs),
    }
}

/// Convert OTLP trace spans to an Arrow RecordBatch.
///
/// Returns a batch using the storage schema (without partition columns).
/// Partition columns (date, hour) are derived from the directory path
/// when writing to storage, not stored in the Parquet file.
///
/// If a redaction engine is provided, attributes matching the configured
/// rules will be dropped, hashed, or replaced before conversion.
pub fn convert_traces_to_arrow(
    request: &ExportTraceServiceRequest,
    redaction: Option<&RedactionEngine>,
) -> Result<ConversionResult, TelemetryError> {
    let schema = traces_storage_schema();

    // Pre-calculate capacity hint
    let capacity: usize = request
        .resource_spans
        .iter()
        .flat_map(|rs| &rs.scope_spans)
        .map(|ss| ss.spans.len())
        .sum();

    if capacity == 0 {
        return Ok(ConversionResult::success(RecordBatch::new_empty(schema)));
    }

    // Create builders for each column
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(capacity, 16);
    let mut span_id = FixedSizeBinaryBuilder::with_capacity(capacity, 8);
    let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(capacity, 8);
    let mut flags = arrow::array::UInt32Builder::with_capacity(capacity);
    let mut trace_state = StringBuilder::with_capacity(capacity, capacity * CAPACITY_ATTRIBUTES);
    let mut start_time_unix_nano = UInt64Builder::with_capacity(capacity);
    let mut end_time_unix_nano = UInt64Builder::with_capacity(capacity);
    let mut duration_ns = UInt64Builder::with_capacity(capacity);
    let mut name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut kind = UInt8Builder::with_capacity(capacity);
    let mut status_code = UInt8Builder::with_capacity(capacity);
    let mut status_message = StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);

    // Instrumentation scope
    let mut scope_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut scope_version =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut scope_attributes = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);

    // Dropped attribute counts
    let mut resource_dropped_attributes_count =
        arrow::array::UInt32Builder::with_capacity(capacity);
    let mut scope_dropped_attributes_count = arrow::array::UInt32Builder::with_capacity(capacity);
    let mut span_dropped_attributes_count = arrow::array::UInt32Builder::with_capacity(capacity);
    let mut span_dropped_events_count = arrow::array::UInt32Builder::with_capacity(capacity);
    let mut span_dropped_links_count = arrow::array::UInt32Builder::with_capacity(capacity);

    // Resource attributes
    let mut service_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut service_version =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut service_namespace =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut deployment_environment_name =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut telemetry_sdk_name =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut telemetry_sdk_version =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut telemetry_sdk_language =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);

    // HTTP attributes
    let mut http_request_method =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut http_response_status_code = Int32Builder::with_capacity(capacity);
    let mut http_route = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut url_scheme = StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut url_path = StringBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut server_address = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut server_port = Int32Builder::with_capacity(capacity);
    let mut error_type = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);

    // Database attributes
    let mut db_system_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut db_operation_name =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut db_collection_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);

    // RPC attributes
    let mut rpc_system = StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut rpc_service = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut rpc_method = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);

    // Messaging attributes
    let mut messaging_system =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut messaging_operation_type =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut messaging_destination_name =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);

    // Network attributes
    let mut network_protocol_name =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut network_protocol_version =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut user_agent_original =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_ATTRIBUTES);

    // Client attributes
    let mut client_address = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut client_port = Int32Builder::with_capacity(capacity);

    // CBOR-encoded remaining attributes
    let mut resource_attributes = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut span_attributes =
        BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_ATTRIBUTES);
    let mut events = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut links = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);

    // Note: Partition columns (project_id, date, hour) are NOT stored in Parquet.
    // They are derived from the directory path when DataFusion reads the files.

    // Keys to exclude from remaining attributes
    let resource_extracted_keys = [
        semconv::SERVICE_NAME,
        semconv::SERVICE_VERSION,
        semconv::SERVICE_NAMESPACE,
        semconv::DEPLOYMENT_ENVIRONMENT_NAME,
        semconv::TELEMETRY_SDK_NAME,
        semconv::TELEMETRY_SDK_VERSION,
        semconv::TELEMETRY_SDK_LANGUAGE,
    ];

    let span_extracted_keys = [
        semconv::HTTP_REQUEST_METHOD,
        semconv::HTTP_RESPONSE_STATUS_CODE,
        semconv::HTTP_ROUTE,
        semconv::URL_SCHEME,
        semconv::URL_PATH,
        semconv::SERVER_ADDRESS,
        semconv::SERVER_PORT,
        semconv::ERROR_TYPE,
        semconv::DB_SYSTEM_NAME,
        semconv::DB_OPERATION_NAME,
        semconv::DB_COLLECTION_NAME,
        semconv::RPC_SYSTEM,
        semconv::RPC_SERVICE,
        semconv::RPC_METHOD,
        semconv::MESSAGING_SYSTEM,
        semconv::MESSAGING_OPERATION_TYPE,
        semconv::MESSAGING_DESTINATION_NAME,
        semconv::NETWORK_PROTOCOL_NAME,
        semconv::NETWORK_PROTOCOL_VERSION,
        semconv::USER_AGENT_ORIGINAL,
        semconv::CLIENT_ADDRESS,
        semconv::CLIENT_PORT,
    ];

    for resource_spans in &request.resource_spans {
        // Extract resource attributes and dropped count once per resource
        let (raw_resource_attrs, res_dropped_count) =
            resource_spans.resource.as_ref().map_or((&[][..], 0), |r| {
                (&r.attributes[..], r.dropped_attributes_count)
            });

        // Apply redaction to resource attributes
        let resource_attrs = maybe_redact_attributes(
            raw_resource_attrs,
            redaction,
            Signal::Traces,
            AttributeScope::Resource,
        );
        let resource_map = attributes_to_map(&resource_attrs);

        let svc_name = extract_string_attr(&resource_map, semconv::SERVICE_NAME)
            .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned());
        let svc_version = extract_string_attr(&resource_map, semconv::SERVICE_VERSION);
        let svc_namespace = extract_string_attr(&resource_map, semconv::SERVICE_NAMESPACE);
        let deploy_env = extract_string_attr(&resource_map, semconv::DEPLOYMENT_ENVIRONMENT_NAME);
        let sdk_name = extract_string_attr(&resource_map, semconv::TELEMETRY_SDK_NAME);
        let sdk_version = extract_string_attr(&resource_map, semconv::TELEMETRY_SDK_VERSION);
        let sdk_language = extract_string_attr(&resource_map, semconv::TELEMETRY_SDK_LANGUAGE);

        let remaining_resource = encode_remaining_attrs(&resource_attrs, &resource_extracted_keys);

        for scope_spans in &resource_spans.scope_spans {
            // Extract instrumentation scope info with optional redaction
            let (sc_name, sc_version, sc_attrs, sc_dropped_count) =
                scope_spans.scope.as_ref().map_or(("", "", None, 0), |s| {
                    let redacted_attrs = maybe_redact_attributes(
                        &s.attributes,
                        redaction,
                        Signal::Traces,
                        AttributeScope::Scope,
                    );
                    let attrs = encode_remaining_attrs(&redacted_attrs, &[]);
                    (
                        s.name.as_str(),
                        s.version.as_str(),
                        attrs,
                        s.dropped_attributes_count,
                    )
                });

            for span in &scope_spans.spans {
                // Core span fields
                let tid = pad_or_truncate(&span.trace_id, 16);
                let sid = pad_or_truncate(&span.span_id, 8);
                trace_id.append_value(&tid)?;
                span_id.append_value(&sid)?;

                if span.parent_span_id.is_empty() {
                    parent_span_id.append_null();
                } else {
                    let psid = pad_or_truncate(&span.parent_span_id, 8);
                    parent_span_id.append_value(&psid)?;
                }

                // W3C trace context flags and state
                flags.append_value(span.flags);
                if span.trace_state.is_empty() {
                    trace_state.append_null();
                } else {
                    trace_state.append_value(&span.trace_state);
                }

                start_time_unix_nano.append_value(span.start_time_unix_nano);
                end_time_unix_nano.append_value(span.end_time_unix_nano);
                duration_ns.append_value(
                    span.end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano),
                );
                name.append_value(&span.name);
                #[allow(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    clippy::as_conversions
                )]
                kind.append_value(span.kind as u8);

                // Status
                #[allow(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    clippy::as_conversions
                )]
                let (sc, sm) = span
                    .status
                    .as_ref()
                    .map_or((0, ""), |s| (s.code as u8, s.message.as_str()));
                status_code.append_value(sc);
                if sm.is_empty() {
                    status_message.append_null();
                } else {
                    status_message.append_value(sm);
                }

                // Instrumentation scope
                if sc_name.is_empty() {
                    scope_name.append_null();
                } else {
                    scope_name.append_value(sc_name);
                }
                if sc_version.is_empty() {
                    scope_version.append_null();
                } else {
                    scope_version.append_value(sc_version);
                }
                append_option_binary(&mut scope_attributes, sc_attrs.as_deref());

                // Dropped attribute counts
                resource_dropped_attributes_count.append_value(res_dropped_count);
                scope_dropped_attributes_count.append_value(sc_dropped_count);
                span_dropped_attributes_count.append_value(span.dropped_attributes_count);
                span_dropped_events_count.append_value(span.dropped_events_count);
                span_dropped_links_count.append_value(span.dropped_links_count);

                // Resource attributes (denormalised)
                service_name.append_value(&svc_name);
                append_option_string(&mut service_version, svc_version.as_deref());
                append_option_string(&mut service_namespace, svc_namespace.as_deref());
                append_option_string(&mut deployment_environment_name, deploy_env.as_deref());
                append_option_string(&mut telemetry_sdk_name, sdk_name.as_deref());
                append_option_string(&mut telemetry_sdk_version, sdk_version.as_deref());
                append_option_string(&mut telemetry_sdk_language, sdk_language.as_deref());

                // Span attributes with optional redaction
                let span_attrs = maybe_redact_attributes(
                    &span.attributes,
                    redaction,
                    Signal::Traces,
                    AttributeScope::Span,
                );
                let span_map = attributes_to_map(&span_attrs);

                append_option_string(
                    &mut http_request_method,
                    extract_string_attr(&span_map, semconv::HTTP_REQUEST_METHOD).as_deref(),
                );
                append_option_i32(
                    &mut http_response_status_code,
                    extract_i32_attr(&span_map, semconv::HTTP_RESPONSE_STATUS_CODE),
                );
                append_option_string(
                    &mut http_route,
                    extract_string_attr(&span_map, semconv::HTTP_ROUTE).as_deref(),
                );
                append_option_string(
                    &mut url_scheme,
                    extract_string_attr(&span_map, semconv::URL_SCHEME).as_deref(),
                );
                append_option_string(
                    &mut url_path,
                    extract_string_attr(&span_map, semconv::URL_PATH).as_deref(),
                );
                append_option_string(
                    &mut server_address,
                    extract_string_attr(&span_map, semconv::SERVER_ADDRESS).as_deref(),
                );
                append_option_i32(
                    &mut server_port,
                    extract_i32_attr(&span_map, semconv::SERVER_PORT),
                );
                append_option_string(
                    &mut error_type,
                    extract_string_attr(&span_map, semconv::ERROR_TYPE).as_deref(),
                );

                append_option_string(
                    &mut db_system_name,
                    extract_string_attr(&span_map, semconv::DB_SYSTEM_NAME).as_deref(),
                );
                append_option_string(
                    &mut db_operation_name,
                    extract_string_attr(&span_map, semconv::DB_OPERATION_NAME).as_deref(),
                );
                append_option_string(
                    &mut db_collection_name,
                    extract_string_attr(&span_map, semconv::DB_COLLECTION_NAME).as_deref(),
                );

                append_option_string(
                    &mut rpc_system,
                    extract_string_attr(&span_map, semconv::RPC_SYSTEM).as_deref(),
                );
                append_option_string(
                    &mut rpc_service,
                    extract_string_attr(&span_map, semconv::RPC_SERVICE).as_deref(),
                );
                append_option_string(
                    &mut rpc_method,
                    extract_string_attr(&span_map, semconv::RPC_METHOD).as_deref(),
                );

                append_option_string(
                    &mut messaging_system,
                    extract_string_attr(&span_map, semconv::MESSAGING_SYSTEM).as_deref(),
                );
                append_option_string(
                    &mut messaging_operation_type,
                    extract_string_attr(&span_map, semconv::MESSAGING_OPERATION_TYPE).as_deref(),
                );
                append_option_string(
                    &mut messaging_destination_name,
                    extract_string_attr(&span_map, semconv::MESSAGING_DESTINATION_NAME).as_deref(),
                );

                append_option_string(
                    &mut network_protocol_name,
                    extract_string_attr(&span_map, semconv::NETWORK_PROTOCOL_NAME).as_deref(),
                );
                append_option_string(
                    &mut network_protocol_version,
                    extract_string_attr(&span_map, semconv::NETWORK_PROTOCOL_VERSION).as_deref(),
                );
                append_option_string(
                    &mut user_agent_original,
                    extract_string_attr(&span_map, semconv::USER_AGENT_ORIGINAL).as_deref(),
                );

                // Client attributes
                append_option_string(
                    &mut client_address,
                    extract_string_attr(&span_map, semconv::CLIENT_ADDRESS).as_deref(),
                );
                append_option_i32(
                    &mut client_port,
                    extract_i32_attr(&span_map, semconv::CLIENT_PORT),
                );

                // Remaining attributes as CBOR
                append_option_binary(&mut resource_attributes, remaining_resource.as_deref());
                append_option_binary(
                    &mut span_attributes,
                    encode_remaining_attrs(&span_attrs, &span_extracted_keys).as_deref(),
                );

                // Events and links as CBOR
                match encode_events(&span.events) {
                    Some(data) if !span.events.is_empty() => events.append_value(&data),
                    _ => events.append_null(),
                }

                match encode_links(&span.links) {
                    Some(data) if !span.links.is_empty() => links.append_value(&data),
                    _ => links.append_null(),
                }
            }
        }
    }

    // Build the record batch
    let columns: Vec<ArrayRef> = vec![
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(parent_span_id.finish()),
        Arc::new(flags.finish()),
        Arc::new(trace_state.finish()),
        Arc::new(start_time_unix_nano.finish()),
        Arc::new(end_time_unix_nano.finish()),
        Arc::new(duration_ns.finish()),
        Arc::new(name.finish()),
        Arc::new(kind.finish()),
        Arc::new(status_code.finish()),
        Arc::new(status_message.finish()),
        Arc::new(scope_name.finish()),
        Arc::new(scope_version.finish()),
        Arc::new(scope_attributes.finish()),
        Arc::new(resource_dropped_attributes_count.finish()),
        Arc::new(scope_dropped_attributes_count.finish()),
        Arc::new(span_dropped_attributes_count.finish()),
        Arc::new(span_dropped_events_count.finish()),
        Arc::new(span_dropped_links_count.finish()),
        Arc::new(service_name.finish()),
        Arc::new(service_version.finish()),
        Arc::new(service_namespace.finish()),
        Arc::new(deployment_environment_name.finish()),
        Arc::new(telemetry_sdk_name.finish()),
        Arc::new(telemetry_sdk_version.finish()),
        Arc::new(telemetry_sdk_language.finish()),
        Arc::new(http_request_method.finish()),
        Arc::new(http_response_status_code.finish()),
        Arc::new(http_route.finish()),
        Arc::new(url_scheme.finish()),
        Arc::new(url_path.finish()),
        Arc::new(server_address.finish()),
        Arc::new(server_port.finish()),
        Arc::new(error_type.finish()),
        Arc::new(db_system_name.finish()),
        Arc::new(db_operation_name.finish()),
        Arc::new(db_collection_name.finish()),
        Arc::new(rpc_system.finish()),
        Arc::new(rpc_service.finish()),
        Arc::new(rpc_method.finish()),
        Arc::new(messaging_system.finish()),
        Arc::new(messaging_operation_type.finish()),
        Arc::new(messaging_destination_name.finish()),
        Arc::new(network_protocol_name.finish()),
        Arc::new(network_protocol_version.finish()),
        Arc::new(user_agent_original.finish()),
        Arc::new(client_address.finish()),
        Arc::new(client_port.finish()),
        Arc::new(resource_attributes.finish()),
        Arc::new(span_attributes.finish()),
        Arc::new(events.finish()),
        Arc::new(links.finish()),
    ];

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(ConversionResult::success(batch))
}

/// Pad or truncate a byte slice to the exact required length.
fn pad_or_truncate(data: &[u8], len: usize) -> Vec<u8> {
    let mut result = vec![0u8; len];
    let copy_len = data.len().min(len);
    if let (Some(dest), Some(src)) = (result.get_mut(..copy_len), data.get(..copy_len)) {
        dest.copy_from_slice(src);
    }
    result
}

/// Helper to append an optional string to a StringBuilder.
fn append_option_string(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(s) => builder.append_value(s),
        None => builder.append_null(),
    }
}

/// Helper to append an optional i32 to an Int32Builder.
fn append_option_i32(builder: &mut Int32Builder, value: Option<i32>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

/// Helper to append optional binary data.
fn append_option_binary(builder: &mut BinaryBuilder, value: Option<&[u8]>) {
    match value {
        Some(v) => builder.append_value(v),
        None => builder.append_null(),
    }
}

/// Encode span events to CBOR.
fn encode_events(events: &[opentelemetry_proto::tonic::trace::v1::span::Event]) -> Option<Vec<u8>> {
    let encoded: Vec<serde_json::Value> = events
        .iter()
        .map(|e| {
            let attrs: HashMap<&str, serde_json::Value> = e
                .attributes
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|v| (kv.key.as_str(), any_value_to_json(v)))
                })
                .collect();
            serde_json::json!({
                "name": e.name,
                "time_unix_nano": e.time_unix_nano,
                "attributes": attrs
            })
        })
        .collect();
    let mut buf = Vec::new();
    ciborium::into_writer(&encoded, &mut buf).ok()?;
    Some(buf)
}

/// Encode span links to CBOR.
fn encode_links(links: &[opentelemetry_proto::tonic::trace::v1::span::Link]) -> Option<Vec<u8>> {
    use base64::Engine;

    let encoded: Vec<serde_json::Value> = links
        .iter()
        .map(|l| {
            let attrs: HashMap<&str, serde_json::Value> = l
                .attributes
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|v| (kv.key.as_str(), any_value_to_json(v)))
                })
                .collect();
            serde_json::json!({
                "trace_id": base64::engine::general_purpose::STANDARD.encode(&l.trace_id),
                "span_id": base64::engine::general_purpose::STANDARD.encode(&l.span_id),
                "trace_state": l.trace_state,
                "attributes": attrs
            })
        })
        .collect();
    let mut buf = Vec::new();
    ciborium::into_writer(&encoded, &mut buf).ok()?;
    Some(buf)
}

/// Convert OTLP metrics to an Arrow RecordBatch.
///
/// Returns a batch using the storage schema (without partition columns).
/// Partition columns (date, hour) are derived from the directory path
/// when writing to storage, not stored in the Parquet file.
///
/// If a redaction engine is provided, attributes matching the configured
/// rules will be dropped, hashed, or replaced before conversion.
pub fn convert_metrics_to_arrow(
    request: &ExportMetricsServiceRequest,
    redaction: Option<&RedactionEngine>,
) -> Result<ConversionResult, TelemetryError> {
    let schema = number_metrics_storage_schema();

    // Count total data points
    let capacity: usize = request
        .resource_metrics
        .iter()
        .flat_map(|rm| &rm.scope_metrics)
        .flat_map(|sm| &sm.metrics)
        .map(count_metric_data_points)
        .sum();

    if capacity == 0 {
        return Ok(ConversionResult::success(RecordBatch::new_empty(schema)));
    }

    // Create builders
    let mut metric_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut metric_description = StringBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut metric_unit = StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut metric_metadata = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut metric_type = UInt8Builder::with_capacity(capacity);
    let mut time_unix_nano = UInt64Builder::with_capacity(capacity);
    let mut start_time_unix_nano = UInt64Builder::with_capacity(capacity);
    let mut data_point_flags = arrow::array::UInt32Builder::with_capacity(capacity);
    let mut value_int = arrow::array::Int64Builder::with_capacity(capacity);
    let mut value_double = arrow::array::Float64Builder::with_capacity(capacity);
    let mut aggregation_temporality = UInt8Builder::with_capacity(capacity);
    let mut is_monotonic = arrow::array::BooleanBuilder::with_capacity(capacity);
    let mut count = UInt64Builder::with_capacity(capacity);
    let mut sum = arrow::array::Float64Builder::with_capacity(capacity);
    let mut min = arrow::array::Float64Builder::with_capacity(capacity);
    let mut max = arrow::array::Float64Builder::with_capacity(capacity);
    let mut histogram_data = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_ATTRIBUTES);

    // Instrumentation scope
    let mut scope_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut scope_version =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut scope_attributes = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);

    // Dropped attribute counts
    let mut resource_dropped_attributes_count =
        arrow::array::UInt32Builder::with_capacity(capacity);
    let mut scope_dropped_attributes_count = arrow::array::UInt32Builder::with_capacity(capacity);

    // Resource attributes
    let mut service_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut service_version =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut service_namespace =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut deployment_environment_name =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut host_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut host_id = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut container_id = StringBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut k8s_pod_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut k8s_namespace_name =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);

    // Remaining attributes
    let mut resource_attributes = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut data_point_attributes =
        BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut exemplars = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);

    // Note: Partition columns (project_id, date, hour) are NOT stored in Parquet.
    // They are derived from the directory path when DataFusion reads the files.

    let resource_extracted_keys = [
        semconv::SERVICE_NAME,
        semconv::SERVICE_VERSION,
        semconv::SERVICE_NAMESPACE,
        semconv::DEPLOYMENT_ENVIRONMENT_NAME,
        semconv::HOST_NAME,
        semconv::HOST_ID,
        semconv::CONTAINER_ID,
        semconv::K8S_POD_NAME,
        semconv::K8S_NAMESPACE_NAME,
    ];

    for resource_metrics in &request.resource_metrics {
        let (raw_resource_attrs, res_dropped_count) = resource_metrics
            .resource
            .as_ref()
            .map_or((&[][..], 0), |r| {
                (&r.attributes[..], r.dropped_attributes_count)
            });

        // Apply redaction to resource attributes
        let resource_attrs = maybe_redact_attributes(
            raw_resource_attrs,
            redaction,
            Signal::Metrics,
            AttributeScope::Resource,
        );
        let resource_map = attributes_to_map(&resource_attrs);

        let svc_name = extract_string_attr(&resource_map, semconv::SERVICE_NAME)
            .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned());
        let svc_version = extract_string_attr(&resource_map, semconv::SERVICE_VERSION);
        let svc_namespace = extract_string_attr(&resource_map, semconv::SERVICE_NAMESPACE);
        let deploy_env = extract_string_attr(&resource_map, semconv::DEPLOYMENT_ENVIRONMENT_NAME);
        let h_name = extract_string_attr(&resource_map, semconv::HOST_NAME);
        let h_id = extract_string_attr(&resource_map, semconv::HOST_ID);
        let c_id = extract_string_attr(&resource_map, semconv::CONTAINER_ID);
        let k8s_pod = extract_string_attr(&resource_map, semconv::K8S_POD_NAME);
        let k8s_ns = extract_string_attr(&resource_map, semconv::K8S_NAMESPACE_NAME);

        let remaining_resource = encode_remaining_attrs(&resource_attrs, &resource_extracted_keys);

        for scope_metrics in &resource_metrics.scope_metrics {
            // Extract instrumentation scope info with optional redaction
            let (sc_name, sc_version, sc_attrs, sc_dropped_count) =
                scope_metrics.scope.as_ref().map_or(("", "", None, 0), |s| {
                    let redacted_attrs = maybe_redact_attributes(
                        &s.attributes,
                        redaction,
                        Signal::Metrics,
                        AttributeScope::Scope,
                    );
                    let attrs = encode_remaining_attrs(&redacted_attrs, &[]);
                    (
                        s.name.as_str(),
                        s.version.as_str(),
                        attrs,
                        s.dropped_attributes_count,
                    )
                });

            for metric in &scope_metrics.metrics {
                use opentelemetry_proto::tonic::metrics::v1::metric::Data;

                let m_name = &metric.name;
                let m_desc = if metric.description.is_empty() {
                    None
                } else {
                    Some(&metric.description)
                };
                let m_unit = if metric.unit.is_empty() {
                    None
                } else {
                    Some(&metric.unit)
                };
                let m_metadata = encode_remaining_attrs(&metric.metadata, &[]);

                match &metric.data {
                    Some(Data::Gauge(gauge)) => {
                        for dp in &gauge.data_points {
                            append_metric_common(
                                &mut metric_name,
                                &mut metric_description,
                                &mut metric_unit,
                                &mut metric_metadata,
                                &mut metric_type,
                                m_name,
                                m_desc,
                                m_unit,
                                &m_metadata,
                                0, // Gauge
                            );
                            append_number_data_point(
                                &mut time_unix_nano,
                                &mut start_time_unix_nano,
                                &mut value_int,
                                &mut value_double,
                                dp,
                            );
                            data_point_flags.append_value(dp.flags);
                            aggregation_temporality.append_null();
                            is_monotonic.append_null();
                            count.append_null();
                            sum.append_null();
                            min.append_null();
                            max.append_null();
                            histogram_data.append_null();
                            append_scope_fields(
                                &mut scope_name,
                                &mut scope_version,
                                &mut scope_attributes,
                                sc_name,
                                sc_version,
                                &sc_attrs,
                            );
                            resource_dropped_attributes_count.append_value(res_dropped_count);
                            scope_dropped_attributes_count.append_value(sc_dropped_count);
                            append_resource_attrs(
                                &mut service_name,
                                &mut service_version,
                                &mut service_namespace,
                                &mut deployment_environment_name,
                                &mut host_name,
                                &mut host_id,
                                &mut container_id,
                                &mut k8s_pod_name,
                                &mut k8s_namespace_name,
                                &svc_name,
                                &svc_version,
                                &svc_namespace,
                                &deploy_env,
                                &h_name,
                                &h_id,
                                &c_id,
                                &k8s_pod,
                                &k8s_ns,
                            );
                            append_option_binary(
                                &mut resource_attributes,
                                remaining_resource.as_deref(),
                            );
                            let redacted_dp_attrs = maybe_redact_attributes(
                                &dp.attributes,
                                redaction,
                                Signal::Metrics,
                                AttributeScope::DataPoint,
                            );
                            append_option_binary(
                                &mut data_point_attributes,
                                encode_remaining_attrs(&redacted_dp_attrs, &[]).as_deref(),
                            );
                            append_exemplars(&mut exemplars, &dp.exemplars);
                        }
                    }
                    Some(Data::Sum(sum_metric)) => {
                        for dp in &sum_metric.data_points {
                            append_metric_common(
                                &mut metric_name,
                                &mut metric_description,
                                &mut metric_unit,
                                &mut metric_metadata,
                                &mut metric_type,
                                m_name,
                                m_desc,
                                m_unit,
                                &m_metadata,
                                1, // Sum
                            );
                            append_number_data_point(
                                &mut time_unix_nano,
                                &mut start_time_unix_nano,
                                &mut value_int,
                                &mut value_double,
                                dp,
                            );
                            data_point_flags.append_value(dp.flags);
                            #[allow(
                                clippy::cast_possible_truncation,
                                clippy::cast_sign_loss,
                                clippy::as_conversions
                            )]
                            aggregation_temporality
                                .append_value(sum_metric.aggregation_temporality as u8);
                            is_monotonic.append_value(sum_metric.is_monotonic);
                            count.append_null();
                            sum.append_null();
                            min.append_null();
                            max.append_null();
                            histogram_data.append_null();
                            append_scope_fields(
                                &mut scope_name,
                                &mut scope_version,
                                &mut scope_attributes,
                                sc_name,
                                sc_version,
                                &sc_attrs,
                            );
                            resource_dropped_attributes_count.append_value(res_dropped_count);
                            scope_dropped_attributes_count.append_value(sc_dropped_count);
                            append_resource_attrs(
                                &mut service_name,
                                &mut service_version,
                                &mut service_namespace,
                                &mut deployment_environment_name,
                                &mut host_name,
                                &mut host_id,
                                &mut container_id,
                                &mut k8s_pod_name,
                                &mut k8s_namespace_name,
                                &svc_name,
                                &svc_version,
                                &svc_namespace,
                                &deploy_env,
                                &h_name,
                                &h_id,
                                &c_id,
                                &k8s_pod,
                                &k8s_ns,
                            );
                            append_option_binary(
                                &mut resource_attributes,
                                remaining_resource.as_deref(),
                            );
                            let redacted_dp_attrs = maybe_redact_attributes(
                                &dp.attributes,
                                redaction,
                                Signal::Metrics,
                                AttributeScope::DataPoint,
                            );
                            append_option_binary(
                                &mut data_point_attributes,
                                encode_remaining_attrs(&redacted_dp_attrs, &[]).as_deref(),
                            );
                            append_exemplars(&mut exemplars, &dp.exemplars);
                        }
                    }
                    Some(Data::Histogram(hist)) => {
                        for dp in &hist.data_points {
                            append_metric_common(
                                &mut metric_name,
                                &mut metric_description,
                                &mut metric_unit,
                                &mut metric_metadata,
                                &mut metric_type,
                                m_name,
                                m_desc,
                                m_unit,
                                &m_metadata,
                                2, // Histogram
                            );
                            time_unix_nano.append_value(dp.time_unix_nano);
                            if dp.start_time_unix_nano == 0 {
                                start_time_unix_nano.append_null();
                            } else {
                                start_time_unix_nano.append_value(dp.start_time_unix_nano);
                            }
                            data_point_flags.append_value(dp.flags);
                            value_int.append_null();
                            value_double.append_null();
                            #[allow(
                                clippy::cast_possible_truncation,
                                clippy::cast_sign_loss,
                                clippy::as_conversions
                            )]
                            aggregation_temporality
                                .append_value(hist.aggregation_temporality as u8);
                            is_monotonic.append_null();
                            count.append_value(dp.count);
                            match dp.sum {
                                Some(s) => sum.append_value(s),
                                None => sum.append_null(),
                            }
                            match dp.min {
                                Some(m) => min.append_value(m),
                                None => min.append_null(),
                            }
                            match dp.max {
                                Some(m) => max.append_value(m),
                                None => max.append_null(),
                            }
                            // Encode histogram bucket data
                            match encode_histogram_buckets(dp) {
                                Some(data) => histogram_data.append_value(&data),
                                None => histogram_data.append_null(),
                            }
                            append_scope_fields(
                                &mut scope_name,
                                &mut scope_version,
                                &mut scope_attributes,
                                sc_name,
                                sc_version,
                                &sc_attrs,
                            );
                            resource_dropped_attributes_count.append_value(res_dropped_count);
                            scope_dropped_attributes_count.append_value(sc_dropped_count);
                            append_resource_attrs(
                                &mut service_name,
                                &mut service_version,
                                &mut service_namespace,
                                &mut deployment_environment_name,
                                &mut host_name,
                                &mut host_id,
                                &mut container_id,
                                &mut k8s_pod_name,
                                &mut k8s_namespace_name,
                                &svc_name,
                                &svc_version,
                                &svc_namespace,
                                &deploy_env,
                                &h_name,
                                &h_id,
                                &c_id,
                                &k8s_pod,
                                &k8s_ns,
                            );
                            append_option_binary(
                                &mut resource_attributes,
                                remaining_resource.as_deref(),
                            );
                            let redacted_dp_attrs = maybe_redact_attributes(
                                &dp.attributes,
                                redaction,
                                Signal::Metrics,
                                AttributeScope::DataPoint,
                            );
                            append_option_binary(
                                &mut data_point_attributes,
                                encode_remaining_attrs(&redacted_dp_attrs, &[]).as_deref(),
                            );
                            append_exemplars(&mut exemplars, &dp.exemplars);
                        }
                    }
                    Some(Data::ExponentialHistogram(exp_hist)) => {
                        for dp in &exp_hist.data_points {
                            append_metric_common(
                                &mut metric_name,
                                &mut metric_description,
                                &mut metric_unit,
                                &mut metric_metadata,
                                &mut metric_type,
                                m_name,
                                m_desc,
                                m_unit,
                                &m_metadata,
                                3, // ExponentialHistogram
                            );
                            time_unix_nano.append_value(dp.time_unix_nano);
                            if dp.start_time_unix_nano == 0 {
                                start_time_unix_nano.append_null();
                            } else {
                                start_time_unix_nano.append_value(dp.start_time_unix_nano);
                            }
                            data_point_flags.append_value(dp.flags);
                            value_int.append_null();
                            value_double.append_null();
                            #[allow(
                                clippy::cast_possible_truncation,
                                clippy::cast_sign_loss,
                                clippy::as_conversions
                            )]
                            aggregation_temporality
                                .append_value(exp_hist.aggregation_temporality as u8);
                            is_monotonic.append_null();
                            count.append_value(dp.count);
                            match dp.sum {
                                Some(s) => sum.append_value(s),
                                None => sum.append_null(),
                            }
                            match dp.min {
                                Some(m) => min.append_value(m),
                                None => min.append_null(),
                            }
                            match dp.max {
                                Some(m) => max.append_value(m),
                                None => max.append_null(),
                            }
                            match encode_exp_histogram(dp) {
                                Some(data) => histogram_data.append_value(&data),
                                None => histogram_data.append_null(),
                            }
                            append_scope_fields(
                                &mut scope_name,
                                &mut scope_version,
                                &mut scope_attributes,
                                sc_name,
                                sc_version,
                                &sc_attrs,
                            );
                            resource_dropped_attributes_count.append_value(res_dropped_count);
                            scope_dropped_attributes_count.append_value(sc_dropped_count);
                            append_resource_attrs(
                                &mut service_name,
                                &mut service_version,
                                &mut service_namespace,
                                &mut deployment_environment_name,
                                &mut host_name,
                                &mut host_id,
                                &mut container_id,
                                &mut k8s_pod_name,
                                &mut k8s_namespace_name,
                                &svc_name,
                                &svc_version,
                                &svc_namespace,
                                &deploy_env,
                                &h_name,
                                &h_id,
                                &c_id,
                                &k8s_pod,
                                &k8s_ns,
                            );
                            append_option_binary(
                                &mut resource_attributes,
                                remaining_resource.as_deref(),
                            );
                            let redacted_dp_attrs = maybe_redact_attributes(
                                &dp.attributes,
                                redaction,
                                Signal::Metrics,
                                AttributeScope::DataPoint,
                            );
                            append_option_binary(
                                &mut data_point_attributes,
                                encode_remaining_attrs(&redacted_dp_attrs, &[]).as_deref(),
                            );
                            append_exemplars(&mut exemplars, &dp.exemplars);
                        }
                    }
                    Some(Data::Summary(summary)) => {
                        for dp in &summary.data_points {
                            append_metric_common(
                                &mut metric_name,
                                &mut metric_description,
                                &mut metric_unit,
                                &mut metric_metadata,
                                &mut metric_type,
                                m_name,
                                m_desc,
                                m_unit,
                                &m_metadata,
                                4, // Summary
                            );
                            time_unix_nano.append_value(dp.time_unix_nano);
                            if dp.start_time_unix_nano == 0 {
                                start_time_unix_nano.append_null();
                            } else {
                                start_time_unix_nano.append_value(dp.start_time_unix_nano);
                            }
                            data_point_flags.append_value(dp.flags);
                            value_int.append_null();
                            value_double.append_null();
                            aggregation_temporality.append_null();
                            is_monotonic.append_null();
                            count.append_value(dp.count);
                            sum.append_value(dp.sum);
                            min.append_null();
                            max.append_null();
                            match encode_summary(dp) {
                                Some(data) => histogram_data.append_value(&data),
                                None => histogram_data.append_null(),
                            }
                            append_scope_fields(
                                &mut scope_name,
                                &mut scope_version,
                                &mut scope_attributes,
                                sc_name,
                                sc_version,
                                &sc_attrs,
                            );
                            resource_dropped_attributes_count.append_value(res_dropped_count);
                            scope_dropped_attributes_count.append_value(sc_dropped_count);
                            append_resource_attrs(
                                &mut service_name,
                                &mut service_version,
                                &mut service_namespace,
                                &mut deployment_environment_name,
                                &mut host_name,
                                &mut host_id,
                                &mut container_id,
                                &mut k8s_pod_name,
                                &mut k8s_namespace_name,
                                &svc_name,
                                &svc_version,
                                &svc_namespace,
                                &deploy_env,
                                &h_name,
                                &h_id,
                                &c_id,
                                &k8s_pod,
                                &k8s_ns,
                            );
                            append_option_binary(
                                &mut resource_attributes,
                                remaining_resource.as_deref(),
                            );
                            let redacted_dp_attrs = maybe_redact_attributes(
                                &dp.attributes,
                                redaction,
                                Signal::Metrics,
                                AttributeScope::DataPoint,
                            );
                            append_option_binary(
                                &mut data_point_attributes,
                                encode_remaining_attrs(&redacted_dp_attrs, &[]).as_deref(),
                            );
                            exemplars.append_null(); // Summary doesn't have exemplars
                        }
                    }
                    None => {}
                }
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(metric_name.finish()),
        Arc::new(metric_description.finish()),
        Arc::new(metric_unit.finish()),
        Arc::new(metric_metadata.finish()),
        Arc::new(metric_type.finish()),
        Arc::new(time_unix_nano.finish()),
        Arc::new(start_time_unix_nano.finish()),
        Arc::new(data_point_flags.finish()),
        Arc::new(value_int.finish()),
        Arc::new(value_double.finish()),
        Arc::new(aggregation_temporality.finish()),
        Arc::new(is_monotonic.finish()),
        Arc::new(count.finish()),
        Arc::new(sum.finish()),
        Arc::new(min.finish()),
        Arc::new(max.finish()),
        Arc::new(histogram_data.finish()),
        Arc::new(scope_name.finish()),
        Arc::new(scope_version.finish()),
        Arc::new(scope_attributes.finish()),
        Arc::new(resource_dropped_attributes_count.finish()),
        Arc::new(scope_dropped_attributes_count.finish()),
        Arc::new(service_name.finish()),
        Arc::new(service_version.finish()),
        Arc::new(service_namespace.finish()),
        Arc::new(deployment_environment_name.finish()),
        Arc::new(host_name.finish()),
        Arc::new(host_id.finish()),
        Arc::new(container_id.finish()),
        Arc::new(k8s_pod_name.finish()),
        Arc::new(k8s_namespace_name.finish()),
        Arc::new(resource_attributes.finish()),
        Arc::new(data_point_attributes.finish()),
        Arc::new(exemplars.finish()),
    ];

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(ConversionResult::success(batch))
}

fn count_metric_data_points(metric: &opentelemetry_proto::tonic::metrics::v1::Metric) -> usize {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data;
    match &metric.data {
        Some(Data::Gauge(g)) => g.data_points.len(),
        Some(Data::Sum(s)) => s.data_points.len(),
        Some(Data::Histogram(h)) => h.data_points.len(),
        Some(Data::ExponentialHistogram(e)) => e.data_points.len(),
        Some(Data::Summary(s)) => s.data_points.len(),
        None => 0,
    }
}

#[allow(clippy::too_many_arguments, clippy::ref_option)]
fn append_metric_common(
    metric_name: &mut StringBuilder,
    metric_description: &mut StringBuilder,
    metric_unit: &mut StringBuilder,
    metric_metadata: &mut BinaryBuilder,
    metric_type: &mut UInt8Builder,
    name: &str,
    desc: Option<&String>,
    unit: Option<&String>,
    metadata: &Option<Vec<u8>>,
    mtype: u8,
) {
    metric_name.append_value(name);
    match desc {
        Some(d) => metric_description.append_value(d),
        None => metric_description.append_null(),
    }
    match unit {
        Some(u) => metric_unit.append_value(u),
        None => metric_unit.append_null(),
    }
    append_option_binary(metric_metadata, metadata.as_deref());
    metric_type.append_value(mtype);
}

fn append_number_data_point(
    time_unix_nano: &mut UInt64Builder,
    start_time_unix_nano: &mut UInt64Builder,
    value_int: &mut arrow::array::Int64Builder,
    value_double: &mut arrow::array::Float64Builder,
    dp: &opentelemetry_proto::tonic::metrics::v1::NumberDataPoint,
) {
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value;
    time_unix_nano.append_value(dp.time_unix_nano);
    if dp.start_time_unix_nano == 0 {
        start_time_unix_nano.append_null();
    } else {
        start_time_unix_nano.append_value(dp.start_time_unix_nano);
    }
    match &dp.value {
        Some(Value::AsInt(i)) => {
            value_int.append_value(*i);
            value_double.append_null();
        }
        Some(Value::AsDouble(d)) => {
            value_int.append_null();
            value_double.append_value(*d);
        }
        None => {
            value_int.append_null();
            value_double.append_null();
        }
    }
}

#[allow(clippy::ref_option)]
fn append_scope_fields(
    scope_name: &mut StringBuilder,
    scope_version: &mut StringBuilder,
    scope_attributes: &mut BinaryBuilder,
    sc_name: &str,
    sc_version: &str,
    sc_attrs: &Option<Vec<u8>>,
) {
    if sc_name.is_empty() {
        scope_name.append_null();
    } else {
        scope_name.append_value(sc_name);
    }
    if sc_version.is_empty() {
        scope_version.append_null();
    } else {
        scope_version.append_value(sc_version);
    }
    append_option_binary(scope_attributes, sc_attrs.as_deref());
}

#[allow(clippy::too_many_arguments, clippy::ref_option)]
fn append_resource_attrs(
    service_name: &mut StringBuilder,
    service_version: &mut StringBuilder,
    service_namespace: &mut StringBuilder,
    deployment_environment_name: &mut StringBuilder,
    host_name: &mut StringBuilder,
    host_id: &mut StringBuilder,
    container_id: &mut StringBuilder,
    k8s_pod_name: &mut StringBuilder,
    k8s_namespace_name: &mut StringBuilder,
    svc_name: &str,
    svc_version: &Option<String>,
    svc_namespace: &Option<String>,
    deploy_env: &Option<String>,
    h_name: &Option<String>,
    h_id: &Option<String>,
    c_id: &Option<String>,
    k8s_pod: &Option<String>,
    k8s_ns: &Option<String>,
) {
    service_name.append_value(svc_name);
    append_option_string(service_version, svc_version.as_deref());
    append_option_string(service_namespace, svc_namespace.as_deref());
    append_option_string(deployment_environment_name, deploy_env.as_deref());
    append_option_string(host_name, h_name.as_deref());
    append_option_string(host_id, h_id.as_deref());
    append_option_string(container_id, c_id.as_deref());
    append_option_string(k8s_pod_name, k8s_pod.as_deref());
    append_option_string(k8s_namespace_name, k8s_ns.as_deref());
}

fn append_exemplars(
    builder: &mut BinaryBuilder,
    exemplars: &[opentelemetry_proto::tonic::metrics::v1::Exemplar],
) {
    use base64::Engine;

    if exemplars.is_empty() {
        builder.append_null();
        return;
    }

    let encoded: Vec<serde_json::Value> = exemplars
        .iter()
        .map(|e| {
            use opentelemetry_proto::tonic::metrics::v1::exemplar::Value;
            let val = match &e.value {
                Some(Value::AsInt(i)) => serde_json::json!(*i),
                Some(Value::AsDouble(d)) => serde_json::json!(*d),
                None => serde_json::Value::Null,
            };
            let filtered_attrs: serde_json::Map<String, serde_json::Value> = e
                .filtered_attributes
                .iter()
                .filter_map(|kv| {
                    kv.value
                        .as_ref()
                        .map(|v| (kv.key.clone(), any_value_to_json(v)))
                })
                .collect();
            serde_json::json!({
                "time_unix_nano": e.time_unix_nano,
                "value": val,
                "trace_id": base64::engine::general_purpose::STANDARD.encode(&e.trace_id),
                "span_id": base64::engine::general_purpose::STANDARD.encode(&e.span_id),
                "filtered_attributes": filtered_attrs,
            })
        })
        .collect();

    let mut buf = Vec::new();
    match ciborium::into_writer(&encoded, &mut buf) {
        Ok(()) => builder.append_value(&buf),
        Err(_) => builder.append_null(),
    }
}

fn encode_histogram_buckets(
    dp: &opentelemetry_proto::tonic::metrics::v1::HistogramDataPoint,
) -> Option<Vec<u8>> {
    let data = serde_json::json!({
        "bucket_counts": dp.bucket_counts,
        "explicit_bounds": dp.explicit_bounds,
    });
    let mut buf = Vec::new();
    ciborium::into_writer(&data, &mut buf).ok()?;
    Some(buf)
}

fn encode_exp_histogram(
    dp: &opentelemetry_proto::tonic::metrics::v1::ExponentialHistogramDataPoint,
) -> Option<Vec<u8>> {
    let positive = dp.positive.as_ref().map(|b| {
        serde_json::json!({
            "offset": b.offset,
            "bucket_counts": b.bucket_counts,
        })
    });
    let negative = dp.negative.as_ref().map(|b| {
        serde_json::json!({
            "offset": b.offset,
            "bucket_counts": b.bucket_counts,
        })
    });
    let data = serde_json::json!({
        "scale": dp.scale,
        "zero_count": dp.zero_count,
        "positive": positive,
        "negative": negative,
    });
    let mut buf = Vec::new();
    ciborium::into_writer(&data, &mut buf).ok()?;
    Some(buf)
}

fn encode_summary(
    dp: &opentelemetry_proto::tonic::metrics::v1::SummaryDataPoint,
) -> Option<Vec<u8>> {
    let quantiles: Vec<serde_json::Value> = dp
        .quantile_values
        .iter()
        .map(|q| {
            serde_json::json!({
                "quantile": q.quantile,
                "value": q.value,
            })
        })
        .collect();
    let data = serde_json::json!({
        "quantile_values": quantiles,
    });
    let mut buf = Vec::new();
    ciborium::into_writer(&data, &mut buf).ok()?;
    Some(buf)
}

/// Convert OTLP logs to an Arrow RecordBatch.
///
/// Returns a batch using the storage schema (without partition columns).
/// Partition columns (date, hour) are derived from the directory path
/// when writing to storage, not stored in the Parquet file.
///
/// If a redaction engine is provided, attributes matching the configured
/// rules will be dropped, hashed, or replaced before conversion.
pub fn convert_logs_to_arrow(
    request: &ExportLogsServiceRequest,
    redaction: Option<&RedactionEngine>,
) -> Result<ConversionResult, TelemetryError> {
    let schema = logs_storage_schema();

    let capacity: usize = request
        .resource_logs
        .iter()
        .flat_map(|rl| &rl.scope_logs)
        .map(|sl| sl.log_records.len())
        .sum();

    if capacity == 0 {
        return Ok(ConversionResult::success(RecordBatch::new_empty(schema)));
    }

    // Create builders
    let mut time_unix_nano = UInt64Builder::with_capacity(capacity);
    let mut observed_time_unix_nano = UInt64Builder::with_capacity(capacity);
    let mut severity_number = UInt8Builder::with_capacity(capacity);
    let mut severity_text =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut flags = arrow::array::UInt32Builder::with_capacity(capacity);
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(capacity, 16);
    let mut span_id = FixedSizeBinaryBuilder::with_capacity(capacity, 8);
    let mut body_string = StringBuilder::with_capacity(capacity, capacity * CAPACITY_LOG_BODY);
    let mut body_bytes = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);

    // Instrumentation scope
    let mut scope_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut scope_version =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut scope_attributes = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);

    // Dropped attribute counts
    let mut resource_dropped_attributes_count =
        arrow::array::UInt32Builder::with_capacity(capacity);
    let mut scope_dropped_attributes_count = arrow::array::UInt32Builder::with_capacity(capacity);
    let mut log_dropped_attributes_count = arrow::array::UInt32Builder::with_capacity(capacity);

    // Resource attributes
    let mut service_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut service_version =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut service_namespace =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_IDENTIFIER);
    let mut deployment_environment_name =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);

    // Log-specific attributes
    let mut log_file_path = StringBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut log_file_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut log_iostream = StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);

    // Exception attributes
    let mut exception_type = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut exception_message =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_ATTRIBUTES);
    let mut exception_stacktrace =
        StringBuilder::with_capacity(capacity, capacity * CAPACITY_STACKTRACE);

    let mut code_function = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut code_filepath = StringBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut code_lineno = Int32Builder::with_capacity(capacity);

    let mut event_name = StringBuilder::with_capacity(capacity, capacity * CAPACITY_NAME);
    let mut event_domain = StringBuilder::with_capacity(capacity, capacity * CAPACITY_SHORT_STRING);
    let mut resource_attributes = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_PATH);
    let mut log_attributes = BinaryBuilder::with_capacity(capacity, capacity * CAPACITY_ATTRIBUTES);

    // Note: Partition columns (project_id, date, hour) are NOT stored in Parquet.
    // They are derived from the directory path when DataFusion reads the files.

    let resource_extracted_keys = [
        semconv::SERVICE_NAME,
        semconv::SERVICE_VERSION,
        semconv::SERVICE_NAMESPACE,
        semconv::DEPLOYMENT_ENVIRONMENT_NAME,
    ];

    let log_extracted_keys = [
        semconv::LOG_FILE_PATH,
        semconv::LOG_FILE_NAME,
        semconv::LOG_IOSTREAM,
        semconv::EXCEPTION_TYPE,
        semconv::EXCEPTION_MESSAGE,
        semconv::EXCEPTION_STACKTRACE,
        semconv::CODE_FUNCTION,
        semconv::CODE_FILEPATH,
        semconv::CODE_LINENO,
        semconv::EVENT_NAME,
        semconv::EVENT_DOMAIN,
    ];

    for resource_logs in &request.resource_logs {
        let (raw_resource_attrs, res_dropped_count) =
            resource_logs.resource.as_ref().map_or((&[][..], 0), |r| {
                (&r.attributes[..], r.dropped_attributes_count)
            });

        // Apply redaction to resource attributes
        let resource_attrs = maybe_redact_attributes(
            raw_resource_attrs,
            redaction,
            Signal::Logs,
            AttributeScope::Resource,
        );
        let resource_map = attributes_to_map(&resource_attrs);

        let svc_name = extract_string_attr(&resource_map, semconv::SERVICE_NAME)
            .unwrap_or_else(|| DEFAULT_SERVICE_NAME.to_owned());
        let svc_version = extract_string_attr(&resource_map, semconv::SERVICE_VERSION);
        let svc_namespace = extract_string_attr(&resource_map, semconv::SERVICE_NAMESPACE);
        let deploy_env = extract_string_attr(&resource_map, semconv::DEPLOYMENT_ENVIRONMENT_NAME);

        let remaining_resource = encode_remaining_attrs(&resource_attrs, &resource_extracted_keys);

        for scope_logs in &resource_logs.scope_logs {
            // Extract instrumentation scope info with optional redaction
            let (sc_name, sc_version, sc_attrs, sc_dropped_count) =
                scope_logs.scope.as_ref().map_or(("", "", None, 0), |s| {
                    let redacted_attrs = maybe_redact_attributes(
                        &s.attributes,
                        redaction,
                        Signal::Logs,
                        AttributeScope::Scope,
                    );
                    let attrs = encode_remaining_attrs(&redacted_attrs, &[]);
                    (
                        s.name.as_str(),
                        s.version.as_str(),
                        attrs,
                        s.dropped_attributes_count,
                    )
                });

            for log in &scope_logs.log_records {
                time_unix_nano.append_value(log.time_unix_nano);
                observed_time_unix_nano.append_value(log.observed_time_unix_nano);
                #[allow(
                    clippy::cast_possible_truncation,
                    clippy::cast_sign_loss,
                    clippy::as_conversions
                )]
                severity_number.append_value(log.severity_number as u8);

                if log.severity_text.is_empty() {
                    severity_text.append_null();
                } else {
                    severity_text.append_value(&log.severity_text);
                }
                flags.append_value(log.flags);

                // Trace correlation
                if log.trace_id.is_empty() {
                    trace_id.append_null();
                } else {
                    let tid = pad_or_truncate(&log.trace_id, 16);
                    trace_id.append_value(&tid)?;
                }

                if log.span_id.is_empty() {
                    span_id.append_null();
                } else {
                    let sid = pad_or_truncate(&log.span_id, 8);
                    span_id.append_value(&sid)?;
                }

                // Body
                if let Some(body) = &log.body {
                    match &body.value {
                        Some(AnyValueKind::StringValue(s)) => {
                            body_string.append_value(s);
                            body_bytes.append_null();
                        }
                        Some(AnyValueKind::BytesValue(b)) => {
                            body_string.append_null();
                            body_bytes.append_value(b);
                        }
                        Some(other) => {
                            // Convert other types to string representation
                            let s = any_value_to_string(&AnyValue {
                                value: Some(other.clone()),
                            });
                            if let Some(str_val) = s {
                                body_string.append_value(&str_val);
                                body_bytes.append_null();
                            } else {
                                body_string.append_null();
                                body_bytes.append_null();
                            }
                        }
                        None => {
                            body_string.append_null();
                            body_bytes.append_null();
                        }
                    }
                } else {
                    body_string.append_null();
                    body_bytes.append_null();
                }

                // Instrumentation scope
                append_scope_fields(
                    &mut scope_name,
                    &mut scope_version,
                    &mut scope_attributes,
                    sc_name,
                    sc_version,
                    &sc_attrs,
                );

                // Dropped attribute counts
                resource_dropped_attributes_count.append_value(res_dropped_count);
                scope_dropped_attributes_count.append_value(sc_dropped_count);
                log_dropped_attributes_count.append_value(log.dropped_attributes_count);

                // Resource attributes
                service_name.append_value(&svc_name);
                append_option_string(&mut service_version, svc_version.as_deref());
                append_option_string(&mut service_namespace, svc_namespace.as_deref());
                append_option_string(&mut deployment_environment_name, deploy_env.as_deref());

                // Log attributes with optional redaction
                let log_attrs = maybe_redact_attributes(
                    &log.attributes,
                    redaction,
                    Signal::Logs,
                    AttributeScope::Log,
                );
                let log_map = attributes_to_map(&log_attrs);

                append_option_string(
                    &mut log_file_path,
                    extract_string_attr(&log_map, semconv::LOG_FILE_PATH).as_deref(),
                );
                append_option_string(
                    &mut log_file_name,
                    extract_string_attr(&log_map, semconv::LOG_FILE_NAME).as_deref(),
                );
                append_option_string(
                    &mut log_iostream,
                    extract_string_attr(&log_map, semconv::LOG_IOSTREAM).as_deref(),
                );
                append_option_string(
                    &mut exception_type,
                    extract_string_attr(&log_map, semconv::EXCEPTION_TYPE).as_deref(),
                );
                append_option_string(
                    &mut exception_message,
                    extract_string_attr(&log_map, semconv::EXCEPTION_MESSAGE).as_deref(),
                );
                append_option_string(
                    &mut exception_stacktrace,
                    extract_string_attr(&log_map, semconv::EXCEPTION_STACKTRACE).as_deref(),
                );
                append_option_string(
                    &mut code_function,
                    extract_string_attr(&log_map, semconv::CODE_FUNCTION).as_deref(),
                );
                append_option_string(
                    &mut code_filepath,
                    extract_string_attr(&log_map, semconv::CODE_FILEPATH).as_deref(),
                );
                append_option_i32(
                    &mut code_lineno,
                    extract_i32_attr(&log_map, semconv::CODE_LINENO),
                );
                append_option_string(
                    &mut event_name,
                    extract_string_attr(&log_map, semconv::EVENT_NAME).as_deref(),
                );
                append_option_string(
                    &mut event_domain,
                    extract_string_attr(&log_map, semconv::EVENT_DOMAIN).as_deref(),
                );
                append_option_binary(&mut resource_attributes, remaining_resource.as_deref());
                append_option_binary(
                    &mut log_attributes,
                    encode_remaining_attrs(&log_attrs, &log_extracted_keys).as_deref(),
                );
            }
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(time_unix_nano.finish()),
        Arc::new(observed_time_unix_nano.finish()),
        Arc::new(severity_number.finish()),
        Arc::new(severity_text.finish()),
        Arc::new(flags.finish()),
        Arc::new(trace_id.finish()),
        Arc::new(span_id.finish()),
        Arc::new(body_string.finish()),
        Arc::new(body_bytes.finish()),
        Arc::new(scope_name.finish()),
        Arc::new(scope_version.finish()),
        Arc::new(scope_attributes.finish()),
        Arc::new(resource_dropped_attributes_count.finish()),
        Arc::new(scope_dropped_attributes_count.finish()),
        Arc::new(log_dropped_attributes_count.finish()),
        Arc::new(service_name.finish()),
        Arc::new(service_version.finish()),
        Arc::new(service_namespace.finish()),
        Arc::new(deployment_environment_name.finish()),
        Arc::new(log_file_path.finish()),
        Arc::new(log_file_name.finish()),
        Arc::new(log_iostream.finish()),
        Arc::new(exception_type.finish()),
        Arc::new(exception_message.finish()),
        Arc::new(exception_stacktrace.finish()),
        Arc::new(code_function.finish()),
        Arc::new(code_filepath.finish()),
        Arc::new(code_lineno.finish()),
        Arc::new(event_name.finish()),
        Arc::new(event_domain.finish()),
        Arc::new(resource_attributes.finish()),
        Arc::new(log_attributes.finish()),
    ];

    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(ConversionResult::success(batch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::InstrumentationScope,
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };

    fn make_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueKind::StringValue(value.to_string())),
            }),
        }
    }

    fn make_int_kv(key: &str, value: i64) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueKind::IntValue(value)),
            }),
        }
    }

    #[test]
    fn convert_simple_span() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "test-service")],
                    dropped_attributes_count: 0,
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    spans: vec![Span {
                        trace_id: vec![1u8; 16],
                        span_id: vec![2u8; 8],
                        parent_span_id: vec![],
                        name: "test-span".to_string(),
                        kind: 1,
                        start_time_unix_nano: 1_704_067_200_000_000_000,
                        end_time_unix_nano: 1_704_067_201_000_000_000,
                        attributes: vec![
                            make_kv("http.request.method", "GET"),
                            make_int_kv("http.response.status_code", 200),
                        ],
                        status: Some(Status {
                            code: 1,
                            message: String::new(),
                        }),
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;

        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 53);

        // Verify service name
        let svc_col = batch
            .column_by_name("service.name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(svc_col.value(0), "test-service");

        // Verify span name
        let name_col = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "test-span");

        // Verify duration
        let dur_col = batch
            .column_by_name("duration_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(dur_col.value(0), 1_000_000_000);

        // Verify HTTP method
        let method_col = batch
            .column_by_name("http.request.method")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(method_col.value(0), "GET");

        // Verify HTTP status code
        let status_col = batch
            .column_by_name("http.response.status_code")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(status_col.value(0), 200);
    }

    #[test]
    fn convert_empty_request() {
        let request = ExportTraceServiceRequest {
            resource_spans: vec![],
        };

        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn convert_multiple_spans_preserves_order() {
        use arrow::array::Array;
        use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "multi-service")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![
                        Span {
                            trace_id: vec![0x01; 16],
                            span_id: vec![0x01; 8],
                            name: "span-a".to_string(),
                            start_time_unix_nano: 1_000_000_000,
                            end_time_unix_nano: 2_000_000_000,
                            ..Default::default()
                        },
                        Span {
                            trace_id: vec![0x02; 16],
                            span_id: vec![0x02; 8],
                            name: "span-b".to_string(),
                            start_time_unix_nano: 3_000_000_000,
                            end_time_unix_nano: 4_000_000_000,
                            ..Default::default()
                        },
                        Span {
                            trace_id: vec![0x03; 16],
                            span_id: vec![0x03; 8],
                            name: "span-c".to_string(),
                            start_time_unix_nano: 5_000_000_000,
                            end_time_unix_nano: 6_000_000_000,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;

        assert_eq!(batch.num_rows(), 3);

        // Verify span names are in the same order as input
        let names = batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "span-a");
        assert_eq!(names.value(1), "span-b");
        assert_eq!(names.value(2), "span-c");

        // Verify trace IDs are distinct
        let trace_ids = batch
            .column_by_name("trace_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(trace_ids.value(0), &[0x01; 16]);
        assert_eq!(trace_ids.value(1), &[0x02; 16]);
        assert_eq!(trace_ids.value(2), &[0x03; 16]);
    }

    #[test]
    fn convert_span_with_all_span_kinds() {
        use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

        let span_kinds = [
            (0, "UNSPECIFIED"),
            (1, "INTERNAL"),
            (2, "SERVER"),
            (3, "CLIENT"),
            (4, "PRODUCER"),
            (5, "CONSUMER"),
        ];

        for (kind, _name) in span_kinds {
            let request = ExportTraceServiceRequest {
                resource_spans: vec![ResourceSpans {
                    resource: Some(Resource {
                        attributes: vec![make_kv("service.name", "kind-test")],
                        ..Default::default()
                    }),
                    scope_spans: vec![ScopeSpans {
                        spans: vec![Span {
                            trace_id: vec![0xAA; 16],
                            span_id: vec![0xBB; 8],
                            name: format!("span-kind-{}", kind),
                            kind: kind as i32,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            };

            let batch = convert_traces_to_arrow(&request, None).unwrap().batch;
            assert_eq!(batch.num_rows(), 1);

            let kind_col = batch
                .column_by_name("kind")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .unwrap();
            assert_eq!(kind_col.value(0), kind as u8, "span kind mismatch");
        }
    }

    #[test]
    fn convert_span_with_parent() {
        use arrow::array::Array;
        use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

        let parent_span_id = vec![0xCC; 8];

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "parent-test")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![0xAA; 16],
                        span_id: vec![0xBB; 8],
                        parent_span_id: parent_span_id.clone(),
                        name: "child-span".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;

        let parent_col = batch
            .column_by_name("parent_span_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();

        assert!(!parent_col.is_null(0), "parent_span_id should not be null");
        assert_eq!(parent_col.value(0), &[0xCC; 8]);
    }

    #[test]
    fn convert_root_span_has_null_parent() {
        use arrow::array::Array;
        use opentelemetry_proto::tonic::trace::v1::ResourceSpans;

        let request = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "root-test")],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![0xAA; 16],
                        span_id: vec![0xBB; 8],
                        parent_span_id: vec![], // Empty = root span
                        name: "root-span".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let batch = convert_traces_to_arrow(&request, None).unwrap().batch;

        let parent_col = batch
            .column_by_name("parent_span_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::FixedSizeBinaryArray>()
            .unwrap();

        assert!(
            parent_col.is_null(0),
            "root span parent_span_id should be null"
        );
    }

    #[test]
    fn convert_gauge_with_exemplar_filtered_attributes() {
        use arrow::array::Array;
        use opentelemetry_proto::tonic::metrics::v1::{
            metric::Data, number_data_point::Value, Exemplar, Gauge, Metric, NumberDataPoint,
            ResourceMetrics, ScopeMetrics,
        };

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![make_kv("service.name", "exemplar-test")],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: vec![Metric {
                        name: "test.gauge".to_string(),
                        data: Some(Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1_704_067_200_000_000_000,
                                value: Some(Value::AsDouble(42.5)),
                                exemplars: vec![Exemplar {
                                    time_unix_nano: 1_704_067_199_000_000_000,
                                    value: Some(
                                        opentelemetry_proto::tonic::metrics::v1::exemplar::Value::AsDouble(
                                            100.0,
                                        ),
                                    ),
                                    trace_id: vec![0xAA; 16],
                                    span_id: vec![0xBB; 8],
                                    filtered_attributes: vec![
                                        make_kv("sample.key", "sample-value"),
                                        make_int_kv("sample.count", 42),
                                    ],
                                }],
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let batch = convert_metrics_to_arrow(&request, None).unwrap().batch;
        assert_eq!(batch.num_rows(), 1);

        let exemplars_col = batch
            .column_by_name("exemplars")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert!(!exemplars_col.is_null(0), "exemplars should not be null");

        let exemplars_bytes = exemplars_col.value(0);
        let decoded: Vec<serde_json::Value> =
            ciborium::from_reader(exemplars_bytes).expect("CBOR decode failed");
        assert_eq!(decoded.len(), 1);

        let exemplar = &decoded[0];
        assert_eq!(exemplar["value"], 100.0);
        assert!(exemplar["filtered_attributes"].is_object());

        let filtered = exemplar["filtered_attributes"].as_object().unwrap();
        assert_eq!(filtered["sample.key"], "sample-value");
        assert_eq!(filtered["sample.count"], 42);
    }
}
