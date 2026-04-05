//! Arrow schema for trace spans.
//!
//! Schema follows [OTel Trace Data Model] with dedicated columns for commonly-queried
//! attributes based on [OTel Semantic Conventions].
//!
//! [OTel Trace Data Model]: https://opentelemetry.io/docs/specs/otel/trace/api/
//! [OTel Semantic Conventions]: https://opentelemetry.io/docs/specs/semconv/

use arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;

/// OpenTelemetry semantic convention attribute names.
///
/// See <https://opentelemetry.io/docs/specs/semconv/> for the full specification.
mod semconv {
    /// Service identifier. Required for all telemetry.
    /// See <https://opentelemetry.io/docs/specs/semconv/resource/#service>
    pub const SERVICE_NAME: &str = "service.name";
    pub const SERVICE_VERSION: &str = "service.version";
    pub const SERVICE_NAMESPACE: &str = "service.namespace";

    /// Deployment environment (e.g. "production", "staging").
    /// See <https://opentelemetry.io/docs/specs/semconv/resource/deployment-environment/>
    pub const DEPLOYMENT_ENVIRONMENT_NAME: &str = "deployment.environment.name";

    /// Telemetry SDK identification.
    /// See <https://opentelemetry.io/docs/specs/semconv/resource/#telemetry-sdk>
    pub const TELEMETRY_SDK_NAME: &str = "telemetry.sdk.name";
    pub const TELEMETRY_SDK_VERSION: &str = "telemetry.sdk.version";
    pub const TELEMETRY_SDK_LANGUAGE: &str = "telemetry.sdk.language";

    /// HTTP span attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/http/http-spans/>
    pub const HTTP_REQUEST_METHOD: &str = "http.request.method";
    pub const HTTP_RESPONSE_STATUS_CODE: &str = "http.response.status_code";
    pub const HTTP_ROUTE: &str = "http.route";
    pub const URL_SCHEME: &str = "url.scheme";
    pub const URL_PATH: &str = "url.path";
    pub const SERVER_ADDRESS: &str = "server.address";
    pub const SERVER_PORT: &str = "server.port";
    pub const ERROR_TYPE: &str = "error.type";

    /// Database span attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/database/database-spans/>
    pub const DB_SYSTEM_NAME: &str = "db.system.name";
    pub const DB_OPERATION_NAME: &str = "db.operation.name";
    pub const DB_COLLECTION_NAME: &str = "db.collection.name";

    /// RPC span attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/rpc/rpc-spans/>
    pub const RPC_SYSTEM: &str = "rpc.system";
    pub const RPC_SERVICE: &str = "rpc.service";
    pub const RPC_METHOD: &str = "rpc.method";

    /// Messaging span attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/>
    pub const MESSAGING_SYSTEM: &str = "messaging.system";
    pub const MESSAGING_OPERATION_TYPE: &str = "messaging.operation.type";
    pub const MESSAGING_DESTINATION_NAME: &str = "messaging.destination.name";

    /// Network attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/attributes-registry/network/>
    pub const NETWORK_PROTOCOL_NAME: &str = "network.protocol.name";
    pub const NETWORK_PROTOCOL_VERSION: &str = "network.protocol.version";
    pub const USER_AGENT_ORIGINAL: &str = "user_agent.original";

    /// Client attributes (conditionally required for client spans).
    /// See <https://opentelemetry.io/docs/specs/semconv/attributes-registry/client/>
    pub const CLIENT_ADDRESS: &str = "client.address";
    pub const CLIENT_PORT: &str = "client.port";
}

/// Create the Arrow schema for trace spans.
///
/// # Field Groups
///
/// **Core fields**: `trace_id`, `span_id`, `parent_span_id`, `flags` (W3C trace context),
/// `trace_state`, timing (`start_time_unix_nano`, `end_time_unix_nano`, `duration_ns`),
/// `name`, `kind` (SpanKind enum), `status_code`, `status_message`
///
/// **Instrumentation scope**: `scope_name`, `scope_version`, `scope_attributes` (CBOR)
///
/// **Dropped counts**: Track SDK-side drops for debugging instrumentation
///
/// **Semantic convention attributes**: Dedicated columns for commonly-queried attributes
/// (service, HTTP, database, RPC, messaging, network, client). Remaining attributes
/// stored as CBOR in `resource_attributes` and `span_attributes`.
///
/// **Events and links**: `events` (CBOR Vec<SpanEvent>), `links` (CBOR Vec<SpanLink>)
///
/// **Partition columns**: `project_id`, `date`, `hour` - extracted from directory path
/// by DataFusion, not stored in Parquet files (see [`traces_storage_schema`]).
pub fn traces_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("parent_span_id", DataType::FixedSizeBinary(8), true),
        Field::new("flags", DataType::UInt32, false),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("start_time_unix_nano", DataType::UInt64, false),
        Field::new("end_time_unix_nano", DataType::UInt64, false),
        Field::new("duration_ns", DataType::UInt64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::UInt8, false),
        Field::new("status_code", DataType::UInt8, false),
        Field::new("status_message", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Binary, true),
        Field::new("resource_dropped_attributes_count", DataType::UInt32, false),
        Field::new("scope_dropped_attributes_count", DataType::UInt32, false),
        Field::new("span_dropped_attributes_count", DataType::UInt32, false),
        Field::new("span_dropped_events_count", DataType::UInt32, false),
        Field::new("span_dropped_links_count", DataType::UInt32, false),
        Field::new(semconv::SERVICE_NAME, DataType::Utf8, false),
        Field::new(semconv::SERVICE_VERSION, DataType::Utf8, true),
        Field::new(semconv::SERVICE_NAMESPACE, DataType::Utf8, true),
        Field::new(semconv::DEPLOYMENT_ENVIRONMENT_NAME, DataType::Utf8, true),
        Field::new(semconv::TELEMETRY_SDK_NAME, DataType::Utf8, true),
        Field::new(semconv::TELEMETRY_SDK_VERSION, DataType::Utf8, true),
        Field::new(semconv::TELEMETRY_SDK_LANGUAGE, DataType::Utf8, true),
        Field::new(semconv::HTTP_REQUEST_METHOD, DataType::Utf8, true),
        Field::new(semconv::HTTP_RESPONSE_STATUS_CODE, DataType::Int32, true),
        Field::new(semconv::HTTP_ROUTE, DataType::Utf8, true),
        Field::new(semconv::URL_SCHEME, DataType::Utf8, true),
        Field::new(semconv::URL_PATH, DataType::Utf8, true),
        Field::new(semconv::SERVER_ADDRESS, DataType::Utf8, true),
        Field::new(semconv::SERVER_PORT, DataType::Int32, true),
        Field::new(semconv::ERROR_TYPE, DataType::Utf8, true),
        Field::new(semconv::DB_SYSTEM_NAME, DataType::Utf8, true),
        Field::new(semconv::DB_OPERATION_NAME, DataType::Utf8, true),
        Field::new(semconv::DB_COLLECTION_NAME, DataType::Utf8, true),
        Field::new(semconv::RPC_SYSTEM, DataType::Utf8, true),
        Field::new(semconv::RPC_SERVICE, DataType::Utf8, true),
        Field::new(semconv::RPC_METHOD, DataType::Utf8, true),
        Field::new(semconv::MESSAGING_SYSTEM, DataType::Utf8, true),
        Field::new(semconv::MESSAGING_OPERATION_TYPE, DataType::Utf8, true),
        Field::new(semconv::MESSAGING_DESTINATION_NAME, DataType::Utf8, true),
        Field::new(semconv::NETWORK_PROTOCOL_NAME, DataType::Utf8, true),
        Field::new(semconv::NETWORK_PROTOCOL_VERSION, DataType::Utf8, true),
        Field::new(semconv::USER_AGENT_ORIGINAL, DataType::Utf8, true),
        Field::new(semconv::CLIENT_ADDRESS, DataType::Utf8, true),
        Field::new(semconv::CLIENT_PORT, DataType::Int32, true),
        Field::new("resource_attributes", DataType::Binary, true),
        Field::new("span_attributes", DataType::Binary, true),
        Field::new("events", DataType::Binary, true),
        Field::new("links", DataType::Binary, true),
        Field::new("project_id", DataType::Utf8, true),
        Field::new("date", DataType::Utf8, false),
        Field::new("hour", DataType::Utf8, false),
    ]))
}

/// Create the storage schema for trace spans (excludes partition columns).
///
/// This schema is used when writing Parquet files. Partition columns (project_id,
/// date, hour) are encoded in the directory path using Hive-style partitioning,
/// not stored in the file itself.
///
/// When querying, DataFusion's `ListingTable` extracts partition values from
/// the directory structure and adds them as virtual columns.
pub fn traces_storage_schema() -> Arc<Schema> {
    let full_schema = traces_schema();
    let fields: Vec<_> = full_schema
        .fields()
        .iter()
        .take(full_schema.fields().len() - super::PARTITION_COLUMN_COUNT)
        .cloned()
        .collect();
    Arc::new(Schema::new(fields))
}

/// Column indices for fast access during conversion.
pub mod columns {
    pub const TRACE_ID: usize = 0;
    pub const SPAN_ID: usize = 1;
    pub const PARENT_SPAN_ID: usize = 2;
    pub const FLAGS: usize = 3;
    pub const TRACE_STATE: usize = 4;
    pub const START_TIME_UNIX_NANO: usize = 5;
    pub const END_TIME_UNIX_NANO: usize = 6;
    pub const DURATION_NS: usize = 7;
    pub const NAME: usize = 8;
    pub const KIND: usize = 9;
    pub const STATUS_CODE: usize = 10;
    pub const STATUS_MESSAGE: usize = 11;
    pub const SCOPE_NAME: usize = 12;
    pub const SCOPE_VERSION: usize = 13;
    pub const SERVICE_NAME: usize = 14;
    // ... remaining indices follow schema order
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PARTITION_COLUMN_COUNT;

    #[test]
    fn schema_has_expected_fields() {
        let schema = traces_schema();

        // Core fields
        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("span_id").is_ok());
        assert!(schema.field_with_name("flags").is_ok());
        assert!(schema.field_with_name("trace_state").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("duration_ns").is_ok());

        // Instrumentation scope
        assert!(schema.field_with_name("scope_name").is_ok());
        assert!(schema.field_with_name("scope_version").is_ok());

        // Resource attributes
        assert!(schema.field_with_name("service.name").is_ok());
        assert!(schema.field_with_name("service.version").is_ok());

        // HTTP attributes
        assert!(schema.field_with_name("http.request.method").is_ok());
        assert!(schema.field_with_name("http.response.status_code").is_ok());

        // Partitioning (query schema only)
        assert!(schema.field_with_name("date").is_ok());
        assert!(schema.field_with_name("hour").is_ok());
    }

    #[test]
    fn storage_schema_excludes_partition_columns() {
        let query_schema = traces_schema();
        let storage_schema = traces_storage_schema();

        // Storage schema should have fewer fields
        assert_eq!(
            storage_schema.fields().len(),
            query_schema.fields().len() - PARTITION_COLUMN_COUNT
        );

        // Core fields should still exist
        assert!(storage_schema.field_with_name("trace_id").is_ok());
        assert!(storage_schema.field_with_name("service.name").is_ok());

        // Partition columns should NOT exist in storage schema
        assert!(storage_schema.field_with_name("date").is_err());
        assert!(storage_schema.field_with_name("hour").is_err());
        assert!(storage_schema.field_with_name("project_id").is_err());
    }

    #[test]
    fn service_name_is_required() {
        let schema = traces_schema();
        let field = schema.field_with_name("service.name").unwrap();
        assert!(!field.is_nullable());
    }

    #[test]
    fn http_fields_are_optional() {
        let schema = traces_schema();
        let field = schema.field_with_name("http.request.method").unwrap();
        assert!(field.is_nullable());
    }

    #[test]
    fn trace_id_is_fixed_size_binary() {
        let schema = traces_schema();
        let field = schema.field_with_name("trace_id").unwrap();
        assert_eq!(*field.data_type(), DataType::FixedSizeBinary(16));
    }
}
