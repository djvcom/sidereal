//! Arrow schema for log records.
//!
//! Schema follows [OTel Logs Data Model] with dedicated columns for commonly-queried
//! attributes based on [OTel Semantic Conventions].
//!
//! [OTel Logs Data Model]: https://opentelemetry.io/docs/specs/otel/logs/data-model/
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

    /// Log file attributes for file-sourced logs.
    /// See <https://opentelemetry.io/docs/specs/semconv/attributes-registry/log/>
    pub const LOG_FILE_PATH: &str = "log.file.path";
    pub const LOG_FILE_NAME: &str = "log.file.name";
    pub const LOG_IOSTREAM: &str = "log.iostream";

    /// Exception attributes for error logs.
    /// See <https://opentelemetry.io/docs/specs/semconv/attributes-registry/exception/>
    pub const EXCEPTION_TYPE: &str = "exception.type";
    pub const EXCEPTION_MESSAGE: &str = "exception.message";
    pub const EXCEPTION_STACKTRACE: &str = "exception.stacktrace";

    /// Source code location attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/attributes-registry/code/>
    pub const CODE_FUNCTION: &str = "code.function";
    pub const CODE_FILEPATH: &str = "code.filepath";
    pub const CODE_LINENO: &str = "code.lineno";

    /// Event attributes for structured events emitted as logs.
    /// See <https://opentelemetry.io/docs/specs/semconv/general/events/>
    pub const EVENT_NAME: &str = "event.name";
    pub const EVENT_DOMAIN: &str = "event.domain";
}

/// OTel severity numbers (1-24).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SeverityNumber {
    Unspecified = 0,
    Trace = 1,
    Trace2 = 2,
    Trace3 = 3,
    Trace4 = 4,
    Debug = 5,
    Debug2 = 6,
    Debug3 = 7,
    Debug4 = 8,
    Info = 9,
    Info2 = 10,
    Info3 = 11,
    Info4 = 12,
    Warn = 13,
    Warn2 = 14,
    Warn3 = 15,
    Warn4 = 16,
    Error = 17,
    Error2 = 18,
    Error3 = 19,
    Error4 = 20,
    Fatal = 21,
    Fatal2 = 22,
    Fatal3 = 23,
    Fatal4 = 24,
}

impl SeverityNumber {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Trace),
            2 => Some(Self::Trace2),
            3 => Some(Self::Trace3),
            4 => Some(Self::Trace4),
            5 => Some(Self::Debug),
            6 => Some(Self::Debug2),
            7 => Some(Self::Debug3),
            8 => Some(Self::Debug4),
            9 => Some(Self::Info),
            10 => Some(Self::Info2),
            11 => Some(Self::Info3),
            12 => Some(Self::Info4),
            13 => Some(Self::Warn),
            14 => Some(Self::Warn2),
            15 => Some(Self::Warn3),
            16 => Some(Self::Warn4),
            17 => Some(Self::Error),
            18 => Some(Self::Error2),
            19 => Some(Self::Error3),
            20 => Some(Self::Error4),
            21 => Some(Self::Fatal),
            22 => Some(Self::Fatal2),
            23 => Some(Self::Fatal3),
            24 => Some(Self::Fatal4),
            _ => None,
        }
    }

    /// Get a human-readable name for the severity level.
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Unspecified => "UNSPECIFIED",
            Self::Trace | Self::Trace2 | Self::Trace3 | Self::Trace4 => "TRACE",
            Self::Debug | Self::Debug2 | Self::Debug3 | Self::Debug4 => "DEBUG",
            Self::Info | Self::Info2 | Self::Info3 | Self::Info4 => "INFO",
            Self::Warn | Self::Warn2 | Self::Warn3 | Self::Warn4 => "WARN",
            Self::Error | Self::Error2 | Self::Error3 | Self::Error4 => "ERROR",
            Self::Fatal | Self::Fatal2 | Self::Fatal3 | Self::Fatal4 => "FATAL",
        }
    }
}

/// Create the Arrow schema for log records.
///
/// # Field Groups
///
/// **Core fields**: `time_unix_nano`, `observed_time_unix_nano`, `severity_number`,
/// `severity_text`, `flags` (data quality indicators)
///
/// **Trace correlation**: `trace_id`, `span_id` for linking logs to distributed traces
///
/// **Body**: `body_string` (text) or `body_bytes` (binary), mutually exclusive
///
/// **Instrumentation scope**: `scope_name`, `scope_version`, `scope_attributes` (CBOR)
///
/// **Dropped counts**: Track SDK-side attribute drops for debugging instrumentation
///
/// **Semantic convention attributes**: Dedicated columns for commonly-queried attributes
/// (service, log file, exception, code location, events). Remaining attributes stored
/// as CBOR in `resource_attributes` and `log_attributes`.
///
/// **Partition columns**: `project_id`, `date`, `hour` - extracted from directory path
/// by DataFusion, not stored in Parquet files (see [`logs_storage_schema`]).
pub fn logs_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("time_unix_nano", DataType::UInt64, false),
        Field::new("observed_time_unix_nano", DataType::UInt64, false),
        Field::new("severity_number", DataType::UInt8, false),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("flags", DataType::UInt32, false),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        Field::new("body_string", DataType::Utf8, true),
        Field::new("body_bytes", DataType::Binary, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Binary, true),
        Field::new("resource_dropped_attributes_count", DataType::UInt32, false),
        Field::new("scope_dropped_attributes_count", DataType::UInt32, false),
        Field::new("log_dropped_attributes_count", DataType::UInt32, false),
        Field::new(semconv::SERVICE_NAME, DataType::Utf8, false),
        Field::new(semconv::SERVICE_VERSION, DataType::Utf8, true),
        Field::new(semconv::SERVICE_NAMESPACE, DataType::Utf8, true),
        Field::new(semconv::DEPLOYMENT_ENVIRONMENT_NAME, DataType::Utf8, true),
        Field::new(semconv::LOG_FILE_PATH, DataType::Utf8, true),
        Field::new(semconv::LOG_FILE_NAME, DataType::Utf8, true),
        Field::new(semconv::LOG_IOSTREAM, DataType::Utf8, true),
        Field::new(semconv::EXCEPTION_TYPE, DataType::Utf8, true),
        Field::new(semconv::EXCEPTION_MESSAGE, DataType::Utf8, true),
        Field::new(semconv::EXCEPTION_STACKTRACE, DataType::Utf8, true),
        Field::new(semconv::CODE_FUNCTION, DataType::Utf8, true),
        Field::new(semconv::CODE_FILEPATH, DataType::Utf8, true),
        Field::new(semconv::CODE_LINENO, DataType::Int32, true),
        Field::new(semconv::EVENT_NAME, DataType::Utf8, true),
        Field::new(semconv::EVENT_DOMAIN, DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Binary, true),
        Field::new("log_attributes", DataType::Binary, true),
        Field::new("project_id", DataType::Utf8, true),
        Field::new("date", DataType::Utf8, false),
        Field::new("hour", DataType::Utf8, false),
    ]))
}

/// Create the storage schema for logs (excludes partition columns).
///
/// This schema is used when writing Parquet files. Partition columns are
/// encoded in the directory path using Hive-style partitioning.
pub fn logs_storage_schema() -> Arc<Schema> {
    let full_schema = logs_schema();
    let fields: Vec<_> = full_schema
        .fields()
        .iter()
        .take(full_schema.fields().len() - super::PARTITION_COLUMN_COUNT)
        .cloned()
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::PARTITION_COLUMN_COUNT;

    #[test]
    fn schema_has_expected_fields() {
        let schema = logs_schema();

        // Core fields
        assert!(schema.field_with_name("time_unix_nano").is_ok());
        assert!(schema.field_with_name("severity_number").is_ok());
        assert!(schema.field_with_name("body_string").is_ok());

        // Trace correlation
        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("span_id").is_ok());

        // Resource attributes
        assert!(schema.field_with_name("service.name").is_ok());

        // Exception attributes
        assert!(schema.field_with_name("exception.type").is_ok());
        assert!(schema.field_with_name("exception.stacktrace").is_ok());

        assert!(schema.field_with_name("code.function").is_ok());
        assert!(schema.field_with_name("code.lineno").is_ok());

        assert!(schema.field_with_name("event.name").is_ok());
        assert!(schema.field_with_name("event.domain").is_ok());

        assert!(schema.field_with_name("date").is_ok());
    }

    #[test]
    fn storage_schema_excludes_partition_columns() {
        let query_schema = logs_schema();
        let storage_schema = logs_storage_schema();

        assert_eq!(
            storage_schema.fields().len(),
            query_schema.fields().len() - PARTITION_COLUMN_COUNT
        );

        // Core fields should still exist
        assert!(storage_schema.field_with_name("time_unix_nano").is_ok());
        assert!(storage_schema.field_with_name("service.name").is_ok());

        // Partition columns should NOT exist in storage schema
        assert!(storage_schema.field_with_name("date").is_err());
        assert!(storage_schema.field_with_name("hour").is_err());
        assert!(storage_schema.field_with_name("project_id").is_err());
    }

    #[test]
    fn severity_number_roundtrip() {
        for i in 0..=24 {
            let sn = SeverityNumber::from_u8(i).unwrap();
            assert_eq!(sn as u8, i);
        }
        assert!(SeverityNumber::from_u8(25).is_none());
    }

    #[test]
    fn severity_names() {
        assert_eq!(SeverityNumber::Info.name(), "INFO");
        assert_eq!(SeverityNumber::Error.name(), "ERROR");
        assert_eq!(SeverityNumber::Fatal.name(), "FATAL");
    }

    #[test]
    fn service_name_is_required() {
        let schema = logs_schema();
        let field = schema.field_with_name("service.name").unwrap();
        assert!(!field.is_nullable());
    }

    #[test]
    fn trace_correlation_is_optional() {
        let schema = logs_schema();
        let trace_id = schema.field_with_name("trace_id").unwrap();
        let span_id = schema.field_with_name("span_id").unwrap();
        assert!(trace_id.is_nullable());
        assert!(span_id.is_nullable());
    }
}
