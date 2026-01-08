//! Arrow schema for metrics.
//!
//! Supports all OTLP metric types: Gauge, Sum, Histogram, ExponentialHistogram, Summary.
//! Uses a single schema with type discrimination and nullable fields.
//!
//! Schema follows [OTel Metrics Data Model] with dedicated columns for commonly-queried
//! attributes based on [OTel Semantic Conventions].
//!
//! [OTel Metrics Data Model]: https://opentelemetry.io/docs/specs/otel/metrics/data-model/
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

    /// Host and infrastructure attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/resource/host/>
    pub const HOST_NAME: &str = "host.name";
    pub const HOST_ID: &str = "host.id";

    /// Container attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/resource/container/>
    pub const CONTAINER_ID: &str = "container.id";

    /// Kubernetes attributes.
    /// See <https://opentelemetry.io/docs/specs/semconv/resource/k8s/>
    pub const K8S_POD_NAME: &str = "k8s.pod.name";
    pub const K8S_NAMESPACE_NAME: &str = "k8s.namespace.name";
}

/// Metric type enum values stored in the `metric_type` column.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    /// Gauge: instantaneous measurement.
    Gauge = 0,
    /// Sum: cumulative or delta counter.
    Sum = 1,
    /// Histogram: bucket-based distribution.
    Histogram = 2,
    /// ExponentialHistogram: exponential bucket distribution.
    ExponentialHistogram = 3,
    /// Summary: quantile summary (legacy).
    Summary = 4,
}

impl MetricType {
    pub const fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Gauge),
            1 => Some(Self::Sum),
            2 => Some(Self::Histogram),
            3 => Some(Self::ExponentialHistogram),
            4 => Some(Self::Summary),
            _ => None,
        }
    }
}

/// Aggregation temporality values.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationTemporality {
    /// Unspecified.
    Unspecified = 0,
    /// Delta: value represents change since last report.
    Delta = 1,
    /// Cumulative: value represents total since start.
    Cumulative = 2,
}

/// Create the Arrow schema for metrics.
///
/// # Field Groups
///
/// **Core fields**: `metric_name`, `metric_description`, `metric_unit`, `metric_metadata` (CBOR),
/// `metric_type` (MetricType enum), `time_unix_nano`, `start_time_unix_nano`, `data_point_flags`
///
/// **Values**: `value_int` or `value_double` (mutually exclusive, for Gauge/Sum)
///
/// **Aggregation**: `aggregation_temporality`, `is_monotonic` (for Sum metrics)
///
/// **Histogram fields**: `count`, `sum`, `min`, `max`, `histogram_data` (CBOR-encoded bucket data)
///
/// **Instrumentation scope**: `scope_name`, `scope_version`, `scope_attributes` (CBOR)
///
/// **Dropped counts**: Track SDK-side drops for debugging instrumentation
///
/// **Semantic convention attributes**: Dedicated columns for commonly-queried attributes
/// (service, host, container, Kubernetes). Remaining attributes stored as CBOR in
/// `resource_attributes` and `data_point_attributes`.
///
/// **Exemplars**: `exemplars` (CBOR array with filtered_attributes, trace correlation)
///
/// **Partition columns**: `project_id`, `date`, `hour` - extracted from directory path
/// by DataFusion, not stored in Parquet files (see [`number_metrics_storage_schema`]).
pub fn number_metrics_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("metric_metadata", DataType::Binary, true),
        Field::new("metric_type", DataType::UInt8, false),
        Field::new("time_unix_nano", DataType::UInt64, false),
        Field::new("start_time_unix_nano", DataType::UInt64, true),
        Field::new("data_point_flags", DataType::UInt32, false),
        Field::new("value_int", DataType::Int64, true),
        Field::new("value_double", DataType::Float64, true),
        Field::new("aggregation_temporality", DataType::UInt8, true),
        Field::new("is_monotonic", DataType::Boolean, true),
        Field::new("count", DataType::UInt64, true),
        Field::new("sum", DataType::Float64, true),
        Field::new("min", DataType::Float64, true),
        Field::new("max", DataType::Float64, true),
        Field::new("histogram_data", DataType::Binary, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Binary, true),
        Field::new("resource_dropped_attributes_count", DataType::UInt32, false),
        Field::new("scope_dropped_attributes_count", DataType::UInt32, false),
        Field::new(semconv::SERVICE_NAME, DataType::Utf8, false),
        Field::new(semconv::SERVICE_VERSION, DataType::Utf8, true),
        Field::new(semconv::SERVICE_NAMESPACE, DataType::Utf8, true),
        Field::new(semconv::DEPLOYMENT_ENVIRONMENT_NAME, DataType::Utf8, true),
        Field::new(semconv::HOST_NAME, DataType::Utf8, true),
        Field::new(semconv::HOST_ID, DataType::Utf8, true),
        Field::new(semconv::CONTAINER_ID, DataType::Utf8, true),
        Field::new(semconv::K8S_POD_NAME, DataType::Utf8, true),
        Field::new(semconv::K8S_NAMESPACE_NAME, DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Binary, true),
        Field::new("data_point_attributes", DataType::Binary, true),
        Field::new("exemplars", DataType::Binary, true),
        Field::new("project_id", DataType::Utf8, true),
        Field::new("date", DataType::Utf8, false),
        Field::new("hour", DataType::Utf8, false),
    ]))
}

/// Create the storage schema for metrics (excludes partition columns).
///
/// This schema is used when writing Parquet files. Partition columns are
/// encoded in the directory path using Hive-style partitioning.
pub fn number_metrics_storage_schema() -> Arc<Schema> {
    let full_schema = number_metrics_schema();
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
        let schema = number_metrics_schema();

        // Core fields
        assert!(schema.field_with_name("metric_name").is_ok());
        assert!(schema.field_with_name("metric_type").is_ok());
        assert!(schema.field_with_name("time_unix_nano").is_ok());

        // Value fields
        assert!(schema.field_with_name("value_int").is_ok());
        assert!(schema.field_with_name("value_double").is_ok());

        // Histogram fields
        assert!(schema.field_with_name("count").is_ok());
        assert!(schema.field_with_name("histogram_data").is_ok());

        // Resource attributes
        assert!(schema.field_with_name("service.name").is_ok());

        // Partitioning (query schema only)
        assert!(schema.field_with_name("date").is_ok());
    }

    #[test]
    fn storage_schema_excludes_partition_columns() {
        let query_schema = number_metrics_schema();
        let storage_schema = number_metrics_storage_schema();

        assert_eq!(
            storage_schema.fields().len(),
            query_schema.fields().len() - PARTITION_COLUMN_COUNT
        );

        // Core fields should still exist
        assert!(storage_schema.field_with_name("metric_name").is_ok());
        assert!(storage_schema.field_with_name("service.name").is_ok());

        // Partition columns should NOT exist in storage schema
        assert!(storage_schema.field_with_name("date").is_err());
        assert!(storage_schema.field_with_name("hour").is_err());
        assert!(storage_schema.field_with_name("project_id").is_err());
    }

    #[test]
    fn metric_type_roundtrip() {
        for i in 0..5 {
            let mt = MetricType::from_u8(i).unwrap();
            assert_eq!(mt as u8, i);
        }
        assert!(MetricType::from_u8(5).is_none());
    }

    #[test]
    fn service_name_is_required() {
        let schema = number_metrics_schema();
        let field = schema.field_with_name("service.name").unwrap();
        assert!(!field.is_nullable());
    }
}
