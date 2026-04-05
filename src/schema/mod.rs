//! Arrow schema definitions for telemetry signals.
//!
//! Schemas follow the OTel semantic conventions with dedicated columns for
//! Required/Conditionally Required/Recommended attributes and CBOR-encoded
//! binary columns for remaining attributes.
//!
//! # Query vs Storage Schemas
//!
//! Each signal type has two schemas:
//!
//! - **Query schema** (e.g., `traces_schema()`): Full schema including partition columns
//!   (`date`, `hour`, `project_id`). Used by DataFusion for query results where partition
//!   values are extracted from the directory structure.
//!
//! - **Storage schema** (e.g., `traces_storage_schema()`): Schema without partition columns.
//!   Used when writing Parquet files. Partition values are encoded in the directory path
//!   (Hive-style: `date=2024-01-15/hour=14/`), not in the file itself.
//!
//! This separation enables DataFusion to use partition pruning for efficient queries.

use arrow::datatypes::DataType;

pub mod logs;
pub mod metrics;
pub mod traces;

pub use logs::{logs_schema, logs_storage_schema};
pub use metrics::{number_metrics_schema, number_metrics_storage_schema};
pub use traces::{traces_schema, traces_storage_schema};

/// Names and types of partition columns used for Hive-style partitioning.
///
/// These columns are NOT stored in Parquet files but are extracted from
/// the directory structure by DataFusion's `ListingTable`.
///
/// Path format: `{signal}/date={YYYY-MM-DD}/hour={HH}/[project={id}/]{ulid}.parquet`
pub const PARTITION_COLUMNS: &[(&str, DataType)] = &[
    // Note: DataFusion extracts partition values as strings from paths,
    // so we use Utf8 for all partition columns.
    ("date", DataType::Utf8),
    ("hour", DataType::Utf8),
];

/// Number of partition columns at the end of query schemas.
///
/// The last N fields in query schemas are partition columns which should
/// be excluded when writing to Parquet.
pub const PARTITION_COLUMN_COUNT: usize = 3; // project_id, date, hour
