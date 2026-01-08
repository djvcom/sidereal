//! Error aggregation from DataFusion query results.
//!
//! Provides query-time aggregation of errors, grouping them by fingerprint
//! and computing statistics.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, FixedSizeBinaryArray, StringArray, UInt64Array};
use chrono::{DateTime, TimeZone, Utc};

use crate::query::QueryEngine;
use crate::TelemetryError;

use super::queries::{
    ErrorAggregationQueryBuilder, ErrorCountQueryBuilder, ErrorSamplesQueryBuilder,
    ErrorTimelineQueryBuilder,
};
use super::types::{
    ErrorFilter, ErrorGroup, ErrorSample, ErrorSortBy, ErrorStats, ErrorTrend, SpanId, TraceId,
};

/// Result of comparing errors between two time periods or versions.
#[derive(Debug, Clone)]
pub struct ErrorComparison {
    /// Errors that only appear in the comparison period (new errors).
    pub new_errors: Vec<ErrorGroup>,
    /// Errors that only appear in the baseline period (resolved errors).
    pub resolved_errors: Vec<ErrorGroup>,
    /// Errors with significantly increased occurrence count.
    pub increased_errors: Vec<ErrorDelta>,
    /// Errors with significantly decreased occurrence count.
    pub decreased_errors: Vec<ErrorDelta>,
    /// Errors with similar occurrence count.
    pub unchanged_errors: Vec<ErrorGroup>,
}

/// Change in error count between baseline and comparison periods.
#[derive(Debug, Clone)]
pub struct ErrorDelta {
    /// The error group.
    pub error: ErrorGroup,
    /// Count in the baseline period.
    pub baseline_count: u64,
    /// Count in the comparison period.
    pub comparison_count: u64,
    /// Percentage change from baseline to comparison.
    pub change_percent: f64,
}

/// Aggregates errors from telemetry data using DataFusion queries.
///
/// Uses query-time aggregation with the `error_fingerprint` UDF to group
/// similar errors and compute statistics.
pub struct ErrorAggregator {
    engine: Arc<QueryEngine>,
}

impl ErrorAggregator {
    /// Create a new error aggregator with the given query engine.
    pub const fn new(engine: Arc<QueryEngine>) -> Self {
        Self { engine }
    }

    /// Get aggregated error groups for a time range.
    ///
    /// Returns error groups sorted by the specified criteria, with counts
    /// and sample trace IDs for drill-down.
    #[tracing::instrument(skip(self))]
    pub async fn get_error_groups(
        &self,
        filter: &ErrorFilter,
        sort_by: ErrorSortBy,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ErrorGroup>, TelemetryError> {
        let sql = ErrorAggregationQueryBuilder::new(filter.clone())
            .sort_by(sort_by)
            .limit(limit)
            .offset(offset)
            .build();

        let batches = self.engine.query(&sql).await?;

        let mut groups = Vec::new();
        for batch in batches {
            let fingerprints = batch
                .column_by_name("fingerprint")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let service_names = batch
                .column_by_name("service_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let first_versions = batch
                .column_by_name("first_version")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let error_types = batch
                .column_by_name("error_type")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let messages = batch
                .column_by_name("message")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let counts = batch
                .column_by_name("count")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            let affected_traces = batch
                .column_by_name("affected_traces")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            let first_seen = batch
                .column_by_name("first_seen_nanos")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            let last_seen = batch
                .column_by_name("last_seen_nanos")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            let sample_trace_ids = batch
                .column_by_name("sample_trace_id")
                .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

            let sample_span_ids = batch
                .column_by_name("sample_span_id")
                .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

            let num_rows = batch.num_rows();
            for i in 0..num_rows {
                let fingerprint = fingerprints
                    .and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .unwrap_or("")
                    .to_owned();

                let service_name = service_names
                    .and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .unwrap_or("unknown")
                    .to_owned();

                let first_version = first_versions.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                });

                let error_type = error_types.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                });

                let message = messages.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                });

                let count = counts.map_or(0, |a| a.value(i));
                let affected = affected_traces.map_or(0, |a| a.value(i));

                let first_seen_ts =
                    first_seen.map_or_else(Utc::now, |a| nanos_to_datetime(a.value(i)));

                let last_seen_ts =
                    last_seen.map_or_else(Utc::now, |a| nanos_to_datetime(a.value(i)));

                let sample_trace_id = sample_trace_ids.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        let bytes = a.value(i);
                        if bytes.len() == 16 {
                            let mut arr: TraceId = [0u8; 16];
                            arr.copy_from_slice(bytes);
                            Some(arr)
                        } else {
                            None
                        }
                    }
                });

                let sample_span_id = sample_span_ids.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        let bytes = a.value(i);
                        if bytes.len() == 8 {
                            let mut arr: SpanId = [0u8; 8];
                            arr.copy_from_slice(bytes);
                            Some(arr)
                        } else {
                            None
                        }
                    }
                });

                groups.push(ErrorGroup {
                    fingerprint,
                    error_type,
                    message,
                    service_name,
                    first_version,
                    first_seen: first_seen_ts,
                    last_seen: last_seen_ts,
                    count,
                    affected_traces: affected,
                    sample_trace_id,
                    sample_span_id,
                });
            }
        }

        Ok(groups)
    }

    /// Get raw error samples for a specific fingerprint.
    ///
    /// Returns individual error occurrences with full context for debugging.
    #[tracing::instrument(skip(self))]
    pub async fn get_error_samples(
        &self,
        fingerprint: &str,
        filter: &ErrorFilter,
        limit: usize,
    ) -> Result<Vec<ErrorSample>, TelemetryError> {
        let sql = ErrorSamplesQueryBuilder::new(fingerprint, filter.clone())
            .limit(limit)
            .build();

        let batches = self.engine.query(&sql).await?;

        let mut samples = Vec::new();
        for batch in batches {
            let trace_ids = batch
                .column_by_name("trace_id")
                .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

            let span_ids = batch
                .column_by_name("span_id")
                .and_then(|c| c.as_any().downcast_ref::<FixedSizeBinaryArray>());

            let operations = batch
                .column_by_name("operation")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let service_names = batch
                .column_by_name("service_name")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let service_versions = batch
                .column_by_name("service_version")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let timestamps = batch
                .column_by_name("start_time_unix_nano")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            let durations = batch
                .column_by_name("duration_ns")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            let error_types = batch
                .column_by_name("error_type")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let messages = batch
                .column_by_name("message")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>());

            let attributes_col = batch
                .column_by_name("span_attributes")
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>());

            let num_rows = batch.num_rows();
            for i in 0..num_rows {
                let trace_id = trace_ids
                    .and_then(|a| {
                        if a.is_null(i) {
                            None
                        } else {
                            let bytes = a.value(i);
                            if bytes.len() == 16 {
                                let mut arr: TraceId = [0u8; 16];
                                arr.copy_from_slice(bytes);
                                Some(arr)
                            } else {
                                None
                            }
                        }
                    })
                    .unwrap_or([0u8; 16]);

                let span_id = span_ids
                    .and_then(|a| {
                        if a.is_null(i) {
                            None
                        } else {
                            let bytes = a.value(i);
                            if bytes.len() == 8 {
                                let mut arr: SpanId = [0u8; 8];
                                arr.copy_from_slice(bytes);
                                Some(arr)
                            } else {
                                None
                            }
                        }
                    })
                    .unwrap_or([0u8; 8]);

                let operation = operations.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                });

                let service_name = service_names
                    .and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .unwrap_or("unknown")
                    .to_owned();

                let service_version = service_versions.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                });

                let timestamp = timestamps.map_or_else(Utc::now, |a| nanos_to_datetime(a.value(i)));

                let duration_ns = durations.map_or(0, |a| a.value(i));

                let error_type = error_types.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                });

                let message = messages.and_then(|a| {
                    if a.is_null(i) {
                        None
                    } else {
                        Some(a.value(i).to_owned())
                    }
                });

                let attributes = attributes_col
                    .and_then(|a| {
                        if a.is_null(i) {
                            None
                        } else {
                            decode_cbor_attributes(a.value(i))
                        }
                    })
                    .unwrap_or_default();

                samples.push(ErrorSample {
                    trace_id,
                    span_id,
                    timestamp,
                    duration_ns,
                    error_type,
                    message,
                    stacktrace: None,
                    attributes,
                    service_name,
                    service_version,
                    operation,
                });
            }
        }

        Ok(samples)
    }

    /// Get hourly error counts for sparkline visualisation.
    #[tracing::instrument(skip(self))]
    pub async fn get_error_timeline(
        &self,
        fingerprint: Option<&str>,
        filter: &ErrorFilter,
    ) -> Result<Vec<(DateTime<Utc>, u64)>, TelemetryError> {
        let mut builder = ErrorTimelineQueryBuilder::new(filter.clone());
        if let Some(fp) = fingerprint {
            builder = builder.fingerprint(fp);
        }
        let sql = builder.build();

        let batches = self.engine.query(&sql).await?;

        let mut timeline = Vec::new();
        for batch in batches {
            let buckets = batch.column_by_name("bucket");
            let counts = batch
                .column_by_name("count")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>());

            let num_rows = batch.num_rows();
            for i in 0..num_rows {
                let ts = buckets
                    .and_then(|c| extract_timestamp(c, i))
                    .unwrap_or_else(Utc::now);

                let count = counts.map_or(0, |a| a.value(i));

                timeline.push((ts, count));
            }
        }

        Ok(timeline)
    }

    /// Get error statistics for a time period.
    #[tracing::instrument(skip(self))]
    pub async fn get_error_stats(
        &self,
        filter: &ErrorFilter,
    ) -> Result<ErrorStats, TelemetryError> {
        let sql = ErrorCountQueryBuilder::new(filter.clone()).build();
        let batches = self.engine.query(&sql).await?;

        let mut total_errors = 0u64;

        for batch in batches {
            if let Some(count_col) = batch
                .column_by_name("error_count")
                .and_then(|c| c.as_any().downcast_ref::<UInt64Array>())
            {
                if !count_col.is_null(0) {
                    total_errors = count_col.value(0);
                }
            }
        }

        let timeline = self.get_error_timeline(None, filter).await?;
        #[allow(clippy::cast_possible_truncation, clippy::as_conversions)]
        let hourly_counts: Vec<u32> = timeline.iter().map(|(_, c)| *c as u32).collect();

        let trend = if hourly_counts.len() >= 2 {
            let mid = hourly_counts.len() / 2;
            let first_half: u64 = hourly_counts
                .get(..mid)
                .map_or(0, |s| s.iter().map(|&c| u64::from(c)).sum());
            let second_half: u64 = hourly_counts
                .get(mid..)
                .map_or(0, |s| s.iter().map(|&c| u64::from(c)).sum());
            ErrorTrend::from_counts(first_half, second_half)
        } else {
            ErrorTrend::Stable
        };

        Ok(ErrorStats {
            total_errors,
            error_rate: None,
            trend,
            hourly_counts,
        })
    }

    /// Compare errors between two time periods or versions.
    ///
    /// Returns categorised errors: new, resolved, increased, decreased, unchanged.
    /// A 10% threshold is used to determine if an error count change is significant.
    #[tracing::instrument(skip(self))]
    pub async fn compare_errors(
        &self,
        baseline_filter: &ErrorFilter,
        comparison_filter: &ErrorFilter,
    ) -> Result<ErrorComparison, TelemetryError> {
        let baseline = self
            .get_error_groups(baseline_filter, ErrorSortBy::Volume, 10000, 0)
            .await?;
        let comparison = self
            .get_error_groups(comparison_filter, ErrorSortBy::Volume, 10000, 0)
            .await?;

        let baseline_map: HashMap<String, ErrorGroup> = baseline
            .into_iter()
            .map(|g| (g.fingerprint.clone(), g))
            .collect();
        let comparison_map: HashMap<String, ErrorGroup> = comparison
            .into_iter()
            .map(|g| (g.fingerprint.clone(), g))
            .collect();

        let mut new_errors = Vec::new();
        let mut resolved_errors = Vec::new();
        let mut increased_errors = Vec::new();
        let mut decreased_errors = Vec::new();
        let mut unchanged_errors = Vec::new();

        for (fp, comp_group) in &comparison_map {
            match baseline_map.get(fp) {
                Some(base_group) => {
                    let baseline_count = base_group.count;
                    let comparison_count = comp_group.count;

                    if baseline_count == 0 {
                        increased_errors.push(ErrorDelta {
                            error: comp_group.clone(),
                            baseline_count,
                            comparison_count,
                            change_percent: 100.0,
                        });
                    } else {
                        #[allow(clippy::cast_precision_loss, clippy::as_conversions)]
                        let change = (comparison_count as f64 - baseline_count as f64)
                            / baseline_count as f64
                            * 100.0;

                        if change > 10.0 {
                            increased_errors.push(ErrorDelta {
                                error: comp_group.clone(),
                                baseline_count,
                                comparison_count,
                                change_percent: change,
                            });
                        } else if change < -10.0 {
                            decreased_errors.push(ErrorDelta {
                                error: comp_group.clone(),
                                baseline_count,
                                comparison_count,
                                change_percent: change,
                            });
                        } else {
                            unchanged_errors.push(comp_group.clone());
                        }
                    }
                }
                None => {
                    new_errors.push(comp_group.clone());
                }
            }
        }

        for (fp, base_group) in &baseline_map {
            if !comparison_map.contains_key(fp) {
                resolved_errors.push(base_group.clone());
            }
        }

        increased_errors.sort_by(|a, b| {
            b.change_percent
                .partial_cmp(&a.change_percent)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        decreased_errors.sort_by(|a, b| {
            a.change_percent
                .partial_cmp(&b.change_percent)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        new_errors.sort_by(|a, b| b.count.cmp(&a.count));
        resolved_errors.sort_by(|a, b| b.count.cmp(&a.count));

        Ok(ErrorComparison {
            new_errors,
            resolved_errors,
            increased_errors,
            decreased_errors,
            unchanged_errors,
        })
    }
}

#[allow(
    clippy::cast_possible_wrap,
    clippy::cast_possible_truncation,
    clippy::as_conversions
)]
fn nanos_to_datetime(nanos: u64) -> DateTime<Utc> {
    let secs = (nanos / 1_000_000_000) as i64;
    let nsecs = (nanos % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nsecs)
        .single()
        .unwrap_or_else(Utc::now)
}

fn extract_timestamp(array: &dyn Array, index: usize) -> Option<DateTime<Utc>> {
    use arrow::array::TimestampSecondArray;
    use arrow::datatypes::DataType;

    if array.is_null(index) {
        return None;
    }

    if let DataType::Timestamp(_, _) = array.data_type() {
        if let Some(ts_array) = array.as_any().downcast_ref::<TimestampSecondArray>() {
            let secs = ts_array.value(index);
            return Utc.timestamp_opt(secs, 0).single();
        }
    }

    None
}

fn decode_cbor_attributes(bytes: &[u8]) -> Option<HashMap<String, String>> {
    let value: ciborium::Value = ciborium::from_reader(bytes).ok()?;

    match value {
        ciborium::Value::Map(pairs) => {
            let mut map = HashMap::new();
            for (k, v) in pairs {
                if let ciborium::Value::Text(key) = k {
                    let value_str = match v {
                        ciborium::Value::Text(s) => s,
                        ciborium::Value::Integer(i) => format!("{}", i128::from(i)),
                        ciborium::Value::Float(f) => format!("{f}"),
                        ciborium::Value::Bool(b) => format!("{b}"),
                        _ => continue,
                    };
                    map.insert(key, value_str);
                }
            }
            Some(map)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nanos_to_datetime_converts_correctly() {
        let nanos = 1704067200_000_000_000u64;
        let dt = nanos_to_datetime(nanos);
        assert_eq!(dt.timestamp(), 1704067200);
    }

    #[test]
    fn decode_cbor_simple_map() {
        let map: Vec<(ciborium::Value, ciborium::Value)> = vec![
            (
                ciborium::Value::Text("key1".to_string()),
                ciborium::Value::Text("value1".to_string()),
            ),
            (
                ciborium::Value::Text("key2".to_string()),
                ciborium::Value::Integer(42.into()),
            ),
        ];
        let value = ciborium::Value::Map(map);
        let mut bytes = Vec::new();
        ciborium::into_writer(&value, &mut bytes).unwrap();

        let result = decode_cbor_attributes(&bytes).unwrap();
        assert_eq!(result.get("key1"), Some(&"value1".to_string()));
        assert_eq!(result.get("key2"), Some(&"42".to_string()));
    }

    #[test]
    fn decode_cbor_empty_returns_empty_map() {
        let value = ciborium::Value::Map(vec![]);
        let mut bytes = Vec::new();
        ciborium::into_writer(&value, &mut bytes).unwrap();

        let result = decode_cbor_attributes(&bytes).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn decode_cbor_invalid_returns_none() {
        let result = decode_cbor_attributes(&[0xff, 0xff]);
        assert!(result.is_none());
    }
}
