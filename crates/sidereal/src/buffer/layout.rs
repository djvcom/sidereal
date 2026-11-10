//! Physical layout of flushed Parquet files.
//!
//! How rows are ordered and indexed inside a file decides how much of it a
//! query has to read. Each signal declares the columns its files are sorted
//! by and the columns that get a bloom filter; the flush path applies both.
//!
//! Sorting clusters equal values into the same data pages, so the min/max
//! statistics Parquet keeps per page become selective: a lookup reads the
//! few pages that can contain the value rather than the whole column. A
//! bloom filter answers "is this value in the file at all?" from a few
//! kilobytes, so files that cannot match are skipped without touching
//! their data.

use arrow::array::RecordBatch;
use arrow::compute::{
    concat_batches, lexsort_to_indices, partition, take_record_batch, SortColumn,
};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::SortingColumn;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;

use crate::config::ParquetConfig;
use crate::storage::Signal;

/// Target false-positive rate for bloom filters.
///
/// One in a hundred files that do not contain a value will be opened
/// needlessly. Halving that costs roughly one more bit per distinct value.
const BLOOM_FILTER_FPP: f64 = 0.01;

/// Maximum rows per data page for sorted files.
///
/// Page statistics can only narrow a lookup down to a page, so smaller
/// pages mean less decoding per match, at the cost of a slightly larger
/// page index. The Parquet default is 20,000.
const DATA_PAGE_ROW_LIMIT: usize = 2_000;

/// Sort order and indexes for one signal's files.
#[derive(Debug, Clone, Copy)]
pub struct FileLayout {
    /// Columns rows are sorted by within each file, most significant first.
    pub sort_columns: &'static [&'static str],
    /// Columns that get a bloom filter.
    pub bloom_columns: &'static [&'static str],
}

impl FileLayout {
    /// The layout used for a signal's files.
    ///
    /// Traces are sorted by trace ID so that all spans of a trace within a
    /// file are adjacent, which is what fetching a trace needs. Metrics and
    /// logs keep arrival order for now.
    #[must_use]
    pub const fn for_signal(signal: Signal) -> Self {
        match signal {
            Signal::Traces => Self {
                sort_columns: &["service.name", "trace_id", "start_time_unix_nano"],
                bloom_columns: &["trace_id"],
            },
            Signal::Metrics | Signal::Logs => Self {
                sort_columns: &[],
                bloom_columns: &[],
            },
        }
    }

    /// Combine a partition's batches into one batch in this layout's order.
    ///
    /// Sort columns missing from the schema are ignored, so a layout can be
    /// applied to any schema without failing.
    ///
    /// # Errors
    ///
    /// Returns an error if the batches do not share `schema` or a sort
    /// column has a type that cannot be ordered.
    pub fn sort(
        &self,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<RecordBatch, ArrowError> {
        let combined = concat_batches(schema, batches)?;

        let sort_columns: Vec<SortColumn> = self
            .sort_columns
            .iter()
            .filter_map(|name| combined.column_by_name(name))
            .map(|values| SortColumn {
                values: values.clone(),
                options: None,
            })
            .collect();
        if sort_columns.is_empty() || combined.num_rows() < 2 {
            return Ok(combined);
        }

        let indices = lexsort_to_indices(&sort_columns, None)?;
        take_record_batch(&combined, &indices)
    }

    /// Writer properties for a file holding `sorted`, a batch produced by
    /// [`FileLayout::sort`].
    ///
    /// Bloom filters are sized from the number of distinct values actually
    /// present, because the Parquet default assumes a million per row
    /// group and would add around a megabyte to every small file.
    #[must_use]
    pub fn writer_properties(
        &self,
        sorted: &RecordBatch,
        config: &ParquetConfig,
    ) -> WriterProperties {
        let mut builder = base_properties(config);
        let schema = sorted.schema();

        let sorting: Vec<SortingColumn> = self
            .sort_columns
            .iter()
            .filter_map(|name| schema.index_of(name).ok())
            .filter_map(|index| i32::try_from(index).ok())
            .map(|column_idx| SortingColumn {
                column_idx,
                descending: false,
                nulls_first: true,
            })
            .collect();
        if !sorting.is_empty() {
            builder = builder
                .set_sorting_columns(Some(sorting))
                .set_data_page_row_count_limit(DATA_PAGE_ROW_LIMIT);
        }

        for name in self.bloom_columns {
            let Some(column) = sorted.column_by_name(name) else {
                continue;
            };
            let path = ColumnPath::from(*name);
            let distinct = distinct_values(self, name, sorted, column);
            builder = builder
                .set_column_bloom_filter_enabled(path.clone(), true)
                .set_column_bloom_filter_fpp(path.clone(), BLOOM_FILTER_FPP)
                .set_column_bloom_filter_ndv(path, distinct);
        }

        builder.build()
    }
}

/// Writer properties shared by every file, sorted or not.
#[must_use]
pub fn base_properties(config: &ParquetConfig) -> WriterPropertiesBuilder {
    WriterProperties::builder()
        .set_compression(compression(&config.compression))
        .set_max_row_group_size(config.row_group_size)
}

/// Map the configured compression name to a Parquet codec.
///
/// Unknown names fall back to zstd with a warning rather than failing a
/// flush.
fn compression(name: &str) -> Compression {
    match name.to_ascii_lowercase().as_str() {
        "zstd" => Compression::ZSTD(ZstdLevel::default()),
        "snappy" => Compression::SNAPPY,
        "lz4" => Compression::LZ4_RAW,
        "none" | "uncompressed" => Compression::UNCOMPRESSED,
        other => {
            tracing::warn!(
                compression = other,
                "Unknown Parquet compression, using zstd"
            );
            Compression::ZSTD(ZstdLevel::default())
        }
    }
}

/// Number of distinct values of `name` in `sorted`, for sizing its bloom
/// filter.
///
/// Exact when the column leads the sort order, where equal values are
/// adjacent and can be counted in one pass. Otherwise the row count is
/// used as an upper bound.
fn distinct_values(
    layout: &FileLayout,
    name: &str,
    sorted: &RecordBatch,
    column: &arrow::array::ArrayRef,
) -> u64 {
    let rows = u64::try_from(sorted.num_rows()).unwrap_or(u64::MAX);
    let leads_sort = layout
        .sort_columns
        .first()
        .is_some_and(|first| *first == name);
    let grouped = if leads_sort {
        Some(column.clone())
    } else {
        arrow::compute::sort(column, None).ok()
    };
    let distinct = grouped
        .and_then(|grouped| partition(std::slice::from_ref(&grouped)).ok())
        .and_then(|partitions| u64::try_from(partitions.len()).ok())
        .unwrap_or(rows);
    distinct.max(1)
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::indexing_slicing
)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::FixedSizeBinary(16), false),
            Field::new("start_time_unix_nano", DataType::UInt64, false),
        ]))
    }

    fn batch(rows: &[(u8, u64)]) -> RecordBatch {
        let mut ids = FixedSizeBinaryBuilder::new(16);
        for (id, _) in rows {
            ids.append_value([*id; 16]).unwrap();
        }
        let times: UInt64Array = rows.iter().map(|(_, t)| *t).collect();
        RecordBatch::try_new(schema(), vec![Arc::new(ids.finish()), Arc::new(times)]).unwrap()
    }

    fn rows(batch: &RecordBatch) -> Vec<(u8, u64)> {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        (0..batch.num_rows())
            .map(|i| (ids.value(i)[0], times.value(i)))
            .collect()
    }

    #[test]
    fn traces_are_sorted_by_trace_then_time_across_batches() {
        let layout = FileLayout::for_signal(Signal::Traces);
        let sorted = layout
            .sort(
                &schema(),
                &[batch(&[(9, 5), (2, 30), (9, 1)]), batch(&[(2, 10), (5, 7)])],
            )
            .unwrap();

        assert_eq!(
            rows(&sorted),
            vec![(2, 10), (2, 30), (5, 7), (9, 1), (9, 5)]
        );
    }

    #[test]
    fn signals_without_a_sort_order_keep_arrival_order() {
        let layout = FileLayout::for_signal(Signal::Logs);
        let sorted = layout
            .sort(&schema(), &[batch(&[(9, 5), (2, 30)]), batch(&[(5, 7)])])
            .unwrap();

        assert_eq!(rows(&sorted), vec![(9, 5), (2, 30), (5, 7)]);
    }

    #[test]
    fn sort_order_and_bloom_filter_are_declared_for_traces() {
        let layout = FileLayout::for_signal(Signal::Traces);
        let sorted = layout
            .sort(&schema(), &[batch(&[(9, 5), (2, 30), (9, 1), (5, 7)])])
            .unwrap();
        let props = layout.writer_properties(&sorted, &ParquetConfig::default());

        let sorting = props.sorting_columns().unwrap();
        assert_eq!(sorting.len(), 2);
        assert_eq!(sorting[0].column_idx, 0);
        assert_eq!(sorting[1].column_idx, 1);

        let bloom = props
            .bloom_filter_properties(&ColumnPath::from("trace_id"))
            .unwrap();
        assert_eq!(bloom.ndv, 3, "sized from distinct trace IDs, not rows");
        assert!(props
            .bloom_filter_properties(&ColumnPath::from("start_time_unix_nano"))
            .is_none());
    }

    #[test]
    fn configured_compression_is_honoured() {
        assert_eq!(compression("snappy"), Compression::SNAPPY);
        assert_eq!(compression("NONE"), Compression::UNCOMPRESSED);
        assert_eq!(compression("lz4"), Compression::LZ4_RAW);
        assert!(matches!(compression("zstd"), Compression::ZSTD(_)));
        assert!(matches!(compression("bogus"), Compression::ZSTD(_)));
    }
}
