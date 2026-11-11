//! Fetching one whole trace by its ID.

use arrow::record_batch::RecordBatch;

use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{lit, Expr};

use super::QueryEngine;
use crate::schema::traces::traces_storage_schema;
use crate::trace_index::TraceIndex;
use crate::TelemetryError;

/// Parse a trace ID written as 32 hexadecimal digits, with or without a
/// `0x` prefix.
#[must_use]
pub fn parse_trace_id(text: &str) -> Option<[u8; 16]> {
    let digits = text
        .strip_prefix("0x")
        .or_else(|| text.strip_prefix("0X"))
        .unwrap_or(text);
    let bytes = hex::decode(digits).ok()?;
    bytes.try_into().ok()
}

/// SQL that selects every span of a trace from the whole `traces` table,
/// oldest first.
fn trace_sql(trace_id: &[u8; 16]) -> String {
    format!(
        "SELECT * FROM traces WHERE trace_id = X'{}' ORDER BY start_time_unix_nano",
        hex::encode(trace_id)
    )
}

/// Fetch every span of a trace, oldest first.
///
/// With an index, only the files that can hold the trace are read, and a
/// trace that is in none of them is answered without reading anything.
/// Without one, every trace file is considered.
///
/// The indexed form reads the files directly rather than through the
/// `traces` table, so its rows carry every stored column but not the
/// `date` and `hour` columns the table derives from file paths.
///
/// # Errors
///
/// Returns an error if the query fails.
pub async fn fetch_trace(
    engine: &QueryEngine,
    index: Option<&TraceIndex>,
    trace_id: &[u8; 16],
) -> Result<Vec<RecordBatch>, TelemetryError> {
    let Some(index) = index else {
        return engine.query(&trace_sql(trace_id)).await;
    };

    let files = index.files(trace_id).await;
    if files.is_empty() {
        return Ok(Vec::new());
    }

    let id = ScalarValue::FixedSizeBinary(16, Some(trace_id.to_vec()));
    let frame = engine
        .read_files(&files, &traces_storage_schema())
        .await?
        .filter(column("trace_id").eq(lit(id)))?
        .sort(vec![column("start_time_unix_nano").sort(true, true)])?;
    engine.execute(frame).await
}

/// A column reference by exact name, without the dotted-name parsing that
/// would split `service.name` into a table and a column.
fn column(name: &str) -> Expr {
    Expr::Column(Column::new_unqualified(name))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn trace_ids_parse_with_or_without_a_prefix() {
        let id = parse_trace_id("5b8efff798038103d269b633813fc60c").unwrap();
        assert_eq!(id[0], 0x5b);
        assert_eq!(id[15], 0x0c);
        assert_eq!(
            parse_trace_id("0x5B8EFFF798038103D269B633813FC60C"),
            Some(id)
        );
        assert_eq!(parse_trace_id("5b8e"), None, "too short");
        assert_eq!(parse_trace_id("zz8efff798038103d269b633813fc60c"), None);
        assert_eq!(parse_trace_id("'; DROP TABLE traces; --"), None);
    }

    #[test]
    fn unindexed_lookup_scans_the_table_by_trace_id() {
        assert_eq!(
            trace_sql(&[0xAB; 16]),
            "SELECT * FROM traces WHERE trace_id = X'abababababababababababababababab' \
             ORDER BY start_time_unix_nano"
        );
    }
}
