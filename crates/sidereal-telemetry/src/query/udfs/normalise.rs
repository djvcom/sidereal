//! UDF for normalising stacktraces.

#![allow(clippy::as_conversions)]

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};

use crate::errors::normalise_stacktrace;

/// Default maximum frames for stacktrace normalisation.
const DEFAULT_MAX_FRAMES: usize = 5;

/// Create the `normalise_stacktrace` UDF.
///
/// SQL: `normalise_stacktrace(stacktrace)`
///
/// Normalises a stacktrace by removing variable data (line numbers, addresses,
/// UUIDs, timestamps) to enable grouping of similar errors.
///
/// # Arguments
///
/// * `stacktrace` - The stacktrace string to normalise
///
/// # Returns
///
/// Normalised stacktrace string.
pub fn create_normalise_stacktrace_udf() -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        "normalise_stacktrace",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(normalise_stacktrace_impl),
    )
}

fn normalise_stacktrace_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let Some(arg) = args.first() else {
        return Err(DataFusionError::Internal(
            "normalise_stacktrace expects 1 argument".to_owned(),
        ));
    };

    match arg {
        ColumnarValue::Scalar(scalar) => {
            let result = match scalar {
                datafusion::scalar::ScalarValue::Utf8(Some(s)) => {
                    let normalised = normalise_stacktrace(s, DEFAULT_MAX_FRAMES);
                    datafusion::scalar::ScalarValue::Utf8(Some(normalised))
                }
                datafusion::scalar::ScalarValue::Utf8(None) => {
                    datafusion::scalar::ScalarValue::Utf8(None)
                }
                _ => {
                    return Err(DataFusionError::Internal(
                        "normalise_stacktrace expects Utf8 argument".to_owned(),
                    ))
                }
            };
            Ok(ColumnarValue::Scalar(result))
        }
        ColumnarValue::Array(array) => {
            let string_array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("normalise_stacktrace expects Utf8 array".to_owned())
                })?;

            let result: StringArray = string_array
                .iter()
                .map(|opt_s| opt_s.map(|s| normalise_stacktrace(s, DEFAULT_MAX_FRAMES)))
                .collect();

            Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_normalise_scalar() {
        let input =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("at foo (file.rs:123)".to_string())));

        let result = normalise_stacktrace_impl(&[input]).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert!(!s.contains(":123"));
                assert!(s.contains("file.rs"));
            }
            _ => panic!("Expected Utf8 scalar"),
        }
    }

    #[test]
    fn test_normalise_array() {
        let array = StringArray::from(vec![
            Some("at foo (file.rs:123)"),
            Some("at bar (other.rs:456)"),
            None,
        ]);
        let input = ColumnarValue::Array(Arc::new(array));

        let result = normalise_stacktrace_impl(&[input]).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(string_arr.len(), 3);
                assert!(!string_arr.value(0).contains(":123"));
                assert!(!string_arr.value(1).contains(":456"));
                assert!(string_arr.is_null(2));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_normalise_null() {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(None));

        let result = normalise_stacktrace_impl(&[input]).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            _ => panic!("Expected null"),
        }
    }
}
