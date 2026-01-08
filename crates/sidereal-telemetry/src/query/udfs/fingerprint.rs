//! UDF for computing error fingerprints.

#![allow(clippy::as_conversions)]

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};

use crate::errors::compute_fingerprint;

/// Create the `error_fingerprint` UDF.
///
/// SQL: `error_fingerprint(error_type, message, stacktrace, service_name)`
///
/// Computes a SHA-256 fingerprint from normalised error components for grouping
/// similar errors.
///
/// # Arguments
///
/// * `error_type` - Exception type (e.g., "NullPointerException"), nullable
/// * `message` - Error message, nullable
/// * `stacktrace` - Full stacktrace, nullable
/// * `service_name` - Service that produced the error
///
/// # Returns
///
/// SHA-256 fingerprint as a 64-character hex string.
pub fn create_error_fingerprint_udf() -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        "error_fingerprint",
        vec![
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
            DataType::Utf8,
        ],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(error_fingerprint_impl),
    )
}

fn error_fingerprint_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (Some(arg0), Some(arg1), Some(arg2), Some(arg3)) =
        (args.first(), args.get(1), args.get(2), args.get(3))
    else {
        return Err(DataFusionError::Internal(
            "error_fingerprint expects 4 arguments".to_owned(),
        ));
    };

    // Check if all arguments are scalars or all are arrays
    let is_scalar = matches!(arg0, ColumnarValue::Scalar(_));

    if is_scalar {
        compute_scalar_fingerprint(arg0, arg1, arg2, arg3)
    } else {
        compute_array_fingerprint(arg0, arg1, arg2, arg3)
    }
}

fn compute_scalar_fingerprint(
    arg0: &ColumnarValue,
    arg1: &ColumnarValue,
    arg2: &ColumnarValue,
    arg3: &ColumnarValue,
) -> Result<ColumnarValue> {
    let error_type = extract_scalar_string(arg0)?;
    let message = extract_scalar_string(arg1)?;
    let stacktrace = extract_scalar_string(arg2)?;
    let service_name = extract_scalar_string(arg3)?.unwrap_or_default();

    let fingerprint = compute_fingerprint(
        error_type.as_deref(),
        message.as_deref(),
        stacktrace.as_deref(),
        &service_name,
    );

    Ok(ColumnarValue::Scalar(
        datafusion::scalar::ScalarValue::Utf8(Some(fingerprint)),
    ))
}

fn compute_array_fingerprint(
    arg0: &ColumnarValue,
    arg1: &ColumnarValue,
    arg2: &ColumnarValue,
    arg3: &ColumnarValue,
) -> Result<ColumnarValue> {
    let (arr0, arr1, arr2, arr3) = match (arg0, arg1, arg2, arg3) {
        (
            ColumnarValue::Array(a0),
            ColumnarValue::Array(a1),
            ColumnarValue::Array(a2),
            ColumnarValue::Array(a3),
        ) => {
            let arr0 = a0.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Internal("error_fingerprint expects Utf8 arrays".to_owned())
            })?;
            let arr1 = a1.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Internal("error_fingerprint expects Utf8 arrays".to_owned())
            })?;
            let arr2 = a2.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Internal("error_fingerprint expects Utf8 arrays".to_owned())
            })?;
            let arr3 = a3.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                DataFusionError::Internal("error_fingerprint expects Utf8 arrays".to_owned())
            })?;
            (arr0, arr1, arr2, arr3)
        }
        _ => {
            return Err(DataFusionError::Internal(
                "Mixed scalar/array arguments not supported".to_owned(),
            ))
        }
    };

    let len = arr0.len();

    let result: StringArray = (0..len)
        .map(|i| {
            let error_type = if arr0.is_null(i) {
                None
            } else {
                Some(arr0.value(i))
            };
            let message = if arr1.is_null(i) {
                None
            } else {
                Some(arr1.value(i))
            };
            let stacktrace = if arr2.is_null(i) {
                None
            } else {
                Some(arr2.value(i))
            };
            let service_name = if arr3.is_null(i) { "" } else { arr3.value(i) };

            let fp = compute_fingerprint(error_type, message, stacktrace, service_name);
            Some(fp)
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn extract_scalar_string(value: &ColumnarValue) -> Result<Option<String>> {
    match value {
        ColumnarValue::Scalar(scalar) => match scalar {
            datafusion::scalar::ScalarValue::Utf8(s) => Ok(s.clone()),
            datafusion::scalar::ScalarValue::Null => Ok(None),
            _ => Err(DataFusionError::Internal(
                "error_fingerprint expects Utf8 arguments".to_owned(),
            )),
        },
        ColumnarValue::Array(_) => Err(DataFusionError::Internal(
            "Expected scalar argument".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_fingerprint_scalar() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("TestError".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Something went wrong".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("at test.rs:1".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("api-service".to_string()))),
        ];

        let result = error_fingerprint_impl(&args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s.len(), 64); // SHA-256 = 64 hex chars
            }
            _ => panic!("Expected Utf8 scalar"),
        }
    }

    #[test]
    fn test_fingerprint_with_nulls() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(None)), // null error_type
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Error message".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)), // null stacktrace
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("api-service".to_string()))),
        ];

        let result = error_fingerprint_impl(&args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s.len(), 64);
            }
            _ => panic!("Expected Utf8 scalar"),
        }
    }

    #[test]
    fn test_fingerprint_array() {
        let args = vec![
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("Error1"),
                Some("Error2"),
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("msg1"),
                Some("msg2"),
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("stack1"),
                Some("stack2"),
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("service1"),
                Some("service2"),
            ]))),
        ];

        let result = error_fingerprint_impl(&args).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(string_arr.len(), 2);
                assert_eq!(string_arr.value(0).len(), 64);
                assert_eq!(string_arr.value(1).len(), 64);
                // Different inputs should produce different fingerprints
                assert_ne!(string_arr.value(0), string_arr.value(1));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_same_error_same_fingerprint() {
        let args1 = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("TestError".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("User id=123 not found".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("at test.rs:100".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("api".to_string()))),
        ];

        let args2 = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("TestError".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("User id=456 not found".to_string()))), // Different ID
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("at test.rs:200".to_string()))), // Different line
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("api".to_string()))),
        ];

        let result1 = error_fingerprint_impl(&args1).unwrap();
        let result2 = error_fingerprint_impl(&args2).unwrap();

        let fp1 = match result1 {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
            _ => panic!("Expected Utf8"),
        };
        let fp2 = match result2 {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s,
            _ => panic!("Expected Utf8"),
        };

        assert_eq!(fp1, fp2);
    }
}
