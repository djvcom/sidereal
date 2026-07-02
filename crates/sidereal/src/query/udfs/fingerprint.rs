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
    let [error_type, message, stacktrace, service] = args else {
        return Err(DataFusionError::Internal(
            "error_fingerprint expects 4 arguments".to_owned(),
        ));
    };

    // All-scalar invocations (constant folding) keep a scalar result; any mix
    // of scalars and arrays is broadcast so literals can be passed alongside
    // columns.
    if args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
    {
        compute_scalar_fingerprint(error_type, message, stacktrace, service)
    } else {
        let arrays = ColumnarValue::values_to_arrays(args)?;
        let [error_types, messages, stacktraces, services] = arrays.as_slice() else {
            return Err(DataFusionError::Internal(
                "error_fingerprint expects 4 arguments".to_owned(),
            ));
        };
        compute_array_fingerprint(error_types, messages, stacktraces, services)
    }
}

fn compute_scalar_fingerprint(
    error_type_arg: &ColumnarValue,
    message_arg: &ColumnarValue,
    stacktrace_arg: &ColumnarValue,
    service_arg: &ColumnarValue,
) -> Result<ColumnarValue> {
    let error_type = extract_scalar_string(error_type_arg)?;
    let message = extract_scalar_string(message_arg)?;
    let stacktrace = extract_scalar_string(stacktrace_arg)?;
    let service_name = extract_scalar_string(service_arg)?.unwrap_or_default();

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
    error_types_array: &ArrayRef,
    messages_array: &ArrayRef,
    stacktraces_array: &ArrayRef,
    services_array: &ArrayRef,
) -> Result<ColumnarValue> {
    let downcast = |array: &ArrayRef| -> Result<StringArray> {
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Internal("error_fingerprint expects Utf8 arrays".to_owned())
            })
    };
    let (error_types, messages, stacktraces, services) = (
        downcast(error_types_array)?,
        downcast(messages_array)?,
        downcast(stacktraces_array)?,
        downcast(services_array)?,
    );

    let len = error_types.len();

    let result: StringArray = (0..len)
        .map(|i| {
            let error_type = if error_types.is_null(i) {
                None
            } else {
                Some(error_types.value(i))
            };
            let message = if messages.is_null(i) {
                None
            } else {
                Some(messages.value(i))
            };
            let stacktrace = if stacktraces.is_null(i) {
                None
            } else {
                Some(stacktraces.value(i))
            };
            let service_name = if services.is_null(i) {
                ""
            } else {
                services.value(i)
            };

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
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::as_conversions,
    clippy::indexing_slicing
)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_fingerprint_scalar() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("TestError".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Something went wrong".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("at test.rs:1".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("api-service".to_owned()))),
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
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Error message".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)), // null stacktrace
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("api-service".to_owned()))),
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
            ColumnarValue::Scalar(_) => panic!("Expected array"),
        }
    }

    #[test]
    fn test_same_error_same_fingerprint() {
        let args1 = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("TestError".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("User id=123 not found".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("at test.rs:100".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("api".to_owned()))),
        ];

        let args2 = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("TestError".to_owned()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("User id=456 not found".to_owned()))), // Different ID
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("at test.rs:200".to_owned()))), // Different line
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("api".to_owned()))),
        ];

        let result1 = error_fingerprint_impl(&args1).unwrap();
        let result2 = error_fingerprint_impl(&args2).unwrap();

        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(fp1))) = result1 else {
            panic!("Expected Utf8")
        };
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(fp2))) = result2 else {
            panic!("Expected Utf8")
        };

        assert_eq!(fp1, fp2);
    }
}
