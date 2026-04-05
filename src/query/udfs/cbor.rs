//! UDF for extracting values from CBOR-encoded attributes.

#![allow(clippy::as_conversions, clippy::manual_let_else)]

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::DataType;
use ciborium::Value as CborValue;
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::{ColumnarValue, ScalarUDF, Volatility};

/// Create the `cbor_extract` UDF.
///
/// SQL: `cbor_extract(cbor_data, key)`
///
/// Extracts a string value from CBOR-encoded binary data.
///
/// # Arguments
///
/// * `cbor_data` - Binary column containing CBOR-encoded map
/// * `key` - Key to extract from the map
///
/// # Returns
///
/// String value if key exists and value is string-convertible, NULL otherwise.
pub fn create_cbor_extract_udf() -> ScalarUDF {
    datafusion::logical_expr::create_udf(
        "cbor_extract",
        vec![DataType::Binary, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(cbor_extract_impl),
    )
}

fn cbor_extract_impl(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (Some(arg0), Some(arg1)) = (args.first(), args.get(1)) else {
        return Err(DataFusionError::Internal(
            "cbor_extract expects 2 arguments".to_owned(),
        ));
    };

    match (arg0, arg1) {
        (ColumnarValue::Scalar(data), ColumnarValue::Scalar(key)) => extract_scalar(data, key),
        (ColumnarValue::Array(data), ColumnarValue::Scalar(key)) => {
            extract_array_with_scalar_key(data, key)
        }
        (ColumnarValue::Array(data), ColumnarValue::Array(keys)) => {
            extract_array_with_array_key(data, keys)
        }
        _ => Err(DataFusionError::Internal(
            "cbor_extract: invalid argument combination".to_owned(),
        )),
    }
}

fn extract_scalar(
    data: &datafusion::scalar::ScalarValue,
    key: &datafusion::scalar::ScalarValue,
) -> Result<ColumnarValue> {
    let data_bytes = match data {
        datafusion::scalar::ScalarValue::Binary(Some(b)) => b,
        datafusion::scalar::ScalarValue::Binary(None) => {
            return Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(None),
            ));
        }
        _ => {
            return Err(DataFusionError::Internal(
                "cbor_extract expects Binary data".to_owned(),
            ))
        }
    };

    let key_str = match key {
        datafusion::scalar::ScalarValue::Utf8(Some(k)) => k,
        datafusion::scalar::ScalarValue::Utf8(None) => {
            return Ok(ColumnarValue::Scalar(
                datafusion::scalar::ScalarValue::Utf8(None),
            ));
        }
        _ => {
            return Err(DataFusionError::Internal(
                "cbor_extract expects Utf8 key".to_owned(),
            ))
        }
    };

    let result = extract_from_cbor(data_bytes, key_str);
    Ok(ColumnarValue::Scalar(
        datafusion::scalar::ScalarValue::Utf8(result),
    ))
}

fn extract_array_with_scalar_key(
    data: &ArrayRef,
    key: &datafusion::scalar::ScalarValue,
) -> Result<ColumnarValue> {
    let binary_array = data
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| DataFusionError::Internal("cbor_extract expects Binary array".to_owned()))?;

    let key_str = match key {
        datafusion::scalar::ScalarValue::Utf8(Some(k)) => k.as_str(),
        datafusion::scalar::ScalarValue::Utf8(None) => {
            // Null key returns all nulls
            let null_array: StringArray = (0..binary_array.len()).map(|_| None::<&str>).collect();
            return Ok(ColumnarValue::Array(Arc::new(null_array) as ArrayRef));
        }
        _ => {
            return Err(DataFusionError::Internal(
                "cbor_extract expects Utf8 key".to_owned(),
            ))
        }
    };

    let result: StringArray = binary_array
        .iter()
        .map(|opt_bytes| opt_bytes.and_then(|bytes| extract_from_cbor(bytes, key_str)))
        .collect();

    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

fn extract_array_with_array_key(data: &ArrayRef, keys: &ArrayRef) -> Result<ColumnarValue> {
    let binary_array = data.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
        DataFusionError::Internal("cbor_extract expects Binary data array".to_owned())
    })?;

    let key_array = keys.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        DataFusionError::Internal("cbor_extract expects Utf8 key array".to_owned())
    })?;

    if binary_array.len() != key_array.len() {
        return Err(DataFusionError::Internal(
            "cbor_extract: arrays must have same length".to_owned(),
        ));
    }

    let result: StringArray = binary_array
        .iter()
        .zip(key_array.iter())
        .map(|(opt_bytes, opt_key)| match (opt_bytes, opt_key) {
            (Some(bytes), Some(key)) => extract_from_cbor(bytes, key),
            _ => None,
        })
        .collect();

    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
}

/// Extract a string value from CBOR-encoded bytes.
fn extract_from_cbor(bytes: &[u8], key: &str) -> Option<String> {
    // Parse CBOR
    let value: CborValue = ciborium::from_reader(bytes).ok()?;

    // Navigate to the key
    let map = match value {
        CborValue::Map(m) => m,
        _ => return None,
    };

    // Find the key
    for (k, v) in map {
        let key_str = match k {
            CborValue::Text(s) => s,
            _ => continue,
        };

        if key_str == key {
            return cbor_value_to_string(&v);
        }
    }

    None
}

/// Convert a CBOR value to a string representation.
fn cbor_value_to_string(value: &CborValue) -> Option<String> {
    match value {
        CborValue::Text(s) => Some(s.clone()),
        CborValue::Integer(i) => Some(format!("{}", i128::from(*i))),
        CborValue::Float(f) => Some(format!("{f}")),
        CborValue::Bool(b) => Some(format!("{b}")),
        // For complex types, return JSON representation
        CborValue::Array(_) | CborValue::Map(_) => serde_json::to_string(value).ok(),
        CborValue::Bytes(b) => Some(hex::encode(b)),
        CborValue::Tag(_, inner) => cbor_value_to_string(inner),
        CborValue::Null | _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BinaryArray, StringArray};
    use datafusion::scalar::ScalarValue;
    use std::sync::Arc;

    fn make_cbor_map(pairs: &[(&str, &str)]) -> Vec<u8> {
        let map: Vec<(CborValue, CborValue)> = pairs
            .iter()
            .map(|(k, v)| {
                (
                    CborValue::Text((*k).to_string()),
                    CborValue::Text((*v).to_string()),
                )
            })
            .collect();
        let value = CborValue::Map(map);
        let mut bytes = Vec::new();
        ciborium::into_writer(&value, &mut bytes).unwrap();
        bytes
    }

    #[test]
    fn test_extract_scalar() {
        let cbor_data = make_cbor_map(&[("user.id", "12345"), ("http.method", "GET")]);

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Binary(Some(cbor_data))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("user.id".to_string()))),
        ];

        let result = cbor_extract_impl(&args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "12345");
            }
            _ => panic!("Expected Utf8 scalar"),
        }
    }

    #[test]
    fn test_extract_missing_key() {
        let cbor_data = make_cbor_map(&[("user.id", "12345")]);

        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Binary(Some(cbor_data))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("nonexistent".to_string()))),
        ];

        let result = cbor_extract_impl(&args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            _ => panic!("Expected null"),
        }
    }

    #[test]
    fn test_extract_null_data() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Binary(None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("key".to_string()))),
        ];

        let result = cbor_extract_impl(&args).unwrap();

        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            _ => panic!("Expected null"),
        }
    }

    #[test]
    fn test_extract_array() {
        let cbor1 = make_cbor_map(&[("key", "value1")]);
        let cbor2 = make_cbor_map(&[("key", "value2")]);

        let args = vec![
            ColumnarValue::Array(Arc::new(BinaryArray::from(vec![
                Some(cbor1.as_slice()),
                Some(cbor2.as_slice()),
            ]))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("key".to_string()))),
        ];

        let result = cbor_extract_impl(&args).unwrap();

        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(string_arr.len(), 2);
                assert_eq!(string_arr.value(0), "value1");
                assert_eq!(string_arr.value(1), "value2");
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_extract_integer_value() {
        let map: Vec<(CborValue, CborValue)> = vec![(
            CborValue::Text("count".to_string()),
            CborValue::Integer(42.into()),
        )];
        let value = CborValue::Map(map);
        let mut bytes = Vec::new();
        ciborium::into_writer(&value, &mut bytes).unwrap();

        let result = extract_from_cbor(&bytes, "count");
        assert_eq!(result, Some("42".to_string()));
    }

    #[test]
    fn test_extract_bool_value() {
        let map: Vec<(CborValue, CborValue)> = vec![(
            CborValue::Text("enabled".to_string()),
            CborValue::Bool(true),
        )];
        let value = CborValue::Map(map);
        let mut bytes = Vec::new();
        ciborium::into_writer(&value, &mut bytes).unwrap();

        let result = extract_from_cbor(&bytes, "enabled");
        assert_eq!(result, Some("true".to_string()));
    }
}
