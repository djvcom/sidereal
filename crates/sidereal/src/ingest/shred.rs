//! Attributes as columns.
//!
//! OpenTelemetry lets every span, log record, data point and resource
//! carry arbitrary attributes. A handful defined by the semantic
//! conventions have dedicated columns; the rest were stored only as one
//! CBOR blob per row, which a query can filter on only by decoding every
//! blob.
//!
//! Shredding writes each attribute as a column of its own as well, so that
//! a filter on any attribute reads one compact, dictionary-encoded column
//! with statistics, exactly as it would for a dedicated one. The blob is
//! still written in full: it remains the complete record of what was
//! received, including values that have no column form.
//!
//! # Column names
//!
//! A shredded column is named `{scope}.{type}.{key}`:
//!
//! - scope: `attr` for the record's own attributes (span, log record or
//!   data point), `resource` for those of the resource that emitted it
//! - type: `str`, `int`, `f64` or `bool`
//! - key: the attribute key, verbatim
//!
//! The type is part of the name because nothing stops two services from
//! sending the same key with different types; each type gets its own
//! column rather than one of them being lost or coerced. Arrays, maps and
//! byte strings are not shredded and live only in the blob.
//!
//! Which columns exist therefore varies from batch to batch and from file
//! to file. [`unify`] brings batches to a common schema before they are
//! written together.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, RecordBatch,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use opentelemetry_proto::tonic::common::v1::{any_value::Value as AnyValueKind, KeyValue};

/// Most distinct shredded columns one batch may have.
///
/// Attribute keys are chosen by whoever instruments the code, and a
/// mistake such as putting an ID in a key would otherwise create a column
/// per request. Keys beyond the limit stay in the blob only.
pub const MAX_SHREDDED_COLUMNS: usize = 512;

/// Longest attribute key that is given a column.
const MAX_KEY_BYTES: usize = 128;

/// Whose attributes a shredded column holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    /// The resource that emitted the record.
    Resource,
    /// The record itself: a span, log record or data point.
    Record,
}

impl Scope {
    const ALL: [Self; 2] = [Self::Resource, Self::Record];

    const fn prefix(self) -> &'static str {
        match self {
            Self::Resource => "resource",
            Self::Record => "attr",
        }
    }
}

/// Value type of a shredded column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueType {
    /// UTF-8 string.
    Str,
    /// 64-bit signed integer.
    Int,
    /// 64-bit float.
    Double,
    /// Boolean.
    Bool,
}

impl ValueType {
    /// Every value type, in the order lookups prefer them.
    pub const ALL: [Self; 4] = [Self::Str, Self::Int, Self::Double, Self::Bool];

    const fn tag(self) -> &'static str {
        match self {
            Self::Str => "str",
            Self::Int => "int",
            Self::Double => "f64",
            Self::Bool => "bool",
        }
    }

    /// The Arrow type of a column holding this value type.
    #[must_use]
    pub const fn data_type(self) -> DataType {
        match self {
            Self::Str => DataType::Utf8,
            Self::Int => DataType::Int64,
            Self::Double => DataType::Float64,
            Self::Bool => DataType::Boolean,
        }
    }
}

/// Name of the column holding attribute `key` of the given scope and type.
#[must_use]
pub fn column_name(scope: Scope, value_type: ValueType, key: &str) -> String {
    format!("{}.{}.{key}", scope.prefix(), value_type.tag())
}

/// Split a shredded column's name into its scope, type and attribute key.
/// Returns `None` for any other column.
#[must_use]
pub fn parse_column_name(name: &str) -> Option<(Scope, ValueType, &str)> {
    let (prefix, rest) = name.split_once('.')?;
    let scope = Scope::ALL.into_iter().find(|s| s.prefix() == prefix)?;
    let (tag, key) = rest.split_once('.')?;
    let value_type = ValueType::ALL.into_iter().find(|t| t.tag() == tag)?;
    (!key.is_empty()).then_some((scope, value_type, key))
}

/// Whether `field` is a shredded attribute column of the expected type.
fn is_shredded(field: &Field) -> bool {
    parse_column_name(field.name())
        .is_some_and(|(_, value_type, _)| *field.data_type() == value_type.data_type())
}

enum Builder {
    Str(StringBuilder),
    Int(Int64Builder),
    Double(Float64Builder),
    Bool(BooleanBuilder),
}

impl Builder {
    fn new(value_type: ValueType) -> Self {
        match value_type {
            ValueType::Str => Self::Str(StringBuilder::new()),
            ValueType::Int => Self::Int(Int64Builder::new()),
            ValueType::Double => Self::Double(Float64Builder::new()),
            ValueType::Bool => Self::Bool(BooleanBuilder::new()),
        }
    }

    fn len(&self) -> usize {
        use arrow::array::ArrayBuilder;
        match self {
            Self::Str(b) => b.len(),
            Self::Int(b) => b.len(),
            Self::Double(b) => b.len(),
            Self::Bool(b) => b.len(),
        }
    }

    /// Append nulls until the builder holds `rows` values.
    fn pad_to(&mut self, rows: usize) {
        let missing = rows.saturating_sub(self.len());
        if missing == 0 {
            return;
        }
        match self {
            Self::Str(b) => (0..missing).for_each(|_| b.append_null()),
            Self::Int(b) => b.append_nulls(missing),
            Self::Double(b) => b.append_nulls(missing),
            Self::Bool(b) => b.append_nulls(missing),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Str(b) => Arc::new(b.finish()),
            Self::Int(b) => Arc::new(b.finish()),
            Self::Double(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
        }
    }
}

/// Index of a (scope, type) pair into the shredder's lookup tables.
const fn slot(scope: Scope, value_type: ValueType) -> usize {
    let scope = match scope {
        Scope::Resource => 0,
        Scope::Record => 1,
    };
    let value_type = match value_type {
        ValueType::Str => 0,
        ValueType::Int => 1,
        ValueType::Double => 2,
        ValueType::Bool => 3,
    };
    scope * 4 + value_type
}

/// Collects attributes row by row into one column per (scope, type, key).
///
/// Rows must be added in order. A column is created when its key is first
/// seen and is null for every row that does not carry the key.
pub struct AttributeShredder {
    /// Attribute key to column index, one table per (scope, type), so a
    /// lookup needs no allocation.
    index: [HashMap<String, usize>; 8],
    columns: Vec<(String, Builder)>,
}

impl Default for AttributeShredder {
    fn default() -> Self {
        Self::new()
    }
}

impl AttributeShredder {
    /// Create an empty shredder.
    #[must_use]
    pub fn new() -> Self {
        Self {
            index: std::array::from_fn(|_| HashMap::new()),
            columns: Vec::new(),
        }
    }

    /// Record the attributes of row number `row`.
    ///
    /// Keys in `excluded` already have dedicated columns and are skipped.
    /// If a key is repeated within the row, the first value wins.
    pub fn add(&mut self, row: usize, scope: Scope, attrs: &[KeyValue], excluded: &[&str]) {
        for attr in attrs {
            let key = attr.key.as_str();
            if key.is_empty() || key.len() > MAX_KEY_BYTES || excluded.contains(&key) {
                continue;
            }
            let Some(value) = attr.value.as_ref().and_then(|v| v.value.as_ref()) else {
                continue;
            };
            let value_type = match value {
                AnyValueKind::StringValue(_) => ValueType::Str,
                AnyValueKind::IntValue(_) => ValueType::Int,
                AnyValueKind::DoubleValue(_) => ValueType::Double,
                AnyValueKind::BoolValue(_) => ValueType::Bool,
                _ => continue,
            };

            let Some(column) = self.column_for(scope, value_type, key) else {
                continue;
            };
            let Some((_, builder)) = self.columns.get_mut(column) else {
                continue;
            };
            if builder.len() > row {
                continue;
            }
            builder.pad_to(row);
            match (builder, value) {
                (Builder::Str(b), AnyValueKind::StringValue(v)) => b.append_value(v),
                (Builder::Int(b), AnyValueKind::IntValue(v)) => b.append_value(*v),
                (Builder::Double(b), AnyValueKind::DoubleValue(v)) => b.append_value(*v),
                (Builder::Bool(b), AnyValueKind::BoolValue(v)) => b.append_value(*v),
                _ => {}
            }
        }
    }

    /// The column for an attribute, created on first use. `None` once the
    /// batch has reached [`MAX_SHREDDED_COLUMNS`].
    fn column_for(&mut self, scope: Scope, value_type: ValueType, key: &str) -> Option<usize> {
        let table = self.index.get_mut(slot(scope, value_type))?;
        if let Some(column) = table.get(key) {
            return Some(*column);
        }
        if self.columns.len() >= MAX_SHREDDED_COLUMNS {
            return None;
        }
        let column = self.columns.len();
        self.columns.push((
            column_name(scope, value_type, key),
            Builder::new(value_type),
        ));
        table.insert(key.to_owned(), column);
        Some(column)
    }

    /// Build a batch of `base` columns followed by the shredded columns,
    /// in name order.
    ///
    /// # Errors
    ///
    /// Returns an error if the columns do not form a valid batch, for
    /// example if `columns` do not all have the same number of rows.
    pub fn finish(
        mut self,
        base: &Schema,
        mut columns: Vec<ArrayRef>,
    ) -> Result<RecordBatch, ArrowError> {
        let rows = columns.first().map_or(0, |column| column.len());
        self.columns.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut fields: Vec<Arc<Field>> = base.fields().iter().cloned().collect();
        for (name, builder) in &mut self.columns {
            builder.pad_to(rows);
            let array = builder.finish();
            fields.push(Arc::new(Field::new(
                name.as_str(),
                array.data_type().clone(),
                true,
            )));
            columns.push(array);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
    }
}

/// Whether `schema` is `base` followed only by shredded attribute columns.
#[must_use]
pub fn extends(base: &Schema, schema: &Schema) -> bool {
    let base_len = base.fields().len();
    schema.fields().len() >= base_len
        && schema
            .fields()
            .iter()
            .zip(base.fields())
            .all(|(actual, expected)| actual == expected)
        && schema
            .fields()
            .iter()
            .skip(base_len)
            .all(|f| is_shredded(f))
}

/// The shredded attribute columns of `schema`, in order.
#[must_use]
pub fn shredded_fields(schema: &Schema) -> Vec<Arc<Field>> {
    schema
        .fields()
        .iter()
        .filter(|field| is_shredded(field))
        .cloned()
        .collect()
}

/// `base` followed by the given shredded columns, deduplicated and in
/// name order.
#[must_use]
pub fn with_shredded(base: &Schema, shredded: impl IntoIterator<Item = Arc<Field>>) -> SchemaRef {
    let extra: BTreeMap<String, Arc<Field>> = shredded
        .into_iter()
        .filter(|field| is_shredded(field) && base.field_with_name(field.name()).is_err())
        .map(|field| (field.name().clone(), field))
        .collect();
    let mut fields: Vec<Arc<Field>> = base.fields().iter().cloned().collect();
    fields.extend(extra.into_values());
    Arc::new(Schema::new(fields))
}

/// Bring batches with differing shredded columns to one schema.
///
/// The result has `base` followed by every shredded column that appears in
/// any batch; a batch lacking a column gets it filled with nulls.
///
/// # Errors
///
/// Returns an error if a batch's schema does not extend `base`.
pub fn unify(
    base: &Schema,
    batches: &[RecordBatch],
) -> Result<(SchemaRef, Vec<RecordBatch>), ArrowError> {
    let mut shredded = Vec::new();
    for batch in batches {
        if !extends(base, &batch.schema()) {
            return Err(ArrowError::SchemaError(
                "batch does not extend the signal's storage schema".to_owned(),
            ));
        }
        shredded.extend(shredded_fields(&batch.schema()));
    }
    let schema = with_shredded(base, shredded);

    let unified = batches
        .iter()
        .map(|batch| {
            if batch.schema() == schema {
                return Ok(batch.clone());
            }
            let columns: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|field| {
                    batch.column_by_name(field.name()).map_or_else(
                        || new_null_array(field.data_type(), batch.num_rows()),
                        Arc::clone,
                    )
                })
                .collect();
            RecordBatch::try_new(Arc::clone(&schema), columns)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((schema, unified))
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
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array};
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue};

    use super::*;

    fn kv(key: &str, value: AnyValueKind) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(AnyValue { value: Some(value) }),
        }
    }

    fn s(key: &str, value: &str) -> KeyValue {
        kv(key, AnyValueKind::StringValue(value.to_owned()))
    }

    fn base() -> Schema {
        Schema::new(vec![Field::new("n", DataType::UInt64, false)])
    }

    fn base_column(rows: u64) -> Vec<ArrayRef> {
        vec![Arc::new((0..rows).collect::<UInt64Array>())]
    }

    fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
        let column = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        (0..column.len())
            .map(|i| (!column.is_null(i)).then(|| column.value(i).to_owned()))
            .collect()
    }

    #[test]
    fn column_names_round_trip() {
        let name = column_name(Scope::Record, ValueType::Str, "tenant.id");
        assert_eq!(name, "attr.str.tenant.id");
        assert_eq!(
            parse_column_name(&name),
            Some((Scope::Record, ValueType::Str, "tenant.id"))
        );
        assert_eq!(
            parse_column_name("resource.f64.host.load"),
            Some((Scope::Resource, ValueType::Double, "host.load"))
        );
        for other in [
            "service.name",
            "attr.str.",
            "attr.blob.x",
            "attr",
            "http.route",
        ] {
            assert_eq!(parse_column_name(other), None, "{other}");
        }
    }

    #[test]
    fn each_attribute_becomes_a_sparse_typed_column() {
        let mut shredder = AttributeShredder::new();
        shredder.add(
            0,
            Scope::Record,
            &[
                s("tenant.id", "acme"),
                kv("retries", AnyValueKind::IntValue(3)),
            ],
            &[],
        );
        // Row 1 carries nothing.
        shredder.add(
            2,
            Scope::Record,
            &[
                s("tenant.id", "globex"),
                kv("ratio", AnyValueKind::DoubleValue(0.5)),
                kv("cached", AnyValueKind::BoolValue(true)),
            ],
            &[],
        );
        shredder.add(2, Scope::Resource, &[s("k8s.pod.name", "api-1")], &[]);
        let batch = shredder.finish(&base(), base_column(4)).unwrap();

        assert_eq!(batch.num_rows(), 4);
        assert_eq!(
            strings(&batch, "attr.str.tenant.id"),
            vec![
                Some("acme".to_owned()),
                None,
                Some("globex".to_owned()),
                None
            ]
        );
        assert_eq!(
            strings(&batch, "resource.str.k8s.pod.name"),
            vec![None, None, Some("api-1".to_owned()), None]
        );
        let retries = batch.column_by_name("attr.int.retries").unwrap();
        let retries = retries.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!((retries.value(0), retries.null_count()), (3, 3));
        let ratio = batch.column_by_name("attr.f64.ratio").unwrap();
        let ratio = ratio.as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((ratio.value(2) - 0.5).abs() < f64::EPSILON);
        let cached = batch.column_by_name("attr.bool.cached").unwrap();
        let cached = cached.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(cached.value(2));

        // Base columns first, then shredded columns in name order.
        let names: Vec<&str> = batch
            .schema_ref()
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(
            names,
            vec![
                "n",
                "attr.bool.cached",
                "attr.f64.ratio",
                "attr.int.retries",
                "attr.str.tenant.id",
                "resource.str.k8s.pod.name"
            ]
        );
        assert!(extends(&base(), &batch.schema()));
    }

    #[test]
    fn the_same_key_with_two_types_gets_two_columns() {
        let mut shredder = AttributeShredder::new();
        shredder.add(0, Scope::Record, &[s("code", "E42")], &[]);
        shredder.add(
            1,
            Scope::Record,
            &[kv("code", AnyValueKind::IntValue(42))],
            &[],
        );
        let batch = shredder.finish(&base(), base_column(2)).unwrap();

        assert_eq!(
            strings(&batch, "attr.str.code"),
            vec![Some("E42".to_owned()), None]
        );
        let ints = batch.column_by_name("attr.int.code").unwrap();
        let ints = ints.as_any().downcast_ref::<Int64Array>().unwrap();
        assert!(ints.is_null(0));
        assert_eq!(ints.value(1), 42);
    }

    #[test]
    fn dedicated_keys_complex_values_and_repeats_are_not_shredded() {
        let mut shredder = AttributeShredder::new();
        shredder.add(
            0,
            Scope::Record,
            &[
                s("http.route", "/x"),
                kv(
                    "tags",
                    AnyValueKind::ArrayValue(ArrayValue { values: Vec::new() }),
                ),
                kv("payload", AnyValueKind::BytesValue(vec![1, 2])),
                s("", "empty key"),
                s("dup", "first"),
                s("dup", "second"),
            ],
            &["http.route"],
        );
        let batch = shredder.finish(&base(), base_column(1)).unwrap();

        assert_eq!(batch.num_columns(), 2, "{:?}", batch.schema());
        assert_eq!(
            strings(&batch, "attr.str.dup"),
            vec![Some("first".to_owned())]
        );
    }

    #[test]
    fn the_number_of_columns_per_batch_is_capped() {
        let mut shredder = AttributeShredder::new();
        let attrs: Vec<KeyValue> = (0..MAX_SHREDDED_COLUMNS + 50)
            .map(|i| s(&format!("key-{i}"), "v"))
            .collect();
        shredder.add(0, Scope::Record, &attrs, &[]);
        // A key that already has a column keeps being recorded.
        shredder.add(1, Scope::Record, &[s("key-0", "again")], &[]);
        let batch = shredder.finish(&base(), base_column(2)).unwrap();

        assert_eq!(batch.num_columns(), 1 + MAX_SHREDDED_COLUMNS);
        assert_eq!(
            strings(&batch, "attr.str.key-0"),
            vec![Some("v".to_owned()), Some("again".to_owned())]
        );
    }

    #[test]
    fn batches_with_different_columns_are_unified_with_nulls() {
        let mut first = AttributeShredder::new();
        first.add(0, Scope::Record, &[s("a", "1")], &[]);
        let first = first.finish(&base(), base_column(1)).unwrap();

        let mut second = AttributeShredder::new();
        second.add(0, Scope::Record, &[s("b", "2")], &[]);
        second.add(1, Scope::Record, &[s("a", "3")], &[]);
        let second = second.finish(&base(), base_column(2)).unwrap();

        let plain = AttributeShredder::new()
            .finish(&base(), base_column(1))
            .unwrap();

        let (schema, unified) = unify(&base(), &[first, second, plain]).unwrap();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["n", "attr.str.a", "attr.str.b"]);

        let combined = arrow::compute::concat_batches(&schema, &unified).unwrap();
        assert_eq!(
            strings(&combined, "attr.str.a"),
            vec![Some("1".to_owned()), None, Some("3".to_owned()), None]
        );
        assert_eq!(
            strings(&combined, "attr.str.b"),
            vec![None, Some("2".to_owned()), None, None]
        );
    }

    #[test]
    fn a_batch_with_foreign_columns_is_rejected() {
        let foreign = Schema::new(vec![
            Field::new("n", DataType::UInt64, false),
            Field::new("surprise", DataType::Utf8, true),
        ]);
        assert!(!extends(&base(), &foreign));

        // A column with a shredded name but the wrong type is not trusted.
        let mistyped = Schema::new(vec![
            Field::new("n", DataType::UInt64, false),
            Field::new("attr.int.x", DataType::Utf8, true),
        ]);
        assert!(!extends(&base(), &mistyped));
    }
}
