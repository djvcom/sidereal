//! Registry of the attribute columns that exist in storage.
//!
//! Attributes are written as columns of their own (see
//! [`crate::ingest::shred`]), so which columns a signal has depends on
//! which attributes have ever been sent. A query has to be planned against
//! a schema that includes them, and this registry is where that schema
//! comes from.
//!
//! Like the catalogue, it holds nothing the bucket does not. It is filled
//! from the files' own footers at startup and from each flush afterwards,
//! and only ever grows: a column whose files have all expired simply reads
//! as null.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use arrow::datatypes::{Field, Schema, SchemaRef};
use futures::stream::{self, StreamExt};
use object_store::path::Path;

use crate::catalogue::CatalogueStore;
use crate::compaction::read_arrow_schema;
use crate::ingest::shred;
use crate::storage::Signal;

/// How many file footers are read at once when loading.
const LOAD_CONCURRENCY: usize = 32;

const SIGNALS: [Signal; 3] = [Signal::Traces, Signal::Metrics, Signal::Logs];

const fn slot(signal: Signal) -> usize {
    match signal {
        Signal::Traces => 0,
        Signal::Metrics => 1,
        Signal::Logs => 2,
    }
}

/// An attribute that stored telemetry carries.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct KnownAttribute {
    /// The attribute key.
    pub key: String,
    /// Where it is found: `span`, `resource`, or `built_in` for the
    /// attributes that have dedicated columns.
    pub scope: &'static str,
    /// The types it has been sent with.
    pub types: Vec<&'static str>,
}

fn type_name(data_type: &arrow::datatypes::DataType) -> &'static str {
    use arrow::datatypes::DataType;
    match data_type {
        DataType::Boolean => "boolean",
        DataType::Float64 => "number",
        data_type if data_type.is_integer() => "integer",
        _ => "text",
    }
}

fn base_schema(signal: Signal) -> SchemaRef {
    match signal {
        Signal::Traces => crate::schema::traces::traces_storage_schema(),
        Signal::Metrics => crate::schema::metrics::number_metrics_storage_schema(),
        Signal::Logs => crate::schema::logs::logs_storage_schema(),
    }
}

/// The attribute columns known to exist, per signal.
#[derive(Debug, Default)]
pub struct AttributeRegistry {
    fields: RwLock<[BTreeMap<String, Arc<Field>>; 3]>,
    version: AtomicU64,
}

impl AttributeRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A number that changes whenever a column is added, so that holders
    /// of a derived schema know when to rebuild it.
    #[must_use]
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Record the attribute columns of a schema that was written for
    /// `signal`. Returns how many were new.
    pub fn observe(&self, signal: Signal, schema: &Schema) -> usize {
        let candidates = shred::shredded_fields(schema);
        if candidates.is_empty() {
            return 0;
        }
        let mut added = 0;
        {
            let mut fields = self.fields.write().unwrap_or_else(PoisonError::into_inner);
            if let Some(known) = fields.get_mut(slot(signal)) {
                for field in candidates {
                    if !known.contains_key(field.name()) {
                        known.insert(field.name().clone(), field);
                        added += 1;
                    }
                }
            }
        }
        if added > 0 {
            self.version.fetch_add(1, Ordering::AcqRel);
        }
        added
    }

    /// `base` followed by every attribute column known for `signal`.
    #[must_use]
    pub fn schema(&self, signal: Signal, base: &Schema) -> SchemaRef {
        let fields = self.fields.read().unwrap_or_else(PoisonError::into_inner);
        let known = fields
            .get(slot(signal))
            .map(|known| known.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        shred::with_shredded(base, known)
    }

    /// The distinct types under which attribute `key` of `scope` is
    /// stored, across all signals: in a dedicated column named after the
    /// attribute, or in attribute columns.
    ///
    /// Used to type an attribute reference before the table it is read
    /// from is known.
    #[must_use]
    pub fn types_of(&self, scope: shred::Scope, key: &str) -> Vec<arrow::datatypes::DataType> {
        let mut types = Vec::new();
        let mut push = |data_type: &arrow::datatypes::DataType| {
            if !types.contains(data_type) {
                types.push(data_type.clone());
            }
        };

        let fields = self.fields.read().unwrap_or_else(PoisonError::into_inner);
        for signal in SIGNALS {
            let base = base_schema(signal);
            if let Ok(field) = base.field_with_name(key) {
                push(field.data_type());
                continue;
            }
            let Some(known) = fields.get(slot(signal)) else {
                continue;
            };
            for value_type in shred::ValueType::ALL {
                if let Some(field) = known.get(&shred::column_name(scope, value_type, key)) {
                    push(field.data_type());
                }
            }
        }
        types
    }

    /// Every attribute a query on `signal` can refer to, in key order:
    /// those stored as attribute columns and those with dedicated columns.
    #[must_use]
    pub fn known(&self, signal: Signal) -> Vec<KnownAttribute> {
        let mut found: BTreeMap<(String, &'static str), Vec<&'static str>> = BTreeMap::new();

        // Dedicated columns are named after the attribute itself, which
        // always contains a dot; plain columns such as `name` are not
        // attributes.
        for field in base_schema(signal).fields() {
            if field.name().contains('.') {
                found
                    .entry((field.name().clone(), "built_in"))
                    .or_default()
                    .push(type_name(field.data_type()));
            }
        }

        let fields = self.fields.read().unwrap_or_else(PoisonError::into_inner);
        if let Some(known) = fields.get(slot(signal)) {
            for field in known.values() {
                if let Some((scope, _, key)) = shred::parse_column_name(field.name()) {
                    let scope = match scope {
                        shred::Scope::Record => "span",
                        shred::Scope::Resource => "resource",
                    };
                    found
                        .entry((key.to_owned(), scope))
                        .or_default()
                        .push(type_name(field.data_type()));
                }
            }
        }

        found
            .into_iter()
            .map(|((key, scope), types)| KnownAttribute { key, scope, types })
            .collect()
    }

    /// Number of attribute columns known for `signal`.
    #[must_use]
    pub fn len(&self, signal: Signal) -> usize {
        let fields = self.fields.read().unwrap_or_else(PoisonError::into_inner);
        fields.get(slot(signal)).map_or(0, BTreeMap::len)
    }

    /// Learn the attribute columns of every data file in the catalogue by
    /// reading the files' footers. Returns the number of files read.
    ///
    /// A file that cannot be read is skipped with a warning: its columns
    /// are picked up when another file or a later flush carries them.
    pub async fn load(&self, catalogue: &CatalogueStore) -> usize {
        let store = catalogue.inner();
        let mut read = 0;
        for signal in SIGNALS {
            let prefix = Path::from(signal.as_str());
            let files: Vec<_> = catalogue
                .objects(Some(&prefix))
                .into_iter()
                .filter(|meta| {
                    meta.location.filename().is_some_and(|name| {
                        name.rsplit_once('.')
                            .is_some_and(|(_, ext)| ext == "parquet")
                    })
                })
                .collect();

            let schemas: Vec<_> = stream::iter(files)
                .map(|meta| {
                    let store = Arc::clone(&store);
                    async move { (read_arrow_schema(&store, &meta).await, meta.location) }
                })
                .buffer_unordered(LOAD_CONCURRENCY)
                .collect()
                .await;

            for (schema, location) in schemas {
                match schema {
                    Ok(Some(schema)) => {
                        self.observe(signal, &schema);
                        read += 1;
                    }
                    Ok(None) => {}
                    Err(error) => {
                        tracing::warn!(%location, %error, "Could not read attribute columns of file");
                    }
                }
            }
        }
        read
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use arrow::datatypes::DataType;

    use super::*;

    fn base() -> Schema {
        Schema::new(vec![Field::new("n", DataType::UInt64, false)])
    }

    fn with(columns: &[(&str, DataType)]) -> Schema {
        let mut fields = vec![Field::new("n", DataType::UInt64, false)];
        fields.extend(
            columns
                .iter()
                .map(|(name, data_type)| Field::new(*name, data_type.clone(), true)),
        );
        Schema::new(fields)
    }

    #[test]
    fn observed_columns_extend_the_signal_schema_and_bump_the_version() {
        let registry = AttributeRegistry::new();
        assert_eq!(registry.schema(Signal::Traces, &base()).fields().len(), 1);

        let added = registry.observe(
            Signal::Traces,
            &with(&[
                ("attr.str.tenant.id", DataType::Utf8),
                ("resource.str.k8s.pod.name", DataType::Utf8),
            ]),
        );
        assert_eq!(added, 2);
        assert_eq!(registry.version(), 1);

        // Seeing the same columns again changes nothing.
        registry.observe(
            Signal::Traces,
            &with(&[("attr.str.tenant.id", DataType::Utf8)]),
        );
        assert_eq!(registry.version(), 1);

        registry.observe(
            Signal::Traces,
            &with(&[("attr.int.retries", DataType::Int64)]),
        );
        assert_eq!(registry.version(), 2);

        let schema = registry.schema(Signal::Traces, &base());
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "n",
                "attr.int.retries",
                "attr.str.tenant.id",
                "resource.str.k8s.pod.name"
            ]
        );

        // Signals are independent.
        assert_eq!(registry.len(Signal::Logs), 0);
        assert_eq!(registry.schema(Signal::Logs, &base()).fields().len(), 1);
    }

    #[test]
    fn columns_that_are_not_attributes_are_ignored() {
        let registry = AttributeRegistry::new();
        let added = registry.observe(
            Signal::Logs,
            &with(&[
                ("surprise", DataType::Utf8),
                ("attr.int.mistyped", DataType::Utf8),
            ]),
        );
        assert_eq!(added, 0);
        assert_eq!(registry.version(), 0);
    }
}
