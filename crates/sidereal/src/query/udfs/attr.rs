//! `attr('key')` and `resource_attr('key')`: read an attribute by name.
//!
//! Attributes are stored as columns whose names encode scope and type
//! (see [`crate::ingest::shred`]), and a few have dedicated columns named
//! after the attribute itself. These two functions hide that: a query
//! names the attribute and gets whichever column holds it.
//!
//! ```sql
//! SELECT COUNT(*) FROM traces
//! WHERE attr('user_agent.original') LIKE '%Googlebot%'
//!   AND attr('product.in_stock') = false
//!   AND resource_attr('k8s.namespace.name') = 'shop'
//! ```
//!
//! The call is replaced while the query is planned, so it costs nothing
//! at run time and the column's statistics, encoding and type all apply:
//!
//! - a dedicated column of that name, if there is one
//! - otherwise the attribute's column, with its stored type
//! - if the same key was sent with several types, the first non-null of
//!   them as a string
//! - if no stored data has the attribute, `NULL`, so a filter on an
//!   attribute nobody has sent yet matches nothing rather than failing

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{plan_err, Column, DFSchema, Result, ScalarValue};
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::{
    cast, ColumnarValue, Expr, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility,
};

use crate::attributes::AttributeRegistry;
use crate::ingest::shred::{column_name, Scope, ValueType};

/// Name of the function that reads a record's own attribute.
pub const ATTR: &str = "attr";
/// Name of the function that reads an attribute of the emitting resource.
pub const RESOURCE_ATTR: &str = "resource_attr";

/// Placeholder that makes the function name known to the SQL planner.
/// Every call is replaced by [`AttributeRewrite`] before execution.
///
/// SQL is type-checked as it is parsed, before the call is replaced, so
/// the placeholder reports the type the attribute is stored with. It
/// cannot see which table is being read, so it asks the registry across
/// all signals and falls back to a string where they disagree; the real
/// column's type takes over once the call is replaced.
#[derive(Debug)]
struct AttributeFunction {
    name: &'static str,
    scope: Scope,
    signature: Signature,
    registry: Option<Arc<AttributeRegistry>>,
}

impl PartialEq for AttributeFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for AttributeFunction {}

impl std::hash::Hash for AttributeFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for AttributeFunction {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let key = args
            .scalar_arguments
            .first()
            .copied()
            .flatten()
            .and_then(|value| value.try_as_str().flatten());
        let mut types = match (key, &self.registry) {
            (Some(key), Some(registry)) => registry.types_of(self.scope, key),
            _ => Vec::new(),
        };
        let data_type = match types.len() {
            1 => types.swap_remove(0),
            _ => DataType::Utf8,
        };
        Ok(Arc::new(Field::new(self.name, data_type, true)))
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{}() takes the attribute key as a string literal",
            self.name
        )
    }
}

/// The `attr` and `resource_attr` functions, for registration.
#[must_use]
pub fn attribute_udfs(registry: Option<&Arc<AttributeRegistry>>) -> Vec<ScalarUDF> {
    [(ATTR, Scope::Record), (RESOURCE_ATTR, Scope::Resource)]
        .into_iter()
        .map(|(name, scope)| {
            ScalarUDF::new_from_impl(AttributeFunction {
                name,
                scope,
                signature: Signature::string(1, Volatility::Immutable),
                registry: registry.cloned(),
            })
        })
        .collect()
}

/// Replaces calls to `attr` and `resource_attr` with the column that
/// holds the attribute.
#[derive(Debug, Default)]
pub struct AttributeRewrite;

impl FunctionRewrite for AttributeRewrite {
    fn name(&self) -> &'static str {
        "attribute_columns"
    }

    fn rewrite(
        &self,
        expr: Expr,
        schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let Expr::ScalarFunction(function) = &expr else {
            return Ok(Transformed::no(expr));
        };
        let scope = match function.name() {
            ATTR => Scope::Record,
            RESOURCE_ATTR => Scope::Resource,
            _ => return Ok(Transformed::no(expr)),
        };
        let key = match function.args.as_slice() {
            [Expr::Literal(value, _)] => value.try_as_str().flatten(),
            _ => None,
        };
        let Some(key) = key else {
            return plan_err!(
                "{}() takes the attribute key as a string literal",
                function.name()
            );
        };
        Ok(Transformed::yes(resolve(scope, key, schema)?))
    }
}

/// The expression that reads attribute `key` from a plan with `schema`.
fn resolve(scope: Scope, key: &str, schema: &DFSchema) -> Result<Expr> {
    // An attribute with a dedicated column is stored there and nowhere else.
    if let Some(column) = column(schema, key)? {
        return Ok(column);
    }

    let mut typed = Vec::new();
    for value_type in ValueType::ALL {
        if let Some(column) = column(schema, &column_name(scope, value_type, key))? {
            typed.push(column);
        }
    }
    Ok(match typed.len() {
        0 => Expr::Literal(ScalarValue::Utf8(None), None),
        1 => typed.swap_remove(0),
        _ => coalesce(
            typed
                .into_iter()
                .map(|column| cast(column, DataType::Utf8))
                .collect(),
        ),
    })
}

/// A reference to the column called `name`, if the schema has one.
///
/// # Errors
///
/// Returns an error if more than one table in the plan has the column,
/// since the call does not say which is meant.
fn column(schema: &DFSchema, name: &str) -> Result<Option<Expr>> {
    if !schema.has_column_with_unqualified_name(name) {
        return Ok(None);
    }
    let (qualifier, field) = schema.qualified_field_with_unqualified_name(name)?;
    Ok(Some(Expr::Column(Column::new(
        qualifier.cloned(),
        field.name(),
    ))))
}
