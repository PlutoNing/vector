use std::collections::BTreeMap;
use vrl::prelude::*;

use crate::vrl_util::is_case_sensitive;
use crate::{
    vrl_util::{self, add_index, evaluate_condition},
    Case, Condition, IndexHandle, TableRegistry, TableSearch,
};

fn get_enrichment_table_record(
    select: Option<Value>,
    enrichment_tables: &TableSearch,
    table: &str,
    case_sensitive: Case,
    wildcard: Option<Value>,
    condition: &[Condition],
    index: Option<IndexHandle>,
) -> Resolved {
    let select = select
        .map(|array| match array {
            Value::Array(arr) => arr
                .iter()
                .map(|value| Ok(value.try_bytes_utf8_lossy()?.to_string()))
                .collect::<std::result::Result<Vec<_>, _>>(),
            value => Err(ValueError::Expected {
                got: value.kind(),
                expected: Kind::array(Collection::any()),
            }),
        })
        .transpose()?;

    let data = enrichment_tables.find_table_row(
        table,
        case_sensitive,
        condition,
        select.as_ref().map(|select| select.as_ref()),
        wildcard.as_ref(),
        index,
    )?;

    Ok(Value::Object(data))
}

#[derive(Debug, Clone)]
pub struct GetEnrichmentTableRecordFn {
    table: String,
    condition: BTreeMap<KeyString, expression::Expr>,
    index: Option<IndexHandle>,
    select: Option<Box<dyn Expression>>,
    wildcard: Option<Box<dyn Expression>>,
    case_sensitive: Case,
    enrichment_tables: TableSearch,
}

impl FunctionExpression for GetEnrichmentTableRecordFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let condition = self
            .condition
            .iter()
            .map(|(key, value)| {
                let value = value.resolve(ctx)?;
                evaluate_condition(key, value)
            })
            .collect::<ExpressionResult<Vec<Condition>>>()?;

        let select = self
            .select
            .as_ref()
            .map(|array| array.resolve(ctx))
            .transpose()?;

        let table = &self.table;
        let case_sensitive = self.case_sensitive;
        let wildcard = self
            .wildcard
            .as_ref()
            .map(|array| array.resolve(ctx))
            .transpose()?;
        let index = self.index;
        let enrichment_tables = &self.enrichment_tables;

        get_enrichment_table_record(
            select,
            enrichment_tables,
            table,
            case_sensitive,
            wildcard,
            &condition,
            index,
        )
    }

    fn type_def(&self, _: &TypeState) -> TypeDef {
        TypeDef::object(Collection::any()).fallible()
    }
}