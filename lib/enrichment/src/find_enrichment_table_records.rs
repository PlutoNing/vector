use std::collections::BTreeMap;
use vrl::prelude::*;


use crate::{
    vrl_util::{evaluate_condition},
    Case, Condition, IndexHandle, TableSearch,
};

fn find_enrichment_table_records(
    select: Option<Value>,
    enrichment_tables: &TableSearch,
    table: &str,
    case_sensitive: Case,
    wildcard: Option<Value>,
    condition: &[Condition],
    index: Option<IndexHandle>,
) -> Resolved {
    let select = select
        .map(|select| match select {
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

    let data = enrichment_tables
        .find_table_rows(
            table,
            case_sensitive,
            condition,
            select.as_ref().map(|select| select.as_ref()),
            wildcard.as_ref(),
            index,
        )?
        .into_iter()
        .map(Value::Object)
        .collect();
    Ok(Value::Array(data))
}

#[derive(Debug, Clone)]
pub struct FindEnrichmentTableRecordsFn {
    table: String,
    condition: BTreeMap<KeyString, expression::Expr>,
    index: Option<IndexHandle>,
    select: Option<Box<dyn Expression>>,
    case_sensitive: Case,
    wildcard: Option<Box<dyn Expression>>,
    enrichment_tables: TableSearch,
}

impl FunctionExpression for FindEnrichmentTableRecordsFn {
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

        find_enrichment_table_records(
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
        TypeDef::array(Collection::from_unknown(Kind::object(Collection::any()))).fallible()
    }
}
