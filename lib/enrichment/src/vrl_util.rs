//! Utilities shared between both VRL functions.


use crate::{Condition};

use vrl::prelude::*;






/// Evaluates the condition object to search the enrichment tables with.
pub(crate) fn evaluate_condition(key: &str, value: Value) -> ExpressionResult<Condition> {
    Ok(match value {
        Value::Object(map) if map.contains_key("from") && map.contains_key("to") => {
            Condition::BetweenDates {
                field: key,
                from: *map
                    .get("from")
                    .expect("should contain from")
                    .as_timestamp()
                    .ok_or("from in condition must be a timestamp")?,
                to: *map
                    .get("to")
                    .expect("should contain to")
                    .as_timestamp()
                    .ok_or("to in condition must be a timestamp")?,
            }
        }
        Value::Object(map) if map.contains_key("from") => Condition::FromDate {
            field: key,
            from: *map
                .get("from")
                .expect("should contain from")
                .as_timestamp()
                .ok_or("from in condition must be a timestamp")?,
        },
        Value::Object(map) if map.contains_key("to") => Condition::ToDate {
            field: key,
            to: *map
                .get("to")
                .expect("should contain to")
                .as_timestamp()
                .ok_or("to in condition must be a timestamp")?,
        },
        _ => Condition::Equals { field: key, value },
    })
}