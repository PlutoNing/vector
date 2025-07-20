#![allow(missing_docs)]


use crate::event::Event;


pub(crate) mod is_log;
pub(crate) mod is_metric;
pub(crate) mod is_trace;
mod vrl;


use self::{
    is_log::{check_is_log},
    is_metric::{check_is_metric},
    is_trace::{check_is_trace},
    vrl::Vrl,
};

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Condition {
    /// Matches an event if it is a log.
    IsLog,

    /// Matches an event if it is a metric.
    IsMetric,

    /// Matches an event if it is a trace.
    IsTrace,

    /// Matches an event with a [Vector Remap Language](https://vector.dev/docs/reference/vrl) (VRL) [boolean expression](https://vector.dev/docs/reference/vrl#boolean-expressions).
    Vrl(Vrl),

    /// Matches any event.
    ///
    /// Used only for internal testing.
    AlwaysPass,

    /// Matches no event.
    ///
    /// Used only for internal testing.
    AlwaysFail,
}

impl Condition {
    /// Checks if a condition is true.
    ///
    /// The event should not be modified, it is only mutable so it can be passed into VRL, but VRL type checking prevents mutation.
    #[allow(dead_code)]
    pub fn check(&self, e: Event) -> (bool, Event) {
        match self {
            Condition::IsLog => check_is_log(e),
            Condition::IsMetric => check_is_metric(e),
            Condition::IsTrace => check_is_trace(e),
            Condition::Vrl(x) => x.check(e),
            Condition::AlwaysPass => (true, e),
            Condition::AlwaysFail => (false, e),
        }
    }
}


pub trait Conditional: std::fmt::Debug {
    /// Checks if a condition is true.
    ///
    /// The event should not be modified, it is only mutable so it can be passed into VRL, but VRL type checking prevents mutation.
    fn check(&self, event: Event) -> (bool, Event);

    /// Checks if a condition is true, with a `Result`-oriented return for easier composition.
    ///
    /// This can be mildly expensive for conditions that do not often match, as it allocates a string for the error
    /// case. As such, it should typically be avoided in hot paths.
    fn check_with_context(&self, e: Event) -> (Result<(), String>, Event) {
        let (result, event) = self.check(e);
        if result {
            (Ok(()), event)
        } else {
            (Err("condition failed".into()), event)
        }
    }
}

pub trait ConditionalConfig: std::fmt::Debug + Send + Sync + dyn_clone::DynClone {
    fn build(
        &self,
        enrichment_tables: &vector_lib::enrichment::TableRegistry,
    ) -> crate::Result<Condition>;
}

dyn_clone::clone_trait_object!(ConditionalConfig);