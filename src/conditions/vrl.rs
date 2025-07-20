
use vector_lib::{emit, TimeZone};
use vrl::compiler::runtime::{Runtime, RuntimeResult, Terminate};
use vrl::compiler::{Program};
use vrl::diagnostic::Formatter;
use vrl::value::Value;

use crate::config::LogNamespace;
use crate::event::TargetEvents;
use crate::{
    conditions::{Conditional},
    event::{Event, VrlTarget},
    internal_events::VrlConditionExecutionError,
};

#[derive(Debug, Clone)]
pub struct Vrl {
    pub(super) program: Program,
    pub(super) source: String,
}

impl Vrl {
    fn run(&self, event: Event) -> (Event, RuntimeResult) {
        let log_namespace = event
            .maybe_as_log()
            .map(|log| log.namespace())
            .unwrap_or(LogNamespace::Legacy);
        let mut target = VrlTarget::new(event, self.program.info(), false);
        // TODO: use timezone from remap config
        let timezone = TimeZone::default();

        let result = Runtime::default().resolve(&mut target, &self.program, &timezone);
        let original_event = match target.into_events(log_namespace) {
            TargetEvents::One(event) => event,
            _ => panic!("Event was modified in a condition. This is an internal compiler error."),
        };
        (original_event, result)
    }
}

impl Conditional for Vrl {
    fn check(&self, event: Event) -> (bool, Event) {
        let (event, result) = self.run(event);

        let result = result
            .map(|value| match value {
                Value::Boolean(boolean) => boolean,
                _ => panic!("VRL condition did not return a boolean type"),
            })
            .unwrap_or_else(|err| {
                emit!(VrlConditionExecutionError {
                    error: err.to_string().as_ref()
                });
                false
            });
        (result, event)
    }

    fn check_with_context(&self, event: Event) -> (Result<(), String>, Event) {
        let (event, result) = self.run(event);

        let value_result = result.map_err(|err| match err {
            Terminate::Abort(err) => {
                let err = Formatter::new(
                    &self.source,
                    vrl::diagnostic::Diagnostic::from(
                        Box::new(err) as Box<dyn vrl::diagnostic::DiagnosticMessage>
                    ),
                )
                .colored()
                .to_string();
                format!("source execution aborted: {}", err)
            }
            Terminate::Error(err) => {
                let err = Formatter::new(
                    &self.source,
                    vrl::diagnostic::Diagnostic::from(
                        Box::new(err) as Box<dyn vrl::diagnostic::DiagnosticMessage>
                    ),
                )
                .colored()
                .to_string();
                format!("source execution failed: {}", err)
            }
        });

        let value = match value_result {
            Ok(value) => value,
            Err(err) => {
                return (Err(err), event);
            }
        };

        let result = match value {
            Value::Boolean(v) if v => Ok(()),
            Value::Boolean(v) if !v => Err("source execution resolved to false".into()),
            _ => Err("source execution resolved to a non-boolean value".into()),
        };
        (result, event)
    }
}