use serde::Serialize;
use agent_lib::{
    event::{ LogEvent, MaybeAsLogMut},
};


/// An event alongside metadata from preprocessing. This is useful for sinks
/// like Splunk HEC that process events prior to encoding.
#[derive(Serialize)]
pub struct ProcessedEvent<E, M> {
    pub event: E,
    pub metadata: M,
}

impl<E, M> MaybeAsLogMut for ProcessedEvent<E, M>
where
    E: MaybeAsLogMut,
{
    fn maybe_as_log_mut(&mut self) -> Option<&mut LogEvent> {
        self.event.maybe_as_log_mut()
    }
}
