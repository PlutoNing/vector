use crate::event::{EstimatedJsonEncodedSizeOf, Event, EventArray, EventContainer, EventRef};
use agent_common::{byte_size_of::ByteSizeOf, json_size::JsonSize};

/// Transforms come in two variants. Functions, or tasks.
///
/// While function transforms can be run out of order, or concurrently, task
/// transforms act as a coordination or barrier point.
pub enum Transform {
    Function(Box<dyn FunctionTransform>),
}

impl Transform {
    /// Create a new function transform.
    ///
    /// These functions are "stateless" and can be run in parallel, without
    /// regard for coordination.
    ///
    /// **Note:** You should prefer to implement this over [`TaskTransform`]
    /// where possible.
    pub fn function(v: impl FunctionTransform + 'static) -> Self {
        Transform::Function(Box::new(v))
    }
}

/// Transforms that are simple, and don't require attention to coordination.
/// You can run them as simple functions over events in any order.
///
/// # Invariants
///
/// * It is an illegal invariant to implement `functionTransform` for a
///   `TaskTransform` or vice versa.
pub trait FunctionTransform: Send + dyn_clone::DynClone + Sync {
    fn transform(&mut self, output: &mut OutputBuffer, event: Event);
}

dyn_clone::clone_trait_object!(FunctionTransform);

#[derive(Debug, Default, Clone)]
pub struct OutputBuffer(Vec<EventArray>);

impl OutputBuffer {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    pub fn push(&mut self, event: Event) {
        // Coalesce multiple pushes of the same type into one array.
        match (event, self.0.last_mut()) {
            (Event::Log(log), Some(EventArray::Logs(logs))) => {
                logs.push(log);
            }
            (Event::Metric(metric), Some(EventArray::Metrics(metrics))) => {
                metrics.push(metric);
            }
            (Event::Trace(trace), Some(EventArray::Traces(traces))) => {
                traces.push(trace);
            }
            (event, _) => {
                self.0.push(event.into());
            }
        }
    }

    pub fn append(&mut self, events: &mut Vec<Event>) {
        for event in events.drain(..) {
            self.push(event);
        }
    }

    pub fn extend(&mut self, events: impl Iterator<Item = Event>) {
        for event in events {
            self.push(event);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.iter().map(EventArray::len).sum()
    }

    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    pub fn first(&self) -> Option<EventRef> {
        self.0.first().and_then(|first| match first {
            EventArray::Logs(l) => l.first().map(Into::into),
            EventArray::Metrics(m) => m.first().map(Into::into),
            EventArray::Traces(t) => t.first().map(Into::into),
        })
    }

    pub fn drain(&mut self) -> impl Iterator<Item = Event> + '_ {
        self.0.drain(..).flat_map(EventArray::into_events)
    }

    pub fn into_events(self) -> impl Iterator<Item = Event> {
        self.0.into_iter().flat_map(EventArray::into_events)
    }
}

impl ByteSizeOf for OutputBuffer {
    fn allocated_bytes(&self) -> usize {
        self.0.iter().map(ByteSizeOf::size_of).sum()
    }
}


impl EstimatedJsonEncodedSizeOf for OutputBuffer {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.0
            .iter()
            .map(EstimatedJsonEncodedSizeOf::estimated_json_encoded_size_of)
            .sum()
    }
}

impl From<Vec<Event>> for OutputBuffer {
    fn from(events: Vec<Event>) -> Self {
        let mut result = Self::default();
        result.extend(events.into_iter());
        result
    }
}
