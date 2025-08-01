use std::{convert::TryInto, fmt::Debug, sync::Arc};

use crate::buffer::EventCount;
use agent_common::{
    byte_size_of::ByteSizeOf, config::ComponentKey, finalization, json_size::JsonSize, EventDataEq,
};
pub use array::{into_event_stream, EventArray, EventContainer, LogArray, MetricArray, TraceArray};
pub use estimated_json_encoded_size_of::EstimatedJsonEncodedSizeOf;
pub use finalization::{
    BatchNotifier, BatchStatus, BatchStatusReceiver, EventFinalizer, EventFinalizers, EventStatus,
    Finalizable,
};
pub use log_event::LogEvent;
pub use metadata::{EventMetadata, WithMetadata};
pub use metric::{Metric, MetricKind, MetricTags, MetricValue, StatisticKind};
pub use r#ref::{EventMutRef, EventRef};
use serde::{Deserialize, Serialize};
pub use trace::TraceEvent;
pub use vrl::value::{KeyString, ObjectMap, Value};

use crate::config::LogNamespace;
use crate::config::OutputId;

pub mod array;
mod estimated_json_encoded_size_of;
mod log_event;
mod metadata;
pub mod metric;
mod r#ref;
mod ser;
mod trace;
pub mod util;

pub const PARTIAL: &str = "_partial";

#[derive(PartialEq, Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    Log(LogEvent),
    Metric(Metric),
    Trace(TraceEvent),
}

impl ByteSizeOf for Event {
    fn allocated_bytes(&self) -> usize {
        match self {
            Event::Log(log_event) => log_event.allocated_bytes(),
            Event::Metric(metric_event) => metric_event.allocated_bytes(),
            Event::Trace(trace_event) => trace_event.allocated_bytes(),
        }
    }
}

impl EstimatedJsonEncodedSizeOf for Event {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        match self {
            Event::Log(log_event) => log_event.estimated_json_encoded_size_of(),
            Event::Metric(metric_event) => metric_event.estimated_json_encoded_size_of(),
            Event::Trace(trace_event) => trace_event.estimated_json_encoded_size_of(),
        }
    }
}

impl EventCount for Event {
    fn event_count(&self) -> usize {
        1
    }
}

impl Finalizable for Event {
    fn take_finalizers(&mut self) -> EventFinalizers {
        match self {
            Event::Log(log_event) => log_event.take_finalizers(),
            Event::Metric(metric) => metric.take_finalizers(),
            Event::Trace(trace_event) => trace_event.take_finalizers(),
        }
    }
}

impl Event {
    /// Return self as a `LogEvent`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Log`.
    pub fn as_log(&self) -> &LogEvent {
        match self {
            Event::Log(log) => log,
            _ => panic!("Failed type coercion, {self:?} is not a log event"),
        }
    }

    /// Return self as a mutable `LogEvent`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Log`.
    pub fn as_mut_log(&mut self) -> &mut LogEvent {
        match self {
            Event::Log(log) => log,
            _ => panic!("Failed type coercion, {self:?} is not a log event"),
        }
    }

    /// Coerces self into a `LogEvent`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Log`.
    pub fn into_log(self) -> LogEvent {
        match self {
            Event::Log(log) => log,
            _ => panic!("Failed type coercion, {self:?} is not a log event"),
        }
    }

    /// Fallibly coerces self into a `LogEvent`
    ///
    /// If the event is a `LogEvent`, then `Some(log_event)` is returned, otherwise `None`.
    pub fn try_into_log(self) -> Option<LogEvent> {
        match self {
            Event::Log(log) => Some(log),
            _ => None,
        }
    }

    /// Return self as a `LogEvent` if possible
    ///
    /// If the event is a `LogEvent`, then `Some(&log_event)` is returned, otherwise `None`.
    pub fn maybe_as_log(&self) -> Option<&LogEvent> {
        match self {
            Event::Log(log) => Some(log),
            _ => None,
        }
    }

    /// Return self as a `Metric`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Metric`.
    pub fn as_metric(&self) -> &Metric {
        match self {
            Event::Metric(metric) => metric,
            _ => panic!("Failed type coercion, {self:?} is not a metric"),
        }
    }

    /// Return self as a mutable `Metric`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Metric`.
    pub fn as_mut_metric(&mut self) -> &mut Metric {
        match self {
            Event::Metric(metric) => metric,
            _ => panic!("Failed type coercion, {self:?} is not a metric"),
        }
    }

    /// Coerces self into `Metric`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Metric`.
    pub fn into_metric(self) -> Metric {
        match self {
            Event::Metric(metric) => metric,
            _ => panic!("Failed type coercion, {self:?} is not a metric"),
        }
    }

    /// Fallibly coerces self into a `Metric`
    ///
    /// If the event is a `Metric`, then `Some(metric)` is returned, otherwise `None`.
    pub fn try_into_metric(self) -> Option<Metric> {
        match self {
            Event::Metric(metric) => Some(metric),
            _ => None,
        }
    }

    /// Return self as a `TraceEvent`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Trace`.
    pub fn as_trace(&self) -> &TraceEvent {
        match self {
            Event::Trace(trace) => trace,
            _ => panic!("Failed type coercion, {self:?} is not a trace event"),
        }
    }

    /// Return self as a mutable `TraceEvent`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Trace`.
    pub fn as_mut_trace(&mut self) -> &mut TraceEvent {
        match self {
            Event::Trace(trace) => trace,
            _ => panic!("Failed type coercion, {self:?} is not a trace event"),
        }
    }

    /// Coerces self into a `TraceEvent`
    ///
    /// # Panics
    ///
    /// This function panics if self is anything other than an `Event::Trace`.
    pub fn into_trace(self) -> TraceEvent {
        match self {
            Event::Trace(trace) => trace,
            _ => panic!("Failed type coercion, {self:?} is not a trace event"),
        }
    }

    /// Fallibly coerces self into a `TraceEvent`
    ///
    /// If the event is a `TraceEvent`, then `Some(trace)` is returned, otherwise `None`.
    pub fn try_into_trace(self) -> Option<TraceEvent> {
        match self {
            Event::Trace(trace) => Some(trace),
            _ => None,
        }
    }

    pub fn metadata(&self) -> &EventMetadata {
        match self {
            Self::Log(log) => log.metadata(),
            Self::Metric(metric) => metric.metadata(),
            Self::Trace(trace) => trace.metadata(),
        }
    }

    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        match self {
            Self::Log(log) => log.metadata_mut(),
            Self::Metric(metric) => metric.metadata_mut(),
            Self::Trace(trace) => trace.metadata_mut(),
        }
    }

    /// Destroy the event and return the metadata.
    pub fn into_metadata(self) -> EventMetadata {
        match self {
            Self::Log(log) => log.into_parts().1,
            Self::Metric(metric) => metric.into_parts().2,
            Self::Trace(trace) => trace.into_parts().1,
        }
    }

    #[must_use]
    pub fn with_batch_notifier(self, batch: &BatchNotifier) -> Self {
        match self {
            Self::Log(log) => log.with_batch_notifier(batch).into(),
            Self::Metric(metric) => metric.with_batch_notifier(batch).into(),
            Self::Trace(trace) => trace.with_batch_notifier(batch).into(),
        }
    }

    #[must_use]
    pub fn with_batch_notifier_option(self, batch: &Option<BatchNotifier>) -> Self {
        match self {
            Self::Log(log) => log.with_batch_notifier_option(batch).into(),
            Self::Metric(metric) => metric.with_batch_notifier_option(batch).into(),
            Self::Trace(trace) => trace.with_batch_notifier_option(batch).into(),
        }
    }

    /// Returns a reference to the event metadata source.
    #[must_use]
    pub fn source_id(&self) -> Option<&Arc<ComponentKey>> {
        self.metadata().source_id()
    }

    /// Sets the `source_id` in the event metadata to the provided value.
    pub fn set_source_id(&mut self, source_id: Arc<ComponentKey>) {
        self.metadata_mut().set_source_id(source_id);
    }

    /// Sets the `upstream_id` in the event metadata to the provided value.
    pub fn set_upstream_id(&mut self, upstream_id: Arc<OutputId>) {
        self.metadata_mut().set_upstream_id(upstream_id);
    }

    /// Sets the `source_type` in the event metadata to the provided value.
    pub fn set_source_type(&mut self, source_type: &'static str) {
        self.metadata_mut().set_source_type(source_type);
    }

    /// Sets the `source_id` in the event metadata to the provided value.
    #[must_use]
    pub fn with_source_id(mut self, source_id: Arc<ComponentKey>) -> Self {
        self.metadata_mut().set_source_id(source_id);
        self
    }

    /// Sets the `source_type` in the event metadata to the provided value.
    #[must_use]
    pub fn with_source_type(mut self, source_type: &'static str) -> Self {
        self.metadata_mut().set_source_type(source_type);
        self
    }

    /// Sets the `upstream_id` in the event metadata to the provided value.
    #[must_use]
    pub fn with_upstream_id(mut self, upstream_id: Arc<OutputId>) -> Self {
        self.metadata_mut().set_upstream_id(upstream_id);
        self
    }

    /// Creates an Event from a JSON value.
    ///
    /// # Errors
    /// If a non-object JSON value is passed in with the `Legacy` namespace, this will return an error.
    pub fn from_json_value(
        value: serde_json::Value,
        log_namespace: LogNamespace,
    ) -> crate::Result<Self> {
        match log_namespace {
            LogNamespace::Vector => Ok(LogEvent::from(Value::from(value)).into()),
            LogNamespace::Legacy => match value {
                serde_json::Value::Object(fields) => Ok(LogEvent::from(
                    fields
                        .into_iter()
                        .map(|(k, v)| (k.into(), v.into()))
                        .collect::<ObjectMap>(),
                )
                .into()),
                _ => Err(crate::Error::from(
                    "Attempted to convert non-Object JSON into an Event.",
                )),
            },
        }
    }
}

impl EventDataEq for Event {
    fn event_data_eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Log(a), Self::Log(b)) => a.event_data_eq(b),
            (Self::Metric(a), Self::Metric(b)) => a.event_data_eq(b),
            (Self::Trace(a), Self::Trace(b)) => a.event_data_eq(b),
            _ => false,
        }
    }
}

impl TryInto<serde_json::Value> for Event {
    type Error = serde_json::Error;

    fn try_into(self) -> Result<serde_json::Value, Self::Error> {
        match self {
            Event::Log(fields) => serde_json::to_value(fields),
            Event::Metric(metric) => serde_json::to_value(metric),
            Event::Trace(fields) => serde_json::to_value(fields),
        }
    }
}

impl From<LogEvent> for Event {
    fn from(log: LogEvent) -> Self {
        Event::Log(log)
    }
}

impl From<Metric> for Event {
    fn from(metric: Metric) -> Self {
        Event::Metric(metric)
    }
}

impl From<TraceEvent> for Event {
    fn from(trace: TraceEvent) -> Self {
        Event::Trace(trace)
    }
}

pub trait MaybeAsLogMut {
    fn maybe_as_log_mut(&mut self) -> Option<&mut LogEvent>;
}

impl MaybeAsLogMut for Event {
    fn maybe_as_log_mut(&mut self) -> Option<&mut LogEvent> {
        match self {
            Event::Log(log) => Some(log),
            _ => None,
        }
    }
}
