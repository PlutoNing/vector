use std::fmt::Debug;

use crate::buffer::EventCount;
use serde::{Deserialize, Serialize};
use vrl::path::PathParseError;
use vrl::path::TargetPath;

use super::{
    EventMetadata,
    LogEvent, ObjectMap, Value,
};

/// Traces are a newtype of `LogEvent`
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct TraceEvent(LogEvent);

impl TraceEvent {
    /// Convert a `TraceEvent` into a tuple of its components
    /// # Panics
    ///
    /// Panics if the fields of the `TraceEvent` are not a `Value::Map`.
    pub fn into_parts(self) -> (ObjectMap, EventMetadata) {
        let (value, metadata) = self.0.into_parts();
        let map = value.into_object().expect("inner value must be a map");
        (map, metadata)
    }

    pub fn from_parts(fields: ObjectMap, metadata: EventMetadata) -> Self {
        Self(LogEvent::from_map(fields, metadata))
    }

    pub fn value(&self) -> &Value {
        self.0.value()
    }

    pub fn value_mut(&mut self) -> &mut Value {
        self.0.value_mut()
    }

    pub fn metadata(&self) -> &EventMetadata {
        self.0.metadata()
    }

    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        self.0.metadata_mut()
    }

    /// Convert a `TraceEvent` into an `ObjectMap` of it's fields
    /// # Panics
    ///
    /// Panics if the fields of the `TraceEvent` are not a `Value::Map`.
    pub fn as_map(&self) -> &ObjectMap {
        self.0.as_map().expect("inner value must be a map")
    }

    /// Parse the specified `path` and if there are no parsing errors, attempt to get a reference to a value.
    /// # Errors
    /// Will return an error if path parsing failed.
    pub fn parse_path_and_get_value(
        &self,
        path: impl AsRef<str>,
    ) -> Result<Option<&Value>, PathParseError> {
        self.0.parse_path_and_get_value(path)
    }

    #[allow(clippy::needless_pass_by_value)] // TargetPath is always a reference
    pub fn get<'a>(&self, key: impl TargetPath<'a>) -> Option<&Value> {
        self.0.get(key)
    }

    pub fn get_mut<'a>(&mut self, key: impl TargetPath<'a>) -> Option<&mut Value> {
        self.0.get_mut(key)
    }

    pub fn contains<'a>(&self, key: impl TargetPath<'a>) -> bool {
        self.0.contains(key)
    }

    pub fn insert<'a>(
        &mut self,
        key: impl TargetPath<'a>,
        value: impl Into<Value> + Debug,
    ) -> Option<Value> {
        self.0.insert(key, value.into())
    }

    pub fn maybe_insert<'a, F: FnOnce() -> Value>(
        &mut self,
        path: Option<impl TargetPath<'a>>,
        value_callback: F,
    ) -> Option<Value> {
        if let Some(path) = path {
            return self.0.insert(path, value_callback());
        }
        None
    }

    pub fn remove<'a>(&mut self, key: impl TargetPath<'a>) -> Option<Value> {
        self.0.remove(key)
    }
}

impl From<LogEvent> for TraceEvent {
    fn from(log: LogEvent) -> Self {
        Self(log)
    }
}

impl From<ObjectMap> for TraceEvent {
    fn from(map: ObjectMap) -> Self {
        Self(map.into())
    }
}

impl EventCount for TraceEvent {
    fn event_count(&self) -> usize {
        1
    }
}

impl AsRef<LogEvent> for TraceEvent {
    fn as_ref(&self) -> &LogEvent {
        &self.0
    }
}

impl AsMut<LogEvent> for TraceEvent {
    fn as_mut(&mut self) -> &mut LogEvent {
        &mut self.0
    }
}
