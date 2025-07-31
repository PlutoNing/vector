use std::sync::Arc;
use std::{collections::HashMap, fmt};

use bitmask_enum::bitmask;
use bytes::Bytes;
use chrono::{DateTime, Utc};

mod log_schema;
pub mod metrics_expiration;
pub mod output_id;
pub mod proxy;

use crate::event::LogEvent;
pub use log_schema::{init_log_schema, log_schema, LogSchema};
use vrl::path;
use vrl::path::{ValuePath,PathPrefix};
pub use output_id::OutputId;
use serde::{Deserialize, Serialize};
pub use agent_common::config::ComponentKey;
use vector_config::configurable_component;
use vrl::value::Value;

use crate::schema;

// This enum should be kept alphabetically sorted as the bitmask value is used when
// sorting sources by data type in the GraphQL API.
#[bitmask(u8)]
#[bitmask_config(flags_iter)]
pub enum DataType {
    Log,
    Metric,
    Trace,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list()
            .entries(
                Self::flags().filter_map(|&(name, value)| self.contains(value).then_some(name)),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Input {
    ty: DataType,
    log_schema_requirement: schema::Requirement,
}

impl Input {/* 调用 */
    pub fn data_type(&self) -> DataType {
        self.ty
    }
/* 调用 */
    pub fn schema_requirement(&self) -> &schema::Requirement {
        &self.log_schema_requirement
    }
/* 调用 */
    pub fn new(ty: DataType) -> Self {
        Self {
            ty,
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    pub fn log() -> Self {
        Self {
            ty: DataType::Log,
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    pub fn metric() -> Self {
        Self {
            ty: DataType::Metric,
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    pub fn trace() -> Self {
        Self {
            ty: DataType::Trace,
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    pub fn all() -> Self {
        Self {
            ty: DataType::all_bits(),
            log_schema_requirement: schema::Requirement::empty(),
        }
    }

    /// Set the schema requirement for this output.
    #[must_use]
    pub fn with_schema_requirement(mut self, schema_requirement: schema::Requirement) -> Self {
        self.log_schema_requirement = schema_requirement;
        self
    }
}
/* 表示什么 */
#[derive(Debug, Clone, PartialEq)]
pub struct SourceOutput {
    pub port: Option<String>,
    pub ty: DataType,

    // NOTE: schema definitions are only implemented/supported for log-type events. There is no
    // inherent blocker to support other types as well, but it'll require additional work to add
    // the relevant schemas, and store them separately in this type.
    pub schema_definition: Option<Arc<schema::Definition>>,
}

impl SourceOutput {
    /// Create a `SourceOutput` of the given data type that contains a single output `Definition`.
    /// If the data type does not contain logs, the schema definition will be ignored.
    /// Designed for use in log sources.
    #[must_use]
    pub fn new_maybe_logs(ty: DataType, schema_definition: schema::Definition) -> Self {
        let schema_definition = ty
            .contains(DataType::Log)
            .then(|| Arc::new(schema_definition));

        Self {
            port: None,
            ty,
            schema_definition,
        }
    }

    /// Create a `SourceOutput` of the given data type that contains no output `Definition`s.
    /// Designed for use in metrics sources.
    ///
    /// Sets the datatype to be [`DataType::Metric`].
    #[must_use]
    pub fn new_metrics() -> Self {/* 调用 */
        Self {
            port: None,
            ty: DataType::Metric,
            schema_definition: None,
        }
    }

    /// Create a `SourceOutput` of the given data type that contains no output `Definition`s.
    /// Designed for use in trace sources.
    ///
    /// Sets the datatype to be [`DataType::Trace`].
    #[must_use]
    pub fn new_traces() -> Self {
        Self {
            port: None,
            ty: DataType::Trace,
            schema_definition: None,
        }
    }

    /// Return the schema [`schema::Definition`] from this output.
    ///
    /// Takes a `schema_enabled` flag to determine if the full definition including the fields
    /// and associated types should be returned, or if a simple definition should be returned.
    /// A simple definition is just the default for the namespace. For the Vector namespace the
    /// meanings are included.
    /// Schema enabled is set in the users configuration.
    #[must_use]
    pub fn schema_definition(&self, schema_enabled: bool) -> Option<schema::Definition> {
        use std::ops::Deref;
/* 调用 */
        self.schema_definition.as_ref().map(|definition| {
            if schema_enabled {
                definition.deref().clone()
            } else {
                let mut new_definition =
                    schema::Definition::default_for_namespace(definition.log_namespaces());
                new_definition.add_meanings(definition.meanings());
                new_definition
            }
        })
    }
}

impl SourceOutput {
    /// Set the port name for this `SourceOutput`.
    #[must_use]
    pub fn with_port(mut self, name: impl Into<String>) -> Self {
        self.port = Some(name.into());
        self
    }
}

fn fmt_helper(
    f: &mut fmt::Formatter<'_>,
    maybe_port: Option<&String>,
    data_type: DataType,
) -> fmt::Result {
    match maybe_port {
        Some(port) => write!(f, "port: \"{port}\",",),
        None => write!(f, "port: None,"),
    }?;
    write!(f, " types: {data_type}")
}

impl fmt::Display for SourceOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_helper(f, self.port.as_ref(), self.ty)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct TransformOutput {
    pub port: Option<String>,
    pub ty: DataType,

    /// For *transforms* if `Datatype` is [`DataType::Log`], if schema is
    /// enabled, at least one definition  should be output. If the transform
    /// has multiple connected sources, it is possible to have multiple output
    /// definitions - one for each input.
    pub log_schema_definitions: HashMap<OutputId, schema::Definition>,
}

impl fmt::Display for TransformOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_helper(f, self.port.as_ref(), self.ty)
    }
}

impl TransformOutput {
    /// Create a `TransformOutput` of the given data type that contains multiple [`schema::Definition`]s.
    /// Designed for use in transforms.
    #[must_use]
    pub fn new(ty: DataType, schema_definitions: HashMap<OutputId, schema::Definition>) -> Self {
        Self {
            port: None,
            ty,
            log_schema_definitions: schema_definitions,
        }
    }

    /// Set the port name for this `Output`.
    #[must_use]
    pub fn with_port(mut self, name: impl Into<String>) -> Self {
        self.port = Some(name.into());
        self
    }

    /// Return the schema [`schema::Definition`] from this output.
    ///
    /// Takes a `schema_enabled` flag to determine if the full definition including the fields
    /// and associated types should be returned, or if a simple definition should be returned.
    /// A simple definition is just the default for the namespace. For the Vector namespace the
    /// meanings are included.
    /// Schema enabled is set in the users configuration.
    #[must_use]
    pub fn schema_definitions(
        &self,
        schema_enabled: bool,
    ) -> HashMap<OutputId, schema::Definition> {
        if schema_enabled {
            self.log_schema_definitions.clone()
        } else {
            self.log_schema_definitions
                .iter()
                .map(|(output, definition)| {
                    let mut new_definition =
                        schema::Definition::default_for_namespace(definition.log_namespaces());
                    new_definition.add_meanings(definition.meanings());
                    (output.clone(), new_definition)
                })
                .collect()
        }
    }
}

/// Simple utility function that can be used by transforms that make no changes to
/// the schema definitions of events.
/// Takes a list of definitions with [`OutputId`] returns them as a [`HashMap`].
pub fn clone_input_definitions(
    input_definitions: &[(OutputId, schema::Definition)],
) -> HashMap<OutputId, schema::Definition> {
    input_definitions
        .iter()
        .map(|(output, definition)| (output.clone(), definition.clone()))
        .collect()
}

#[derive(Debug, Copy, Clone, PartialEq, Serialize, Deserialize, PartialOrd, Ord, Eq)]
pub enum LogNamespace {
    /// Vector native namespacing
    ///
    /// Deserialized data is placed in the root of the event.
    /// Extra data is placed in "event metadata"
    Vector,

    /// This is the legacy namespacing.
    ///
    /// All data is set in the root of the event. Since this can lead
    /// to collisions, deserialized data has priority over metadata
    Legacy,
}

/// The user-facing config for log namespace is a bool (enabling or disabling the "Log Namespacing" feature).
/// Internally, this is converted to a enum.
impl From<bool> for LogNamespace {
    fn from(x: bool) -> Self {
        if x {
            LogNamespace::Vector
        } else {
            LogNamespace::Legacy
        }
    }
}

impl Default for LogNamespace {
    fn default() -> Self {
        Self::Legacy
    }
}

/// A shortcut to specify no `LegacyKey` should be used (since otherwise a turbofish would be required)
pub const NO_LEGACY_KEY: Option<LegacyKey<&'static str>> = None;

pub enum LegacyKey<T> {
    /// Always insert the data, even if the field previously existed
    Overwrite(T),
    /// Only insert the data if the field is currently empty
    InsertIfEmpty(T),
}

impl LogNamespace {
    /// Vector: This is added to "event metadata", nested under the source name.
    ///
    /// Legacy: This is stored on the event root, only if a field with that name doesn't already exist.
    pub fn insert_source_metadata<'a>(
        &self,
        source_name: &'a str,
        log: &mut LogEvent,
        legacy_key: Option<LegacyKey<impl ValuePath<'a>>>,
        metadata_key: impl ValuePath<'a>,
        value: impl Into<Value>,
    ) {
        match self {
            LogNamespace::Vector => {
                log.metadata_mut()
                    .value_mut()
                    .insert(path!(source_name).concat(metadata_key), value);
            }
            LogNamespace::Legacy => match legacy_key {
                None => { /* don't insert */ }
                Some(LegacyKey::Overwrite(key)) => {
                    log.insert((PathPrefix::Event, key), value);
                }
                Some(LegacyKey::InsertIfEmpty(key)) => {
                    log.try_insert((PathPrefix::Event, key), value);
                }
            },
        }
    }

    /// Vector: This is retrieved from the "event metadata", nested under the source name.
    ///
    /// Legacy: This is retrieved from the event.
    pub fn get_source_metadata<'a, 'b>(
        &self,
        source_name: &'a str,
        log: &'b LogEvent,
        legacy_key: impl ValuePath<'a>,
        metadata_key: impl ValuePath<'a>,
    ) -> Option<&'b Value> {
        match self {
            LogNamespace::Vector => log
                .metadata()
                .value()
                .get(path!(source_name).concat(metadata_key)),
            LogNamespace::Legacy => log.get((PathPrefix::Event, legacy_key)),
        }
    }

    /// Vector: The `ingest_timestamp`, and `source_type` fields are added to "event metadata", nested
    /// under the name "vector". This data will be marked as read-only in VRL.
    ///
    /// Legacy: The values of `source_type_key`, and `timestamp_key` are stored as keys on the event root,
    /// only if a field with that name doesn't already exist.
    pub fn insert_standard_vector_source_metadata(
        &self,
        log: &mut LogEvent,
        source_name: &'static str,
        now: DateTime<Utc>,
    ) {
        self.insert_vector_metadata(
            log,
            log_schema().source_type_key(),
            path!("source_type"),
            Bytes::from_static(source_name.as_bytes()),
        );
        self.insert_vector_metadata(
            log,
            log_schema().timestamp_key(),
            path!("ingest_timestamp"),
            now,
        );
    }

    /// Vector: This is added to the "event metadata", nested under the name "vector". This data
    /// will be marked as read-only in VRL.
    ///
    /// Legacy: This is stored on the event root, only if a field with that name doesn't already exist.
    pub fn insert_vector_metadata<'a>(
        &self,
        log: &mut LogEvent,
        legacy_key: Option<impl ValuePath<'a>>,
        metadata_key: impl ValuePath<'a>,
        value: impl Into<Value>,
    ) {
        match self {
            LogNamespace::Vector => {
                log.metadata_mut()
                    .value_mut()
                    .insert(path!("vector").concat(metadata_key), value);
            }
            LogNamespace::Legacy => {
                if let Some(legacy_key) = legacy_key {
                    log.try_insert((PathPrefix::Event, legacy_key), value);
                }
            }
        }
    }

    /// Vector: This is retrieved from the "event metadata", nested under the name "vector".
    ///
    /// Legacy: This is retrieved from the event.
    pub fn get_vector_metadata<'a, 'b>(
        &self,
        log: &'b LogEvent,
        legacy_key: impl ValuePath<'a>,
        metadata_key: impl ValuePath<'a>,
    ) -> Option<&'b Value> {
        match self {
            LogNamespace::Vector => log
                .metadata()
                .value()
                .get(path!("vector").concat(metadata_key)),
            LogNamespace::Legacy => log.get((PathPrefix::Event, legacy_key)),
        }
    }

    pub fn new_log_from_data(&self, value: impl Into<Value>) -> LogEvent {
        match self {
            LogNamespace::Vector | LogNamespace::Legacy => LogEvent::from(value.into()),
        }
    }

    // combine a global (self) and local value to get the actual namespace
    #[must_use]
    pub fn merge(&self, override_value: Option<impl Into<LogNamespace>>) -> LogNamespace {
        override_value.map_or(*self, Into::into)
    }
}