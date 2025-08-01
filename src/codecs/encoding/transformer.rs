#![deny(missing_docs)]

use chrono::{DateTime, Utc};
use core::fmt::Debug;
use std::collections::BTreeMap;

use ordered_float::NotNan;
use serde::{Deserialize, Deserializer};
use agent_lib::configurable::configurable_component;
use agent_lib::event::{LogEvent, MaybeAsLogMut};
/// ========================ownedValuePath impl ======================
use vrl::{event_path, path::PathPrefix};
use agent_lib::schema::meaning;
use vrl::path::OwnedValuePath;
/// ========================ownedValuePath impl ======================
pub use vrl::path::{PathParseError, ValuePath};
use vrl::value::KeyString;
use vrl::value::Value;
pub use agent_lib::config::is_default;
use crate::{event::Event};
/// ========================ownedValuePath impl ======================
/// A wrapper around `OwnedValuePath` that allows it to be used in Vector config.
/// This requires a valid path to be used. If you want to allow optional paths,
/// use [optional_path::optionalValuePath].
#[configurable_component]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "proptest", derive(proptest_derive::Arbitrary))]
#[serde(try_from = "String", into = "String")]
pub struct ConfigValuePath(pub OwnedValuePath);

impl TryFrom<String> for ConfigValuePath {
    type Error = PathParseError;

    fn try_from(src: String) -> Result<Self, Self::Error> {
        OwnedValuePath::try_from(src).map(ConfigValuePath)
    }
}

impl TryFrom<KeyString> for ConfigValuePath {
    type Error = PathParseError;

    fn try_from(src: KeyString) -> Result<Self, Self::Error> {
        OwnedValuePath::try_from(String::from(src)).map(ConfigValuePath)
    }
}

impl From<ConfigValuePath> for String {
    fn from(owned: ConfigValuePath) -> Self {
        String::from(owned.0)
    }
}

impl<'a> ValuePath<'a> for &'a ConfigValuePath {
    type Iter = <&'a OwnedValuePath as ValuePath<'a>>::Iter;

    fn segment_iter(&self) -> Self::Iter {
        (&self.0).segment_iter()
    }
}

/// Transformations to prepare an event for serialization.
#[configurable_component(no_deser)]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Transformer {
    /// List of fields that are included in the encoded event.
    #[serde(default, skip_serializing_if = "is_default")]
    only_fields: Option<Vec<ConfigValuePath>>,

    /// List of fields that are excluded from the encoded event.
    #[serde(default, skip_serializing_if = "is_default")]
    except_fields: Option<Vec<ConfigValuePath>>,

    /// Format used for timestamp fields.
    #[serde(default, skip_serializing_if = "is_default")]
    timestamp_format: Option<TimestampFormat>,
}
/* transform也要实现反序列化接口?  sink file会走到这里*/
impl<'de> Deserialize<'de> for Transformer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct TransformerInner {
            #[serde(default)]
            only_fields: Option<Vec<OwnedValuePath>>,
            #[serde(default)]
            except_fields: Option<Vec<OwnedValuePath>>,
            #[serde(default)]
            timestamp_format: Option<TimestampFormat>,
        }
        /* 构建一个transformer */
        let inner: TransformerInner = Deserialize::deserialize(deserializer)?;
        Self::new(
            inner
                .only_fields
                .map(|v| v.iter().map(|p| ConfigValuePath(p.clone())).collect()),
            inner
                .except_fields
                .map(|v| v.iter().map(|p| ConfigValuePath(p.clone())).collect()),
            inner.timestamp_format,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl Transformer {
    /// Creates a new `Transformer`.
    ///
    /// Returns `Err` if `only_fields` and `except_fields` fail validation, i.e. are not mutually
    /// exclusive.
    pub fn new(
        only_fields: Option<Vec<ConfigValuePath>>,
        except_fields: Option<Vec<ConfigValuePath>>,
        timestamp_format: Option<TimestampFormat>,
    ) -> Result<Self, crate::Error> {
        Self::validate_fields(only_fields.as_ref(), except_fields.as_ref())?;

        Ok(Self {
            only_fields,
            except_fields,
            timestamp_format,
        })
    }

    /// Get the `Transformer`'s `except_fields`.
    pub const fn except_fields(&self) -> &Option<Vec<ConfigValuePath>> {
        &self.except_fields
    }

    /// Get the `Transformer`'s `timestamp_format`.
    pub const fn timestamp_format(&self) -> &Option<TimestampFormat> {
        &self.timestamp_format
    }

    /// Check if `except_fields` and `only_fields` items are mutually exclusive.
    ///
    /// If an error is returned, the entire encoding configuration should be considered inoperable.
    fn validate_fields(
        only_fields: Option<&Vec<ConfigValuePath>>,
        except_fields: Option<&Vec<ConfigValuePath>>,
    ) -> crate::Result<()> {
        if let (Some(only_fields), Some(except_fields)) = (only_fields, except_fields) {
            if except_fields
                .iter()
                .any(|f| only_fields.iter().any(|v| v == f))
            {
                return Err(
                    "`except_fields` and `only_fields` should be mutually exclusive.".into(),
                );
            }
        }
        Ok(())
    }

    /// Prepare an event for serialization by the given transformation rules.
    pub fn transform(&self, event: &mut Event) {
        // Rules are currently applied to logs only.
        if let Some(log) = event.maybe_as_log_mut() {
            // Ordering in here should not matter.
            self.apply_except_fields(log);
            self.apply_only_fields(log);
            self.apply_timestamp_format(log);
        }
    }

    fn apply_only_fields(&self, log: &mut LogEvent) {
        if let Some(only_fields) = self.only_fields.as_ref() {
            let mut old_value = std::mem::replace(log.value_mut(), Value::Object(BTreeMap::new()));

            for field in only_fields {
                if let Some(value) = old_value.remove(field, true) {
                    log.insert((PathPrefix::Event, field), value);
                }
            }

            // We may need the service field to apply tags to emitted metrics after the log message has been pruned. If there
            // is a service meaning, we move this value to `dropped_fields` in the metadata.
            // If the field is still in the new log message after pruning it will have been removed from `old_value` above.
            let service_path = log
                .metadata()
                .schema_definition()
                .meaning_path(meaning::SERVICE);
            if let Some(service_path) = service_path {
                let mut new_log = LogEvent::from(old_value);
                if let Some(service) = new_log.remove(service_path) {
                    log.metadata_mut()
                        .add_dropped_field(meaning::SERVICE.into(), service);
                }
            }
        }
    }

    fn apply_except_fields(&self, log: &mut LogEvent) {
        if let Some(except_fields) = self.except_fields.as_ref() {
            for field in except_fields {
                let value_path = &field.0;
                let value = log.remove((PathPrefix::Event, value_path));

                let service_path = log
                    .metadata()
                    .schema_definition()
                    .meaning_path(meaning::SERVICE);
                // If we are removing the service field we need to store this in a `dropped_fields` list as we may need to
                // refer to this later when emitting metrics.
                if let (Some(v), Some(service_path)) = (value, service_path) {
                    if service_path.path == *value_path {
                        log.metadata_mut()
                            .add_dropped_field(meaning::SERVICE.into(), v);
                    }
                }
            }
        }
    }

    fn format_timestamps<F, T>(&self, log: &mut LogEvent, extract: F)
    where
        F: Fn(&DateTime<Utc>) -> T,
        T: Into<Value>,
    {
        if log.value().is_object() {
            let mut unix_timestamps = Vec::new();
            for (k, v) in log.all_event_fields().expect("must be an object") {
                if let Value::Timestamp(ts) = v {
                    unix_timestamps.push((k.clone(), extract(ts).into()));
                }
            }
            for (k, v) in unix_timestamps {
                log.parse_path_and_insert(k, v).unwrap();
            }
        } else {
            // root is not an object
            let timestamp = if let Value::Timestamp(ts) = log.value() {
                Some(extract(ts))
            } else {
                None
            };
            if let Some(ts) = timestamp {
                log.insert(event_path!(), ts.into());
            }
        }
    }

    fn apply_timestamp_format(&self, log: &mut LogEvent) {
        if let Some(timestamp_format) = self.timestamp_format.as_ref() {
            match timestamp_format {
                TimestampFormat::Unix => self.format_timestamps(log, |ts| ts.timestamp()),
                TimestampFormat::UnixMs => self.format_timestamps(log, |ts| ts.timestamp_millis()),
                TimestampFormat::UnixUs => self.format_timestamps(log, |ts| ts.timestamp_micros()),
                TimestampFormat::UnixNs => self.format_timestamps(log, |ts| {
                    ts.timestamp_nanos_opt().expect("Timestamp out of range")
                }),
                TimestampFormat::UnixFloat => self.format_timestamps(log, |ts| {
                    NotNan::new(ts.timestamp_micros() as f64 / 1e6).unwrap()
                }),
                // RFC3339 is the default serialization of a timestamp.
                TimestampFormat::Rfc3339 => (),
            }
        }
    }
}

#[configurable_component]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
/// The format in which a timestamp should be represented.
pub enum TimestampFormat {
    /// Represent the timestamp as a Unix timestamp.
    Unix,

    /// Represent the timestamp as a RFC 3339 timestamp.
    Rfc3339,

    /// Represent the timestamp as a Unix timestamp in milliseconds.
    UnixMs,

    /// Represent the timestamp as a Unix timestamp in microseconds
    UnixUs,

    /// Represent the timestamp as a Unix timestamp in nanoseconds.
    UnixNs,

    /// Represent the timestamp as a Unix timestamp in floating point.
    UnixFloat,
}
