use std::collections::{BTreeMap, BTreeSet};

use vrl::owned_value_path;
use vrl::path::TargetPath;
use vrl::path::{OwnedTargetPath, OwnedValuePath, PathPrefix};
use vrl::value::{kind::Collection, Kind};

use crate::config::{log_schema, LegacyKey, LogNamespace};

/// The definition of a schema.
///
/// This struct contains all the information needed to inspect the schema of an event emitted by
/// a source/transform.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Definition {
    /// The type of the event
    event_kind: Kind,

    /// The type of the metadata.
    metadata_kind: Kind,

    /// Semantic meaning assigned to fields within the collection.
    ///
    /// The value within this map points to a path inside the `event_kind`.
    /// Meanings currently can't point to metadata.
    meaning: BTreeMap<String, MeaningPointer>,

    /// Type definitions of components can change depending on the log namespace chosen.
    /// This records which ones are possible.
    /// An empty set means the definition can't be for a log
    log_namespaces: BTreeSet<LogNamespace>,
}

/// In regular use, a semantic meaning points to exactly _one_ location in the collection. However,
/// when merging two [`Definition`]s, we need to be able to allow for two definitions with the same
/// semantic meaning identifier to be merged together.
///
/// We cannot error when this happens, because a follow-up component (such as the `remap`
/// transform) might rectify the issue of having a semantic meaning with multiple pointers.
///
/// Because of this, we encapsulate this state in an enum. The schema validation step done by the
/// sink builder, will return an error if the definition stores an "invalid" meaning pointer.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
enum MeaningPointer {
    Valid(OwnedTargetPath),
    Invalid(BTreeSet<OwnedTargetPath>),
}

impl MeaningPointer {
    fn merge(self, other: Self) -> Self {
        let set = match (self, other) {
            (Self::Valid(lhs), Self::Valid(rhs)) if lhs == rhs => return Self::Valid(lhs),
            (Self::Valid(lhs), Self::Valid(rhs)) => BTreeSet::from([lhs, rhs]),
            (Self::Valid(lhs), Self::Invalid(mut rhs)) => {
                rhs.insert(lhs);
                rhs
            }
            (Self::Invalid(mut lhs), Self::Valid(rhs)) => {
                lhs.insert(rhs);
                lhs
            }
            (Self::Invalid(mut lhs), Self::Invalid(rhs)) => {
                lhs.extend(rhs);
                lhs
            }
        };

        Self::Invalid(set)
    }
}

impl Definition {
    /// The most general possible definition. The `Kind` is `any`, and all `log_namespaces` are enabled.
    pub fn any() -> Self {
        Self {
            event_kind: Kind::any(),
            metadata_kind: Kind::any(),
            meaning: BTreeMap::default(),
            log_namespaces: [LogNamespace::Legacy, LogNamespace::Vector].into(),
        }
    }

    /// Creates a new definition that is of the event kind specified, and an empty object for metadata.
    /// There are no meanings.
    /// The `log_namespaces` are used to list the possible namespaces the schema is for.
    pub fn new_with_default_metadata(
        event_kind: Kind,
        log_namespaces: impl Into<BTreeSet<LogNamespace>>,
    ) -> Self {
        Self {
            event_kind,
            metadata_kind: Kind::object(Collection::any()),
            meaning: BTreeMap::default(),
            log_namespaces: log_namespaces.into(),
        }
    }

    /// Creates a new definition, specifying both the event and metadata kind.
    /// There are no meanings.
    /// The `log_namespaces` are used to list the possible namespaces the schema is for.
    pub fn new(
        event_kind: Kind,
        metadata_kind: Kind,
        log_namespaces: impl Into<BTreeSet<LogNamespace>>,
    ) -> Self {
        Self {
            event_kind,
            metadata_kind,
            meaning: BTreeMap::default(),
            log_namespaces: log_namespaces.into(),
        }
    }

    /// An object with any fields, and the `Legacy` namespace.
    /// This is the default schema for a source that does not explicitly provide one yet.
    pub fn default_legacy_namespace() -> Self {
        Self::new_with_default_metadata(Kind::any_object(), [LogNamespace::Legacy])
    }

    /// An object with no fields, and the `Legacy` namespace.
    /// This is what most sources use for the legacy namespace.
    pub fn empty_legacy_namespace() -> Self {
        Self::new_with_default_metadata(Kind::object(Collection::empty()), [LogNamespace::Legacy])
    }

    /// Returns the source schema for a source that produce the listed log namespaces,
    /// but an explicit schema was not provided.
    pub fn default_for_namespace(log_namespaces: &BTreeSet<LogNamespace>) -> Self {
        let is_legacy = log_namespaces.contains(&LogNamespace::Legacy);
        let is_vector = log_namespaces.contains(&LogNamespace::Vector);
        match (is_legacy, is_vector) {
            (false, false) => Self::new_with_default_metadata(Kind::any(), []),
            (true, false) => Self::default_legacy_namespace(),
            (false, true) => Self::new_with_default_metadata(Kind::any(), [LogNamespace::Vector]),
            (true, true) => Self::any(),
        }
    }

    /// The set of possible log namespaces that events can use. When merged, this is the union of all inputs.
    pub fn log_namespaces(&self) -> &BTreeSet<LogNamespace> {
        &self.log_namespaces
    }

    /// Adds the `source_type` and `ingest_timestamp` metadata fields, which are added to every Vector source.
    /// This function should be called in the same order as the values are actually inserted into the event.
    #[must_use]
    pub fn with_standard_vector_source_metadata(self) -> Self {
        self.with_vector_metadata(
            log_schema().source_type_key(),
            &owned_value_path!("source_type"),
            Kind::bytes(),
            None,
        )
        .with_vector_metadata(
            log_schema().timestamp_key(),
            &owned_value_path!("ingest_timestamp"),
            Kind::timestamp(),
            None,
        )
    }

    /// This should be used wherever `LogNamespace::insert_source_metadata` is used to insert metadata.
    /// This automatically detects which log namespaces are used, and also automatically
    /// determines if there are possible conflicts from existing field names (usually from the selected decoder).
    /// This function should be called in the same order as the values are actually inserted into the event.
    #[must_use]
    pub fn with_source_metadata(
        self,
        source_name: &str,
        legacy_path: Option<LegacyKey<OwnedValuePath>>,
        vector_path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        self.with_namespaced_metadata(source_name, legacy_path, vector_path, kind, meaning)
    }

    /// This should be used wherever `LogNamespace::insert_vector_metadata` is used to insert metadata.
    /// This automatically detects which log namespaces are used, and also automatically
    /// determines if there are possible conflicts from existing field names (usually from the selected decoder).
    /// This function should be called in the same order as the values are actually inserted into the event.
    #[must_use]
    pub fn with_vector_metadata(
        self,
        legacy_path: Option<&OwnedValuePath>,
        vector_path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        self.with_namespaced_metadata(
            "vector",
            legacy_path.cloned().map(LegacyKey::InsertIfEmpty),
            vector_path,
            kind,
            meaning,
        )
    }

    /// This generalizes the `LogNamespace::insert_*` methods for type definitions.
    /// This assumes the legacy key is either guaranteed to not collide or is inserted with `try_insert`.
    fn with_namespaced_metadata(
        self,
        prefix: &str,
        legacy_path: Option<LegacyKey<OwnedValuePath>>,
        vector_path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        let legacy_definition = legacy_path.and_then(|legacy_path| {
            if self.log_namespaces.contains(&LogNamespace::Legacy) {
                match legacy_path {
                    LegacyKey::InsertIfEmpty(legacy_path) => Some(self.clone().try_with_field(
                        &legacy_path,
                        kind.clone(),
                        meaning,
                    )),
                    LegacyKey::Overwrite(legacy_path) => Some(self.clone().with_event_field(
                        &legacy_path,
                        kind.clone(),
                        meaning,
                    )),
                }
            } else {
                None
            }
        });

        let vector_definition = if self.log_namespaces.contains(&LogNamespace::Vector) {
            Some(self.clone().with_metadata_field(
                &vector_path.with_field_prefix(prefix),
                kind,
                meaning,
            ))
        } else {
            None
        };

        match (legacy_definition, vector_definition) {
            (Some(a), Some(b)) => a.merge(b),
            (Some(x), _) | (_, Some(x)) => x,
            (None, None) => self,
        }
    }

    /// Add type information for an event or metadata field.
    /// A non-root required field means the root type must be an object, so the type will be automatically
    /// restricted to an object.
    ///
    /// # Panics
    /// - If the path is not root, and the definition does not allow the type to be an object.
    #[must_use]
    pub fn with_field(
        self,
        target_path: &OwnedTargetPath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        match target_path.prefix {
            PathPrefix::Event => self.with_event_field(&target_path.path, kind, meaning),
            PathPrefix::Metadata => self.with_metadata_field(&target_path.path, kind, meaning),
        }
    }

    /// Add type information for an event field.
    /// A non-root required field means the root type must be an object, so the type will be automatically
    /// restricted to an object.
    ///
    /// # Panics
    /// - If the path is not root, and the definition does not allow the type to be an object.
    /// - Provided path has one or more coalesced segments (e.g. `.(foo | bar)`).
    #[must_use]
    pub fn with_event_field(
        mut self,
        path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        if !path.is_root() {
            assert!(
                self.event_kind.as_object().is_some(),
                "Setting a field on a value that cannot be an object"
            );
        }

        self.event_kind.set_at_path(path, kind);

        if let Some(meaning) = meaning {
            self.meaning.insert(
                meaning.to_owned(),
                MeaningPointer::Valid(OwnedTargetPath::event(path.clone())),
            );
        }

        self
    }

    /// Add type information for an event field.
    /// This inserts type information similar to `LogEvent::try_insert`.
    #[must_use]
    pub fn try_with_field(
        mut self,
        path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        let existing_type = self.event_kind.at_path(path);

        if existing_type.is_undefined() {
            // Guaranteed to never be set, so the insertion will always succeed.
            self.with_event_field(path, kind, meaning)
        } else if !existing_type.contains_undefined() {
            // Guaranteed to always be set (or is never), so the insertion will always fail.
            self
        } else {
            // Not sure if the insertion will be successful. The type definition should contain both
            // possibilities. The meaning is not set, since it can't be relied on.

            let success_definition = self.clone().with_event_field(path, kind, None);
            // If the existing type contains `undefined`, the new type will always be used, so remove it.
            self.event_kind
                .set_at_path(path, existing_type.without_undefined());
            self.merge(success_definition)
        }
    }

    /// Add type information for an event field.
    /// A non-root required field means the root type must be an object, so the type will be automatically
    /// restricted to an object.
    ///
    /// # Panics
    /// - If the path is not root, and the definition does not allow the type to be an object
    /// - Provided path has one or more coalesced segments (e.g. `.(foo | bar)`).
    #[must_use]
    pub fn with_metadata_field(
        mut self,
        path: &OwnedValuePath,
        kind: Kind,
        meaning: Option<&str>,
    ) -> Self {
        if !path.is_root() {
            assert!(
                self.metadata_kind.as_object().is_some(),
                "Setting a field on a value that cannot be an object"
            );
        }

        self.metadata_kind.set_at_path(path, kind);

        if let Some(meaning) = meaning {
            self.meaning.insert(
                meaning.to_owned(),
                MeaningPointer::Valid(OwnedTargetPath::metadata(path.clone())),
            );
        }

        self
    }

    /// Add type information for an optional event field.
    ///
    /// # Panics
    ///
    /// See `Definition::require_field`.
    #[must_use]
    pub fn optional_field(self, path: &OwnedValuePath, kind: Kind, meaning: Option<&str>) -> Self {
        self.with_event_field(path, kind.or_undefined(), meaning)
    }

    /// Register a semantic meaning for the definition.
    ///
    /// # Panics
    ///
    /// This method panics if the provided path points to an unknown location in the collection.
    #[must_use]
    pub fn with_meaning(mut self, target_path: OwnedTargetPath, meaning: &str) -> Self {
        self.add_meaning(target_path, meaning);
        self
    }

    /// Adds the meaning pointing to the given path to our list of meanings.
    ///
    /// # Panics
    ///
    /// This method panics if the provided path points to an unknown location in the collection.
    pub fn add_meaning(&mut self, target_path: OwnedTargetPath, meaning: &str) {
        self.try_with_meaning(target_path, meaning)
            .unwrap_or_else(|err| panic!("{}", err));
    }

    /// Register a semantic meaning for the definition.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided path points to an unknown location in the collection.
    pub fn try_with_meaning(
        &mut self,
        target_path: OwnedTargetPath,
        meaning: &str,
    ) -> Result<(), &'static str> {
        match target_path.prefix {
            PathPrefix::Event
                if !self
                    .event_kind
                    .at_path(&target_path.path)
                    .contains_any_defined() =>
            {
                Err("meaning must point to a valid path")
            }

            PathPrefix::Metadata
                if !self
                    .metadata_kind
                    .at_path(&target_path.path)
                    .contains_any_defined() =>
            {
                Err("meaning must point to a valid path")
            }

            _ => {
                self.meaning
                    .insert(meaning.to_owned(), MeaningPointer::Valid(target_path));
                Ok(())
            }
        }
    }

    /// Set the kind for all unknown fields.
    #[must_use]
    pub fn unknown_fields(mut self, unknown: impl Into<Kind>) -> Self {
        let unknown = unknown.into();
        if let Some(object) = self.event_kind.as_object_mut() {
            object.set_unknown(unknown.clone());
        }
        if let Some(array) = self.event_kind.as_array_mut() {
            array.set_unknown(unknown);
        }
        self
    }

    /// Merge `other` definition into `self`.
    ///
    /// This just takes the union of both definitions.
    #[must_use]
    pub fn merge(mut self, mut other: Self) -> Self {
        for (other_id, other_meaning) in other.meaning {
            let meaning = match self.meaning.remove(&other_id) {
                Some(this_meaning) => this_meaning.merge(other_meaning),
                None => other_meaning,
            };

            self.meaning.insert(other_id, meaning);
        }

        self.event_kind = self.event_kind.union(other.event_kind);
        self.metadata_kind = self.metadata_kind.union(other.metadata_kind);
        self.log_namespaces.append(&mut other.log_namespaces);
        self
    }

    /// If the schema definition depends on the `LogNamespace`, this combines the individual
    /// definitions for each `LogNamespace`.
    pub fn combine_log_namespaces(
        log_namespaces: &BTreeSet<LogNamespace>,
        legacy: Self,
        vector: Self,
    ) -> Self {
        let mut combined =
            Definition::new_with_default_metadata(Kind::never(), log_namespaces.clone());

        if log_namespaces.contains(&LogNamespace::Legacy) {
            combined = combined.merge(legacy);
        }
        if log_namespaces.contains(&LogNamespace::Vector) {
            combined = combined.merge(vector);
        }
        combined
    }

    /// Returns an `OwnedTargetPath` into an event, based on the provided `meaning`, if the meaning exists.
    pub fn meaning_path(&self, meaning: &str) -> Option<&OwnedTargetPath> {
        match self.meaning.get(meaning) {
            Some(MeaningPointer::Valid(path)) => Some(path),
            None | Some(MeaningPointer::Invalid(_)) => None,
        }
    }

    pub fn invalid_meaning(&self, meaning: &str) -> Option<&BTreeSet<OwnedTargetPath>> {
        match &self.meaning.get(meaning) {
            Some(MeaningPointer::Invalid(paths)) => Some(paths),
            None | Some(MeaningPointer::Valid(_)) => None,
        }
    }

    pub fn meanings(&self) -> impl Iterator<Item = (&String, &OwnedTargetPath)> {
        self.meaning
            .iter()
            .filter_map(|(id, pointer)| match pointer {
                MeaningPointer::Valid(path) => Some((id, path)),
                MeaningPointer::Invalid(_) => None,
            })
    }

    /// Adds the meanings provided by an iterator over the given meanings.
    ///
    /// # Panics
    ///
    /// This method panics if the provided path from any of the incoming meanings point to
    /// an unknown location in the collection.
    pub fn add_meanings<'a>(
        &'a mut self,
        meanings: impl Iterator<Item = (&'a String, &'a OwnedTargetPath)>,
    ) {
        for (meaning, path) in meanings {
            self.add_meaning(path.clone(), meaning);
        }
    }

    pub fn event_kind(&self) -> &Kind {
        &self.event_kind
    }

    pub fn event_kind_mut(&mut self) -> &mut Kind {
        &mut self.event_kind
    }

    pub fn metadata_kind(&self) -> &Kind {
        &self.metadata_kind
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn kind_at<'a>(&self, target_path: impl TargetPath<'a>) -> Kind {
        match target_path.prefix() {
            PathPrefix::Event => self.event_kind.at_path(target_path.value_path()),
            PathPrefix::Metadata => self.metadata_kind.at_path(target_path.value_path()),
        }
    }
}
