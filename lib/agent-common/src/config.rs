use std::{
    cmp::{Ord, Ordering, PartialOrd},
    fmt,
};

use agent_config::{configurable_component, ConfigurableString};

/// Component identifier.
#[configurable_component(no_deser, no_ser)]
#[derive(::serde::Deserialize, ::serde::Serialize)]
#[serde(from = "String", into = "String")]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ComponentKey {
    /// Component ID.
    id: String,
}

impl ComponentKey {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub fn join<D: fmt::Display>(&self, name: D) -> Self {
        Self {
            // ports and inner component use the same naming convention
            id: self.port(name),
        }
    }

    pub fn port<D: fmt::Display>(&self, name: D) -> String {
        format!("{}.{name}", self.id)
    }

    #[must_use]
    pub fn into_id(self) -> String {
        self.id
    }
}

impl AsRef<ComponentKey> for ComponentKey {
    fn as_ref(&self) -> &ComponentKey {
        self
    }
}

impl From<String> for ComponentKey {
    fn from(id: String) -> Self {
        Self { id }
    }
}

impl From<&str> for ComponentKey {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

impl From<ComponentKey> for String {
    fn from(key: ComponentKey) -> Self {
        key.into_id()
    }
}

impl fmt::Display for ComponentKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.id.fmt(f)
    }
}

impl Ord for ComponentKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for ComponentKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl ConfigurableString for ComponentKey {}
