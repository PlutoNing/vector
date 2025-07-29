mod optional_path;

use std::fmt;
use vector_config_macros::configurable_component;

pub use vrl::path::{
    parse_target_path, parse_value_path, BorrowedSegment, OwnedSegment, OwnedTargetPath,
    OwnedValuePath, PathConcat, PathParseError, PathPrefix, TargetPath, ValuePath,
};
use vrl::value::KeyString;

/// A wrapper around `OwnedTargetPath` that allows it to be used in Vector config
/// with prefix default to `PathPrefix::Event`
#[configurable_component]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "proptest", derive(proptest_derive::Arbitrary))]
#[serde(try_from = "String", into = "String")]
pub struct ConfigTargetPath(pub OwnedTargetPath);

impl fmt::Display for ConfigTargetPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for ConfigTargetPath {
    type Error = PathParseError;

    fn try_from(src: String) -> Result<Self, Self::Error> {
        OwnedTargetPath::try_from(src).map(ConfigTargetPath)
    }
}

impl TryFrom<KeyString> for ConfigTargetPath {
    type Error = PathParseError;

    fn try_from(src: KeyString) -> Result<Self, Self::Error> {
        OwnedTargetPath::try_from(src).map(ConfigTargetPath)
    }
}

impl From<ConfigTargetPath> for String {
    fn from(owned: ConfigTargetPath) -> Self {
        String::from(owned.0)
    }
}

impl<'a> TargetPath<'a> for &'a ConfigTargetPath {
    type ValuePath = &'a OwnedValuePath;

    fn prefix(&self) -> PathPrefix {
        self.0.prefix
    }

    fn value_path(&self) -> Self::ValuePath {
        &self.0.path
    }
}

#[cfg(any(test, feature = "test"))]
impl From<&str> for ConfigTargetPath {
    fn from(path: &str) -> Self {
        ConfigTargetPath::try_from(path.to_string()).unwrap()
    }
}
