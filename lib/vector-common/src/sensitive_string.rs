use vector_config::{configurable_component, ConfigurableString};

/// Wrapper for sensitive strings containing credentials
#[configurable_component(no_deser, no_ser)]
#[derive(::serde::Deserialize, ::serde::Serialize)]
#[serde(from = "String", into = "String")]
#[configurable(metadata(sensitive))]
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SensitiveString(String);

impl From<String> for SensitiveString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<SensitiveString> for String {
    fn from(value: SensitiveString) -> Self {
        value.0
    }
}

impl ConfigurableString for SensitiveString {}

impl std::fmt::Display for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "**REDACTED**")
    }
}

impl std::fmt::Debug for SensitiveString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // we keep the double quotes here to keep the String behavior
        write!(f, "\"**REDACTED**\"")
    }
}

impl SensitiveString {
    #[must_use]
    pub fn inner(&self) -> &str {
        self.0.as_str()
    }
}