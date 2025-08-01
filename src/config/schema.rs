use agent_lib::config::LogNamespace;
use agent_lib::configurable::configurable_component;

pub(crate) use crate::schema::Definition;

/// Schema options.
#[configurable_component]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Options {
    /// Whether or not schema is enabled.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Whether or not schema validation is enabled.
    #[serde(default = "default_validation")]
    pub validation: bool,

    /// Whether or not to enable log namespacing.
    pub log_namespace: Option<bool>,
}

impl Options {
    /// Gets the value of the globally configured log namespace, or the default if it wasn't set.
    pub fn log_namespace(self) -> LogNamespace {
        self.log_namespace
            .map_or(LogNamespace::Legacy, |use_vector_namespace| {
                use_vector_namespace.into()
            })
    }

    /// Merges two schema options together.
    pub fn append(&mut self, with: Self, errors: &mut Vec<String>) {
        if self.log_namespace.is_some()
            && with.log_namespace.is_some()
            && self.log_namespace != with.log_namespace
        {
            errors.push(
                format!("conflicting values for 'log_namespace' found. Both {:?} and {:?} used in the same component",
                        self.log_namespace(), with.log_namespace())
            );
        }
        if let Some(log_namespace) = with.log_namespace {
            self.log_namespace = Some(log_namespace);
        }

        // If either config enables these flags, it is enabled.
        self.enabled |= with.enabled;
        self.validation |= with.validation;
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            validation: default_validation(),
            log_namespace: None,
        }
    }
}

const fn default_enabled() -> bool {
    false
}

const fn default_validation() -> bool {
    false
}