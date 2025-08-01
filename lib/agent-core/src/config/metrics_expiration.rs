use agent_config::configurable_component;



/// Configuration for metric name matcher.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "Metric name matcher type."))]
pub enum MetricNameMatcherConfig {
    /// Only considers exact name matches.
    Exact {
        /// The exact metric name.
        value: String,
    },
    /// Compares metric name to the provided pattern.
    Regex {
        /// Pattern to compare to.
        pattern: String,
    },
}

/// Configuration for metric labels matcher.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "Metric label matcher type."))]
pub enum MetricLabelMatcher {
    /// Looks for an exact match of one label key value pair.
    Exact {
        /// Metric key to look for.
        key: String,
        /// The exact metric label value.
        value: String,
    },
    /// Compares label value with given key to the provided pattern.
    Regex {
        /// Metric key to look for.
        key: String,
        /// Pattern to compare metric label value to.
        value_pattern: String,
    },
}

/// Configuration for metric labels matcher group.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "Metric label group matcher type."))]
pub enum MetricLabelMatcherConfig {
    /// Checks that any of the provided matchers can be applied to given metric.
    Any {
        /// List of matchers to check.
        matchers: Vec<MetricLabelMatcher>,
    },
    /// Checks that all of the provided matchers can be applied to given metric.
    All {
        /// List of matchers to check.
        matchers: Vec<MetricLabelMatcher>,
    },
}