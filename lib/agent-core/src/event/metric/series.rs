use core::fmt;


use agent_config::configurable_component;

use super::{write_list, write_word, MetricTags, TagValue};

/// Metrics series.
#[configurable_component]
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct MetricSeries {
    #[serde(flatten)]
    pub name: MetricName,

    #[configurable(derived)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<MetricTags>,
}

impl MetricSeries {
    /// Gets a reference to the name of the series.
    pub fn name(&self) -> &MetricName {
        &self.name
    }

    /// Gets a mutable reference to the name of the series.
    pub fn name_mut(&mut self) -> &mut MetricName {
        &mut self.name
    }

    /// Gets an optional reference to the tags of the series.
    pub fn tags(&self) -> Option<&MetricTags> {
        self.tags.as_ref()
    }

    /// Gets an optional mutable reference to the tags of the series.
    pub fn tags_mut(&mut self) -> &mut Option<MetricTags> {
        &mut self.tags
    }

    /// Sets or updates the string value of a tag.
    ///
    /// *Note:* This will create the tags map if it is not present.
    pub fn replace_tag(&mut self, key: String, value: impl Into<TagValue>) -> Option<String> {
        (self.tags.get_or_insert_with(Default::default)).replace(key, value)
    }

    pub fn set_multi_value_tag(&mut self, key: String, values: impl IntoIterator<Item = TagValue>) {
        (self.tags.get_or_insert_with(Default::default)).set_multi_value(key, values);
    }

    /// Removes all the tags.
    pub fn remove_tags(&mut self) {
        self.tags = None;
    }

    /// Removes the tag entry for the named key, if it exists, and returns the old value.
    ///
    /// *Note:* This will drop the tags map if the tag was the last entry in it.
    pub fn remove_tag(&mut self, key: &str) -> Option<String> {
        match &mut self.tags {
            None => None,
            Some(tags) => {
                let result = tags.remove(key);
                if tags.is_empty() {
                    self.tags = None;
                }
                result
            }
        }
    }
}



/// Metric name.
#[configurable_component]
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct MetricName {
    /// The name of the metric.
    ///
    /// This would typically be a name for the metric itself, unrelated to where the metric
    /// originates from. For example, if the metric represented the amount of used system memory, it
    /// may be called `memory.used`.
    pub name: String,

    /// The namespace of the metric.
    /* 指标的命名空间。命名空间为指标提供分组，当指标名称本身过于通用时很有用。
    例如，虽然指标名称可能是memory.used表示已用系统内存量，但命名空间可以通过
    system表示整个系统的已用内存总量，
    或通过表示agent专用的已用系统内存量等方式进行区分。 */
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

impl MetricName {
    /// Gets a reference to the name component of this name.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Gets a mutable reference to the name component of this name.
    pub fn name_mut(&mut self) -> &mut String {
        &mut self.name
    }

    /// Gets a reference to the namespace component of this name.
    pub fn namespace(&self) -> Option<&String> {
        self.namespace.as_ref()
    }

    /// Gets a mutable reference to the namespace component of this name.
    pub fn namespace_mut(&mut self) -> &mut Option<String> {
        &mut self.namespace
    }
}

impl fmt::Display for MetricSeries {
    /// Display a metric series name using something like Prometheus' text format:
    ///
    /// ```text
    /// NAMESPACE_NAME{TAGS}
    /// ```
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        if let Some(namespace) = &self.name.namespace {
            write_word(fmt, namespace)?;
            write!(fmt, "_")?;
        }
        write_word(fmt, &self.name.name)?;
        write!(fmt, "{{")?;
        if let Some(tags) = &self.tags {
            write_list(fmt, ",", tags.iter_all(), |fmt, (tag, value)| {
                write_word(fmt, tag).and_then(|()| match value {
                    Some(value) => write!(fmt, "={value:?}"),
                    None => Ok(()),
                })
            })?;
        }
        write!(fmt, "}}")
    }
}


