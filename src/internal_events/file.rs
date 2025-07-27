use metrics::{counter, gauge};
use std::borrow::Cow;
use vector_lib::{
    configurable::configurable_component,
    internal_event::{InternalEvent},
};

/// Configuration of internal metrics for file-based components.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct FileInternalMetricsConfig {
    /// Whether or not to include the "file" tag on the component's corresponding internal metrics.
    ///
    /// This is useful for distinguishing between different files while monitoring. However, the tag's
    /// cardinality is unbounded.
    #[serde(default = "crate::serde::default_false")]
    pub include_file_tag: bool,
}

#[derive(Debug)]
pub struct FileOpen {
    pub count: usize,
}

impl InternalEvent for FileOpen {
    fn emit(self) {
        gauge!("open_files").set(self.count as f64);
    }
}

#[derive(Debug)]
pub struct FileBytesSent<'a> {
    pub byte_size: usize,
    pub file: Cow<'a, str>,
    pub include_file_metric_tag: bool,
}

impl InternalEvent for FileBytesSent<'_> {
    fn emit(self) {
        trace!(
            message = "Bytes sent.",
            byte_size = %self.byte_size,
            protocol = "file",
            file = %self.file,
        );
        if self.include_file_metric_tag {
            counter!(
                "component_sent_bytes_total",
                "protocol" => "file",
                "file" => self.file.clone().into_owned(),
            )
        } else {
            counter!(
                "component_sent_bytes_total",
                "protocol" => "file",
            )
        }
        .increment(self.byte_size as u64);
    }
}