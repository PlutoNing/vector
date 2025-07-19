use metrics::{counter, gauge};
use std::borrow::Cow;
use vector_lib::{
    configurable::configurable_component,
    internal_event::{ComponentEventsDropped, InternalEvent, UNINTENTIONAL},
};

use vector_lib::internal_event::{error_stage, error_type};

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

#[derive(Debug)]
pub struct FileIoError<'a, P> {
    pub error: std::io::Error,
    pub code: &'static str,
    pub message: &'static str,
    pub path: &'a P,
    pub dropped_events: usize,
}

impl<P: std::fmt::Debug> InternalEvent for FileIoError<'_, P> {
    fn emit(self) {
        error!(
            message = %self.message,
            path = ?self.path,
            error = %self.error,
            error_code = %self.code,
            error_type = error_type::IO_FAILED,
            stage = error_stage::SENDING,
        );
        counter!(
            "component_errors_total",
            "error_code" => self.code,
            "error_type" => error_type::IO_FAILED,
            "stage" => error_stage::SENDING,
        )
        .increment(1);

        if self.dropped_events > 0 {
            emit!(ComponentEventsDropped::<UNINTENTIONAL> {
                count: self.dropped_events,
                reason: self.message,
            });
        }
    }
}