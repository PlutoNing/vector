//! A reusable line aggregation implementation.

#![deny(missing_docs)]

use std::{

    hash::Hash,


    time::Duration,
};





use regex::bytes::Regex;

use vector_lib::configurable::configurable_component;

/// Mode of operation of the line aggregator.
#[configurable_component]
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// All consecutive lines matching this pattern are included in the group.
    ///
    /// The first line (the line that matched the start pattern) does not need to match the `ContinueThrough` pattern.
    ///
    /// This is useful in cases such as a Java stack trace, where some indicator in the line (such as a leading
    /// whitespace) indicates that it is an extension of the proceeding line.
    ContinueThrough,

    /// All consecutive lines matching this pattern, plus one additional line, are included in the group.
    ///
    /// This is useful in cases where a log message ends with a continuation marker, such as a backslash, indicating
    /// that the following line is part of the same message.
    ContinuePast,

    /// All consecutive lines not matching this pattern are included in the group.
    ///
    /// This is useful where a log line contains a marker indicating that it begins a new message.
    HaltBefore,

    /// All consecutive lines, up to and including the first line matching this pattern, are included in the group.
    ///
    /// This is useful where a log line ends with a termination marker, such as a semicolon.
    HaltWith,
}

/// Configuration of multi-line aggregation.
#[derive(Clone, Debug)]
pub struct Config {
    /// Regular expression pattern that is used to match the start of a new message.
    pub start_pattern: Regex,

    /// Regular expression pattern that is used to determine whether or not more lines should be read.
    ///
    /// This setting must be configured in conjunction with `mode`.
    pub condition_pattern: Regex,

    /// Aggregation mode.
    ///
    /// This setting must be configured in conjunction with `condition_pattern`.
    pub mode: Mode,

    /// The maximum amount of time to wait for the next additional line, in milliseconds.
    ///
    /// Once this timeout is reached, the buffered message is guaranteed to be flushed, even if incomplete.
    pub timeout: Duration,
}

impl Config {
    /// Build `Config` from legacy `file` source line aggregator configuration
    /// params.
    pub fn for_legacy(marker: Regex, timeout_ms: u64) -> Self {
        let start_pattern = marker;
        let condition_pattern = start_pattern.clone();
        let mode = Mode::HaltBefore;
        let timeout = Duration::from_millis(timeout_ms);

        Self {
            start_pattern,
            condition_pattern,
            mode,
            timeout,
        }
    }
}


/// Specifies the amount of lines to emit in response to a single input line.
/// We have to emit either one or two lines.
pub enum Emit<T> {
    /// Emit one line.
    One(T),
    /// Emit two lines, in the order they're specified.
    Two(T, T),
}
