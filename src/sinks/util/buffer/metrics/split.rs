use std::collections::VecDeque;

use agent_lib::event::{Metric};

#[allow(clippy::large_enum_variant)]
enum SplitState {
    Single(Option<Metric>),
    Multiple(VecDeque<Metric>),
}

/// An iterator that returns the result of a metric split operation.
pub struct SplitIterator {
    state: SplitState,
}

impl SplitIterator {
    /// Creates an iterator for a single metric.
    pub const fn single(metric: Metric) -> Self {
        Self {
            state: SplitState::Single(Some(metric)),
        }
    }

    /// Creates an iterator for multiple metrics.
    pub fn multiple<I>(metrics: I) -> Self
    where
        I: Into<VecDeque<Metric>>,
    {
        Self {
            state: SplitState::Multiple(metrics.into()),
        }
    }
}

impl Iterator for SplitIterator {
    type Item = Metric;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            SplitState::Single(metric) => metric.take(),
            SplitState::Multiple(metrics) => metrics.pop_front(),
        }
    }
}

/// Splits a metric into potentially multiple metrics.
///
/// In some cases, a single metric may represent multiple fundamental metrics: an aggregated summary or histogram can
/// represent a count, sum, and subtotals for a given measurement. These metrics may be able to be handled
/// natively/directly in a sink, but in other cases, those fundamental metrics may need to be extracted and operated on individually.
///
/// This trait defines a simple interface for defining custom rules about what metrics to split and when to split them.
pub trait MetricSplit {
    /// Attempts to split the metric.
    ///
    /// The returned iterator will either return only the input metric if no splitting occurred, or all resulting
    /// metrics that were created as a result of the split.
    fn split(&mut self, input: Metric) -> SplitIterator;
}

/// A self-contained metric splitter.
///
/// The splitter state is stored internally, and it can only be created from a splitter implementation that is either
/// `Default` or is constructed ahead of time, so it is primarily useful for constructing a usable splitter via implicit
/// conversion methods or when no special parameters are required for configuring the underlying splitter.
pub struct MetricSplitter<S> {
    splitter: S,
}

impl<S: MetricSplit> MetricSplitter<S> {
    /// Attempts to split the metric.
    ///
    /// For more information about splitting, see the documentation for [`MetricSplit::split`].
    pub fn split(&mut self, input: Metric) -> SplitIterator {
        self.splitter.split(input)
    }
}

impl<S: Default> Default for MetricSplitter<S> {
    fn default() -> Self {
        Self {
            splitter: S::default(),
        }
    }
}

impl<S> From<S> for MetricSplitter<S> {
    fn from(splitter: S) -> Self {
        Self { splitter }
    }
}