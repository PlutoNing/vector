use std::{marker::PhantomData, time::Duration};

use derivative::Derivative;

use snafu::Snafu;

use agent_lib::json_size::JsonSize;

use crate::event::EventFinalizers;

// * Provide sensible sink default 10 MB with 1s timeout. Don't allow chaining builder methods on
//   that.

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum BatchError {
    #[snafu(display("This sink does not allow setting `max_bytes`"))]
    BytesNotAllowed,
    #[snafu(display("`max_bytes` must be greater than zero"))]
    InvalidMaxBytes,
    #[snafu(display("`max_events` must be greater than zero"))]
    InvalidMaxEvents,
    #[snafu(display("`timeout_secs` must be greater than zero"))]
    InvalidTimeout,
    #[snafu(display("provided `max_bytes` exceeds the maximum limit of {}", limit))]
    MaxBytesExceeded { limit: usize },
    #[snafu(display("provided `max_events` exceeds the maximum limit of {}", limit))]
    MaxEventsExceeded { limit: usize },
}

pub trait SinkBatchSettings {
    const MAX_EVENTS: Option<usize>;
    const MAX_BYTES: Option<usize>;
    const TIMEOUT_SECS: f64;
}

/// Reasonable default batch settings for sinks with timeliness concerns, limited by event count.
#[derive(Clone, Copy, Debug, Default)]
pub struct RealtimeEventBasedDefaultBatchSettings;

impl SinkBatchSettings for RealtimeEventBasedDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = Some(1000);
    const MAX_BYTES: Option<usize> = None;
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Reasonable default batch settings for sinks with timeliness concerns, limited by byte size.
#[derive(Clone, Copy, Debug, Default)]
pub struct RealtimeSizeBasedDefaultBatchSettings;

impl SinkBatchSettings for RealtimeSizeBasedDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(10_000_000);
    const TIMEOUT_SECS: f64 = 1.0;
}

/// Reasonable default batch settings for sinks focused on shipping fewer-but-larger batches,
/// limited by byte size.
#[derive(Clone, Copy, Debug, Default)]
pub struct BulkSizeBasedDefaultBatchSettings;

impl SinkBatchSettings for BulkSizeBasedDefaultBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = Some(10_000_000);
    const TIMEOUT_SECS: f64 = 300.0;
}

/// "Default" batch settings when a sink handles batch settings entirely on its own.
///
/// This has very few usages, but can be notably seen in the Kafka sink, where the values are used
/// to configure `librdkafka` itself rather than being passed as `BatchSettings`/`batcherSettings`
/// to components in the sink itself.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoDefaultsBatchSettings;

impl SinkBatchSettings for NoDefaultsBatchSettings {
    const MAX_EVENTS: Option<usize> = None;
    const MAX_BYTES: Option<usize> = None;
    const TIMEOUT_SECS: f64 = 1.0;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Merged;

#[derive(Clone, Copy, Debug, Default)]
pub struct Unmerged;

#[derive(Debug, Derivative)]
#[derivative(Clone(bound = ""))]
#[derivative(Copy(bound = ""))]
pub struct BatchSize<B> {
    pub bytes: usize,
    pub events: usize,
    // This type marker is used to drive type inference, which allows us
    // to call the right Batch::get_Settings_defaults without explicitly
    // naming the type in BatchSettings::parse_config.
    _type_marker: PhantomData<B>,
}

impl<B> BatchSize<B> {
    pub const fn const_default() -> Self {
        BatchSize {
            bytes: usize::MAX,
            events: usize::MAX,
            _type_marker: PhantomData,
        }
    }
}

impl<B> Default for BatchSize<B> {
    fn default() -> Self {
        BatchSize::const_default()
    }
}

#[derive(Debug, Derivative)]
#[derivative(Clone(bound = ""))]
#[derivative(Copy(bound = ""))]
pub struct BatchSettings<B> {
    pub size: BatchSize<B>,
    pub timeout: Duration,
}

impl<B> Default for BatchSettings<B> {
    fn default() -> Self {
        BatchSettings {
            size: BatchSize {
                bytes: 10_000_000,
                events: usize::MAX,
                _type_marker: PhantomData,
            },
            timeout: Duration::from_secs(1),
        }
    }
}

/// This enum provides the result of a push operation, indicating if the
/// event was added and the fullness state of the buffer.
#[must_use]
#[derive(Debug, Eq, PartialEq)]
pub enum PushResult<T> {
    /// Event was added, with an indicator if the buffer is now full
    Ok(bool),
    /// Event could not be added because it would overflow the
    /// buffer. Since push takes ownership of the event, it must be
    /// returned here.
    Overflow(T),
}

pub trait Batch: Sized {
    type Input;
    type Output;

    fn push(&mut self, item: Self::Input) -> PushResult<Self::Input>;
    fn is_empty(&self) -> bool;
    fn fresh(&self) -> Self;
    fn finish(self) -> Self::Output;
    fn num_items(&self) -> usize;
}

#[derive(Debug)]
pub struct EncodedBatch<I> {
    pub items: I,
    pub finalizers: EventFinalizers,
    pub count: usize,
    pub byte_size: usize,
    pub json_byte_size: JsonSize,
}
