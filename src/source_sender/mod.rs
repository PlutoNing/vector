#![allow(missing_docs)]::
use std::{collections::HashMap, fmt, sync::Arc, time::Instant};

use chrono::Utc;
use futures::{Stream, StreamExt};
use metrics::{histogram, Histogram};
use tracing::Span;
use vector_lib::buffers::topology::channel::{self, LimitedReceiver, LimitedSender};
use vector_lib::buffers::EventCount;
use vector_lib::event::array::EventArrayIntoIter;
#[cfg(any(test))]
use vector_lib::event::{into_event_stream, EventStatus};
use vector_lib::json_size::JsonSize;
use vector_lib::{
    config::{log_schema, SourceOutput},
    event::{array, Event, EventArray, EventContainer, EventRef},
    internal_event::{
        self, CountByteSize, EventsSent, InternalEventHandle as _, Registered, DEFAULT_OUTPUT,
    },
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
};
use vrl::value::Value;

mod errors;

use crate::config::{ComponentKey, OutputId};
use crate::schema::Definition;
pub use errors::{ClosedError, StreamSendError};

pub(crate) const CHUNK_SIZE: usize = 1000;

#[cfg(any(test))]
const TEST_BUFFER_SIZE: usize = 100;

const LAG_TIME_NAME: &str = "source_lag_time_seconds";

/// SourceSenderItem is a thin wrapper around [EventArray] used to track the send duration of a batch.
///
/// This is needed because the send duration is calculated as the difference between when the batch
/// is sent from the origin component to when the batch is enqueued on the receiving component's input buffer.
/// For sources in particular, this requires the batch to be enqueued on two channels: the origin component's pump
/// channel and then the receiving component's input buffer.
#[derive(Debug)]
pub struct SourceSenderItem {
    /// The batch of events to send.
    pub events: EventArray,
    /// Reference instant used to calculate send duration.
    pub send_reference: Instant,
}

impl ByteSizeOf for SourceSenderItem {
    fn allocated_bytes(&self) -> usize {
        self.events.allocated_bytes()
    }
}

impl EventCount for SourceSenderItem {
    fn event_count(&self) -> usize {
        self.events.event_count()
    }
}

impl EstimatedJsonEncodedSizeOf for SourceSenderItem {
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        self.events.estimated_json_encoded_size_of()
    }
}

impl EventContainer for SourceSenderItem {
    type IntoIter = EventArrayIntoIter;

    fn len(&self) -> usize {
        self.events.len()
    }

    fn into_events(self) -> Self::IntoIter {
        self.events.into_events()
    }
}

impl From<SourceSenderItem> for EventArray {
    fn from(val: SourceSenderItem) -> Self {
        val.events
    }
}
/* 感觉像是个buf? */
pub struct Builder {
    buf_size: usize,
    default_output: Option<Output>, /* 是channel的tx */
    named_outputs: HashMap<String, Output>,
    lag_time: Option<Histogram>,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            buf_size: CHUNK_SIZE,
            default_output: None,
            named_outputs: Default::default(),
            lag_time: Some(histogram!(LAG_TIME_NAME)),
        }
    }
}

impl Builder {
    pub const fn with_buffer(mut self, n: usize) -> Self {
        self.buf_size = n;
        self
    }
/* 初始化builder的self.default_output . 返回对应的rx */
    pub fn add_source_output(
        &mut self,
        output: SourceOutput,/*  */
        component_key: ComponentKey, /* 可能是source的Id */
    ) -> LimitedReceiver<SourceSenderItem> {
        let lag_time = self.lag_time.clone();
        let log_definition = output.schema_definition.clone();
        let output_id = OutputId {
            component: component_key,
            port: output.port.clone(),
        };
        match output.port {
            None => {
                let (output, rx) = Output::new_with_buffer(
                    self.buf_size,
                    DEFAULT_OUTPUT.to_owned(),
                    lag_time,
                    log_definition,
                    output_id,
                );/* output是channel 的tx */
                self.default_output = Some(output);
                rx /* 返回channel的rx */
            }
            Some(name) => {
                let (output, rx) = Output::new_with_buffer(
                    self.buf_size,
                    name.clone(),
                    lag_time,
                    log_definition,
                    output_id,
                );
                self.named_outputs.insert(name, output);
                rx
            }
        }
    }
/* 从builder获取一个source? */
    pub fn build(self) -> SourceSender {
        SourceSender {
            default_output: self.default_output,
            named_outputs: self.named_outputs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceSender {
    // The default output is optional because some sources, e.g. `datadog_agent`
    // , can be configured to only output to named outputs.
    default_output: Option<Output>,
    named_outputs: HashMap<String, Output>,
}

impl SourceSender {/* 为什么这里需要个builder */
    pub fn builder() -> Builder {
        Builder::default()
    }

    #[cfg(any(test))]
    pub fn new_test_sender_with_buffer(n: usize) -> (Self, LimitedReceiver<SourceSenderItem>) {
        let lag_time = Some(histogram!(LAG_TIME_NAME));
        let output_id = OutputId {
            component: "test".to_string().into(),
            port: None,
        };
        let (default_output, rx) =
            Output::new_with_buffer(n, DEFAULT_OUTPUT.to_owned(), lag_time, None, output_id);
        (
            Self {
                default_output: Some(default_output),
                named_outputs: Default::default(),
            },
            rx,
        )
    }

    #[cfg(any(test))]
    pub fn new_test() -> (Self, impl Stream<Item = Event> + Unpin) {
        let (pipe, recv) = Self::new_test_sender_with_buffer(TEST_BUFFER_SIZE);
        let recv = recv.into_stream().flat_map(into_event_stream);
        (pipe, recv)
    }

    #[cfg(any(test))]
    pub fn new_test_finalize(status: EventStatus) -> (Self, impl Stream<Item = Event> + Unpin) {
        let (pipe, recv) = Self::new_test_sender_with_buffer(TEST_BUFFER_SIZE);
        // In a source test pipeline, there is no sink to acknowledge
        // events, so we have to add a map to the receiver to handle the
        // finalization.
        let recv = recv.into_stream().flat_map(move |mut item| {
            item.events.iter_events_mut().for_each(|mut event| {
                let metadata = event.metadata_mut();
                metadata.update_status(status);
                metadata.update_sources();
            });
            into_event_stream(item)
        });
        (pipe, recv)
    }

    #[cfg(any(test))]
    pub fn new_test_errors(
        error_at: impl Fn(usize) -> bool,
    ) -> (Self, impl Stream<Item = Event> + Unpin) {
        let (pipe, recv) = Self::new_test_sender_with_buffer(TEST_BUFFER_SIZE);
        // In a source test pipeline, there is no sink to acknowledge
        // events, so we have to add a map to the receiver to handle the
        // finalization.
        let mut count: usize = 0;
        let recv = recv.into_stream().flat_map(move |mut item| {
            let status = if error_at(count) {
                EventStatus::Errored
            } else {
                EventStatus::Delivered
            };
            count += 1;
            item.events.iter_events_mut().for_each(|mut event| {
                let metadata = event.metadata_mut();
                metadata.update_status(status);
                metadata.update_sources();
            });
            into_event_stream(item)
        });
        (pipe, recv)
    }

    #[cfg(any(test))]
    pub fn add_outputs(
        &mut self,
        status: EventStatus,
        name: String,
    ) -> impl Stream<Item = SourceSenderItem> + Unpin {
        // The lag_time parameter here will need to be filled in if this function is ever used for
        // non-test situations.
        let output_id = OutputId {
            component: "test".to_string().into(),
            port: Some(name.clone()),
        };
        let (output, recv) = Output::new_with_buffer(100, name.clone(), None, None, output_id);
        let recv = recv.into_stream().map(move |mut item| {
            item.events.iter_events_mut().for_each(|mut event| {
                let metadata = event.metadata_mut();
                metadata.update_status(status);
                metadata.update_sources();
            });
            item
        });
        self.named_outputs.insert(name, output);
        recv
    }
/* 发送到buffer的tx */
    /// Get a mutable reference to the default output, panicking if none exists.
    const fn default_output_mut(&mut self) -> &mut Output {
        self.default_output.as_mut().expect("no default output")
    }

    /// Send an event to the default output.
    ///
    /// This internally handles emitting [EventsSent] and [ComponentEventsDropped] events.
    pub async fn send_event(&mut self, event: impl Into<EventArray>) -> Result<(), ClosedError> {
        self.default_output_mut().send_event(event).await
    }

    /// Send a stream of events to the default output.
    ///
    /// This internally handles emitting [EventsSent] and [ComponentEventsDropped] events.
    pub async fn send_event_stream<S, E>(&mut self, events: S) -> Result<(), ClosedError>
    where
        S: Stream<Item = E> + Unpin,
        E: Into<Event> + ByteSizeOf,
    {
        self.default_output_mut().send_event_stream(events).await
    }

    /// Send a batch of events to the default output.
    /// 发送指标出去
    /// This internally handles emitting [EventsSent] and [ComponentEventsDropped] events.
    pub async fn send_batch<I, E>(&mut self, events: I) -> Result<(), ClosedError>
    where
        E: Into<Event> + ByteSizeOf,
        I: IntoIterator<Item = E>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {/* 发送指标 */
        self.default_output_mut().send_batch(events).await
    }

    /// Send a batch of events event to a named output.
    ///
    /// This internally handles emitting [EventsSent] and [ComponentEventsDropped] events.
    pub async fn send_batch_named<I, E>(&mut self, name: &str, events: I) -> Result<(), ClosedError>
    where
        E: Into<Event> + ByteSizeOf,
        I: IntoIterator<Item = E>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        self.named_outputs
            .get_mut(name)
            .expect("unknown output")
            .send_batch(events)
            .await
    }
}

/// UnsentEvents tracks the number of events yet to be sent in the buffer. This is used to
/// increment the appropriate counters when a future is not polled to completion. Particularly,
/// this is known to happen in a Warp server when a client sends a new HTTP request on a TCP
/// connection that already has a pending request.
///
/// If its internal count is greater than 0 when dropped, the appropriate [ComponentEventsDropped]
/// event is emitted.
struct UnsentEventCount {
    count: usize,
    span: Span,
}

impl UnsentEventCount {
    fn new(count: usize) -> Self {
        Self {
            count,
            span: Span::current(),
        }
    }

    const fn decr(&mut self, count: usize) {
        self.count = self.count.saturating_sub(count);
    }

    const fn discard(&mut self) {
        self.count = 0;
    }
}

impl Drop for UnsentEventCount {
    fn drop(&mut self) {
        if self.count > 0 {
            let _enter = self.span.enter();
        }
    }
}
/* 表示一个output? */
#[derive(Clone)]
struct Output {
    sender: LimitedSender<SourceSenderItem>, /* 是tx channel */
    lag_time: Option<Histogram>,
    events_sent: Registered<EventsSent>,
    /// The schema definition that will be attached to Log events sent through here
    log_definition: Option<Arc<Definition>>,
    /// The OutputId related to this source sender. This is set as the `upstream_id` in
    /// `EventMetadata` for all event sent through here.
    output_id: Arc<OutputId>, /* 里面包含了source id */
}

impl fmt::Debug for Output {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Output")
            .field("sender", &self.sender)
            .field("output_id", &self.output_id)
            // `metrics::Histogram` is missing `impl Debug`
            .finish()
    }
}

impl Output {/* 新建一个output, 制定了buffer大小 */
    fn new_with_buffer(
        n: usize,  /* buffer大小 */
        output: String,
        lag_time: Option<Histogram>,
        log_definition: Option<Arc<Definition>>,
        output_id: OutputId,
    ) -> (Self, LimitedReceiver<SourceSenderItem>) {/* 看来output是基于channel的 */
        let (tx, rx) = channel::limited(n);
        (
            Self {
                sender: tx,
                lag_time,
                events_sent: register!(EventsSent::from(internal_event::Output(Some(
                    output.into()
                )))),
                log_definition,
                output_id: Arc::new(output_id),
            },
            rx,
        )
    }

    async fn send(
        &mut self,
        mut events: EventArray,
        unsent_event_count: &mut UnsentEventCount,
    ) -> Result<(), ClosedError> {
        let send_reference = Instant::now();
        let reference = Utc::now().timestamp_millis();
        events
            .iter_events()
            .for_each(|event| self.emit_lag_time(event, reference));

        events.iter_events_mut().for_each(|mut event| {
            // attach runtime schema definitions from the source
            if let Some(log_definition) = &self.log_definition {
                event.metadata_mut().set_schema_definition(log_definition);
            }
            event
                .metadata_mut()
                .set_upstream_id(Arc::clone(&self.output_id));
        });

        let byte_size = events.estimated_json_encoded_size_of();
        let count = events.len();
        self.sender
            .send(SourceSenderItem {
                events,
                send_reference,
            })
            .await
            .map_err(|_| ClosedError)?;
        self.events_sent.emit(CountByteSize(count, byte_size));
        unsent_event_count.decr(count);
        Ok(())
    }

    async fn send_event(&mut self, event: impl Into<EventArray>) -> Result<(), ClosedError> {
        let event: EventArray = event.into();
        // It's possible that the caller stops polling this future while it is blocked waiting
        // on `self.send()`. When that happens, we use `UnsentEventCount` to correctly emit
        // `ComponentEventsDropped` events.
        let mut unsent_event_count = UnsentEventCount::new(event.len());
        self.send(event, &mut unsent_event_count).await
    }

    async fn send_event_stream<S, E>(&mut self, events: S) -> Result<(), ClosedError>
    where
        S: Stream<Item = E> + Unpin,
        E: Into<Event> + ByteSizeOf,
    {
        let mut stream = events.ready_chunks(CHUNK_SIZE);
        while let Some(events) = stream.next().await {
            self.send_batch(events.into_iter()).await?;
        }
        Ok(())
    }
/*  */
    async fn send_batch<I, E>(&mut self, events: I) -> Result<(), ClosedError>
    where
        E: Into<Event> + ByteSizeOf,
        I: IntoIterator<Item = E>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        // It's possible that the caller stops polling this future while it is blocked waiting
        // on `self.send()`. When that happens, we use `UnsentEventCount` to correctly emit
        // `ComponentEventsDropped` events.
        let events = events.into_iter().map(Into::into);
        let mut unsent_event_count = UnsentEventCount::new(events.len());
        for events in array::events_into_arrays(events, Some(CHUNK_SIZE)) {
            self.send(events, &mut unsent_event_count)
                .await
                .inspect_err(|_| {
                    // The unsent event count is discarded here because the callee emits the
                    // `StreamClosedError`.
                    unsent_event_count.discard();
                })?;
        }
        Ok(())
    }

    /// Calculate the difference between the reference time and the
    /// timestamp stored in the given event reference, and emit the
    /// different, as expressed in milliseconds, as a histogram.
    fn emit_lag_time(&self, event: EventRef<'_>, reference: i64) {
        if let Some(lag_time_metric) = &self.lag_time {
            let timestamp = match event {
                EventRef::Log(log) => {
                    log_schema()
                        .timestamp_key_target_path()
                        .and_then(|timestamp_key| {
                            log.get(timestamp_key).and_then(get_timestamp_millis)
                        })
                }
                EventRef::Metric(metric) => metric
                    .timestamp()
                    .map(|timestamp| timestamp.timestamp_millis()),
                EventRef::Trace(trace) => {
                    log_schema()
                        .timestamp_key_target_path()
                        .and_then(|timestamp_key| {
                            trace.get(timestamp_key).and_then(get_timestamp_millis)
                        })
                }
            };
            if let Some(timestamp) = timestamp {
                // This will truncate precision for values larger than 2**52, but at that point the user
                // probably has much larger problems than precision.
                let lag_time = (reference - timestamp) as f64 / 1000.0;
                lag_time_metric.record(lag_time);
            }
        }
    }
}

const fn get_timestamp_millis(value: &Value) -> Option<i64> {
    match value {
        Value::Timestamp(timestamp) => Some(timestamp.timestamp_millis()),
        _ => None,
    }
}