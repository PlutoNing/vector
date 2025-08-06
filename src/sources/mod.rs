#![allow(missing_docs)]
use snafu::Snafu;

#[cfg(feature = "sources-host_metrics")]
pub mod host_metrics;
pub use crate::common::Source;

use std::{
    cmp, fmt,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use async_stream::stream;
use crossbeam_queue::ArrayQueue;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, TryAcquireError};

use agent_lib::config::InMemoryBufferable;

#[allow(dead_code)] // Easier than listing out all the features that use this
/// Common build errors
#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },
}
use std::{collections::HashMap, time::Instant};
use crate::config::{ComponentKey, OutputId};
use crate::schema::Definition;
use agent_lib::config::EventCount;
use agent_lib::event::array::EventArrayIntoIter;
#[cfg(any(test))]
use agent_lib::event::{into_event_stream, EventStatus};
use agent_lib::json_size::JsonSize;
use agent_lib::{
    config::{log_schema, SourceOutput},
    event::{array, Event, EventArray, EventContainer, EventRef},
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
};
use chrono::Utc;
use futures::{Stream, StreamExt};
use metrics::{histogram, Histogram};
use tracing::Span;
use vrl::value::Value;

use tokio::sync::mpsc;

pub const CHUNK_SIZE: usize = 1000;

#[cfg(any(test))]
const TEST_BUFFER_SIZE: usize = 100;
pub const DEFAULT_OUTPUT: &str = "_default";
const LAG_TIME_NAME: &str = "source_lag_time_seconds";
#[derive(Clone, Debug)]
pub struct ClosedError;

impl fmt::Display for ClosedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Sender is closed.")
    }
}

impl std::error::Error for ClosedError {}

impl From<mpsc::error::SendError<Event>> for ClosedError {
    fn from(_: mpsc::error::SendError<Event>) -> Self {
        Self
    }
}

impl From<mpsc::error::SendError<EventArray>> for ClosedError {
    fn from(_: mpsc::error::SendError<EventArray>) -> Self {
        Self
    }
}

impl<T> From<SendError<T>> for ClosedError {
    fn from(_: SendError<T>) -> Self {
        Self
    }
}
/* 作为一个source event的实体 */
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
/* 从source sender item转为event array */
impl From<SourceSenderItem> for EventArray {
    fn from(val: SourceSenderItem) -> Self {
        val.events
    }
}

pub struct Builder {
    // 缓冲区大小，默认1000个事件
    buf_size: usize,
    // 默认输出通道（可选）
    default_output: Option<Output>, /* 是channel的tx */
    // 命名输出通道映射表
    named_outputs: HashMap<String, Output>,
    // 延迟监控直方图
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
    /* 初始化一个指定buf大小的builder */
    pub const fn with_buffer(mut self, n: usize) -> Self {
        self.buf_size = n;
        self
    }
    /* 初始化builder的self.default_output . 返回对应的rx */
    pub fn add_source_output(
        &mut self,
        output: SourceOutput,        /*  */
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
                ); /* output是channel 的tx */
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

/* 用于发送事件出去
调用内部default_output的send接口 */
#[derive(Debug, Clone)]
pub struct SourceSender {
    default_output: Option<Output>,
    named_outputs: HashMap<String, Output>,
}

impl SourceSender {
    /*  */
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

    /// 发送一个事件到默认输出( self.default_output)
    ///
    /// This internally handles emitting [eventsSent]  events.
    pub async fn send_event(&mut self, event: impl Into<EventArray>) -> Result<(), ClosedError> {
        self.default_output_mut().send_event(event).await
    }

    /// 发送stream事件到默认输出
    /// 使用的是Output的能力
    /// This internally handles emitting [eventsSent] events.
    pub async fn send_event_stream<S, E>(&mut self, events: S) -> Result<(), ClosedError>
    where
        S: Stream<Item = E> + Unpin,
        E: Into<Event> + ByteSizeOf,
    {
        self.default_output_mut().send_event_stream(events).await
    }

    /// Send a batch of events to the default output.
    /// 发送指标出去
    /// 比如HostMetrics的capture_metrics获取的host metrics
    /// This internally handles emitting [eventsSent] events.
    pub async fn send_batch<I, E>(&mut self, events: I) -> Result<(), ClosedError>
    where
        E: Into<Event> + ByteSizeOf,
        I: IntoIterator<Item = E>,
        <I as IntoIterator>::IntoIter: ExactSizeIterator,
    {
        /* 发送指标 */
        self.default_output_mut().send_batch(events).await
    }

    /// Send a batch of events event to a named output.
    ///
    /// This internally handles emitting [eventsSent] events.
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

/// __跟踪未发送事件__：记录还有多少事件在缓冲区中
/// __异常处理__：当发送操作被中断时，自动统计丢失的事件数量
/// __监控告警__：发出 `ComponentEventsDropped` 事件，便于监控和调试
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

#[derive(Debug, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "receiver disconnected")
    }
}

impl<T: fmt::Debug> std::error::Error for SendError<T> {}

/// Error returned by `LimitedSender::try_send`.
#[derive(Debug, PartialEq, Eq)]
pub enum TrySendError<T> {
    InsufficientCapacity(T),
    Disconnected(T),
}

impl<T> TrySendError<T> {
    pub fn into_inner(self) -> T {
        match self {
            Self::InsufficientCapacity(item) | Self::Disconnected(item) => item,
        }
    }
}

impl<T> fmt::Display for TrySendError<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientCapacity(_) => {
                write!(fmt, "channel lacks sufficient capacity for send")
            }
            Self::Disconnected(_) => write!(fmt, "receiver disconnected"),
        }
    }
}

impl<T: fmt::Debug> std::error::Error for TrySendError<T> {}

#[derive(Debug)]
struct Inner<T> {
    data: Arc<ArrayQueue<(OwnedSemaphorePermit, T)>>,
    limit: usize,
    limiter: Arc<Semaphore>,
    read_waker: Arc<Notify>,
}

impl<T> Clone for Inner<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            limit: self.limit,
            limiter: self.limiter.clone(),
            read_waker: self.read_waker.clone(),
        }
    }
}

#[derive(Debug)]
pub struct LimitedReceiver<T> {
    inner: Inner<T>,
}

impl<T: Send + 'static> LimitedReceiver<T> {
    /// Gets the number of items that this channel could accept.
    pub fn available_capacity(&self) -> usize {
        self.inner.limiter.available_permits()
    }

    pub async fn next(&mut self) -> Option<T> {
        loop {
            if let Some((_permit, item)) = self.inner.data.pop() {
                return Some(item);
            }

            // There wasn't an item for us to pop, so see if the channel is actually closed.  If so,
            // then it's time for us to close up shop as well.
            if self.inner.limiter.is_closed() {
                return None;
            }

            // We're not closed, so we need to wait for a writer to tell us they made some
            // progress.  This might end up being a spurious wakeup since `Notify` will
            // store a wake-up if there are no waiters, but oh well.
            self.inner.read_waker.notified().await;
        }
    }

    pub fn into_stream(self) -> Pin<Box<dyn Stream<Item = T> + Send>> {
        let mut receiver = self;
        Box::pin(stream! {
            while let Some(item) = receiver.next().await {
                yield item;
            }
        })
    }
}

impl<T> Drop for LimitedReceiver<T> {
    fn drop(&mut self) {
        // Notify senders that the channel is now closed by closing the semaphore.  Any pending
        // acquisitions will be awoken and notified that the semaphore is closed, and further new
        // sends will immediately see the semaphore is closed.
        self.inner.limiter.close();
    }
}

pub fn limited<T>(limit: usize) -> (LimitedSender<T>, LimitedReceiver<T>) {
    let inner = Inner {
        data: Arc::new(ArrayQueue::new(limit)),
        limit,
        limiter: Arc::new(Semaphore::new(limit)),
        read_waker: Arc::new(Notify::new()),
    };

    let sender = LimitedSender {
        inner: inner.clone(),
        sender_count: Arc::new(AtomicUsize::new(1)),
    };
    let receiver = LimitedReceiver { inner };

    (sender, receiver)
}

/* 作为Output的sender成员,  用于发送事件
核心是个tx */
#[derive(Debug)]
pub struct LimitedSender<T> {
    inner: Inner<T>,
    sender_count: Arc<AtomicUsize>,
}

impl<T: InMemoryBufferable> LimitedSender<T> {
    #[allow(clippy::cast_possible_truncation)]
    fn get_required_permits_for_item(&self, item: &T) -> u32 {
        // We have to limit the number of permits we ask for to the overall limit since we're always
        // willing to store more items than the limit if the queue is entirely empty, because
        // otherwise we might deadlock ourselves by not being able to send a single item.
        cmp::min(self.inner.limit, item.event_count()) as u32
    }

    /// Gets the number of items that this channel could accept.
    pub fn available_capacity(&self) -> usize {
        self.inner.limiter.available_permits()
    }

    /* 把事件加入内部的 */
    /// Sends an item into the channel.
    ///
    /// # Errors
    ///
    /// If the receiver has disconnected (does not exist anymore), then `Err(SendError)` be returned
    /// with the given `item`.
    pub async fn send(&mut self, item: T) -> Result<(), SendError<T>> {
        // Calculate how many permits we need, and wait until we can acquire all of them.
        let permits_required = self.get_required_permits_for_item(&item);
        let Ok(permits) = self
            .inner
            .limiter
            .clone()
            .acquire_many_owned(permits_required)
            .await
        else {
            return Err(SendError(item));
        };

        self.inner
            .data
            .push((permits, item))
            .unwrap_or_else(|_| unreachable!("acquired permits but channel reported being full"));
        self.inner.read_waker.notify_one();

        trace!("Sent item.");

        Ok(())
    }

    /// Attempts to send an item into the channel.
    ///
    /// # Errors
    ///
    /// If the receiver has disconnected (does not exist anymore), then
    /// `Err(TrySendError::Disconnected)` be returned with the given `item`. If the channel has
    /// insufficient capacity for the item, then `Err(TrySendError::InsufficientCapacity)` will be
    /// returned with the given `item`.
    ///
    /// # Panics
    ///
    /// Will panic if adding ack amount overflows.
    pub fn try_send(&mut self, item: T) -> Result<(), TrySendError<T>> {
        // Calculate how many permits we need, and try to acquire them all without waiting.
        let permits_required = self.get_required_permits_for_item(&item);
        let permits = match self
            .inner
            .limiter
            .clone()
            .try_acquire_many_owned(permits_required)
        {
            Ok(permits) => permits,
            Err(ae) => {
                return match ae {
                    TryAcquireError::NoPermits => Err(TrySendError::InsufficientCapacity(item)),
                    TryAcquireError::Closed => Err(TrySendError::Disconnected(item)),
                }
            }
        };

        self.inner
            .data
            .push((permits, item))
            .unwrap_or_else(|_| unreachable!("acquired permits but channel reported being full"));
        self.inner.read_waker.notify_one();

        trace!("Attempt to send item succeeded.");

        Ok(())
    }
}

impl<T> Clone for LimitedSender<T> {
    fn clone(&self) -> Self {
        self.sender_count.fetch_add(1, Ordering::SeqCst);

        Self {
            inner: self.inner.clone(),
            sender_count: Arc::clone(&self.sender_count),
        }
    }
}

impl<T> Drop for LimitedSender<T> {
    fn drop(&mut self) {
        // If we're the last sender to drop, close the semaphore on our way out the door.
        if self.sender_count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.inner.limiter.close();
            self.inner.read_waker.notify_one();
        }
    }
}

/* 表示一个output?
一个source sender包裹一个Output用于输出事件 */
#[derive(Clone)]
struct Output {
    sender: LimitedSender<SourceSenderItem>, /* 是tx channel */
    lag_time: Option<Histogram>,
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

impl Output {
    /* 新建一个output, 制定了buffer大小 */
    fn new_with_buffer(
        n: usize, /* buffer大小 */
        _output: String,
        lag_time: Option<Histogram>,
        log_definition: Option<Arc<Definition>>,
        output_id: OutputId,
    ) -> (Self, LimitedReceiver<SourceSenderItem>) {
        let (tx, rx) = limited(n);
        (
            Self {
                sender: tx,
                lag_time,
                log_definition,
                output_id: Arc::new(output_id),
            },
            rx,
        )
    }

    /* 发送一个source event出去 */
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

        /* 初始化数组里的event的meta里的source id等信息 */
        events.iter_events_mut().for_each(|mut event| {
            // attach runtime schema definitions from the source
            // 设置log schema
            if let Some(log_definition) = &self.log_definition {
                event.metadata_mut().set_schema_definition(log_definition);
            }
            /* 设置source id */
            event
                .metadata_mut()
                .set_upstream_id(Arc::clone(&self.output_id));
        });


        let count = events.len();
        /* 调用Output的tx channel来发送出去
        先加入sender的inner array */
        self.sender
            .send(SourceSenderItem {
                events,
                send_reference,
            })
            .await
            .map_err(|_| ClosedError)?;
        unsent_event_count.decr(count);
        Ok(())
    }

    /* 发送事件 */
    async fn send_event(&mut self, event: impl Into<EventArray>) -> Result<(), ClosedError> {
        let event: EventArray = event.into();
        // It's possible that the caller stops polling this future while it is blocked waiting
        // on `self.send()`. When that happens, we use `UnsentEventCount` to correctly emit
        //  events.
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
        // events.
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
