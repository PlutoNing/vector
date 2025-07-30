use std::time::Instant;

use async_recursion::async_recursion;
use derivative::Derivative;

use tracing::Span;
use vector_common::internal_event::{register, InternalEventHandle, Registered};

use super::limited_queue::LimitedSender;
use crate::buffers::buffer_usage_data::BufferUsageHandle;
use std::time::Duration;
use vector_lib::buffers::{Bufferable,WhenFull,};
use metrics::{histogram, Histogram};
use vector_common::registered_event;
registered_event! {
    BufferSendDuration {
        stage: usize,
    } => {
        send_duration: Histogram = histogram!("buffer_send_duration_seconds", "stage" => self.stage.to_string()),
    }

    fn emit(&self, duration: Duration) {
        self.send_duration.record(duration);
    }
}
/// Adapter for papering over various sender backends.
#[derive(Clone, Debug)]
pub enum SenderAdapter<T: Bufferable> {
    /// The in-memory channel buffer.
    InMemory(LimitedSender<T>),
}

impl<T: Bufferable> From<LimitedSender<T>> for SenderAdapter<T> {
    fn from(v: LimitedSender<T>) -> Self {
        Self::InMemory(v)
    }
}

impl<T> SenderAdapter<T>
where
    T: Bufferable,
{
    pub(crate) async fn send(&mut self, item: T) -> crate::Result<()> {
        match self {
            Self::InMemory(tx) => tx.send(item).await.map_err(Into::into),
        }
    }

    pub(crate) async fn try_send(&mut self, item: T) -> crate::Result<Option<T>> {
        match self {
            Self::InMemory(tx) => tx
                .try_send(item)
                .map(|()| None)
                .or_else(|e| Ok(Some(e.into_inner()))),
        }
    }

    pub(crate) async fn flush(&mut self) -> crate::Result<()> {
        match self {
            Self::InMemory(_) => Ok(()),
        }
    }

    pub fn capacity(&self) -> Option<usize> {
        match self {
            Self::InMemory(tx) => Some(tx.available_capacity()),
        }
    }
}

/// A buffer sender.
///
/// The sender handles sending events into the buffer, as well as the behavior around handling
/// events when the internal channel is full.
///
/// When creating a buffer sender/receiver pair, callers can specify the "when full" behavior of the
/// sender.  This controls how events are handled when the internal channel is full.  Three modes
/// are possible:
/// - block
/// - drop newest
/// - overflow
///
/// In "block" mode, callers are simply forced to wait until the channel has enough capacity to
/// accept the event.  In "drop newest" mode, any event being sent when the channel is full will be
/// dropped and proceed no further. In "overflow" mode, events will be sent to another buffer
/// sender.  Callers can specify the overflow sender to use when constructing their buffers initially.
///
/// TODO: We should eventually rework `BufferSender`/`BufferReceiver` so that they contain a vector
/// of the fields we already have here, but instead of cascading via calling into `overflow`, we'd
/// linearize the nesting instead, so that `BufferSender` would only ever be calling the underlying
/// `SenderAdapter` instances instead... which would let us get rid of the boxing and
/// `#[async_recursion]` stuff.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BufferSender<T: Bufferable> {
    base: SenderAdapter<T>,
    overflow: Option<Box<BufferSender<T>>>,
    when_full: WhenFull,
    instrumentation: Option<BufferUsageHandle>,
    #[derivative(Debug = "ignore")]
    send_duration: Option<Registered<BufferSendDuration>>,
}

impl<T: Bufferable> BufferSender<T> {
    /// Creates a new [`BufferSender`] wrapping the given channel sender.
    pub fn new(base: SenderAdapter<T>, when_full: WhenFull) -> Self {
        Self {
            base,
            overflow: None,
            when_full,
            instrumentation: None,
            send_duration: None,
        }
    }

    /// Creates a new [`BufferSender`] wrapping the given channel sender and overflow sender.
    pub fn with_overflow(base: SenderAdapter<T>, overflow: BufferSender<T>) -> Self {
        Self {
            base,
            overflow: Some(Box::new(overflow)),
            when_full: WhenFull::Overflow,
            instrumentation: None,
            send_duration: None,
        }
    }

    /// Configures this sender to instrument the items passing through it.
    pub fn with_usage_instrumentation(&mut self, handle: BufferUsageHandle) {
        self.instrumentation = Some(handle);
    }

    /// Configures this sender to instrument the send duration.
    pub fn with_send_duration_instrumentation(&mut self, stage: usize, span: &Span) {
        let _enter = span.enter();
        self.send_duration = Some(register(BufferSendDuration { stage }));
    }
}

impl<T: Bufferable> BufferSender<T> {
    #[async_recursion]
    pub async fn send(&mut self, item: T, send_reference: Option<Instant>) -> crate::Result<()> {
        let item_sizing = self
            .instrumentation
            .as_ref()
            .map(|_| (item.event_count(), item.size_of()));

        let mut sent_to_base = true;
        let mut was_dropped = false;
        match self.when_full {
            WhenFull::Block => self.base.send(item).await?,
            WhenFull::DropNewest => {
                if self.base.try_send(item).await?.is_some() {
                    was_dropped = true;
                }
            }
            WhenFull::Overflow => {
                if let Some(item) = self.base.try_send(item).await? {
                    sent_to_base = false;
                    self.overflow
                        .as_mut()
                        .unwrap_or_else(|| unreachable!("overflow must exist"))
                        .send(item, send_reference)
                        .await?;
                }
            }
        }

        if sent_to_base || was_dropped {
            if let (Some(send_duration), Some(send_reference)) =
                (self.send_duration.as_ref(), send_reference)
            {
                send_duration.emit(send_reference.elapsed());
            }
        }

        if let Some(instrumentation) = self.instrumentation.as_ref() {
            if let Some((item_count, item_size)) = item_sizing {
                if sent_to_base {
                    instrumentation.increment_received_event_count_and_byte_size(
                        item_count as u64,
                        item_size as u64,
                    );
                }

                if was_dropped {
                    instrumentation.increment_dropped_event_count_and_byte_size(
                        item_count as u64,
                        item_size as u64,
                        true,
                    );
                }
            }
        }

        Ok(())
    }

    #[async_recursion]
    pub async fn flush(&mut self) -> crate::Result<()> {
        self.base.flush().await?;
        if let Some(overflow) = self.overflow.as_mut() {
            overflow.flush().await?;
        }

        Ok(())
    }
}
