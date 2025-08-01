use std::{
    mem,
    pin::Pin,
    task::{ready, Context, Poll},
};

use async_recursion::async_recursion;
use futures::Stream;
use tokio::select;
use tokio_util::sync::ReusableBoxFuture;


use super::limited_queue::LimitedReceiver;
use crate::buffers::{
    buffer_usage_data::BufferUsageHandle,
};
use agent_lib::config::Bufferable;
/// Adapter for papering over various receiver backends.
#[derive(Debug)]
pub enum ReceiverAdapter<T: Bufferable> {
    /// The in-memory channel buffer.
    InMemory(LimitedReceiver<T>),
}

impl<T: Bufferable> From<LimitedReceiver<T>> for ReceiverAdapter<T> {
    fn from(v: LimitedReceiver<T>) -> Self {
        Self::InMemory(v)
    }
}

impl<T> ReceiverAdapter<T>
where
    T: Bufferable,
{
    pub(crate) async fn next(&mut self) -> Option<T> {
        match self {
            ReceiverAdapter::InMemory(rx) => rx.next().await,
        }
    }
}

/// A buffer receiver.
///
/// The receiver handles retrieving events from the buffer, regardless of the overall buffer configuration.
///
/// If a buffer was configured to operate in "overflow" mode, then the receiver will be responsible
/// for querying the overflow buffer as well.  The ordering of events when operating in "overflow"
/// is undefined, as the receiver will try to manage polling both its own buffer, as well as the
/// overflow buffer, in order to fairly balance throughput.
#[derive(Debug)]
pub struct BufferReceiver<T: Bufferable> {
    base: ReceiverAdapter<T>,
    overflow: Option<Box<BufferReceiver<T>>>,
    instrumentation: Option<BufferUsageHandle>,
}

impl<T: Bufferable> BufferReceiver<T> {
    /// Creates a new [`BufferReceiver`] wrapping the given channel receiver.
    pub fn new(base: ReceiverAdapter<T>) -> Self {
        Self {
            base,
            overflow: None,
            instrumentation: None,
        }
    }

    /// Creates a new [`BufferReceiver`] wrapping the given channel receiver and overflow receiver.
    pub fn with_overflow(base: ReceiverAdapter<T>, overflow: BufferReceiver<T>) -> Self {
        Self {
            base,
            overflow: Some(Box::new(overflow)),
            instrumentation: None,
        }
    }

    /// Configures this receiver to instrument the items passing through it.
    pub fn with_usage_instrumentation(&mut self, handle: BufferUsageHandle) {
        self.instrumentation = Some(handle);
    }

    #[async_recursion]
    pub async fn next(&mut self) -> Option<T> {
        // We want to poll both our base and overflow receivers without waiting for one or the
        // other to entirely drain before checking the other.  This ensures that we're fairly
        // servicing both receivers, and avoiding stalls in one or the other.
        //
        // This is primarily important in situations where an overflow-triggering event has
        // occurred, and is over, and items are flowing through the base receiver.  If we waited to
        // entirely drain the overflow receiver, we might cause another small stall of the pipeline
        // attached to the base receiver.
        let overflow = self.overflow.as_mut().map(Pin::new);

        let (item, from_base) = match overflow {
            None => match self.base.next().await {
                Some(item) => (item, true),
                None => return None,
            },
            Some(mut overflow) => {
                select! {
                    Some(item) = overflow.next() => (item, false),
                    Some(item) = self.base.next() => (item, true),
                    else => return None,
                }
            }
        };

        // If instrumentation is enabled, and we got the item from the base receiver, then and only
        // then do we track sending the event out.
        if let Some(handle) = self.instrumentation.as_ref() {
            if from_base {
                handle.increment_sent_event_count_and_byte_size(
                    item.event_count() as u64,
                    item.size_of() as u64,
                );
            }
        }

        Some(item)
    }

    pub fn into_stream(self) -> BufferReceiverStream<T> {
        BufferReceiverStream::new(self)
    }
}

enum StreamState<T: Bufferable> {
    Idle(BufferReceiver<T>),
    Polling,
    Closed,
}

pub struct BufferReceiverStream<T: Bufferable> {
    state: StreamState<T>,
    recv_fut: ReusableBoxFuture<'static, (Option<T>, BufferReceiver<T>)>,
}

impl<T: Bufferable> BufferReceiverStream<T> {
    pub fn new(receiver: BufferReceiver<T>) -> Self {
        Self {
            state: StreamState::Idle(receiver),
            recv_fut: ReusableBoxFuture::new(make_recv_future(None)),
        }
    }
}

impl<T: Bufferable> Stream for BufferReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match mem::replace(&mut self.state, StreamState::Polling) {
                s @ StreamState::Closed => {
                    self.state = s;
                    return Poll::Ready(None);
                }
                StreamState::Idle(receiver) => {
                    self.recv_fut.set(make_recv_future(Some(receiver)));
                }
                StreamState::Polling => {
                    let (result, receiver) = ready!(self.recv_fut.poll(cx));
                    self.state = if result.is_none() {
                        StreamState::Closed
                    } else {
                        StreamState::Idle(receiver)
                    };

                    return Poll::Ready(result);
                }
            }
        }
    }
}

async fn make_recv_future<T: Bufferable>(
    receiver: Option<BufferReceiver<T>>,
) -> (Option<T>, BufferReceiver<T>) {
    match receiver {
        None => panic!("invalid to poll future in uninitialized state"),
        Some(mut receiver) => {
            let result = receiver.next().await;
            (result, receiver)
        }
    }
}
