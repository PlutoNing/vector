use async_recursion::async_recursion;
use derivative::Derivative;

use agent_lib::config::{Bufferable, WhenFull};

use crate::sources::LimitedSender;

/// 不同buffer的包装
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
/// TODO: We should eventually rework `bufferSender`/`bufferReceiver` so that they contain a vector
/// of the fields we already have here, but instead of cascading via calling into `overflow`, we'd
/// linearize the nesting instead, so that `bufferSender` would only ever be calling the underlying
/// `SenderAdapter` instances instead... which would let us get rid of the boxing and
/// `#[async_recursion]` stuff.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct BufferSender<T: Bufferable> {
    base: SenderAdapter<T>,
    overflow: Option<Box<BufferSender<T>>>,
    when_full: WhenFull,
}

impl<T: Bufferable> BufferSender<T> {
    /// Creates a new [`BufferSender`] wrapping the given channel sender.
    pub fn new(base: SenderAdapter<T>, when_full: WhenFull) -> Self {
        Self {
            base,
            overflow: None,
            when_full,
        }
    }

    /// Creates a new [`BufferSender`] wrapping the given channel sender and overflow sender.
    pub fn with_overflow(base: SenderAdapter<T>, overflow: BufferSender<T>) -> Self {
        Self {
            base,
            overflow: Some(Box::new(overflow)),
            when_full: WhenFull::Overflow,
        }
    }
}

impl<T: Bufferable> BufferSender<T> {
    #[async_recursion]
    pub async fn send(&mut self, item: T) -> crate::Result<()> {
        match self.when_full {
            WhenFull::Block => self.base.send(item).await,
            WhenFull::DropNewest => self.base.try_send(item).await.map(|_| ()),
            WhenFull::Overflow => {
                if let Some(item) = self.base.try_send(item).await? {
                    self.overflow.as_mut().unwrap().send(item).await
                } else {
                    Ok(())
                }
            }
        }
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
