use std::pin::Pin;

use futures::{
    task::{Context, Poll},
    {Stream, StreamExt},
};

const DEFAULT_CAPACITY: usize = 1024;

/// A stream combinator aimed at improving the performance of decoder streams under load.
///
/// This is similar in spirit to `StreamExt::ready_chunks`, but built specifically for the
/// particular result tuple returned by decoding streams. The more general `FoldReady` is left as
/// an exercise to the reader.
pub struct ReadyFrames<T, U, E> {
    inner: T,
    enqueued: Vec<U>,
    enqueued_size: usize,
    error_slot: Option<E>,
    enqueued_limit: usize,
}

impl<T, U, E> ReadyFrames<T, U, E>
where
    T: Stream<Item = Result<(U, usize), E>> + Unpin,
    U: Unpin,
    E: Unpin,
{
    /// Create a new `ReadyChunks` by wrapping a decoder stream, most commonly a `FramedRead`.
    pub fn new(inner: T) -> Self {
        Self::with_capacity(inner, DEFAULT_CAPACITY)
    }

    /// Create a new `ReadyChunks` with a specified capacity by wrapping a decoder stream, most
    /// commonly a `FramedRead`.
    ///
    /// The specified capacity is a soft limit, and chunks may be returned that contain more than
    /// that number of items.
    pub fn with_capacity(inner: T, cap: usize) -> Self {
        Self {
            inner,
            enqueued: Vec::with_capacity(cap),
            enqueued_size: 0,
            error_slot: None,
            enqueued_limit: cap,
        }
    }

    /// Returns a reference to the underlying stream.
    pub const fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Returns a mutable reference to the underlying stream.
    pub const fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    fn flush(&mut self) -> (Vec<U>, usize) {
        let frames = std::mem::take(&mut self.enqueued);
        let size = self.enqueued_size;
        self.enqueued_size = 0;
        (frames, size)
    }
}

impl<T, U, E> Stream for ReadyFrames<T, U, E>
where
    T: Stream<Item = Result<(U, usize), E>> + Unpin,
    U: Unpin,
    E: Unpin,
{
    type Item = Result<(Vec<U>, usize), E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(error) = self.error_slot.take() {
            return Poll::Ready(Some(Err(error)));
        }

        loop {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok((frame, size)))) => {
                    self.enqueued.push(frame);
                    self.enqueued_size += size;
                    if self.enqueued.len() >= self.enqueued_limit {
                        return Poll::Ready(Some(Ok(self.flush())));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    if self.enqueued.is_empty() {
                        return Poll::Ready(Some(Err(error)));
                    } else {
                        self.error_slot = Some(error);
                        return Poll::Ready(Some(Ok(self.flush())));
                    }
                }
                Poll::Ready(None) => {
                    if !self.enqueued.is_empty() {
                        return Poll::Ready(Some(Ok(self.flush())));
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Poll::Pending => {
                    if !self.enqueued.is_empty() {
                        return Poll::Ready(Some(Ok(self.flush())));
                    } else {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}