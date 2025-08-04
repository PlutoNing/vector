use std::{error::Error, num::NonZeroUsize};

use async_trait::async_trait;
use snafu::{ResultExt, Snafu};
use tracing::Span;

use crate::buffers::buffer_usage_data::{BufferUsage, BufferUsageHandle};
use crate::buffers::topology::channel::{BufferReceiver, BufferSender,ReceiverAdapter, SenderAdapter};
use crate::buffers::variants::{MemoryBuffer};
use agent_lib::config::{Bufferable,WhenFull,};
/// Value that can be used as a stage in a buffer topology.
#[async_trait]
pub trait IntoBuffer<T: Bufferable>: Send {
    /// Gets whether or not this buffer stage provides its own instrumentation, or if it should be
    /// instrumented from the outside.
    ///
    /// As some buffer stages, like the in-memory channel, never have a chance to catch the values
    /// in the middle of the channel without introducing an unnecessary hop, [`BufferSender`] and
    /// [`BufferReceiver`] can be configured to instrument all events flowing through directly.
    ///
    /// When instrumentation is provided in this way, [`agent_common::byte_size_of::ByteSizeOf`]
    ///  is used to calculate the size of the event going both into and out of the buffer.
    fn provides_instrumentation(&self) -> bool {
        false
    }

    /// Converts this value into a sender and receiver pair suitable for use in a buffer topology.
    async fn into_buffer_parts(
        self: Box<Self>,
        usage_handle: BufferUsageHandle,
    ) -> Result<(SenderAdapter<T>, ReceiverAdapter<T>), Box<dyn Error + Send + Sync>>;
}

#[derive(Debug, Snafu)]
pub enum TopologyError {
    #[snafu(display("buffer topology cannot be empty"))]
    EmptyTopology,
    #[snafu(display(
        "stage {} configured with block/drop newest behavior in front of subsequent stage",
        stage_idx
    ))]
    NextStageNotUsed { stage_idx: usize },
    #[snafu(display("last stage in buffer topology cannot be set to overflow mode"))]
    OverflowWhenLast,
    #[snafu(display("failed to build individual stage {}: {}", stage_idx, source))]
    FailedToBuildStage {
        stage_idx: usize,
        source: Box<dyn Error + Send + Sync>,
    },
    #[snafu(display(
        "multiple components with segmented acknowledgements cannot be used in the same buffer"
    ))]
    StackedAcks,
}

/* 里面包裹一个buffer(实现了into buffer接口, 目前存在内存buffer)
用于加入buffer topology的stages成员 */
struct TopologyStage<T: Bufferable> {
    untransformed: Box<dyn IntoBuffer<T>>,
    when_full: WhenFull,
}

/// Builder for constructing buffer topologies.
pub struct TopologyBuilder<T: Bufferable> {
    /* 所使用的buffer type, 目前仅实现内存buffer */
    stages: Vec<TopologyStage<T>>,
}

impl<T: Bufferable> TopologyBuilder<T> {
    /// 把stage这个buffer加入buffer topology.
    pub fn stage<S>(&mut self, stage: S, when_full: WhenFull) -> &mut Self
    where
        S: IntoBuffer<T> + 'static,
    {
        self.stages.push(TopologyStage {
            untransformed: Box::new(stage),
            when_full,
        });
        self
    }

    /// 构建stages里的buffer拓扑
    pub async fn build(
        self,
        buffer_id: String,
        span: Span,
    ) -> Result<(BufferSender<T>, BufferReceiver<T>), TopologyError> {
        // We pop stages off in reverse order to build from the inside out.
        let mut buffer_usage = BufferUsage::from_span(span.clone());
        let mut current_stage = None; /* 里面是buffer的sender和receiver */

        for (stage_idx, stage) in self.stages.into_iter().enumerate().rev() {
            // Make sure the stage is valid for our current builder state.
            match stage.when_full {
                // The innermost stage can't be set to overflow, there's nothing else to overflow _to_.
                WhenFull::Overflow => {
                    if current_stage.is_none() {
                        return Err(TopologyError::OverflowWhenLast);
                    }
                }
                // If there's already an inner stage, then blocking or dropping the newest events
                // doesn't no sense.  Overflowing is the only valid transition to another stage.
                WhenFull::Block | WhenFull::DropNewest => {
                    if current_stage.is_some() {
                        return Err(TopologyError::NextStageNotUsed { stage_idx });
                    }
                }
            }

            // Create the buffer usage handle for this stage and initialize it as we create the
            // sender/receiver.  This is slightly awkward since we just end up actually giving
            // the handle to the `BufferSender`/`BufferReceiver` wrappers, but that's the price we
            // have to pay for letting each stage function in an opaque way when wrapped.
            let usage_handle = buffer_usage.add_stage(stage_idx);
            let provides_instrumentation = stage.untransformed.provides_instrumentation();
            let (sender, receiver) = stage
                .untransformed
                .into_buffer_parts(usage_handle.clone())
                .await
                .context(FailedToBuildStageSnafu { stage_idx })?;

            let (mut sender, mut receiver) = match current_stage.take() {
                None => (
                    BufferSender::new(sender, stage.when_full),
                    BufferReceiver::new(receiver),
                ),
                Some((current_sender, current_receiver)) => (
                    BufferSender::with_overflow(sender, current_sender),
                    BufferReceiver::with_overflow(receiver, current_receiver),
                ),
            };

            sender.with_send_duration_instrumentation(stage_idx, &span);
            if !provides_instrumentation {
                sender.with_usage_instrumentation(usage_handle.clone());
                receiver.with_usage_instrumentation(usage_handle);
            }

            current_stage = Some((sender, receiver));
        }

        let (sender, receiver) = current_stage.ok_or(TopologyError::EmptyTopology)?;

        // Install the buffer usage handler since we successfully created the buffer topology.  This
        // spawns it in the background and periodically emits aggregated metrics about each of the
        // buffer stages.
        buffer_usage.install(buffer_id.as_str());

        Ok((sender, receiver))
    }
}

impl<T: Bufferable> TopologyBuilder<T> {
    /// Creates a memory-only buffer topology.
    ///
    /// The overflow mode (i.e. `WhenFull`) can be configured to either block or drop the newest
    /// values, but cannot be configured to use overflow mode.  If overflow mode is selected, it
    /// will be changed to blocking mode.
    ///
    /// This is a convenience method for `vector` as it is used for inter-transform channels, and we
    /// can simplifying needing to require callers to do all the boilerplate to create the builder,
    /// create the stage, installing buffer usage metrics that aren't required, and so on.
    ///
    #[allow(clippy::print_stderr)]
    pub async fn standalone_memory(
        max_events: NonZeroUsize,
        when_full: WhenFull,
        receiver_span: &Span,
    ) -> (BufferSender<T>, BufferReceiver<T>) {
        let usage_handle = BufferUsageHandle::noop();

        let memory_buffer = Box::new(MemoryBuffer::new(max_events));
        let (sender, receiver) = memory_buffer
            .into_buffer_parts(usage_handle.clone())
            .await
            .unwrap_or_else(|_| unreachable!("should not fail to directly create a memory buffer"));

        let mode = match when_full {
            WhenFull::Overflow => WhenFull::Block,
            m => m,
        };
        let mut sender = BufferSender::new(sender, mode);
        sender.with_send_duration_instrumentation(0, receiver_span);
        let receiver = BufferReceiver::new(receiver);

        (sender, receiver)
    }
}

impl<T: Bufferable> Default for TopologyBuilder<T> {
    fn default() -> Self {
        Self { stages: Vec::new() }
    }
}