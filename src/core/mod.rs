//! A collection of codecs that can be used to transform between bytes streams /
//! byte messages, byte frames and structured events.

/// doc
pub mod fanout;
use agent_lib::config::EventCount;
pub use fanout::{ControlChannel,ControlMessage,Fanout};
/// doc
pub mod sink;
pub mod global_options;

pub mod rpc_cli;
pub mod rpc_server;
pub mod rpc_service;
use agent_config::configurable_component;

use std::fmt::Debug;

/// Event handling behavior when a buffer is full.
#[configurable_component]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WhenFull {
    /// Wait for free space in the buffer.
    ///
    /// This applies backpressure up the topology, signalling that sources should slow down
    /// the acceptance/consumption of events. This means that while no data is lost, data will pile
    /// up at the edge.
    #[default]
    Block,

    /// Drops the event instead of waiting for free space in buffer.
    ///
    /// The event will be intentionally dropped. This mode is typically used when performance is the
    /// highest priority, and it is preferable to temporarily lose events rather than cause a
    /// slowdown in the acceptance/consumption of events.
    DropNewest,

    /// Overflows to the next stage in the buffer topology.
    ///
    /// If the current buffer stage is full, attempt to send this event to the next buffer stage.
    /// That stage may also be configured overflow, and so on, but ultimately the last stage in a
    /// buffer topology must use one of the other handling behaviors. This means that next stage may
    /// potentially be able to buffer the event, but it may also block or drop the event.
    ///
    /// This mode can only be used when two or more buffer stages are configured.
    #[configurable(metadata(docs::hidden))]
    Overflow,
}

/// An item that can be buffered in memory.
///
/// This supertrait serves as the base trait for any item that can be pushed into a memory buffer.
/// It is a relaxed version of `Bufferable` that allows for items that are not `encodable` (e.g., `Instant`),
/// which is an unnecessary constraint for memory buffers.
pub trait InMemoryBufferable: EventCount + Debug + Send + Sync + Unpin + Sized + 'static {}

// Blanket implementation for anything that is already in-memory bufferable.
impl<T> InMemoryBufferable for T where T: EventCount + Debug + Send + Sync + Unpin + Sized + 'static {}

/// An item that can be buffered.
///
/// This supertrait serves as the base trait for any item that can be pushed into a buffer.
pub trait Bufferable: InMemoryBufferable {}

// Blanket implementation for anything that is already bufferable.
impl<T> Bufferable for T where T: InMemoryBufferable {}

