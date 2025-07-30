use bytes::{Buf, BufMut};
use std::error;

use vector_config::configurable_component;

use std::fmt::Debug;

use vector_common::byte_size_of::ByteSizeOf;
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
pub trait InMemoryBufferable:
    ByteSizeOf + EventCount + Debug + Send + Sync + Unpin + Sized + 'static
{
}

// Blanket implementation for anything that is already in-memory bufferable.
impl<T> InMemoryBufferable for T where
    T: ByteSizeOf + EventCount + Debug + Send + Sync + Unpin + Sized + 'static
{
}

/// An item that can be buffered.
///
/// This supertrait serves as the base trait for any item that can be pushed into a buffer.
pub trait Bufferable: InMemoryBufferable + Encodable {}

// Blanket implementation for anything that is already bufferable.
impl<T> Bufferable for T where T: InMemoryBufferable + Encodable {}

pub trait EventCount {
    fn event_count(&self) -> usize;
}

impl<T> EventCount for Vec<T>
where
    T: EventCount,
{
    fn event_count(&self) -> usize {
        self.iter().map(EventCount::event_count).sum()
    }
}

impl<T> EventCount for &T
where
    T: EventCount,
{
    fn event_count(&self) -> usize {
        (*self).event_count()
    }
}

#[track_caller]
pub fn spawn_named<T>(
    task: impl std::future::Future<Output = T> + Send + 'static,
    _name: &str,
) -> tokio::task::JoinHandle<T>
where
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    return tokio::task::Builder::new()
        .name(_name)
        .spawn(task)
        .expect("tokio task should spawn");

    #[cfg(not(tokio_unstable))]
    tokio::spawn(task)
}

/// An object that can encode and decode itself to and from a buffer.
///
/// # Metadata
///
/// While an encoding implementation is typically fixed i.e. `MyJsonEncoderType` only encodes and
/// decodes JSON, we want to provide the ability to change encodings and schemas over time without
/// fundamentally changing all of the code in the buffer implementations.
///
/// We provide the ability to express "metadata" about the encoding implementation such that any
/// relevant information can be included alongside the encoded object, and then passed back when
/// decoding is required.
///
/// ## Implementation
///
/// As designed, an implementor would define a primary encoding scheme, schema version, and so on,
/// that matched how an object would be encoded.  This is acquired from `get_metadata` by code that
/// depends on `Encodable` and will be stored alongside the encoded object.  When the encoded object
/// is later read back, and the caller wants to decode it, they would also read the metadata and do
/// two things: check that the metadata is still valid for this implementation by calling
/// `can_decode` and then pass it along to the `decode` call itself.
///
/// ## Verifying ability to decode
///
/// Calling `can_decode` first allows callers to check if the encoding implementation supports the
/// parameters used to encode the given object, which provides a means to allow for versioning,
/// schema evolution, and more.  Practically speaking, an implementation might bump the version of
/// its schema, but still support the old version for some time, and so `can_decode` might simply
/// check that the metadata represents the current version of the schema, or the last version, but
/// no other versions would be allowed.  When the old version of the schema was finally removed and
/// no longer supported, `can_decode` would no longer say it could decode any object whose metadata
/// referenced that old version.
///
/// The `can_decode` method is provided separately, instead of being lumped together in the `decode`
/// call, as a means to distinguish a lack of decoding support for a given metadata from a general
/// decoding failure.
///
/// ## Metadata-aware decoding
///
/// Likewise, the call to `decode` is given the metadata that was stored with the encoded object so
/// that it knows exactly what parameters were originally used and thus how it needs approach
/// decoding the object.
///
/// ## Metadata format and meaning
///
/// Ostensibly, the metadata would represent either some sort of numeric version identifier, or
/// could be used in a bitflags-style fashion, where each bit represents a particular piece of
/// information: encoding type, schema version, whether specific information is present in the
/// encoded object, and so on.
pub trait Encodable: Sized {
    type Metadata: Copy;
    type EncodeError: error::Error + Send + Sync + 'static;
    type DecodeError: error::Error + Send + Sync + 'static;

    /// Gets the version metadata associated with this encoding scheme.
    ///
    /// The value provided is ostensibly used as a bitfield-esque container, or potentially as a raw
    /// numeric version identifier, that identifies how a value was encoded, as well as any other
    /// information that may be necessary to successfully decode it.
    fn get_metadata() -> Self::Metadata;

    /// Whether or not this encoding scheme can understand and successfully decode a value based on
    /// the given version metadata that was bundled with the value.
    fn can_decode(metadata: Self::Metadata) -> bool;

    /// Attempts to encode this value into the given buffer.
    ///
    /// # Errors
    ///
    /// If there is an error while attempting to encode this value, an error variant will be
    /// returned describing the error.
    ///
    /// Practically speaking, based on the API, encoding errors should generally only occur if there
    /// is insufficient space in the buffer to fully encode this value.  However, this is not
    /// guaranteed.
    fn encode<B: BufMut>(self, buffer: &mut B) -> std::result::Result<(), Self::EncodeError>;

    /// Gets the encoded size, in bytes, of this value, if available.
    ///
    /// Not all types can know ahead of time how many bytes they will occupy when encoded, hence the
    /// fallibility of this method.
    fn encoded_size(&self) -> Option<usize> {
        None
    }

    /// Attempts to decode an instance of this type from the given buffer and metadata.
    ///
    /// # Errors
    ///
    /// If there is an error while attempting to decode a value from the given buffer, or the given
    /// metadata is not valid for the implementation, an error variant will be returned describing
    /// the error.
    fn decode<B: Buf + Clone>(
        metadata: Self::Metadata,
        buffer: B,
    ) -> std::result::Result<Self, Self::DecodeError>;
}
