//! A collection of formats that can be used to convert from structured events
//! to byte frames.

#![deny(missing_docs)]

pub mod common;
mod json;
mod text;
use std::fmt::Debug;

use dyn_clone::DynClone;
pub use json::{JsonSerializer, JsonSerializerConfig};
pub use text::{TextSerializer, TextSerializerConfig};
pub use common::get_serializer_schema_requirement;
use agent_lib::event::Event;

/// Serialize a structured event into a byte frame.
pub trait Serializer:
    tokio_util::codec::Encoder<Event, Error = agent_common::Error> + DynClone + Debug + Send + Sync
{
}

/// Default implementation for `Serializer`s that implement
/// `tokio_util::codec::Encoder`.
impl<Encoder> Serializer for Encoder where
    Encoder: tokio_util::codec::Encoder<Event, Error = agent_common::Error>
        + Clone
        + Debug
        + Send
        + Sync
{
}

dyn_clone::clone_trait_object!(Serializer);
