//! A collection of codecs that can be used to transform between bytes streams /
//! byte messages, byte frames and structured events.

/// doc
pub mod fanout;
pub use fanout::{ControlChannel,ControlMessage,Fanout};
/// doc
pub mod sink;
pub mod global_options;