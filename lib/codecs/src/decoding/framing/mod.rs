//! A collection of framing methods that can be used to convert from byte frames
//! with defined boundaries to byte chunks.

#![deny(missing_docs)]

use std::{any::Any};



use tokio_util::codec::LinesCodecError;

use super::StreamDecodingError;

/// An error that occurred while producing byte frames from a byte stream / byte
/// message.
///
/// It requires conformance to `TcpError` so that we can determine whether the
/// error is recoverable or if trying to continue will lead to hanging up the
/// TCP source indefinitely.
pub trait FramingError: std::error::Error + StreamDecodingError + Send + Sync + Any {
    /// Coerces the error to a `dyn Any`.
    /// This is useful for downcasting the error to a concrete type
    fn as_any(&self) -> &dyn Any;
}

impl std::error::Error for BoxedFramingError {}

impl FramingError for std::io::Error {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

impl FramingError for LinesCodecError {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }
}

impl<T> From<T> for BoxedFramingError
where
    T: FramingError + 'static,
{
    fn from(value: T) -> Self {
        Box::new(value)
    }
}

/// A `Box` containing a `FramingError`.
pub type BoxedFramingError = Box<dyn FramingError>;

impl StreamDecodingError for BoxedFramingError {
    fn can_continue(&self) -> bool {
        self.as_ref().can_continue()
    }
}
