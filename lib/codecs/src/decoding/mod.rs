//! A collection of support structures that are used in the process of decoding
//! bytes into events.

mod error;
pub mod format;
pub mod framing;


pub use error::StreamDecodingError;
pub use framing::{
    BoxedFramingError,
    FramingError,
};

use std::fmt::Debug;


/// An error that occurred while decoding structured events from a byte stream /
/// byte messages.
#[derive(Debug)]
pub enum Error {
    /// The error occurred while producing byte frames from the byte stream /
    /// byte messages.
    FramingError(BoxedFramingError),
    /// The error occurred while parsing structured events from a byte frame.
    ParsingError(vector_common::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FramingError(error) => write!(formatter, "FramingError({})", error),
            Self::ParsingError(error) => write!(formatter, "ParsingError({})", error),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::FramingError(Box::new(error))
    }
}

impl StreamDecodingError for Error {
    fn can_continue(&self) -> bool {
        match self {
            Self::FramingError(error) => error.can_continue(),
            Self::ParsingError(_) => true,
        }
    }
}
