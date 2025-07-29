//! A collection of support structures that are used in the process of decoding
//! bytes into events.

mod error;
pub mod format;
pub mod framing;


pub use error::StreamDecodingError;
pub use framing::{
    FramingError,
};

use std::fmt::Debug;


/// An error that occurred while decoding structured events from a byte stream /
/// byte messages.
#[derive(Debug)]
pub enum Error {
    /// The error occurred while parsing structured events from a byte frame.
    ParsingError(vector_common::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParsingError(error) => write!(formatter, "ParsingError({})", error),
        }
    }
}

impl std::error::Error for Error {}

impl StreamDecodingError for Error {
    fn can_continue(&self) -> bool {
        match self {

            Self::ParsingError(_) => true,
        }
    }
}
