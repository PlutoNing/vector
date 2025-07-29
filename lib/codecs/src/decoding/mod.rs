//! A collection of support structures that are used in the process of decoding
//! bytes into events.

mod error;
pub mod format;
pub mod framing;

use bytes::{Bytes, BytesMut};
pub use error::StreamDecodingError;
pub use format::{
    BoxedDeserializer,
};
#[cfg(feature = "syslog")]
pub use format::{SyslogDeserializer, SyslogDeserializerConfig, SyslogDeserializerOptions};
pub use framing::{
    BoxedFramer, BoxedFramingError,
    FramingError,
};
use smallvec::SmallVec;
use std::fmt::Debug;
use vector_core::{
    config::{LogNamespace},
    event::Event,

};

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


/// Produce byte frames from a byte stream / byte message.
#[derive(Debug, Clone)]
pub enum Framer {
    /// Uses an opaque `Framer` implementation for framing.
    Boxed(BoxedFramer),
}

impl tokio_util::codec::Decoder for Framer {
    type Item = Bytes;
    type Error = BoxedFramingError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self {
            Framer::Boxed(framer) => framer.decode(src),
        }
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self {
            Framer::Boxed(framer) => framer.decode_eof(src),
        }
    }
}

/// Parse structured events from bytes.
#[derive(Clone)]
pub enum Deserializer {
    /// Uses an opaque `Deserializer` implementation for deserialization.
    Boxed(BoxedDeserializer),
}

impl format::Deserializer for Deserializer {
    fn parse(
        &self,
        bytes: Bytes,
        log_namespace: LogNamespace,
    ) -> vector_common::Result<SmallVec<[Event; 1]>> {
        match self {
            Deserializer::Boxed(deserializer) => deserializer.parse(bytes, log_namespace),
        }
    }
}