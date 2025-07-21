//! A collection of support structures that are used in the process of decoding
//! bytes into events.

mod error;
pub mod format;
pub mod framing;

use bytes::{Bytes, BytesMut};
pub use error::StreamDecodingError;
pub use format::{
    BoxedDeserializer,
    JsonDeserializer, JsonDeserializerConfig, JsonDeserializerOptions,
};
#[cfg(feature = "syslog")]
pub use format::{SyslogDeserializer, SyslogDeserializerConfig, SyslogDeserializerOptions};
pub use framing::{
    BoxedFramer, BoxedFramingError, BytesDecoder, BytesDecoderConfig, CharacterDelimitedDecoder,
    CharacterDelimitedDecoderConfig, CharacterDelimitedDecoderOptions,
    FramingError, LengthDelimitedDecoder,
    LengthDelimitedDecoderConfig, NewlineDelimitedDecoder, NewlineDelimitedDecoderConfig,
    NewlineDelimitedDecoderOptions, OctetCountingDecoder, OctetCountingDecoderConfig,
    OctetCountingDecoderOptions,
};
use smallvec::SmallVec;
use std::fmt::Debug;
use vector_config::configurable_component;
use vector_core::{
    config::{DataType, LogNamespace},
    event::Event,
    schema,
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

/// Framing configuration.
///
/// Framing handles how events are separated when encoded in a raw byte form, where each event is
/// a frame that must be prefixed, or delimited, in a way that marks where an event begins and
/// ends within the byte stream.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "method", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The framing method."))]
pub enum FramingConfig {
    /// Byte frames are passed through as-is according to the underlying I/O boundaries (for example, split between messages or stream segments).
    Bytes,

    /// Byte frames which are delimited by a chosen character.
    CharacterDelimited(CharacterDelimitedDecoderConfig),

    /// Byte frames which are prefixed by an unsigned big-endian 32-bit integer indicating the length.
    LengthDelimited(LengthDelimitedDecoderConfig),

    /// Byte frames which are delimited by a newline character.
    NewlineDelimited(NewlineDelimitedDecoderConfig),

    /// Byte frames according to the [octet counting][octet_counting] format.
    ///
    /// [octet_counting]: https://tools.ietf.org/html/rfc6587#section-3.4.1
    OctetCounting(OctetCountingDecoderConfig),
}

impl From<BytesDecoderConfig> for FramingConfig {
    fn from(_: BytesDecoderConfig) -> Self {
        Self::Bytes
    }
}

impl From<CharacterDelimitedDecoderConfig> for FramingConfig {
    fn from(config: CharacterDelimitedDecoderConfig) -> Self {
        Self::CharacterDelimited(config)
    }
}

impl From<LengthDelimitedDecoderConfig> for FramingConfig {
    fn from(config: LengthDelimitedDecoderConfig) -> Self {
        Self::LengthDelimited(config)
    }
}

impl From<NewlineDelimitedDecoderConfig> for FramingConfig {
    fn from(config: NewlineDelimitedDecoderConfig) -> Self {
        Self::NewlineDelimited(config)
    }
}

impl From<OctetCountingDecoderConfig> for FramingConfig {
    fn from(config: OctetCountingDecoderConfig) -> Self {
        Self::OctetCounting(config)
    }
}

impl FramingConfig {
    /// Build the `Framer` from this configuration.
    pub fn build(&self) -> Framer {
        match self {
            FramingConfig::Bytes => Framer::Bytes(BytesDecoderConfig.build()),
            FramingConfig::CharacterDelimited(config) => Framer::CharacterDelimited(config.build()),
            FramingConfig::LengthDelimited(config) => Framer::LengthDelimited(config.build()),
            FramingConfig::NewlineDelimited(config) => Framer::NewlineDelimited(config.build()),
            FramingConfig::OctetCounting(config) => Framer::OctetCounting(config.build()),
        }
    }
}

/// Produce byte frames from a byte stream / byte message.
#[derive(Debug, Clone)]
pub enum Framer {
    /// Uses a `BytesDecoder` for framing.
    Bytes(BytesDecoder),
    /// Uses a `CharacterDelimitedDecoder` for framing.
    CharacterDelimited(CharacterDelimitedDecoder),
    /// Uses a `LengthDelimitedDecoder` for framing.
    LengthDelimited(LengthDelimitedDecoder),
    /// Uses a `NewlineDelimitedDecoder` for framing.
    NewlineDelimited(NewlineDelimitedDecoder),
    /// Uses a `OctetCountingDecoder` for framing.
    OctetCounting(OctetCountingDecoder),
    /// Uses an opaque `Framer` implementation for framing.
    Boxed(BoxedFramer),
}

impl tokio_util::codec::Decoder for Framer {
    type Item = Bytes;
    type Error = BoxedFramingError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self {
            Framer::Bytes(framer) => framer.decode(src),
            Framer::CharacterDelimited(framer) => framer.decode(src),
            Framer::LengthDelimited(framer) => framer.decode(src),
            Framer::NewlineDelimited(framer) => framer.decode(src),
            Framer::OctetCounting(framer) => framer.decode(src),
            Framer::Boxed(framer) => framer.decode(src),
        }
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self {
            Framer::Bytes(framer) => framer.decode_eof(src),
            Framer::CharacterDelimited(framer) => framer.decode_eof(src),
            Framer::LengthDelimited(framer) => framer.decode_eof(src),
            Framer::NewlineDelimited(framer) => framer.decode_eof(src),
            Framer::OctetCounting(framer) => framer.decode_eof(src),
            Framer::Boxed(framer) => framer.decode_eof(src),
        }
    }
}

/// Configures how events are decoded from raw bytes. Note some decoders can also determine the event output
/// type (log, metric, trace).
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "codec", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The codec to use for decoding events."))]
pub enum DeserializerConfig {
    /// Decodes the raw bytes as [JSON][json].
    ///
    /// [json]: https://www.json.org/
    Json(JsonDeserializerConfig),
}

impl From<JsonDeserializerConfig> for DeserializerConfig {
    fn from(config: JsonDeserializerConfig) -> Self {
        Self::Json(config)
    }
}

impl DeserializerConfig {
    /// Build the `Deserializer` from this configuration.
    pub fn build(&self) -> vector_common::Result<Deserializer> {
        match self {
            DeserializerConfig::Json(config) => Ok(Deserializer::Json(config.build())),
        }
    }

    /// Return an appropriate default framer for the given deserializer
    pub fn default_stream_framing(&self) -> FramingConfig {
        match self {
            DeserializerConfig::Json(_) => {
                FramingConfig::NewlineDelimited(Default::default())
            }
        }
    }

    /// Returns an appropriate default framing config for the given deserializer with message based inputs.
    pub fn default_message_based_framing(&self) -> FramingConfig {
        match self {
            _ => FramingConfig::Bytes
        }
    }

    /// Return the type of event build by this deserializer.
    pub fn output_type(&self) -> DataType {
        match self {
            DeserializerConfig::Json(config) => config.output_type(),
        }
    }

    /// The schema produced by the deserializer.
    pub fn schema_definition(&self, log_namespace: LogNamespace) -> schema::Definition {
        match self {
            DeserializerConfig::Json(config) => config.schema_definition(log_namespace),
        }
    }

    /// Get the HTTP content type.
    pub const fn content_type(&self, framer: &FramingConfig) -> &'static str {
        match (&self, framer) {
            (
                DeserializerConfig::Json(_),
                FramingConfig::NewlineDelimited(_),
            ) => "application/x-ndjson",
            (
                DeserializerConfig::Json(_),
                FramingConfig::CharacterDelimited(CharacterDelimitedDecoderConfig {
                    character_delimited:
                        CharacterDelimitedDecoderOptions {
                            delimiter: b',',
                            max_length: Some(usize::MAX),
                        },
                }),
            ) => "application/json",
            (
                DeserializerConfig::Json(_),
                _,
            ) => "text/plain",
            #[cfg(feature = "syslog")]
            (DeserializerConfig::Syslog(_), _) => "text/plain",
        }
    }
}

/// Parse structured events from bytes.
#[derive(Clone)]
pub enum Deserializer {
    /// Uses a `JsonDeserializer` for deserialization.
    Json(JsonDeserializer),
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
            Deserializer::Json(deserializer) => deserializer.parse(bytes, log_namespace),
            Deserializer::Boxed(deserializer) => deserializer.parse(bytes, log_namespace),
        }
    }
}