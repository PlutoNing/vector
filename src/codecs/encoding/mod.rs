mod config;
mod encoder;
mod transformer;

pub use config::{EncodingConfig, EncodingConfigWithFraming, SinkType};
pub use encoder::Encoder;
pub use transformer::{TimestampFormat, Transformer};
mod format;
mod framing;

use std::fmt::Debug;

use bytes::BytesMut;
pub use format::{
    get_serializer_schema_requirement,
    JsonSerializer, JsonSerializerConfig,
    TextSerializerConfig,TextSerializer
};
pub use framing::{
    BoxedFramer, BoxedFramingError, CharacterDelimitedEncoder,
    CharacterDelimitedEncoderConfig,
    NewlineDelimitedEncoder, NewlineDelimitedEncoderConfig,
};
use vector_config::configurable_component;
use vector_lib::{config::DataType, event::Event, schema};

/// An error that occurred while building an encoder.
// pub type BuildError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// An error that occurred while encoding structured events into byte frames.
#[derive(Debug)]
pub enum Error {
    /// The error occurred while encoding the byte frame boundaries.
    FramingError(BoxedFramingError),
    /// The error occurred while serializing a structured event into bytes.
    SerializingError(vector_common::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FramingError(error) => write!(formatter, "FramingError({})", error),
            Self::SerializingError(error) => write!(formatter, "SerializingError({})", error),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::FramingError(Box::new(error))
    }
}

/// Framing configuration.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
#[serde(tag = "method", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The framing method."))]
pub enum FramingConfig {
    /// Event data is delimited by a single ASCII (7-bit) character.
    CharacterDelimited(CharacterDelimitedEncoderConfig),
    /// Event data is delimited by a newline (LF) character.
    NewlineDelimited,
}

impl From<CharacterDelimitedEncoderConfig> for FramingConfig {
    fn from(config: CharacterDelimitedEncoderConfig) -> Self {
        Self::CharacterDelimited(config)
    }
}

impl From<NewlineDelimitedEncoderConfig> for FramingConfig {
    fn from(_: NewlineDelimitedEncoderConfig) -> Self {
        Self::NewlineDelimited
    }
}

impl FramingConfig {
    /// Build the `Framer` from this configuration.
    pub fn build(&self) -> Framer {
        match self {
            FramingConfig::CharacterDelimited(config) => Framer::CharacterDelimited(config.build()),
            FramingConfig::NewlineDelimited => {
                Framer::NewlineDelimited(NewlineDelimitedEncoderConfig.build())
            }
        }
    }
}

/// Produce a byte stream from byte frames.
#[derive(Debug, Clone)]
pub enum Framer {
    /// Uses a `CharacterDelimitedEncoder` for framing.
    CharacterDelimited(CharacterDelimitedEncoder),
    /// Uses a `NewlineDelimitedEncoder` for framing.
    NewlineDelimited(NewlineDelimitedEncoder),
    /// Uses an opaque `Encoder` implementation for framing.
    Boxed(BoxedFramer),
}

impl From<CharacterDelimitedEncoder> for Framer {
    fn from(encoder: CharacterDelimitedEncoder) -> Self {
        Self::CharacterDelimited(encoder)
    }
}

impl From<NewlineDelimitedEncoder> for Framer {
    fn from(encoder: NewlineDelimitedEncoder) -> Self {
        Self::NewlineDelimited(encoder)
    }
}

impl From<BoxedFramer> for Framer {
    fn from(encoder: BoxedFramer) -> Self {
        Self::Boxed(encoder)
    }
}

impl tokio_util::codec::Encoder<()> for Framer {
    type Error = BoxedFramingError;

    fn encode(&mut self, _: (), buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match self {
            Framer::CharacterDelimited(framer) => framer.encode((), buffer),
            Framer::NewlineDelimited(framer) => framer.encode((), buffer),
            Framer::Boxed(framer) => framer.encode((), buffer),
        }
    }
}

/// Serializer configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(tag = "codec", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "The codec to use for encoding events."))]
pub enum SerializerConfig {
    /// Encodes an event as [JSON][json].
    ///
    /// [json]: https://www.json.org/
    Json(JsonSerializerConfig),
    /// Plain text encoding.
    ///
    /// This encoding uses the `message` field of a log event. For metrics, it uses an
    /// encoding that resembles the Prometheus export format.
    ///
    /// Be careful if you are modifying your log events (for example, by using a `remap`
    /// transform) and removing the message field while doing additional parsing on it, as this
    /// could lead to the encoding emitting empty strings for the given event.
    Text(TextSerializerConfig),
}

impl From<JsonSerializerConfig> for SerializerConfig {
    fn from(config: JsonSerializerConfig) -> Self {
        Self::Json(config)
    }
}

impl From<TextSerializerConfig> for SerializerConfig {
    fn from(config: TextSerializerConfig) -> Self {
        Self::Text(config)
    }
}

impl SerializerConfig {
    /// Build the `Serializer` from this configuration.
    pub fn build(&self) -> Result<Serializer, Box<dyn std::error::Error + Send + Sync + 'static>> {
        match self {
            SerializerConfig::Json(config) => Ok(Serializer::Json(config.build())),
            SerializerConfig::Text(config) => Ok(Serializer::Text(config.build())),
        }
    }

    /// Return an appropriate default framer for the given serializer.
    pub fn default_stream_framing(&self) -> FramingConfig {
        match self {
            SerializerConfig::Json(_)
            | SerializerConfig::Text(_) => FramingConfig::NewlineDelimited
        }
    }

    /// The data type of events that are accepted by this `Serializer`.
    pub fn input_type(&self) -> DataType {
        match self {
            SerializerConfig::Json(config) => config.input_type(),/* host to file sink */
            SerializerConfig::Text(config) => config.input_type(),
        }
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        match self {
            SerializerConfig::Json(config) => config.schema_requirement(),
            SerializerConfig::Text(config) => config.schema_requirement(),
        }
    }
}

/// Serialize structured events as bytes.
#[derive(Debug, Clone)]
pub enum Serializer {
    /// Uses a `JsonSerializer` for serialization.
    Json(JsonSerializer),
    /// Uses a `TextSerializer` for serialization.
    Text(TextSerializer),
}

impl Serializer {
    /// Check if the serializer supports encoding an event to JSON via `Serializer::to_json_value`.
    pub fn supports_json(&self) -> bool {
        match self {
            Serializer::Json(_) => true,
            Serializer::Text(_) => false,
        }
    }

    /// Encode event and represent it as JSON value.
    ///
    /// # Panics
    ///
    /// Panics if the serializer does not support encoding to JSON. Call `Serializer::supports_json`
    /// if you need to determine the capability to encode to JSON at runtime.
    pub fn to_json_value(&self, event: Event) -> Result<serde_json::Value, vector_common::Error> {
        match self {
            Serializer::Json(serializer) => serializer.to_json_value(event),
            Serializer::Text(_) => {
                panic!("Serializer does not support JSON")
            }
        }
    }
}

impl From<JsonSerializer> for Serializer {
    fn from(serializer: JsonSerializer) -> Self {
        Self::Json(serializer)
    }
}


impl From<TextSerializer> for Serializer {
    fn from(serializer: TextSerializer) -> Self {
        Self::Text(serializer)
    }
}

impl tokio_util::codec::Encoder<Event> for Serializer {
    type Error = vector_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match self {
            Serializer::Json(serializer) => serializer.encode(event, buffer),
            Serializer::Text(serializer) => serializer.encode(event, buffer),
        }
    }
}
