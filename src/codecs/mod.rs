//! A collection of codecs that can be used to transform between bytes streams /
//! byte messages, byte frames and structured events.

#![deny(missing_docs)]

mod encoding;

pub use encoding::{
    Encoder, EncodingConfig, EncodingConfigWithFraming, Framer, FramingConfig, JsonSerializer,
    JsonSerializerConfig, NewlineDelimitedEncoder, Serializer, SerializerConfig, SinkType,
};
