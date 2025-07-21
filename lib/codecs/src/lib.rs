//! A collection of codecs that can be used to transform between bytes streams /
//! byte messages, byte frames and structured events.

#![deny(missing_docs)]
#![deny(warnings)]
pub mod decoding;
pub mod encoding;

pub use decoding::{
    BytesDecoder, BytesDecoderConfig,

    StreamDecodingError,
};
pub use encoding::{
    CharacterDelimitedEncoder, CharacterDelimitedEncoderConfig,
    JsonSerializer,
    JsonSerializerConfig,
    NewlineDelimitedEncoder, NewlineDelimitedEncoderConfig,
    TextSerializer, TextSerializerConfig,
};
use vector_config_macros::configurable_component;

/// The user configuration to choose the metric tag strategy.
#[configurable_component]
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum MetricTagValues {
    /// Tag values are exposed as single strings, the same as they were before this config
    /// option. Tags with multiple values show the last assigned value, and null values
    /// are ignored.
    #[default]
    Single,
    /// All tags are exposed as arrays of either string or null values.
    Full,
}
