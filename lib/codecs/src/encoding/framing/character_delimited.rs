use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;
use vector_config::configurable_component;

use super::BoxedFramingError;

/// Config used to build a `CharacterDelimitedEncoder`.
#[configurable_component]
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CharacterDelimitedEncoderConfig {
    /// Options for the character delimited encoder.
    pub character_delimited: CharacterDelimitedEncoderOptions,
}

impl CharacterDelimitedEncoderConfig {
    /// Creates a `CharacterDelimitedEncoderConfig` with the specified delimiter.
    pub const fn new(delimiter: u8) -> Self {
        Self {
            character_delimited: CharacterDelimitedEncoderOptions { delimiter },
        }
    }

    /// Build the `CharacterDelimitedEncoder` from this configuration.
    pub const fn build(&self) -> CharacterDelimitedEncoder {
        CharacterDelimitedEncoder::new(self.character_delimited.delimiter)
    }
}

/// Configuration for character-delimited framing.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CharacterDelimitedEncoderOptions {
    /// The ASCII (7-bit) character that delimits byte sequences.
    #[configurable(metadata(docs::type_override = "ascii_char"))]
    #[serde(with = "vector_core::serde::ascii_char")]
    pub delimiter: u8,
}
/* 字符分割的文本编码器 */
/// An encoder for handling bytes that are delimited by (a) chosen character(s).
#[derive(Debug, Clone)]
pub struct CharacterDelimitedEncoder {
    /// The character that delimits byte sequences.
    pub delimiter: u8,
}

impl CharacterDelimitedEncoder {
    /// Creates a `CharacterDelimitedEncoder` with the specified delimiter.
    pub const fn new(delimiter: u8) -> Self {
        Self { delimiter }
    }
}

impl Encoder<()> for CharacterDelimitedEncoder {
    type Error = BoxedFramingError;

    fn encode(&mut self, _: (), buffer: &mut BytesMut) -> Result<(), BoxedFramingError> {
        buffer.put_u8(self.delimiter);
        Ok(())
    }
}