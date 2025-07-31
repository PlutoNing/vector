use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;
use agent_config::configurable_component;

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

pub mod ascii_char {
    use serde::{de, Deserialize, Deserializer, Serializer};

    /// Deserialize an ASCII character as `u8`.
    ///
    /// # Errors
    ///
    /// If the item fails to be deserialized as a character, of the character to
    /// be deserialized is not part of the ASCII range, an error is returned.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<u8, D::Error>
    where
        D: Deserializer<'de>,
    {
        let character = char::deserialize(deserializer)?;
        if character.is_ascii() {
            Ok(character as u8)
        } else {
            Err(de::Error::custom(format!(
                "invalid character: {character}, expected character in ASCII range"
            )))
        }
    }

    /// Serialize an `u8` as ASCII character.
    ///
    /// # Errors
    ///
    /// Does not error.
    pub fn serialize<S>(character: &u8, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_char(*character as char)
    }
}

/// Configuration for character-delimited framing.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CharacterDelimitedEncoderOptions {
    /// The ASCII (7-bit) character that delimits byte sequences.
    #[configurable(metadata(docs::type_override = "ascii_char"))]
    #[serde(with = "ascii_char")]
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
