use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use tokio_util::codec::Encoder;

use super::{BoxedFramingError, CharacterDelimitedEncoder};

/// Config used to build a `NewlineDelimitedEncoder`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct NewlineDelimitedEncoderConfig;

impl NewlineDelimitedEncoderConfig {
    /// Creates a new `NewlineDelimitedEncoderConfig`.
    pub fn new() -> Self {
        Default::default()
    }

    /// Build the `NewlineDelimitedEncoder` from this configuration.
    pub fn build(&self) -> NewlineDelimitedEncoder {
        NewlineDelimitedEncoder::default()
    }
}

/// A codec for handling bytes that are delimited by (a) newline(s).
#[derive(Debug, Clone)]
pub struct NewlineDelimitedEncoder(CharacterDelimitedEncoder);
/* 新行encoder是什么 */
impl Default for NewlineDelimitedEncoder {
    fn default() -> Self {
        Self(CharacterDelimitedEncoder::new(b'\n'))
    }
}

impl Encoder<()> for NewlineDelimitedEncoder {
    type Error = BoxedFramingError;

    fn encode(&mut self, _: (), buffer: &mut BytesMut) -> Result<(), BoxedFramingError> {
        self.0.encode((), buffer)
    }
}