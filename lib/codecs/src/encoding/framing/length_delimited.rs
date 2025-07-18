use bytes::BytesMut;
use derivative::Derivative;
use tokio_util::codec::{Encoder, LengthDelimitedCodec};
use vector_config::configurable_component;

use crate::common::length_delimited::LengthDelimitedCoderOptions;

use super::BoxedFramingError;

/// Config used to build a `LengthDelimitedEncoder`.
#[configurable_component]
#[derive(Debug, Clone, Derivative, Eq, PartialEq)]
#[derivative(Default)]
pub struct LengthDelimitedEncoderConfig {
    /// Options for the length delimited decoder.
    #[serde(skip_serializing_if = "vector_core::serde::is_default")]
    pub length_delimited: LengthDelimitedCoderOptions,
}

impl LengthDelimitedEncoderConfig {
    /// Build the `LengthDelimitedEncoder` from this configuration.
    pub fn build(&self) -> LengthDelimitedEncoder {
        LengthDelimitedEncoder::new(&self.length_delimited)
    }
}

/// An encoder for handling bytes that are delimited by a length header.
#[derive(Debug, Clone)]
pub struct LengthDelimitedEncoder {
    codec: LengthDelimitedCodec,
    inner_buffer: BytesMut,
}

impl LengthDelimitedEncoder {
    /// Creates a new `LengthDelimitedEncoder`.
    pub fn new(config: &LengthDelimitedCoderOptions) -> Self {
        Self {
            codec: config.build_codec(),
            inner_buffer: BytesMut::new(),
        }
    }
}

impl Default for LengthDelimitedEncoder {
    fn default() -> Self {
        Self {
            codec: LengthDelimitedCodec::new(),
            inner_buffer: BytesMut::new(),
        }
    }
}

impl Encoder<()> for LengthDelimitedEncoder {
    type Error = BoxedFramingError;

    fn encode(&mut self, _: (), buffer: &mut BytesMut) -> Result<(), BoxedFramingError> {
        self.inner_buffer.clear();
        self.inner_buffer.extend_from_slice(buffer);
        buffer.clear();
        let bytes = self.inner_buffer.split().freeze();
        self.codec.encode(bytes, buffer)?;
        Ok(())
    }
}