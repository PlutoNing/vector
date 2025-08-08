use crate::codecs::{
    NewlineDelimitedEncoder,
    encoding::{Framer, FramingConfig, Serializer, SerializerConfig},
      
};
use crate::codecs::encoding::CharacterDelimitedEncoder;
use agent_lib::configurable::configurable_component;
/* 比如说sink file时, 构建encoder config */
/// Encoding configuration.
#[configurable_component]
#[derive(Clone, Debug)]
/// Configures how events are encoded into raw bytes.
/// The selected encoding also determines which input types (logs, metrics, traces) are supported.
pub struct EncodingConfig { /* 分别负责序列化和转换 */
    #[serde(flatten)]
    encoding: SerializerConfig,
}

impl EncodingConfig {
    /// Creates a new `EncodingConfig` with the provided `SerializerConfig` and `transformer`.
    pub const fn new(encoding: SerializerConfig) -> Self {
        Self {
            encoding,
        }
    }

    /// Get the encoding configuration.
    pub const fn config(&self) -> &SerializerConfig {
        &self.encoding
    }
/* 根据类型新建一个序列化器, 比如文本的序列化器 */
    /// Build the `Serializer` for this config.
    pub fn build(&self) -> crate::Result<Serializer> {
        self.encoding.build()
    }
}

impl<T> From<T> for EncodingConfig
where
    T: Into<SerializerConfig>,
{
    fn from(encoding: T) -> Self {
        Self {
            encoding: encoding.into(),
        }
    }
}
/// Encoding configuration.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct EncodingConfigWithFraming {/* 里面有framer和encoder */
    #[configurable(derived)]
    framing: Option<FramingConfig>,

    #[configurable(derived)]
    encoding: EncodingConfig,
}

impl EncodingConfigWithFraming {
    /// Creates a new `EncodingConfigWithFraming` with the provided `FramingConfig`,
    /// `SerializerConfig` and `transformer`.
    pub const fn new(
        framing: Option<FramingConfig>,
        encoding: SerializerConfig,
    ) -> Self {
        Self {
            framing,
            encoding: EncodingConfig {
                encoding,
            },
        }
    }


    /// Get the encoding configuration.
    pub const fn config(&self) -> (&Option<FramingConfig>, &SerializerConfig) {
        (&self.framing, &self.encoding.encoding)
    }

    /// doc
    pub fn build(&self, sink_type: SinkType) -> crate::Result<(Framer, Serializer)> {
        let framer = self.framing.as_ref().map(|framing| framing.build());
        let serializer = self.encoding.build()?;

        let framer = match (framer, &serializer) {
            (Some(framer), _) => framer,
            (None, Serializer::Json(_)) => match sink_type {
                SinkType::StreamBased => NewlineDelimitedEncoder::default().into(),
                SinkType::MessageBased => CharacterDelimitedEncoder::new(b',').into(),
            },
        };

        Ok((framer, serializer))
    }
}

/// The way a sink processes outgoing events.
pub enum SinkType {
    /// Events are sent in a continuous stream.
    StreamBased,
    /// Events are sent in a batch as a message.
    MessageBased,
}

impl<F, S> From<(Option<F>, S)> for EncodingConfigWithFraming
where
    F: Into<FramingConfig>,
    S: Into<SerializerConfig>,
{
    fn from((framing, encoding): (Option<F>, S)) -> Self {
        Self {
            framing: framing.map(Into::into),
            encoding: encoding.into().into(),
        }
    }
}