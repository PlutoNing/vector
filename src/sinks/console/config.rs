use futures::{future, FutureExt};
use tokio::io;
use vector_lib::codecs::{
    encoding::{Framer, FramingConfig},
    JsonSerializerConfig,
};
use vector_lib::configurable::configurable_component;

use crate::{
    codecs::{Encoder, EncodingConfigWithFraming, SinkType},
    config::{AcknowledgementsConfig, GenerateConfig, Input, SinkConfig, SinkContext},
    sinks::{console::sink::WriterSink, Healthcheck, VectorSink},
};

/// The [standard stream][standard_streams] to write to.
///
/// [standard_streams]: https://en.wikipedia.org/wiki/Standard_streams
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(rename_all = "lowercase")]
pub enum Target {/* 表示是到stdout还是stderr */
    /// Write output to [STDOUT][stdout].
    ///
    /// [stdout]: https://en.wikipedia.org/wiki/Standard_streams#Standard_output_(stdout)
    #[derivative(Default)]
    Stdout,

    /// Write output to [STDERR][stderr].
    ///
    /// [stderr]: https://en.wikipedia.org/wiki/Standard_streams#Standard_error_(stderr)
    Stderr,
}

/// Configuration for the `console` sink.
#[configurable_component(sink(
    "console",
    "Display observability events in the console, which can be useful for debugging purposes."
))]
#[derive(Clone, Debug)]
#[serde(deny_unknown_fields)]
pub struct ConsoleSinkConfig {
    #[configurable(derived)]
    #[serde(default = "default_target")]
    pub target: Target, /*  */

    #[serde(flatten)]
    pub encoding: EncodingConfigWithFraming, /*  */

    #[configurable(derived)]
    #[serde(
        default,
        deserialize_with = "crate::serde::bool_or_struct",
        skip_serializing_if = "crate::serde::is_default"
    )]
    pub acknowledgements: AcknowledgementsConfig, /* 需要ack? */
}

const fn default_target() -> Target {
    Target::Stdout
}

impl GenerateConfig for ConsoleSinkConfig {
    /* 获取一个默认的? */
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self {
            target: Target::Stdout,
            encoding: (None::<FramingConfig>, JsonSerializerConfig::default()).into(),
            acknowledgements: Default::default(),
        })
        .unwrap()
    }
}
/* 构建一个sink?  开始构建指定的output了*/
#[async_trait::async_trait]
#[typetag::serde(name = "console")]
impl SinkConfig for ConsoleSinkConfig {
    /* 构建指定的sink */
    async fn build(&self, _cx: SinkContext) -> crate::Result<(VectorSink, Healthcheck)> {
        let transformer = self.encoding.transformer();
        /* Result<(Framer, Serializer)可能分别是字符分割编码器,和文本序列化器 */
        let (framer, serializer) = self.encoding.build(SinkType::StreamBased)?; 
        /* 把(framer, serializer)组合为encoder */
        let encoder = Encoder::<Framer>::new(framer, serializer);

        let sink: VectorSink = match self.target {
            Target::Stdout => VectorSink::from_event_streamsink(WriterSink {
                output: io::stdout(),
                transformer,
                encoder,
            }),
            Target::Stderr => VectorSink::from_event_streamsink(WriterSink {
                output: io::stderr(),
                transformer,
                encoder,
            }),
        };

        Ok((sink, future::ok(()).boxed()))
    }

    fn input(&self) -> Input {
        Input::new(self.encoding.config().1.input_type())
    }

    fn acknowledgements(&self) -> &AcknowledgementsConfig {
        &self.acknowledgements
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_config() {
        crate::test_util::test_generate_config::<ConsoleSinkConfig>();
    }
}
