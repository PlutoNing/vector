use crate::codecs::get_serializer_schema_requirement;
use bytes::{BufMut, BytesMut};
use tokio_util::codec::Encoder;
use agent_config_macros::configurable_component;
use agent_lib::{config::DataType, event::Event, schema};

use crate::codecs::MetricTagValues;

/// Config used to build a `TextSerializer`.
#[configurable_component]
#[derive(Debug, Clone, Default)]
pub struct TextSerializerConfig {
    /// Controls how metric tag values are encoded.
    ///
    /// When set to `single`, only the last non-bare value of tags are displayed with the
    /// metric.  When set to `full`, all metric tags are exposed as separate assignments.
    #[serde(default, skip_serializing_if = "agent_lib::config::is_default")]
    pub metric_tag_values: MetricTagValues,
}
/* 文本的序列化 */
impl TextSerializerConfig {
    /// Creates a new `TextSerializerConfig`.
    pub const fn new(metric_tag_values: MetricTagValues) -> Self {
        Self { metric_tag_values }
    }

    /// Build the `TextSerializer` from this configuration.
    pub const fn build(&self) -> TextSerializer {
        TextSerializer::new(self.metric_tag_values)
    }

    /// The data type of events that are accepted by `TextSerializer`.
    pub fn input_type(&self) -> DataType {
        DataType::Log | DataType::Metric
    }

    /// The schema required by the serializer.
    pub fn schema_requirement(&self) -> schema::Requirement {
        get_serializer_schema_requirement()
    }
}

/// Serializer that converts a log to bytes by extracting the message key, or converts a metric
/// to bytes by calling its `Display` implementation.
///
/// This serializer exists to emulate the behavior of the `StandardEncoding::Text` for backwards
/// compatibility, until it is phased out completely.
#[derive(Debug, Clone)]
pub struct TextSerializer {
    metric_tag_values: MetricTagValues,
}
/* 新建一个文本的序列化 */
impl TextSerializer {
    /// Creates a new `TextSerializer`.
    pub const fn new(metric_tag_values: MetricTagValues) -> Self {
        Self { metric_tag_values }
    }
}

impl Encoder<Event> for TextSerializer {
    type Error = agent_common::Error;

    fn encode(&mut self, event: Event, buffer: &mut BytesMut) -> Result<(), Self::Error> {
        match event {
            Event::Log(log) => {
                if let Some(bytes) = log.get_message().map(|value| value.coerce_to_bytes()) {
                    buffer.put(bytes);
                }
            }
            Event::Metric(mut metric) => {
                if self.metric_tag_values == MetricTagValues::Single {
                    metric.reduce_tags_to_single();
                }
                let bytes = metric.to_string();
                buffer.put(bytes.as_ref());
            }
            Event::Trace(_) => {}
        };

        Ok(())
    }
}