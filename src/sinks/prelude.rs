//! Prelude module for sinks which will re-export the symbols that most
//! stream based sinks are likely to use.

pub use async_trait::async_trait;
pub use futures::{future, future::BoxFuture, stream::BoxStream, FutureExt, StreamExt};
pub use tower::{Service, ServiceBuilder};
pub use agent_lib::config::EventCount;
pub use agent_lib::configurable::configurable_component;
pub use agent_lib::{
    config::Input,
    event::Value,
    schema::Requirement,
    ByteSizeOf, EstimatedJsonEncodedSizeOf,
};
// use crate::core::sink::{StreamSink, VectorSink};
pub use agent_lib::{
    finalization::{EventFinalizers, EventStatus, Finalizable},
    json_size::JsonSize,
};

pub use crate::{
    internal_event::CountByteSize,
    codecs::{Encoder, EncodingConfig, Transformer},
    config::{DataType, GenerateConfig, SinkConfig, SinkContext},
    event::{Event, LogEvent},
    sinks::util::{
        request_builder::default_request_builder_concurrency_limit,
        retries::{RetryAction, RetryLogic},
        Compression, Concurrency, NoDefaultsBatchSettings, SinkBatchSettings,
    },
    template::{Template, TemplateParseError, UnsignedIntTemplate},
};
