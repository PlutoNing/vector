//! Prelude module for sinks which will re-export the symbols that most
//! stream based sinks are likely to use.

pub use async_trait::async_trait;
pub use futures::{future, future::BoxFuture, stream::BoxStream, FutureExt, StreamExt};
pub use tower::{Service, ServiceBuilder};
pub use vector_lib::buffers::EventCount;
pub use vector_lib::configurable::configurable_component;
pub use vector_lib::stream::{BatcherSettings};
pub use vector_lib::{
    config::{AcknowledgementsConfig, Input},
    event::Value,
    partition::Partitioner,
    schema::Requirement,
    sink::{StreamSink, VectorSink},

    ByteSizeOf, EstimatedJsonEncodedSizeOf,
};
pub use vector_lib::{
    finalization::{EventFinalizers, EventStatus, Finalizable},
    internal_event::{CountByteSize, TaggedEventsSent},
    json_size::JsonSize,

};

pub use crate::{
    codecs::{Encoder, EncodingConfig, Transformer},
    config::{DataType, GenerateConfig, SinkConfig, SinkContext},
    event::{Event, LogEvent},
    sinks::{
        util::{


            request_builder::{default_request_builder_concurrency_limit},
            retries::{RetryAction, RetryLogic},

            BatchConfig, Compression, Concurrency, NoDefaultsBatchSettings,
            SinkBatchSettings,
        },
    },
    template::{Template, TemplateParseError, UnsignedIntTemplate},
};
