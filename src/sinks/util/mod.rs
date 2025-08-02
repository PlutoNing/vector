// https://github.com/mcarton/rust-derivative/issues/112
#[allow(clippy::non_canonical_clone_impl)]
pub mod batch;
pub mod buffer;
pub mod builder;
pub mod compressor;
pub mod expiring_hash_map;
pub mod metadata;
pub mod normalizer;
pub mod processed_event;
pub mod request_builder;
pub mod retries;
pub mod service;
pub mod sink;
pub mod sqlite_service;
pub mod statistic;
use std::{borrow::Cow, fs::File, future::Future, io::Read, path::{Path, PathBuf}};

pub use batch::{
    Batch, BatchSettings, BatchSize, BulkSizeBasedDefaultBatchSettings, Merged,
    NoDefaultsBatchSettings, PushResult, RealtimeEventBasedDefaultBatchSettings,
    RealtimeSizeBasedDefaultBatchSettings, SinkBatchSettings, Unmerged,
};
pub use buffer::{json::BoxedRawValue, Compression};

pub use compressor::Compressor;
use futures::stream;
pub use normalizer::Normalizer;
use rand::{distr::Alphanumeric, rng, Rng};
pub use request_builder::IncrementalRequestBuilder;
pub use service::Concurrency;
pub use sink::StreamSink;
use snafu::Snafu;

use agent_lib::{event::Event, json_size::JsonSize, TimeZone};

use crate::{
    config::SinkContext,
    core::sink::VectorSink,
    event::EventFinalizers,
    sinks::file::{FileSink, FileSinkConfig},
};
use chrono::{FixedOffset, Offset, Utc};
pub fn random_string(len: usize) -> String {
    rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
}

pub fn lines_from_file<P: AsRef<Path>>(path: P) -> Vec<String> {
    trace!(message = "Reading file.", path = %path.as_ref().display());
    let mut file = File::open(path).unwrap();
    let mut output = String::new();
    file.read_to_string(&mut output).unwrap();
    output.lines().map(|s| s.to_owned()).collect()
}

pub fn temp_dir() -> PathBuf {
    let path = std::env::temp_dir();
    let dir_name = random_string(16);
    path.join(dir_name)
}

#[derive(Debug, Snafu)]
enum SinkBuildError {
    #[snafu(display("Missing host in address field"))]
    MissingHost,
    #[snafu(display("Missing port in address field"))]
    MissingPort,
}
/// Convenience wrapper for running sink tests
pub async fn assert_sink_compliance<T>(_tags: &[&str], f: impl Future<Output = T>) -> T {
    let result = f.await;
    result
}
pub async fn run_assert_sink(config: &FileSinkConfig, events: impl Iterator<Item = Event> + Send) {
    assert_sink_compliance(&[], async move {
        let sink = FileSink::new(config, SinkContext::default()).unwrap();
        VectorSink::from_event_streamsink(sink)
            .run(Box::pin(stream::iter(events.map(Into::into))))
            .await
            .expect("Running sink failed")
    })
    .await;
}

pub fn temp_file() -> PathBuf {
    let path = std::env::temp_dir();
    let file_name = random_string(16);
    path.join(file_name + ".log")
}
#[derive(Debug)]
pub struct EncodedEvent<I> {
    pub item: I,
    pub finalizers: EventFinalizers,
    pub byte_size: usize,
    pub json_byte_size: JsonSize,
}

impl<I> EncodedEvent<I> {
    /// Create a trivial input with no metadata. This method will be
    /// removed when all sinks are converted.
    pub fn new(item: I, byte_size: usize, json_byte_size: JsonSize) -> Self {
        Self {
            item,
            finalizers: Default::default(),
            byte_size,
            json_byte_size,
        }
    }

    // This should be:
    // ```impl<F, I: From<F>> From<EncodedEvent<F>> for EncodedEvent<I>```
    // however, the compiler rejects that due to conflicting
    // implementations of `From` due to the generic
    // ```impl<T> From<T> for T```
    pub fn from<F>(that: EncodedEvent<F>) -> Self
    where
        I: From<F>,
    {
        Self {
            item: I::from(that.item),
            finalizers: that.finalizers,
            byte_size: that.byte_size,
            json_byte_size: that.json_byte_size,
        }
    }

    /// Remap the item using an adapter
    pub fn map<T>(self, doit: impl Fn(I) -> T) -> EncodedEvent<T> {
        EncodedEvent {
            item: doit(self.item),
            finalizers: self.finalizers,
            byte_size: self.byte_size,
            json_byte_size: self.json_byte_size,
        }
    }
}

/// Joins namespace with name via delimiter if namespace is present.
pub fn encode_namespace<'a>(
    namespace: Option<&str>,
    delimiter: char,
    name: impl Into<Cow<'a, str>>,
) -> String {
    let name = name.into();
    namespace
        .map(|namespace| format!("{}{}{}", namespace, delimiter, name))
        .unwrap_or_else(|| name.into_owned())
}

/// Marker trait for types that can hold a batch of events
pub trait ElementCount {
    fn element_count(&self) -> usize;
}

impl<T> ElementCount for Vec<T> {
    fn element_count(&self) -> usize {
        self.len()
    }
}

pub fn timezone_to_offset(tz: TimeZone) -> Option<FixedOffset> {
    match tz {
        TimeZone::Local => Some(*Utc::now().with_timezone(&chrono::Local).offset()),
        TimeZone::Named(tz) => Some(Utc::now().with_timezone(&tz).offset().fix()),
    }
}
