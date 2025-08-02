pub mod console;
pub mod file;
pub mod sqlite;
pub mod util;

use agent_lib::event::{BatchNotifier, Event, EventArray, Metric, MetricKind, MetricTags, MetricValue};
use chrono::{DateTime, SubsecRound, Utc};
pub use console::*;
use futures::{stream, Stream};
use rand::{rng, Rng};
pub use sqlite::*;
pub use util::*;
use futures::stream::{StreamExt};   // ← 新增 StreamExt


pub fn random_metrics_with_stream_timestamp(
    count: usize,
    batch: Option<BatchNotifier>,
    tags: Option<MetricTags>,
    timestamp: DateTime<Utc>,
    timestamp_offset: std::time::Duration,
) -> (Vec<Event>, impl Stream<Item = EventArray>) {
    let events: Vec<_> = (0..count)
        .map(|index| {
            let ts = timestamp + (timestamp_offset * index as u32);
            Event::Metric(
                Metric::new(
                    format!("counter_{}", rng().random::<u32>()),
                    MetricKind::Incremental,
                    MetricValue::Counter {
                        value: index as f64,
                    },
                )
                .with_timestamp(Some(ts))
                .with_tags(tags.clone()),
            )
            // this ensures we get Origin Metadata, with an undefined service but that's ok.
            .with_source_type("a_source_like_none_other")
        })
        .collect();

    let stream = map_event_batch_stream(stream::iter(events.clone()), batch);
    (events, stream)
}
pub fn map_event_batch_stream(
    stream: impl Stream<Item = Event>,
    batch: Option<BatchNotifier>,
) -> impl Stream<Item = EventArray> {
    stream.map(move |event| event.with_batch_notifier_option(&batch).into())
}
pub fn random_metrics_with_stream(
    count: usize,
    batch: Option<BatchNotifier>,
    tags: Option<MetricTags>,
) -> (Vec<Event>, impl Stream<Item = EventArray>) {
    random_metrics_with_stream_timestamp(
        count,
        batch,
        tags,
        Utc::now().trunc_subsecs(3),
        std::time::Duration::from_secs(2),
    )
}