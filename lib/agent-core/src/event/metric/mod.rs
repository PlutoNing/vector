#[cfg(feature = "vrl")]
use std::convert::TryFrom;

#[cfg(feature = "vrl")]
use vrl::compiler::value::VrlValueConvert;

use std::{
    convert::AsRef,
    fmt::{self, Display, Formatter},
    num::NonZeroU32,
};

use agent_common::{byte_size_of::ByteSizeOf, json_size::JsonSize};
use agent_config::configurable_component;
use chrono::{DateTime, Utc};

use super::{
    estimated_json_encoded_size_of::EstimatedJsonEncodedSizeOf, EventMetadata,
};

mod data;
pub use self::data::*;

mod series;
pub use self::series::*;

mod tags;
pub use self::tags::*;

mod value;
pub use self::value::*;

#[macro_export]
macro_rules! metric_tags {
    () => { $crate::event::MetricTags::default() };

    ($($key:expr => $value:expr,)+) => { $crate::metric_tags!($($key => $value),+) };

    ($($key:expr => $value:expr),*) => {
        [
            $( ($key.into(), $crate::event::metric::TagValue::from($value)), )*
        ].into_iter().collect::<$crate::event::MetricTags>()
    };
}

/// A metric.
#[configurable_component]
#[derive(Clone, Debug, PartialEq)]
pub struct Metric {
    #[serde(flatten)]
    pub(super) series: MetricSeries,

    #[serde(flatten)]
    pub(super) data: MetricData,

    /// Internal event metadata.
    #[serde(skip, default = "EventMetadata::default")]
    metadata: EventMetadata,
}
/* 表示一个指标 */
impl Metric {
    /* 有调用 */
    /// Creates a new `Metric` with the given `name`, `kind`, and `value`.
    pub fn new<T: Into<String>>(name: T, kind: MetricKind, value: MetricValue) -> Self {
        Self::new_with_metadata(name, kind, value, EventMetadata::default())
    }
    /* 有调用 */
    /// Creates a new `Metric` with the given `name`, `kind`, `value`, and `metadata`.
    pub fn new_with_metadata<T: Into<String>>(
        name: T,
        kind: MetricKind,
        value: MetricValue,
        metadata: EventMetadata,
    ) -> Self {
        Self {
            series: MetricSeries {
                name: MetricName {
                    name: name.into(),
                    namespace: None,
                },
                tags: None,
            },
            data: MetricData {
                time: MetricTime {
                    timestamp: None,
                    interval_ms: None,
                },
                kind,
                value,
            },
            metadata,
        }
    }

    /// Consumes this metric, returning it with an updated series based on the given `name`.
    #[inline]
    #[must_use]
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.series.name.name = name.into();
        self
    }

    /// Consumes this metric, returning it with an updated series based on the given `namespace`.
    #[inline]
    #[must_use]
    pub fn with_namespace<T: Into<String>>(mut self, namespace: Option<T>) -> Self {
        self.series.name.namespace = namespace.map(Into::into);
        self
    }

    /// Consumes this metric, returning it with an updated timestamp.
    #[inline]
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: Option<DateTime<Utc>>) -> Self {
        self.data.time.timestamp = timestamp;
        self
    }

    /// Consumes this metric, returning it with an updated interval.
    #[inline]
    #[must_use]
    pub fn with_interval_ms(mut self, interval_ms: Option<NonZeroU32>) -> Self {
        self.data.time.interval_ms = interval_ms;
        self
    }

    /// Consumes this metric, returning it with an updated series based on the given `tags`.
    #[inline]
    #[must_use]
    pub fn with_tags(mut self, tags: Option<MetricTags>) -> Self {
        self.series.tags = tags;
        self
    }

    /// Consumes this metric, returning it with an updated value.
    #[inline]
    #[must_use]
    pub fn with_value(mut self, value: MetricValue) -> Self {
        self.data.value = value;
        self
    }

    /// Gets a reference to the series of this metric.
    ///
    /// The "series" is the name of the metric itself, including any tags. In other words, it is the unique identifier
    /// for a metric, although metrics of different values (counter vs gauge) may be able to co-exist in outside metrics
    /// implementations with identical series.
    pub fn series(&self) -> &MetricSeries {
        &self.series
    }

    /// Gets a reference to the data of this metric.
    pub fn data(&self) -> &MetricData {
        &self.data
    }

    /// Gets a mutable reference to the data of this metric.
    pub fn data_mut(&mut self) -> &mut MetricData {
        &mut self.data
    }

    /// Gets a reference to the metadata of this metric.
    pub fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }

    /// Gets a mutable reference to the metadata of this metric.
    pub fn metadata_mut(&mut self) -> &mut EventMetadata {
        &mut self.metadata
    }

    /// Gets a reference to the name of this metric.
    ///
    /// The name of the metric does not include the namespace or tags.
    #[inline]
    pub fn name(&self) -> &str {
        &self.series.name.name
    }

    /// Gets a reference to the namespace of this metric, if it exists.
    #[inline]
    pub fn namespace(&self) -> Option<&str> {
        self.series.name.namespace.as_deref()
    }

    /// Takes the namespace out of this metric, if it exists, leaving it empty.
    #[inline]
    pub fn take_namespace(&mut self) -> Option<String> {
        self.series.name.namespace.take()
    }

    /// Gets a reference to the tags of this metric, if they exist.
    #[inline]
    pub fn tags(&self) -> Option<&MetricTags> {
        self.series.tags.as_ref()
    }

    /// Gets a mutable reference to the tags of this metric, if they exist.
    #[inline]
    pub fn tags_mut(&mut self) -> Option<&mut MetricTags> {
        self.series.tags.as_mut()
    }

    /// Gets a reference to the timestamp of this metric, if it exists.
    #[inline]
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.data.time.timestamp
    }

    /// Gets a reference to the interval (in milliseconds) covered by this metric, if it exists.
    #[inline]
    pub fn interval_ms(&self) -> Option<NonZeroU32> {
        self.data.time.interval_ms
    }

    /// Gets a reference to the value of this metric.
    #[inline]
    pub fn value(&self) -> &MetricValue {
        &self.data.value
    }

    /// Gets a mutable reference to the value of this metric.
    #[inline]
    pub fn value_mut(&mut self) -> &mut MetricValue {
        &mut self.data.value
    }

    /// Gets the kind of this metric.
    #[inline]
    pub fn kind(&self) -> MetricKind {
        self.data.kind
    }

    /// Gets the time information of this metric.
    #[inline]
    pub fn time(&self) -> MetricTime {
        self.data.time
    }

    /// Decomposes a `Metric` into its individual parts.
    #[inline]
    pub fn into_parts(self) -> (MetricSeries, MetricData, EventMetadata) {
        (self.series, self.data, self.metadata)
    }

    /// Creates a `Metric` directly from the raw components of another metric.
    #[inline]
    pub fn from_parts(series: MetricSeries, data: MetricData, metadata: EventMetadata) -> Self {
        Self {
            series,
            data,
            metadata,
        }
    }

    /// Consumes this metric, returning it as an absolute metric.
    ///
    /// If the metric was already absolute, nothing is changed.
    #[must_use]
    pub fn into_absolute(self) -> Self {
        Self {
            series: self.series,
            data: self.data.into_absolute(),
            metadata: self.metadata,
        }
    }

    /// Consumes this metric, returning it as an incremental metric.
    ///
    /// If the metric was already incremental, nothing is changed.
    #[must_use]
    pub fn into_incremental(self) -> Self {
        Self {
            series: self.series,
            data: self.data.into_incremental(),
            metadata: self.metadata,
        }
    }

    /// Removes a tag from this metric, returning the value of the tag if the tag was previously in the metric.
    pub fn remove_tag(&mut self, key: &str) -> Option<String> {
        self.series.remove_tag(key)
    }

    /// Removes all the tags.
    pub fn remove_tags(&mut self) {
        self.series.remove_tags();
    }

    /// Returns `true` if `name` tag is present, and matches the provided `value`
    pub fn tag_matches(&self, name: &str, value: &str) -> bool {
        self.tags()
            .filter(|t| t.get(name).filter(|v| *v == value).is_some())
            .is_some()
    }

    /// Returns the string value of a tag, if it exists
    pub fn tag_value(&self, name: &str) -> Option<String> {
        self.tags().and_then(|t| t.get(name)).map(ToOwned::to_owned)
    }

    /// Inserts a tag into this metric.
    ///
    /// If the metric did not have this tag, `None` will be returned. Otherwise, `Some(String)` will be returned,
    /// containing the previous value of the tag.
    ///
    /// *Note:* This will create the tags map if it is not present.
    pub fn replace_tag(&mut self, name: String, value: String) -> Option<String> {
        self.series.replace_tag(name, value)
    }

    pub fn set_multi_value_tag(
        &mut self,
        name: String,
        values: impl IntoIterator<Item = TagValue>,
    ) {
        self.series.set_multi_value_tag(name, values);
    }

    /// Zeroes out the data in this metric.
    pub fn zero(&mut self) {
        self.data.zero();
    }

    /// Adds the data from the `other` metric to this one.
    ///
    /// The other metric must be incremental and contain the same value type as this one.
    #[must_use]
    pub fn add(&mut self, other: impl AsRef<MetricData>) -> bool {
        self.data.add(other.as_ref())
    }

    /// Updates this metric by adding the data from `other`.
    #[must_use]
    pub fn update(&mut self, other: impl AsRef<MetricData>) -> bool {
        self.data.update(other.as_ref())
    }

    /// Subtracts the data from the `other` metric from this one.
    ///
    /// The other metric must contain the same value type as this one.
    #[must_use]
    pub fn subtract(&mut self, other: impl AsRef<MetricData>) -> bool {
        self.data.subtract(other.as_ref())
    }

    /// Reduces all the tag values to their single value, discarding any for which that value would
    /// be null. If the result is empty, the tag set is dropped.
    pub fn reduce_tags_to_single(&mut self) {
        if let Some(tags) = &mut self.series.tags {
            tags.reduce_to_single();
            if tags.is_empty() {
                self.series.tags = None;
            }
        }
    }
}

impl AsRef<MetricData> for Metric {
    fn as_ref(&self) -> &MetricData {
        &self.data
    }
}

impl AsRef<MetricValue> for Metric {
    fn as_ref(&self) -> &MetricValue {
        &self.data.value
    }
}

impl Display for Metric {
    /// Display a metric using something like Prometheus' text format:
    ///
    /// ```text
    /// TIMESTAMP NAMESPACE_NAME{TAGS} KIND DATA
    /// ```
    ///
    /// TIMESTAMP is in ISO 8601 format with UTC time zone.
    ///
    /// KIND is either `=` for absolute metrics, or `+` for incremental
    /// metrics.
    ///
    /// DATA is dependent on the type of metric, and is a simplified
    /// representation of the data contents. In particular,
    /// distributions, histograms, and summaries are represented as a
    /// list of `X@Y` words, where `X` is the rate, count, or quantile,
    /// and `Y` is the value or bucket.
    ///
    /// example:
    /// ```text
    /// 2020-08-12T20:23:37.248661343Z vector_received_bytes_total{component_kind="sink",component_type="blackhole"} = 6391
    /// ```
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        /* 有调用 */
        if let Some(timestamp) = &self.data.time.timestamp {
            write!(fmt, "{timestamp:?} ")?;
        }
        let kind = match self.data.kind {
            MetricKind::Absolute => '=',
            MetricKind::Incremental => '+',
        };
        self.series.fmt(fmt)?;
        write!(fmt, " {kind} ")?;
        self.data.value.fmt(fmt)
    }
}

impl ByteSizeOf for Metric {
    /* 有调用 */
    fn allocated_bytes(&self) -> usize {
        self.series.allocated_bytes()
            + self.data.allocated_bytes()
            + self.metadata.allocated_bytes()
    }
}

impl EstimatedJsonEncodedSizeOf for Metric {
    /* 有调用 */
    fn estimated_json_encoded_size_of(&self) -> JsonSize {
        // TODO: For now we're using the in-memory representation of the metric, but we'll convert
        // this to actually calculate the JSON encoded size in the near future.
        self.size_of().into()
    }
}

/// 指标可以是绝对值或增量值
#[configurable_component]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    /// Incremental metric.
    Incremental,

    /// Absolute metric.
    Absolute,
}

#[cfg(feature = "vrl")]
impl TryFrom<vrl::value::Value> for MetricKind {
    type Error = String;

    fn try_from(value: vrl::value::Value) -> Result<Self, Self::Error> {
        let value = value.try_bytes().map_err(|e| e.to_string())?;
        match std::str::from_utf8(&value).map_err(|e| e.to_string())? {
            "incremental" => Ok(Self::Incremental),
            "absolute" => Ok(Self::Absolute),
            value => Err(format!(
                "invalid metric kind {value}, metric kind must be `absolute` or `incremental`"
            )),
        }
    }
}

#[cfg(feature = "vrl")]
impl From<MetricKind> for vrl::value::Value {
    fn from(kind: MetricKind) -> Self {
        match kind {
            MetricKind::Incremental => "incremental".into(),
            MetricKind::Absolute => "absolute".into(),
        }
    }
}

#[macro_export]
macro_rules! samples {
    ( $( $value:expr => $rate:expr ),* ) => {
        vec![ $( $crate::event::metric::Sample { value: $value, rate: $rate }, )* ]
    }
}

#[macro_export]
macro_rules! buckets {
    ( $( $limit:expr => $count:expr ),* ) => {
        vec![ $( $crate::event::metric::Bucket { upper_limit: $limit, count: $count }, )* ]
    }
}

#[macro_export]
macro_rules! quantiles {
    ( $( $q:expr => $value:expr ),* ) => {
        vec![ $( $crate::event::metric::Quantile { quantile: $q, value: $value }, )* ]
    }
}

fn write_list<I, T, W>(
    fmt: &mut Formatter<'_>,
    sep: &str,
    items: I,
    writer: W,
) -> Result<(), fmt::Error>
where
    I: IntoIterator<Item = T>,
    W: Fn(&mut Formatter<'_>, T) -> Result<(), fmt::Error>,
{
    let mut this_sep = "";
    for item in items {
        write!(fmt, "{this_sep}")?;
        writer(fmt, item)?;
        this_sep = sep;
    }
    Ok(())
}

fn write_word(fmt: &mut Formatter<'_>, word: &str) -> Result<(), fmt::Error> {
    if word.contains(|c: char| !c.is_ascii_alphanumeric() && c != '_') {
        write!(fmt, "{word:?}")
    } else {
        write!(fmt, "{word}")
    }
}

pub fn samples_to_buckets(samples: &[Sample], buckets: &[f64]) -> (Vec<Bucket>, u64, f64) {
    let mut counts = vec![0; buckets.len()];
    let mut sum = 0.0;
    let mut count = 0;
    for sample in samples {
        let rate = u64::from(sample.rate);

        if let Some((i, _)) = buckets
            .iter()
            .enumerate()
            .find(|&(_, b)| *b >= sample.value)
        {
            counts[i] += rate;
        }

        sum += sample.value * f64::from(sample.rate);
        count += rate;
    }

    let buckets = buckets
        .iter()
        .zip(counts.iter())
        .map(|(b, c)| Bucket {
            upper_limit: *b,
            count: *c,
        })
        .collect();

    (buckets, count, sum)
}
