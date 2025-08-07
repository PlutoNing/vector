use core::fmt;
use std::collections::BTreeSet;

use serde::{de, Deserialize, Deserializer, Serialize, Serializer};


use agent_config::configurable_component;

use crate::float_eq;

use super::{write_list, write_word};

const INFINITY: &str = "inf";
const NEG_INFINITY: &str = "-inf";
const NAN: &str = "NaN";

/// Metric value.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(rename_all = "snake_case")]
/// Container for the actual value of a metric.
pub enum MetricValue {
    /// A cumulative numerical value that can only increase or be reset to zero.
    Counter {
        /// The value of the counter.
        value: f64,
    },

    /// A single numerical value that can arbitrarily go up and down.
    Gauge {
        /// The value of the gauge.
        value: f64,
    },

    /// A set of (unordered) unique values for a key.
    Set {
        /// The values in the set.
        values: BTreeSet<String>,
    },

    /// A set of observations without any aggregation or sampling.
    Distribution {
        /// The observed values within this distribution.
        samples: Vec<Sample>,

        /// The type of statistics to derive for this distribution.
        statistic: StatisticKind,
    },

    /// A set of observations which are counted into buckets.
    ///
    /// It also contains the total count of all observations and their sum to allow calculating the mean.
    AggregatedHistogram {
        /// The buckets within this histogram.
        buckets: Vec<Bucket>,

        /// The total number of observations contained within this histogram.
        count: u64,

        /// The sum of all observations contained within this histogram.
        sum: f64,
    },

    /// A set of observations which are represented by quantiles.
    ///
    /// Each quantile contains the upper value of the quantile (0 <= φ <= 1). It also contains the total count of all
    /// observations and their sum to allow calculating the mean.
    AggregatedSummary {
        /// The quantiles measured from this summary.
        quantiles: Vec<Quantile>,

        /// The total number of observations contained within this summary.
        count: u64,

        /// The sum of all observations contained within this histogram.
        sum: f64,
    },
}

impl MetricValue {
    /// Returns `true` if the value is empty.
    ///
    /// Emptiness is dictated by whether or not the value has any samples or measurements present. Consequently, scalar
    /// values (counter, gauge) are never considered empty.
    pub fn is_empty(&self) -> bool {
        match self {
            MetricValue::Counter { .. } | MetricValue::Gauge { .. } => false,

            MetricValue::Set { values } => values.is_empty(),

            MetricValue::Distribution { samples, .. } => samples.is_empty(),

            MetricValue::AggregatedSummary { count, .. }
            | MetricValue::AggregatedHistogram { count, .. } => *count == 0,
        }
    }

    /// Gets the name of this value as a string.
    ///
    /// This maps to the name of the enum variant itself.
    pub fn as_name(&self) -> &'static str {
        match self {
            Self::Counter { .. } => "counter",
            Self::Gauge { .. } => "gauge",
            Self::Set { .. } => "set",
            Self::Distribution { .. } => "distribution",
            Self::AggregatedHistogram { .. } => "aggregated histogram",
            Self::AggregatedSummary { .. } => "aggregated summary",
        }
    }

    /// Zeroes out all the values contained in this value.
    /* 清零此值中包含的所有值。直方图和摘要指标类型保留所有桶值向量，同时将计数清零。分布指标清空所有值。 */
    pub fn zero(&mut self) {
        match self {
            Self::Counter { value } | Self::Gauge { value } => *value = 0.0,
            Self::Set { values } => values.clear(),
            Self::Distribution { samples, .. } => samples.clear(),
            Self::AggregatedHistogram {
                buckets,
                count,
                sum,
            } => {
                for bucket in buckets {
                    bucket.count = 0;
                }
                *count = 0;
                *sum = 0.0;
            }
            Self::AggregatedSummary {
                quantiles,
                sum,
                count,
            } => {
                for quantile in quantiles {
                    quantile.value = 0.0;
                }
                *count = 0;
                *sum = 0.0;
            }
        }
    }

    /// Adds another value to this one.
    ///
    /// If the other value is not the same type, or if they are but their defining characteristics of the value are
    /// different (i.e. aggregated histograms with different bucket layouts), then `false` is returned.  Otherwise,
    /// `true` is returned.
    #[must_use]
    pub fn add(&mut self, other: &Self) -> bool {
        match (self, other) {
            (Self::Counter { ref mut value }, Self::Counter { value: value2 })
            | (Self::Gauge { ref mut value }, Self::Gauge { value: value2 }) => {
                *value += value2;
                true
            }
            (Self::Set { ref mut values }, Self::Set { values: values2 }) => {
                values.extend(values2.iter().map(Into::into));
                true
            }
            (
                Self::Distribution {
                    ref mut samples,
                    statistic: statistic_a,
                },
                Self::Distribution {
                    samples: samples2,
                    statistic: statistic_b,
                },
            ) if statistic_a == statistic_b => {
                samples.extend_from_slice(samples2);
                true
            }
            (
                Self::AggregatedHistogram {
                    ref mut buckets,
                    ref mut count,
                    ref mut sum,
                },
                Self::AggregatedHistogram {
                    buckets: buckets2,
                    count: count2,
                    sum: sum2,
                },
            ) if buckets.len() == buckets2.len()
                && buckets
                    .iter()
                    .zip(buckets2.iter())
                    .all(|(b1, b2)| b1.upper_limit == b2.upper_limit) =>
            {
                for (b1, b2) in buckets.iter_mut().zip(buckets2) {
                    b1.count += b2.count;
                }
                *count += count2;
                *sum += sum2;
                true
            }
            _ => false,
        }
    }

    /// Subtracts another value from this one.
    ///
    /// If the other value is not the same type, or if they are but their defining characteristics of the value are
    /// different (i.e. aggregated histograms with different bucket layouts), then `false` is returned.  Otherwise,
    /// `true` is returned.
    #[must_use]
    pub fn subtract(&mut self, other: &Self) -> bool {
        match (self, other) {
            // Counters are monotonic, they should _never_ go backwards unless reset to 0 due to
            // process restart, etc.  Thus, being able to generate negative deltas would violate
            // that.  Whether a counter is reset to 0, or if it incorrectly warps to a previous
            // value, it doesn't matter: we're going to reinitialize it.
            (Self::Counter { ref mut value }, Self::Counter { value: value2 })
                if *value >= *value2 =>
            {
                *value -= value2;
                true
            }
            (Self::Gauge { ref mut value }, Self::Gauge { value: value2 }) => {
                *value -= value2;
                true
            }
            (Self::Set { ref mut values }, Self::Set { values: values2 }) => {
                for item in values2 {
                    values.remove(item);
                }
                true
            }
            (
                Self::Distribution {
                    ref mut samples,
                    statistic: statistic_a,
                },
                Self::Distribution {
                    samples: samples2,
                    statistic: statistic_b,
                },
            ) if statistic_a == statistic_b => {
                // This is an ugly algorithm, but the use of a HashSet or equivalent is complicated by neither Hash nor
                // Eq being implemented for the f64 part of Sample.
                //
                // TODO: This logic does not work if a value is repeated within a distribution. For example, if the
                // current distribution is [1, 2, 3, 1, 2, 3] and the previous distribution is [1, 2, 3], this would
                // yield a result of [].
                //
                // The only reasonable way we could provide subtraction, I believe, is if we required the ordering to
                // stay the same, such that we would just take the samples from the non-overlapping region as the delta.
                // In the above example: length of samples from `other` would be 3, so delta would be
                // `self.samples[3..]`.
                *samples = samples
                    .iter()
                    .copied()
                    .filter(|sample| samples2.iter().all(|sample2| sample != sample2))
                    .collect();
                true
            }
            // Aggregated histograms, at least in Prometheus, are also typically monotonic in terms of growth.
            // Subtracting them in reverse -- e.g.. subtracting a newer one with more values from an older one with
            // fewer values -- would not make sense, since buckets should never be able to have negative counts... and
            // it's not clear that a saturating subtraction is technically correct either.  Instead, we avoid having to
            // make that decision, and simply force the metric to be reinitialized.
            (
                Self::AggregatedHistogram {
                    ref mut buckets,
                    ref mut count,
                    ref mut sum,
                },
                Self::AggregatedHistogram {
                    buckets: buckets2,
                    count: count2,
                    sum: sum2,
                },
            ) if *count >= *count2
                && buckets.len() == buckets2.len()
                && buckets
                    .iter()
                    .zip(buckets2.iter())
                    .all(|(b1, b2)| b1.upper_limit == b2.upper_limit) =>
            {
                for (b1, b2) in buckets.iter_mut().zip(buckets2) {
                    b1.count -= b2.count;
                }
                *count -= count2;
                *sum -= sum2;
                true
            }
            _ => false,
        }
    }
}



impl PartialEq for MetricValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Counter { value: l_value }, Self::Counter { value: r_value })
            | (Self::Gauge { value: l_value }, Self::Gauge { value: r_value }) => {
                float_eq(*l_value, *r_value)
            }
            (Self::Set { values: l_values }, Self::Set { values: r_values }) => {
                l_values == r_values
            }
            (
                Self::Distribution {
                    samples: l_samples,
                    statistic: l_statistic,
                },
                Self::Distribution {
                    samples: r_samples,
                    statistic: r_statistic,
                },
            ) => l_samples == r_samples && l_statistic == r_statistic,
            (
                Self::AggregatedHistogram {
                    buckets: l_buckets,
                    count: l_count,
                    sum: l_sum,
                },
                Self::AggregatedHistogram {
                    buckets: r_buckets,
                    count: r_count,
                    sum: r_sum,
                },
            ) => l_buckets == r_buckets && l_count == r_count && float_eq(*l_sum, *r_sum),
            (
                Self::AggregatedSummary {
                    quantiles: l_quantiles,
                    count: l_count,
                    sum: l_sum,
                },
                Self::AggregatedSummary {
                    quantiles: r_quantiles,
                    count: r_count,
                    sum: r_sum,
                },
            ) => l_quantiles == r_quantiles && l_count == r_count && float_eq(*l_sum, *r_sum),
            _ => false,
        }
    }
}

impl fmt::Display for MetricValue {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            MetricValue::Counter { value } | MetricValue::Gauge { value } => {
                write!(fmt, "{value}")
            }
            MetricValue::Set { values } => {
                write_list(fmt, " ", values.iter(), |fmt, value| write_word(fmt, value))
            }
            MetricValue::Distribution { samples, statistic } => {
                write!(
                    fmt,
                    "{} ",
                    match statistic {
                        StatisticKind::Histogram => "histogram",
                        StatisticKind::Summary => "summary",
                    }
                )?;
                write_list(fmt, " ", samples, |fmt, sample| {
                    write!(fmt, "{}@{}", sample.rate, sample.value)
                })
            }
            MetricValue::AggregatedHistogram {
                buckets,
                count,
                sum,
            } => {
                write!(fmt, "count={count} sum={sum} ")?;
                write_list(fmt, " ", buckets, |fmt, bucket| {
                    write!(fmt, "{}@{}", bucket.count, bucket.upper_limit)
                })
            }
            MetricValue::AggregatedSummary {
                quantiles,
                count,
                sum,
            } => {
                write!(fmt, "count={count} sum={sum} ")?;
                write_list(fmt, " ", quantiles, |fmt, quantile| {
                    write!(fmt, "{}@{}", quantile.quantile, quantile.value)
                })
            }
        }
    }
}

// Currently, VRL can only read the type of the value and doesn't consider any actual metric values.
#[cfg(feature = "vrl")]
impl From<MetricValue> for vrl::value::Value {
    fn from(value: MetricValue) -> Self {
        value.as_name().into()
    }
}

/// Type of statistics to generate for a distribution.
#[configurable_component]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum StatisticKind {
    /// A histogram representation.
    Histogram,

    /// Corresponds to Datadog's Distribution Metric
    /// <https://docs.datadoghq.com/developers/metrics/types/?tab=distribution#definition>
    Summary,
}

/// A single observation.
#[configurable_component]
#[derive(Clone, Copy, Debug)]
pub struct Sample {
    /// The value of the observation.
    pub value: f64,

    /// The rate at which the value was observed.
    pub rate: u32,
}

impl PartialEq for Sample {
    fn eq(&self, other: &Self) -> bool {
        self.rate == other.rate && float_eq(self.value, other.value)
    }
}



/// Custom serialization function which converts special `f64` values to strings.
/// Non-special values are serialized as numbers.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn serialize_f64<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if value.is_infinite() {
        serializer.serialize_str(if *value > 0.0 { INFINITY } else { NEG_INFINITY })
    } else if value.is_nan() {
        serializer.serialize_str(NAN)
    } else {
        serializer.serialize_f64(*value)
    }
}

/// Custom deserialization function for handling special f64 values.
fn deserialize_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    struct UpperLimitVisitor;

    impl de::Visitor<'_> for UpperLimitVisitor {
        type Value = f64;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a number or a special string value")
        }

        fn visit_f64<E: de::Error>(self, value: f64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
            match value {
                NAN => Ok(f64::NAN),
                INFINITY => Ok(f64::INFINITY),
                NEG_INFINITY => Ok(f64::NEG_INFINITY),
                _ => Err(E::custom("unsupported string value")),
            }
        }
    }

    deserializer.deserialize_any(UpperLimitVisitor)
}

/// A histogram bucket.
///
/// Histogram buckets represent the `count` of observations where the value of the observations does
/// not exceed the specified `upper_limit`.
#[configurable_component(no_deser, no_ser)]
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Bucket {
    /// The upper limit of values in the bucket.
    #[serde(serialize_with = "serialize_f64", deserialize_with = "deserialize_f64")]
    pub upper_limit: f64,

    /// The number of values tracked in this bucket.
    pub count: u64,
}

impl PartialEq for Bucket {
    fn eq(&self, other: &Self) -> bool {
        self.count == other.count && float_eq(self.upper_limit, other.upper_limit)
    }
}



/// A single quantile observation.
///
/// Quantiles themselves are "cut points dividing the range of a probability distribution into
/// continuous intervals with equal probabilities". [[1][quantiles_wikipedia]].
///
/// We use quantiles to measure the value along these probability distributions for representing
/// client-side aggregations of distributions, which represent a collection of observations over a
/// specific time window.
///
/// In general, we typically use the term "quantile" to represent the concept of _percentiles_,
/// which deal with whole integers -- 0, 1, 2, .., 99, 100 -- even though quantiles are
/// floating-point numbers and can represent higher-precision cut points, such as 0.9999, or the
/// 99.99th percentile.
///
/// [quantiles_wikipedia]: https://en.wikipedia.org/wiki/Quantile
#[configurable_component]
#[derive(Clone, Copy, Debug)]
pub struct Quantile {
    /// The value of the quantile.
    ///
    /// This value must be between 0.0 and 1.0, inclusive.
    pub quantile: f64,

    /// The estimated value of the given quantile within the probability distribution.
    pub value: f64,
}

impl PartialEq for Quantile {
    fn eq(&self, other: &Self) -> bool {
        float_eq(self.quantile, other.quantile) && float_eq(self.value, other.value)
    }
}

impl Quantile {
    /// Renders this quantile as a string, scaled to be a percentile.
    ///
    /// Up to four significant digits are maintained, but the resulting string will be without a decimal point.
    ///
    /// For example, a quantile of 0.25, which represents a percentile of 25, will be rendered as "25" and a quantile of
    /// 0.9999, which represents a percentile of 99.99, will be rendered as "9999". A quantile of 0.99999, which
    /// represents a percentile of 99.999, would also be rendered as "9999", though.
    pub fn to_percentile_string(&self) -> String {
        let clamped = self.quantile.clamp(0.0, 1.0) * 100.0;
        clamped
            .to_string()
            .chars()
            .take(5)
            .filter(|c| *c != '.')
            .collect()
    }

    /// Renders this quantile as a string.
    ///
    /// Up to four significant digits are maintained.
    ///
    /// For example, a quantile of 0.25 will be rendered as "0.25", and a quantile of 0.9999 will be rendered as
    /// "0.9999", but a quantile of 0.99999 will be rendered as "0.9999".
    pub fn to_quantile_string(&self) -> String {
        let clamped = self.quantile.clamp(0.0, 1.0);
        clamped.to_string().chars().take(6).collect()
    }
}


