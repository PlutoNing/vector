use std::collections::BTreeMap;

use mlua::prelude::*;

use super::super::{
    metric::TagValue,
    metric::{self, MetricSketch, MetricTags, TagValueSet},
    Metric, MetricKind, MetricValue, StatisticKind,
};
use super::util::{table_to_timestamp, timestamp_to_table};
use crate::metrics::AgentDDSketch;

pub struct LuaMetric {
    pub metric: Metric,
    pub multi_value_tags: bool,
}

pub struct LuaMetricTags {
    pub tags: MetricTags,
    pub multi_value_tags: bool,
}

impl IntoLua for MetricKind {
    #![allow(clippy::wrong_self_convention)] // this trait is defined by mlua
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        let kind = match self {
            MetricKind::Absolute => "absolute",
            MetricKind::Incremental => "incremental",
        };
        lua.create_string(kind).map(LuaValue::String)
    }
}

impl FromLua for MetricKind {
    fn from_lua(value: LuaValue, _: &Lua) -> LuaResult<Self> {
        match value {
            LuaValue::String(s) if s == "absolute" => Ok(MetricKind::Absolute),
            LuaValue::String(s) if s == "incremental" => Ok(MetricKind::Incremental),
            _ => Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("MetricKind"),
                message: Some(
                    "Metric kind should be either \"incremental\" or \"absolute\"".to_string(),
                ),
            }),
        }
    }
}

impl IntoLua for StatisticKind {
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        let kind = match self {
            StatisticKind::Summary => "summary",
            StatisticKind::Histogram => "histogram",
        };
        lua.create_string(kind).map(LuaValue::String)
    }
}

impl FromLua for StatisticKind {
    fn from_lua(value: LuaValue, _: &Lua) -> LuaResult<Self> {
        match value {
            LuaValue::String(s) if s == "summary" => Ok(StatisticKind::Summary),
            LuaValue::String(s) if s == "histogram" => Ok(StatisticKind::Histogram),
            _ => Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("StatisticKind"),
                message: Some(
                    "Statistic kind should be either \"summary\" or \"histogram\"".to_string(),
                ),
            }),
        }
    }
}

impl FromLua for TagValueSet {
    fn from_lua(value: LuaValue, _: &Lua) -> LuaResult<Self> {
        match value {
            LuaValue::Nil => Ok(Self::Single(TagValue::Bare)),
            LuaValue::Table(table) => {
                let mut string_values: Vec<String> = vec![];
                for value in table.sequence_values() {
                    match value {
                        Ok(value) => string_values.push(value),
                        Err(_) => unimplemented!(),
                    }
                }
                Ok(Self::from(string_values))
            }
            LuaValue::String(x) => Ok(Self::from([x.to_string_lossy().to_string()])),
            _ => Err(mlua::Error::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("metric tag value"),
                message: None,
            }),
        }
    }
}

impl FromLua for MetricTags {
    fn from_lua(value: LuaValue, lua: &Lua) -> LuaResult<Self> {
        Ok(Self(BTreeMap::from_lua(value, lua)?))
    }
}

impl IntoLua for LuaMetricTags {
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        if self.multi_value_tags {
            Ok(LuaValue::Table(lua.create_table_from(
                self.tags.0.into_iter().map(|(key, value)| {
                    let value: Vec<_> = value
                        .into_iter()
                        .filter_map(|tag_value| tag_value.into_option().into_lua(lua).ok())
                        .collect();
                    (key, value)
                }),
            )?))
        } else {
            Ok(LuaValue::Table(
                lua.create_table_from(self.tags.iter_single())?,
            ))
        }
    }
}

impl IntoLua for LuaMetric {
    #![allow(clippy::wrong_self_convention)] // this trait is defined by mlua
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        let tbl = lua.create_table()?;

        tbl.raw_set("name", self.metric.name())?;
        if let Some(namespace) = self.metric.namespace() {
            tbl.raw_set("namespace", namespace)?;
        }
        if let Some(ts) = self.metric.data.time.timestamp {
            tbl.raw_set("timestamp", timestamp_to_table(lua, ts)?)?;
        }
        if let Some(i) = self.metric.data.time.interval_ms {
            tbl.raw_set("interval_ms", i.get())?;
        }
        if let Some(tags) = self.metric.series.tags {
            tbl.raw_set(
                "tags",
                LuaMetricTags {
                    tags,
                    multi_value_tags: self.multi_value_tags,
                },
            )?;
        }
        tbl.raw_set("kind", self.metric.data.kind)?;

        match self.metric.data.value {
            MetricValue::Counter { value } => {
                let counter = lua.create_table()?;
                counter.raw_set("value", value)?;
                tbl.raw_set("counter", counter)?;
            }
            MetricValue::Gauge { value } => {
                let gauge = lua.create_table()?;
                gauge.raw_set("value", value)?;
                tbl.raw_set("gauge", gauge)?;
            }
            MetricValue::Set { values } => {
                let set = lua.create_table()?;
                set.raw_set("values", lua.create_sequence_from(values.into_iter())?)?;
                tbl.raw_set("set", set)?;
            }
            MetricValue::Distribution { samples, statistic } => {
                let distribution = lua.create_table()?;
                let sample_rates: Vec<_> = samples.iter().map(|s| s.rate).collect();
                let values: Vec<_> = samples.into_iter().map(|s| s.value).collect();
                distribution.raw_set("values", values)?;
                distribution.raw_set("sample_rates", sample_rates)?;
                distribution.raw_set("statistic", statistic)?;
                tbl.raw_set("distribution", distribution)?;
            }
            MetricValue::AggregatedHistogram {
                buckets,
                count,
                sum,
            } => {
                let aggregated_histogram = lua.create_table()?;
                let counts: Vec<_> = buckets.iter().map(|b| b.count).collect();
                let buckets: Vec<_> = buckets.into_iter().map(|b| b.upper_limit).collect();
                aggregated_histogram.raw_set("buckets", buckets)?;
                aggregated_histogram.raw_set("counts", counts)?;
                aggregated_histogram.raw_set("count", count)?;
                aggregated_histogram.raw_set("sum", sum)?;
                tbl.raw_set("aggregated_histogram", aggregated_histogram)?;
            }
            MetricValue::AggregatedSummary {
                quantiles,
                count,
                sum,
            } => {
                let aggregated_summary = lua.create_table()?;
                let values: Vec<_> = quantiles.iter().map(|q| q.value).collect();
                let quantiles: Vec<_> = quantiles.into_iter().map(|q| q.quantile).collect();
                aggregated_summary.raw_set("quantiles", quantiles)?;
                aggregated_summary.raw_set("values", values)?;
                aggregated_summary.raw_set("count", count)?;
                aggregated_summary.raw_set("sum", sum)?;
                tbl.raw_set("aggregated_summary", aggregated_summary)?;
            }
            MetricValue::Sketch { sketch } => {
                let sketch_tbl = match sketch {
                    MetricSketch::AgentDDSketch(ddsketch) => {
                        let sketch_tbl = lua.create_table()?;
                        sketch_tbl.raw_set("type", "ddsketch")?;
                        sketch_tbl.raw_set("count", ddsketch.count())?;
                        sketch_tbl.raw_set("min", ddsketch.min())?;
                        sketch_tbl.raw_set("max", ddsketch.max())?;
                        sketch_tbl.raw_set("sum", ddsketch.sum())?;
                        sketch_tbl.raw_set("avg", ddsketch.avg())?;

                        let bin_map = ddsketch.bin_map();
                        sketch_tbl.raw_set("k", bin_map.keys)?;
                        sketch_tbl.raw_set("n", bin_map.counts)?;
                        sketch_tbl
                    }
                };

                tbl.raw_set("sketch", sketch_tbl)?;
            }
        }

        Ok(LuaValue::Table(tbl))
    }
}

impl FromLua for Metric {
    #[allow(clippy::too_many_lines)]
    fn from_lua(value: LuaValue, _: &Lua) -> LuaResult<Self> {
        let table = match &value {
            LuaValue::Table(table) => table,
            other => {
                return Err(LuaError::FromLuaConversionError {
                    from: other.type_name(),
                    to: String::from("Metric"),
                    message: Some("Metric should be a Lua table".to_string()),
                })
            }
        };

        let name: String = table.raw_get("name")?;
        let timestamp = table
            .raw_get::<Option<LuaTable>>("timestamp")?
            .map(table_to_timestamp)
            .transpose()?;
        let interval_ms: Option<u32> = table.raw_get("interval_ms")?;
        let namespace: Option<String> = table.raw_get("namespace")?;
        let tags: Option<MetricTags> = table.raw_get("tags")?;
        let kind = table
            .raw_get::<Option<MetricKind>>("kind")?
            .unwrap_or(MetricKind::Absolute);

        let value = if let Some(counter) = table.raw_get::<Option<LuaTable>>("counter")? {
            MetricValue::Counter {
                value: counter.raw_get("value")?,
            }
        } else if let Some(gauge) = table.raw_get::<Option<LuaTable>>("gauge")? {
            MetricValue::Gauge {
                value: gauge.raw_get("value")?,
            }
        } else if let Some(set) = table.raw_get::<Option<LuaTable>>("set")? {
            MetricValue::Set {
                values: set.raw_get("values")?,
            }
        } else if let Some(distribution) = table.raw_get::<Option<LuaTable>>("distribution")? {
            let values: Vec<f64> = distribution.raw_get("values")?;
            let rates: Vec<u32> = distribution.raw_get("sample_rates")?;
            MetricValue::Distribution {
                samples: metric::zip_samples(values, rates),
                statistic: distribution.raw_get("statistic")?,
            }
        } else if let Some(aggregated_histogram) =
            table.raw_get::<Option<LuaTable>>("aggregated_histogram")?
        {
            let counts: Vec<u64> = aggregated_histogram.raw_get("counts")?;
            let buckets: Vec<f64> = aggregated_histogram.raw_get("buckets")?;
            let count = counts.iter().sum();
            MetricValue::AggregatedHistogram {
                buckets: metric::zip_buckets(buckets, counts),
                count,
                sum: aggregated_histogram.raw_get("sum")?,
            }
        } else if let Some(aggregated_summary) =
            table.raw_get::<Option<LuaTable>>("aggregated_summary")?
        {
            let quantiles: Vec<f64> = aggregated_summary.raw_get("quantiles")?;
            let values: Vec<f64> = aggregated_summary.raw_get("values")?;
            MetricValue::AggregatedSummary {
                quantiles: metric::zip_quantiles(quantiles, values),
                count: aggregated_summary.raw_get("count")?,
                sum: aggregated_summary.raw_get("sum")?,
            }
        } else if let Some(sketch) = table.raw_get::<Option<LuaTable>>("sketch")? {
            let sketch_type: String = sketch.raw_get("type")?;
            match sketch_type.as_str() {
                "ddsketch" => {
                    let count: u32 = sketch.raw_get("count")?;
                    let min: f64 = sketch.raw_get("min")?;
                    let max: f64 = sketch.raw_get("max")?;
                    let sum: f64 = sketch.raw_get("sum")?;
                    let avg: f64 = sketch.raw_get("avg")?;
                    let k: Vec<i16> = sketch.raw_get("k")?;
                    let n: Vec<u16> = sketch.raw_get("n")?;

                    AgentDDSketch::from_raw(count, min, max, sum, avg, &k, &n)
                        .map(|sketch| MetricValue::Sketch {
                            sketch: MetricSketch::AgentDDSketch(sketch),
                        })
                        .ok_or(LuaError::FromLuaConversionError {
                            from: value.type_name(),
                            to: String::from("Metric"),
                            message: Some(
                                "Invalid structure for converting to AgentDDSketch".to_string(),
                            ),
                        })?
                }
                x => {
                    return Err(LuaError::FromLuaConversionError {
                        from: value.type_name(),
                        to: String::from("Metric"),
                        message: Some(format!("Invalid sketch type '{x}' given")),
                    })
                }
            }
        } else {
            return Err(LuaError::FromLuaConversionError {
                from: value.type_name(),
                to: String::from("Metric"),
                message: Some("Cannot find metric value, expected presence one of \"counter\", \"gauge\", \"set\", \"distribution\", \"aggregated_histogram\", \"aggregated_summary\"".to_string()),
            });
        };

        Ok(Metric::new(name, kind, value)
            .with_namespace(namespace)
            .with_tags(tags)
            .with_timestamp(timestamp)
            .with_interval_ms(interval_ms.and_then(std::num::NonZeroU32::new)))
    }
}