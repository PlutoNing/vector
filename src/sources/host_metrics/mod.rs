use std::{
    path::{Path},
    time::Duration,
};


use agent_lib::config::LogNamespace;
use agent_lib::configurable::configurable_component;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use glob::{Pattern, PatternError};
use heim::units::ratio::ratio;
use heim::units::time::second;
use serde_with::serde_as;
use tokio::time;
use tokio_stream::wrappers::IntervalStream;

use crate::common::ShutdownSignal;
use crate::{
    config::{SourceConfig, SourceContext, SourceOutput},
    event::metric::{Metric, MetricKind, MetricTags, MetricValue},
SourceSender,
};

mod cpu;
mod disk;
mod memory;
mod network;

/// Collector types.
#[serde_as]
#[configurable_component]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Collector {
    /// cgroup的metrics
    CGroups,

    /// CPU utilize.
    Cpu,

    /// disk I/O utilize.
    Disk,

    /// system load average.
    Load,

    /// host info.
    Host,

    /// memory utilize.
    Memory,

    /// network utilize.
    Network,
}

/// Filtering configuration.
#[configurable_component]
#[derive(Clone, Debug, Default)]
struct FilterList {
    /// 允许的模式
    includes: Option<Vec<PatternWrapper>>,

    /// 排除的模式.
    excludes: Option<Vec<PatternWrapper>>,
}

/// Configuration for the `host_metrics` source.
#[serde_as] /* 启用 serde_with 的功能，允许在序列化和反序列化时使用自定义的格式。 */
#[configurable_component(source("host_metrics", "Collect metric data from the local system."))]
/* 自定义的属性宏，用于标记这个结构体是一个可配置的组件，类型为 source，并提供描述信息。 */
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)] /* 使用 Derivative 宏为结构体实现 Default 特征。 */
#[serde(deny_unknown_fields)] /* 在反序列化时，如果遇到未知字段，将会导致错误。 */
pub struct HostMetricsConfig {
    /// The interval between metric gathering, in seconds.
    #[serde_as(as = "serde_with::DurationSeconds<u64>")]
    #[serde(default = "default_scrape_interval")]
    #[configurable(metadata(docs::human_name = "Scrape Interval"))]
    /* 为配置生成文档时提供人类可读的名称。 */
    pub scrape_interval_secs: Duration,

    /// The list of host metric collector services to use.
    ///
    /// Defaults to all collectors.
    #[configurable(metadata(docs::examples = "example_collectors()"))]
    #[derivative(Default(value = "default_collectors()"))]
    #[serde(default = "default_collectors")]
    pub collectors: Option<Vec<Collector>>,

    /* 默认为host */
    /// Overrides the default namespace for the metrics emitted by the source.
    #[derivative(Default(value = "default_namespace()"))]
    #[serde(default = "default_namespace")]
    pub namespace: Option<String>,

    #[configurable(derived)]
    #[serde(default)]
    pub disk: disk::DiskConfig,
    #[configurable(derived)]
    #[serde(default)]
    pub cpu: cpu::CpuConfig,

    #[configurable(derived)]
    #[serde(default)]
    pub network: network::NetworkConfig,
}

/* 默认收集间隔 */
const fn default_scrape_interval() -> Duration {
    Duration::from_secs(15)
}

pub fn default_namespace() -> Option<String> {
    Some(String::from("host"))
}

/* 默认的启用的collector */
const fn example_collectors() -> [&'static str; 7] {
    [
        "cgroups", "cpu", "disk", "load", "host", "memory", "network",
    ]
}

fn default_collectors() -> Option<Vec<Collector>> {
    let mut collectors = vec![
        Collector::Cpu,
        Collector::Disk,
        Collector::Load,
        Collector::Host,
        Collector::Memory,
        Collector::Network,
    ];

    collectors.push(Collector::CGroups);

    Some(collectors)
}

fn example_devices() -> FilterList {
    FilterList {
        includes: Some(vec!["sda".try_into().unwrap()]),
        excludes: Some(vec!["dm-*".try_into().unwrap()]),
    }
}

fn default_all_devices() -> FilterList {
    FilterList {
        includes: Some(vec!["*".try_into().unwrap()]),
        excludes: None,
    }
}

impl_generate_config_from_default!(HostMetricsConfig);

/* 每一个source都要实现SourceConfig接口, 提供outputs, sources等 */
#[async_trait::async_trait]
#[typetag::serde(name = "host_metrics")]
impl SourceConfig for HostMetricsConfig {
    /* source ctx包含了各种东西 */
    async fn build(&self, cx: SourceContext) -> crate::Result<super::Source> {
        init_roots();
        let mut config = self.clone();
        config.namespace = config.namespace.filter(|namespace| !namespace.is_empty());
        Ok(Box::pin(config.run(cx.out, cx.shutdown)))
    }

    /* 创建一个metrics的source Output */
    fn outputs(&self, _global_log_namespace: LogNamespace) -> Vec<SourceOutput> {
        vec![SourceOutput::new_metrics()]
    }
}
/*  */
impl HostMetricsConfig {
    /// Set the interval to collect internal metrics.
    pub fn scrape_interval_secs(&mut self, value: f64) {
        self.scrape_interval_secs = Duration::from_secs_f64(value);
    }
    /* 定时运行, 获取指标, 发送出去 */
    async fn run(self, mut out: SourceSender, shutdown: ShutdownSignal) -> Result<(), ()> {
        let duration = self.scrape_interval_secs;
        /* 创建定时器，每隔`duration`时间触发一次 */
        let mut interval = IntervalStream::new(time::interval(duration)).take_until(shutdown);

        let mut generator = HostMetrics::new(self);
        while interval.next().await.is_some() {
            /* 获取指标 */
            let metrics = generator.capture_metrics().await;
            let count = metrics.len();
            if (out.send_batch(metrics).await).is_err() {
                /* 发送出去 */
                error!(
                    "Failed to send host metrics batch, stream closed, count: {}",
                    count
                );
                return Err(());
            }
        }

        Ok(())
    }

    /* 检查指定的collector是否启用 */
    fn has_collector(&self, collector: Collector) -> bool {
        match &self.collectors {
            None => true,
            Some(collectors) => collectors.iter().any(|&c| c == collector),
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct MetricsFilter {
    pub cpu: Option<Vec<String>>,
    pub memory: Option<Vec<String>>,
    pub network: Option<Vec<String>>,
    pub disk: Option<Vec<String>>,
}

/* 代表一个HostMetrics的获取器
HostMetrics config的run函数定时调用, 用来获取metrics */
pub struct HostMetrics {
    config: HostMetricsConfig,
    metrics_filter: MetricsFilter,
}

impl HostMetrics {
    fn should_collect_metric(&self, metric_name: &str) -> bool {
        match &self.metrics_filter.cpu {
            None => true, // 如果没有指定，采集所有指标
            Some(metrics) => metrics.contains(&metric_name.to_string()),
        }
    }
    pub fn new(config: HostMetricsConfig) -> Self {
        let metrics_filter = MetricsFilter {
            cpu: config.cpu.metrics.clone(),
            memory: None, // 后续扩展
            network: None,
            disk: None,
        };
        Self {
            config,
            metrics_filter,
        }
    }

    pub fn buffer(&self) -> MetricsBuffer {
        MetricsBuffer::new(self.config.namespace.clone())
    }
    /* 获取系统的指标 */
    async fn capture_metrics(&mut self) -> Vec<Metric> {
        let mut buffer = self.buffer();
        if self.config.has_collector(Collector::Cpu) {
            self.cpu_metrics(&mut buffer).await;
        }
        if self.config.has_collector(Collector::Disk) {
            self.disk_metrics(&mut buffer).await;
        }
        if self.config.has_collector(Collector::Load) {
            self.loadavg_metrics(&mut buffer).await;
        }
        if self.config.has_collector(Collector::Host) {
            self.host_metrics(&mut buffer).await;
        }
        if self.config.has_collector(Collector::Memory) {
            self.memory_metrics(&mut buffer).await;
        }
        if self.config.has_collector(Collector::Network) {
            self.network_metrics(&mut buffer).await;
        }

        buffer.metrics
    }

    /* 获取系统的load */
    pub async fn loadavg_metrics(&self, output: &mut MetricsBuffer) {
        output.name = "load";

        match heim::cpu::os::unix::loadavg().await {
            Ok(loadavg) => {
                /* 这里gauge创建三个metric, 加入 */
                output.gauge(
                    "load1",
                    loadavg.0.get::<ratio>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "load5",
                    loadavg.1.get::<ratio>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "load15",
                    loadavg.2.get::<ratio>() as f64,
                    MetricTags::default(),
                );
            }
            Err(_error) => {
                error!("Failed to load average info");
            }
        }
    }

    /* 获取host级别的metrics */
    pub async fn host_metrics(&self, output: &mut MetricsBuffer) {
        output.name = "host";
        match heim::host::uptime().await {
            Ok(time) => output.gauge("uptime", time.get::<second>(), MetricTags::default()),
            Err(_error) => {
                error!("Failed to load host uptime info");
            }
        }

        match heim::host::boot_time().await {
            Ok(time) => output.gauge("boot_time", time.get::<second>(), MetricTags::default()),
            Err(_error) => {
                error!("Failed to load host boot time info");
            }
        }
        // 扩展信息
        if let Ok(_info) = heim::host::platform().await {
            output.gauge("server_hostname", 1.0, MetricTags::default());
        }
    }
}
/* 一组metric */
#[derive(Default)]
pub struct MetricsBuffer {
    pub metrics: Vec<Metric>,
    name: &'static str,
    host: Option<String>,
    timestamp: DateTime<Utc>,
    namespace: Option<String>,
}

impl MetricsBuffer {
    /*  */
    fn new(namespace: Option<String>) -> Self {
        Self {
            metrics: Vec::new(),
            name: "",
            host: crate::get_hostname().ok(),
            timestamp: Utc::now(),
            namespace,
        }
    }

    fn tags(&self, mut tags: MetricTags) -> MetricTags {
        tags.replace("collector".into(), self.name.to_string());
        if let Some(host) = &self.host {
            tags.replace("host".into(), host.clone());
        }
        tags
    }

    fn counter(&mut self, name: &str, value: f64, tags: MetricTags) {
        self.metrics.push(
            Metric::new(name, MetricKind::Absolute, MetricValue::Counter { value })
                .with_namespace(self.namespace.clone())
                .with_tags(Some(self.tags(tags)))
                .with_timestamp(Some(self.timestamp)),
        )
    }
    /*创建一个指定的metric 加入 */
    fn gauge(&mut self, name: &str, value: f64, tags: MetricTags) {
        self.metrics.push(
            Metric::new(name, MetricKind::Absolute, MetricValue::Gauge { value })
                .with_namespace(self.namespace.clone())
                .with_tags(Some(self.tags(tags)))
                .with_timestamp(Some(self.timestamp)),
        )
    }
}

fn filter_result_sync<T, E>(result: Result<T, E>, _message: &'static str) -> Option<T>
where
    E: std::error::Error,
{
    result
        .map_err(|_error| error!("filter_result_sync error"))
        .ok()
}

async fn filter_result<T, E>(result: Result<T, E>, message: &'static str) -> Option<T>
where
    E: std::error::Error,
{
    filter_result_sync(result, message)
}
/* 构建一些fs root */
#[allow(clippy::missing_const_for_fn)]
fn init_roots() {
    {
        use std::sync::Once;

        static INIT: Once = Once::new();

        INIT.call_once(|| {
            match std::env::var_os("PROCFS_ROOT") {
                Some(procfs_root) => {
                    info!(
                        message = "PROCFS_ROOT is set in envvars. Using custom for procfs.",
                        custom = ?procfs_root
                    );
                    heim::os::linux::set_procfs_root(std::path::PathBuf::from(&procfs_root));
                }
                None => info!("PROCFS_ROOT is unset. Using default '/proc' for procfs root."),
            };

            match std::env::var_os("SYSFS_ROOT") {
                Some(sysfs_root) => {
                    info!(
                        message = "SYSFS_ROOT is set in envvars. Using custom for sysfs.",
                        custom = ?sysfs_root
                    );
                    heim::os::linux::set_sysfs_root(std::path::PathBuf::from(&sysfs_root));
                }
                None => info!("SYSFS_ROOT is unset. Using default '/sys' for sysfs root."),
            }
        });
    };
}

impl FilterList {
    fn contains<T, M>(&self, value: &Option<T>, matches: M) -> bool
    where
        M: Fn(&PatternWrapper, &T) -> bool,
    {
        (match (&self.includes, value) {
            // No includes list includes everything
            (None, _) => true,
            // Includes list matched against empty value returns false
            (Some(_), None) => false,
            // Otherwise find the given value
            (Some(includes), Some(value)) => includes.iter().any(|pattern| matches(pattern, value)),
        }) && match (&self.excludes, value) {
            // No excludes, list excludes nothing
            (None, _) => true,
            // No value, never excluded
            (Some(_), None) => true,
            // Otherwise find the given value
            (Some(excludes), Some(value)) => {
                !excludes.iter().any(|pattern| matches(pattern, value))
            }
        }
    }

    fn contains_str(&self, value: Option<&str>) -> bool {
        self.contains(&value, |pattern, s| pattern.matches_str(s))
    }

    fn contains_path(&self, value: Option<&Path>) -> bool {
        self.contains(&value, |pattern, path| pattern.matches_path(path))
    }
}

/* 表示host metric filter list的一个模式 */
/// compiled Unix shell-style pattern.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(try_from = "String", into = "String")]
struct PatternWrapper(Pattern);

impl PatternWrapper {
    fn matches_str(&self, s: &str) -> bool {
        self.0.matches(s)
    }

    fn matches_path(&self, p: &Path) -> bool {
        self.0.matches_path(p)
    }
}

impl TryFrom<String> for PatternWrapper {
    type Error = PatternError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Pattern::new(value.as_ref()).map(PatternWrapper)
    }
}

impl TryFrom<&str> for PatternWrapper {
    type Error = PatternError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.to_string().try_into()
    }
}

impl From<PatternWrapper> for String {
    fn from(pattern: PatternWrapper) -> Self {
        pattern.0.to_string()
    }
}
