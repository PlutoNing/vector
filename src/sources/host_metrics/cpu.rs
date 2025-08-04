use agent_config::configurable_component;
use agent_lib::{event::MetricTags, metric_tags};
use futures::StreamExt;
use heim::cpu::os::linux::CpuTimeExt;
use heim::units::time::second;
use std::io;
use std::num::ParseIntError;
use tokio::fs;

use super::{filter_result, HostMetrics};

/// Options for the CPU metrics collector.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct CpuConfig {
    /// List of CPU metric names to collect.
    ///
    /// Available metrics: "cpu_seconds_total", "logical_cpus", "physical_cpus",
    /// "cpu_softirq_per_cpu", "cpu_softirq_all", "context_switches_total",
    /// "interrupts_total", "processes_total", "procs_running", "procs_blocked"
    #[serde(default)]
    pub metrics: Option<Vec<String>>,
}

const MODE: &str = "mode";
const CPU_SECS_TOTAL: &str = "cpu_seconds_total";
const LOGICAL_CPUS: &str = "logical_cpus";
const PHYSICAL_CPUS: &str = "physical_cpus";
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct ProcStat {
    /// CPU时间统计（每个CPU核心）
    pub cpu_times: Vec<CpuTime>,
    /// 系统级统计
    pub system_stats: SystemStats,
    /// 中断统计（总中断数）
    pub interrupts_total: u64,
    pub softirq_per_cpu: Vec<CpuSoftirqBreakdown>, // 每个CPU的软中断
}
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct CpuTime {
    pub cpu_id: String,  // "cpu", "cpu0", "cpu1", ...
    pub user: u64,       // 用户态时间
    pub nice: u64,       // nice值为负的进程时间
    pub system: u64,     // 内核态时间
    pub idle: u64,       // 空闲时间
    pub iowait: u64,     // I/O等待时间
    pub irq: u64,        // 硬中断时间
    pub softirq: u64,    // 软中断时间
    pub steal: u64,      // 被其他虚拟机偷取的时间
    pub guest: u64,      // 运行虚拟机的时间
    pub guest_nice: u64, // 运行nice值为负的虚拟机时间
}

/// 系统级统计

#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct SystemStats {
    pub context_switches: u64, // 上下文切换总数
    pub boot_time: u64,        // 系统启动时间（Unix时间戳）
    pub processes_total: u64,  // 创建的进程总数
    pub procs_running: u64,    // 当前运行进程数
    pub procs_blocked: u64,    // 当前阻塞进程数
}

/// 每个CPU的软中断分类统计
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct CpuSoftirqBreakdown {
    pub hi: u64,
    pub timer: u64,
    pub net_tx: u64,
    pub net_rx: u64,
    pub block: u64,
    pub irq_poll: u64,
    pub tasklet: u64,
    pub sched: u64,
    pub hrtimer: u64,
    pub rcu: u64,
}
/// 统一的错误类型
#[derive(Debug)]
pub enum ProcStatError {
    Io(io::Error),
    Parse(ParseIntError),
}

impl std::fmt::Display for ProcStatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcStatError::Io(e) => write!(f, "IO错误: {}", e),
            ProcStatError::Parse(e) => write!(f, "解析错误: {}", e),
        }
    }
}

impl std::error::Error for ProcStatError {}
impl From<ParseIntError> for ProcStatError {
    fn from(err: ParseIntError) -> Self {
        ProcStatError::Parse(err)
    }
}

impl ProcStat {
    /// 从文件内容创建 ProcStat 实例
    pub fn parse_from_files(
        stat_content: &str,
        softirq_content: &str,
    ) -> Result<Self, ProcStatError> {
        let mut proc_stat = ProcStat::default();

        // 解析CPU时间
        for line in stat_content.lines() {
            if line.starts_with("cpu") {
                let cpu_time = parse_cpu_time_line(line)?;
                proc_stat.cpu_times.push(cpu_time);
            }
        }

        // 解析系统级统计
        for line in stat_content.lines() {
            let mut parts = line.split_whitespace();
            if let Some(key) = parts.next() {
                match key {
                    "ctxt" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.context_switches =
                                value.parse().map_err(ProcStatError::Parse)?;
                        }
                    }
                    "btime" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.boot_time =
                                value.parse().map_err(ProcStatError::Parse)?;
                        }
                    }
                    "processes" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.processes_total =
                                value.parse().map_err(ProcStatError::Parse)?;
                        }
                    }
                    "procs_running" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.procs_running =
                                value.parse().map_err(ProcStatError::Parse)?;
                        }
                    }
                    "procs_blocked" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.procs_blocked =
                                value.parse().map_err(ProcStatError::Parse)?;
                        }
                    }
                    "intr" => {
                        if let Some(value) = parts.next() {
                            proc_stat.interrupts_total =
                                value.parse().map_err(ProcStatError::Parse)?;
                        }
                    }
                    _ => {}
                }
            }
        }

        // 从 /proc/softirqs 解析 per-CPU 软中断
        proc_stat.softirq_per_cpu = parse_softirq_content(softirq_content)?;

        Ok(proc_stat)
    }

    /// 从 `/proc/stat` 和 `/proc/softirqs` 文件创建新的 ProcStat 实例
    pub async fn new() -> Result<Self, ProcStatError> {
        let stat_content = fs::read_to_string("/proc/stat")
            .await
            .map_err(ProcStatError::Io)?;
        let softirq_content = fs::read_to_string("/proc/softirqs")
            .await
            .map_err(ProcStatError::Io)?;

        ProcStat::parse_from_files(&stat_content, &softirq_content)
    }

    /// 从指定路径创建（用于测试）
    #[allow(dead_code)]
    pub async fn from_path<P: AsRef<std::path::Path>>(
        stat_path: P,
        softirq_path: P,
    ) -> Result<Self, ProcStatError> {
        let stat_content = fs::read_to_string(stat_path)
            .await
            .map_err(ProcStatError::Io)?;
        let softirq_content = fs::read_to_string(softirq_path)
            .await
            .map_err(ProcStatError::Io)?;

        ProcStat::parse_from_files(&stat_content, &softirq_content)
    }
}

fn parse_cpu_time_line(line: &str) -> Result<CpuTime, ProcStatError> {
    let mut parts = line.split_whitespace();
    let cpu_id = parts.next().unwrap_or("cpu").to_string();

    let values: Vec<u64> = parts.take(10).filter_map(|v| v.parse().ok()).collect();

    let mut cpu_time = CpuTime {
        cpu_id,
        ..Default::default()
    };

    if values.len() >= 1 {
        cpu_time.user = values[0];
    }
    if values.len() >= 2 {
        cpu_time.nice = values[1];
    }
    if values.len() >= 3 {
        cpu_time.system = values[2];
    }
    if values.len() >= 4 {
        cpu_time.idle = values[3];
    }
    if values.len() >= 5 {
        cpu_time.iowait = values[4];
    }
    if values.len() >= 6 {
        cpu_time.irq = values[5];
    }
    if values.len() >= 7 {
        cpu_time.softirq = values[6];
    }
    if values.len() >= 8 {
        cpu_time.steal = values[7];
    }
    if values.len() >= 9 {
        cpu_time.guest = values[8];
    }
    if values.len() >= 10 {
        cpu_time.guest_nice = values[9];
    }

    Ok(cpu_time)
}

fn parse_softirq_content(content: &str) -> Result<Vec<CpuSoftirqBreakdown>, ProcStatError> {
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return Ok(Vec::new());
    }

    // 第一行是表头，格式: "                CPU0       CPU1       CPU2 ..."
    let header_line = lines[0];
    let cpu_count = header_line.split_whitespace().count();

    if cpu_count == 0 {
        return Ok(Vec::new());
    }

    let mut softirq_data = vec![CpuSoftirqBreakdown::default(); cpu_count];

    // 解析每种软中断类型
    for line in &lines[1..] {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < cpu_count + 1 {
            continue;
        }

        let irq_type = parts[0];
        let values: Vec<u64> = parts[1..=cpu_count]
            .iter()
            .filter_map(|v| v.parse().ok())
            .collect();

        if values.len() != cpu_count {
            continue;
        }

        // 根据软中断类型填充对应字段
        for (cpu_idx, &value) in values.iter().enumerate() {
            match irq_type.trim_end_matches(':') {
                "HI" => softirq_data[cpu_idx].hi = value,
                "TIMER" => softirq_data[cpu_idx].timer = value,
                "NET_TX" => softirq_data[cpu_idx].net_tx = value,
                "NET_RX" => softirq_data[cpu_idx].net_rx = value,
                "BLOCK" => softirq_data[cpu_idx].block = value,
                "IRQ_POLL" => softirq_data[cpu_idx].irq_poll = value,
                "TASKLET" => softirq_data[cpu_idx].tasklet = value,
                "SCHED" => softirq_data[cpu_idx].sched = value,
                "HRTIMER" => softirq_data[cpu_idx].hrtimer = value,
                "RCU" => softirq_data[cpu_idx].rcu = value,
                _ => {}
            }
        }
    }

    Ok(softirq_data)
}

impl HostMetrics {
    /// 采集Per-CPU软中断指标（每个软中断类型一个指标，包含per-CPU计数）
    pub fn cpu_softirq_per_cpu(&self, proc_stat: &ProcStat, output: &mut super::MetricsBuffer) {
        if proc_stat.softirq_per_cpu.is_empty() {
            return;
        }

        // 使用match表达式代替闭包数组，避免类型不匹配问题
        let softirq_names = [
            "hi", "timer", "net_tx", "net_rx", "block", "irq_poll", "tasklet", "sched", "hrtimer",
            "rcu",
        ];

        for (type_idx, type_name) in softirq_names.iter().enumerate() {
            let metric_name = format!("cpu_softirq_{}_total", type_name);
            // 检查是否应该采集该指标
            if !self.should_collect_metric(&metric_name) {
                continue;
            }
            let mut tags = MetricTags::default();

            // 计算该软中断类型的总值
            let mut total = 0u64;

            // 为每个CPU添加该软中断类型的值
            for (cpu_idx, cpu_softirq) in proc_stat.softirq_per_cpu.iter().enumerate() {
                let cpu_key = format!("cpu{}", cpu_idx);
                let value = match type_idx {
                    0 => cpu_softirq.hi,
                    1 => cpu_softirq.timer,
                    2 => cpu_softirq.net_tx,
                    3 => cpu_softirq.net_rx,
                    4 => cpu_softirq.block,
                    5 => cpu_softirq.irq_poll,
                    6 => cpu_softirq.tasklet,
                    7 => cpu_softirq.sched,
                    8 => cpu_softirq.hrtimer,
                    9 => cpu_softirq.rcu,
                    _ => 0,
                };
                tags.insert(cpu_key, value.to_string());
                total += value;
            }
            

            // 输出该软中断类型的指标，包含per-CPU计数
            output.counter(&metric_name, total as f64, tags);
        }
    }
    /// 采集全局软中断指标
    ///
    /// 生成单个指标 `cpu_softirq_all`，值为所有软中断类型的总和，
    /// 每个软中断类型的全局总值作为标签
    pub fn cpu_softirq_all(&self, proc_stat: &ProcStat, output: &mut super::MetricsBuffer) {
        if !self.should_collect_metric("cpu_softirq_all") {
            return;
        }
        if proc_stat.softirq_per_cpu.is_empty() {
            return;
        }

        // 计算全局软中断值（从所有CPU求和）
        let mut global_softirq = CpuSoftirqBreakdown::default();
        for cpu_softirq in &proc_stat.softirq_per_cpu {
            global_softirq.hi += cpu_softirq.hi;
            global_softirq.timer += cpu_softirq.timer;
            global_softirq.net_tx += cpu_softirq.net_tx;
            global_softirq.net_rx += cpu_softirq.net_rx;
            global_softirq.block += cpu_softirq.block;
            global_softirq.irq_poll += cpu_softirq.irq_poll;
            global_softirq.tasklet += cpu_softirq.tasklet;
            global_softirq.sched += cpu_softirq.sched;
            global_softirq.hrtimer += cpu_softirq.hrtimer;
            global_softirq.rcu += cpu_softirq.rcu;
        }

        // 计算总软中断数
        let total_softirq = global_softirq.hi
            + global_softirq.timer
            + global_softirq.net_tx
            + global_softirq.net_rx
            + global_softirq.block
            + global_softirq.irq_poll
            + global_softirq.tasklet
            + global_softirq.sched
            + global_softirq.hrtimer
            + global_softirq.rcu;

        // 构建标签：每个软中断类型的全局总值
        let softirq_tags = metric_tags!(
            "hi" => global_softirq.hi.to_string(),
            "timer" => global_softirq.timer.to_string(),
            "net_tx" => global_softirq.net_tx.to_string(),
            "net_rx" => global_softirq.net_rx.to_string(),
            "block" => global_softirq.block.to_string(),
            "irq_poll" => global_softirq.irq_poll.to_string(),
            "tasklet" => global_softirq.tasklet.to_string(),
            "sched" => global_softirq.sched.to_string(),
            "hrtimer" => global_softirq.hrtimer.to_string(),
            "rcu" => global_softirq.rcu.to_string()
        );

        // 使用counter类型，值为总软中断数，所有分类作为标签
        output.counter("cpu_softirq_all", total_softirq as f64, softirq_tags);
    }

    pub async fn cpu_metrics(&self, output: &mut super::MetricsBuffer) {
        // adds the metrics from cpu time for each cpu
        match heim::cpu::times().await {
            Ok(times) => {
                let times: Vec<_> = times
                    .filter_map(|result| filter_result(result, "Failed to load/parse CPU time."))
                    .collect()
                    .await;
                output.name = "cpu";
                for (index, times) in times.into_iter().enumerate() {
                    let tags = |name: &str| metric_tags!(MODE => name, "cpu" => index.to_string());
                    output.counter(CPU_SECS_TOTAL, times.idle().get::<second>(), tags("idle"));

                    output.counter(
                        CPU_SECS_TOTAL,
                        times.io_wait().get::<second>(),
                        tags("io_wait"),
                    );

                    output.counter(CPU_SECS_TOTAL, times.nice().get::<second>(), tags("nice"));
                    output.counter(
                        CPU_SECS_TOTAL,
                        times.system().get::<second>(),
                        tags("system"),
                    );
                    output.counter(CPU_SECS_TOTAL, times.user().get::<second>(), tags("user"));
                }
            }
            Err(error) => {
                error!("Failed to load CPU times: {}", error);
            }
        }
        // adds the logical cpu count gauge
        match heim::cpu::logical_count().await {
            Ok(count) => output.gauge(LOGICAL_CPUS, count as f64, MetricTags::default()),
            Err(error) => {
                error!("Failed to load logical CPU count: {}", error);
            }
        }
        // adds the physical cpu count gauge
        match heim::cpu::physical_count().await {
            Ok(Some(count)) => output.gauge(PHYSICAL_CPUS, count as f64, MetricTags::default()),
            Ok(None) => {
                error!("Unable to determine physical CPU count");
            }
            Err(error) => {
                error!("Failed to load physical CPU count: {}", error);
            }
        }
        // 添加对自定义指标的采集
        match ProcStat::new().await {
            Ok(proc_stat) => {
                output.name = "cpu";

                // 1. CPU时间指标（每个CPU核心，每种模式一个指标）
                for cpu_time in &proc_stat.cpu_times {
                    if cpu_time.cpu_id == "cpu" {
                        continue; // 跳过总CPU行
                    }

                    let cpu_index = cpu_time.cpu_id.trim_start_matches("cpu");

                    // 为每个CPU核心生成7种模式的指标
                    let modes = [
                        ("user", cpu_time.user),
                        ("nice", cpu_time.nice),
                        ("system", cpu_time.system),
                        ("idle", cpu_time.idle),
                        ("iowait", cpu_time.iowait),
                        ("irq", cpu_time.irq),
                        ("softirq", cpu_time.softirq),
                    ];

                    for (mode, value) in modes {
                        let tags = metric_tags!(
                            "cpu" => cpu_index,
                            "mode" => mode
                        );
                        output.counter("cpu_seconds_total", value as f64, tags);
                    }
                }

                self.cpu_softirq_per_cpu(&proc_stat, output);
                self.cpu_softirq_all(&proc_stat, output);
                // 3. 系统级指标
                output.counter(
                    "context_switches_total",
                    proc_stat.system_stats.context_switches as f64,
                    MetricTags::default(),
                );
                output.counter(
                    "interrupts_total",
                    proc_stat.interrupts_total as f64,
                    MetricTags::default(),
                );
                output.counter(
                    "processes_total",
                    proc_stat.system_stats.processes_total as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "procs_running",
                    proc_stat.system_stats.procs_running as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "procs_blocked",
                    proc_stat.system_stats.procs_blocked as f64,
                    MetricTags::default(),
                );

                // info!(
                //     "成功采集自定义CPU指标: {}个CPU核心, {}个软中断类型",
                //     proc_stat.cpu_times.len().saturating_sub(1),
                //     10
                // );
            }
            Err(e) => {
                error!("自定义CPU指标采集失败: {}", e);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_softirq_content() {
        let softirq_content = r#"                CPU0       CPU1       CPU2       CPU3
HI:          1000       1100       1200       1300
TIMER:       2000       2100       2200       2300
NET_TX:      3000       3100       3200       3300
NET_RX:      4000       4100       4200       4300
BLOCK:       5000       5100       5200       5300
IRQ_POLL:    6000       6100       6200       6300
TASKLET:     7000       7100       7200       7300
SCHED:       8000       8100       8200       8300
HRTIMER:     9000       9100       9200       9300
RCU:        10000      10100      10200      10300"#;

        let result = parse_softirq_content(softirq_content).unwrap();
        assert_eq!(result.len(), 4);

        assert_eq!(result[0].hi, 1000);
        assert_eq!(result[0].timer, 2000);
        assert_eq!(result[0].net_rx, 4000);

        assert_eq!(result[1].hi, 1100);
        assert_eq!(result[1].timer, 2100);
        assert_eq!(result[1].net_rx, 4100);
    }

    #[test]
    fn test_empty_stat() {
        let empty = "";
        let proc_stat = ProcStat::parse_from_files(empty, "").unwrap();
        assert!(proc_stat.cpu_times.is_empty());
        assert!(proc_stat.softirq_per_cpu.is_empty());
        assert_eq!(proc_stat.system_stats.context_switches, 0);
    }

    #[test]
    fn test_parse_from_files() {
        let stat_content = r#"cpu  100 200 300 400 500 600 700 800 900 1000
cpu0 10 20 30 40 50 60 70 80 90 100
cpu1 15 25 35 45 55 65 75 85 95 105
ctxt 123456
btime 1609459200
processes 1000
procs_running 2
procs_blocked 0
intr 500000"#;

        let softirq_content = r#"                CPU0       CPU1
HI:          1000       1100
TIMER:       2000       2100
NET_TX:      3000       3100
NET_RX:      4000       4100
BLOCK:       5000       5100
IRQ_POLL:    6000       6100
TASKLET:     7000       7100
SCHED:       8000       8100
HRTIMER:     9000       9100
RCU:        10000      10100"#;

        let proc_stat = ProcStat::parse_from_files(stat_content, softirq_content).unwrap();

        assert_eq!(proc_stat.cpu_times.len(), 3); // cpu + cpu0 + cpu1
        assert_eq!(proc_stat.softirq_per_cpu.len(), 2);
        assert_eq!(proc_stat.system_stats.context_switches, 123456);

        assert_eq!(proc_stat.softirq_per_cpu[0].hi, 1000);
        assert_eq!(proc_stat.softirq_per_cpu[0].net_rx, 4000);
        assert_eq!(proc_stat.softirq_per_cpu[1].timer, 2100);
        assert_eq!(proc_stat.softirq_per_cpu[1].rcu, 10100);
    }
}
