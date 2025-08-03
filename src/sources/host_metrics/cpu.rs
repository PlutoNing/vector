use std::{num::ParseIntError, str::FromStr};

use agent_lib::{event::MetricTags, metric_tags};
use futures::StreamExt;
#[cfg(target_os = "linux")]
use heim::cpu::os::linux::CpuTimeExt;
use heim::units::time::second;

use super::{filter_result, HostMetrics};

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
    /// 软中断分类统计
    pub softirq_breakdown: SoftirqBreakdown,
    /// 中断统计（总中断数）
    pub interrupts_total: u64,
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

/// 软中断分类统计
#[allow(dead_code)]
#[derive(Debug, Default, Clone)]
pub struct SoftirqBreakdown {
    pub hi: u64,       // 高优先级软中断
    pub timer: u64,    // 定时器软中断
    pub net_tx: u64,   // 网络发送软中断
    pub net_rx: u64,   // 网络接收软中断
    pub block: u64,    // 块设备软中断
    pub irq_poll: u64, // IRQ轮询软中断
    pub tasklet: u64,  // 任务队列软中断
    pub sched: u64,    // 调度软中断
    pub hrtimer: u64,  // 高精度定时器软中断
    pub rcu: u64,      // RCU软中断
}

impl FromStr for ProcStat {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut proc_stat = ProcStat::default();
        let mut lines = s.lines();

        // 解析CPU时间
        for line in &mut lines {
            if line.starts_with("cpu") {
                let cpu_time = CpuTime::from_str(line)?;
                proc_stat.cpu_times.push(cpu_time);
            } else {
                break; // CPU部分结束
            }
        }

        // 解析剩余行
        for line in lines {
            let mut parts = line.split_whitespace();
            if let Some(key) = parts.next() {
                match key {
                    "ctxt" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.context_switches = value.parse()?;
                        }
                    }
                    "btime" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.boot_time = value.parse()?;
                        }
                    }
                    "processes" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.processes_total = value.parse()?;
                        }
                    }
                    "procs_running" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.procs_running = value.parse()?;
                        }
                    }
                    "procs_blocked" => {
                        if let Some(value) = parts.next() {
                            proc_stat.system_stats.procs_blocked = value.parse()?;
                        }
                    }
                    "intr" => {
                        if let Some(value) = parts.next() {
                            proc_stat.interrupts_total = value.parse()?;
                        }
                    }
                    "softirq" => {
                        let values: Vec<u64> =
                            parts.take(10).filter_map(|v| v.parse().ok()).collect();

                        if values.len() >= 10 {
                            proc_stat.softirq_breakdown = SoftirqBreakdown {
                                hi: values[0],
                                timer: values[1],
                                net_tx: values[2],
                                net_rx: values[3],
                                block: values[4],
                                irq_poll: values[5],
                                tasklet: values[6],
                                sched: values[7],
                                hrtimer: values[8],
                                rcu: values[9],
                            };
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(proc_stat)
    }
}

impl FromStr for CpuTime {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split_whitespace();
        let cpu_id = parts.next().unwrap_or("cpu").to_string();

        let values: Vec<u64> = parts.take(10).filter_map(|v| v.parse().ok()).collect();

        // 填充默认值，防止数据不足
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
}

impl HostMetrics {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_proc_stat() {
        let stat_content = r#"cpu  2777199 646 1642986 386905069 191059 0 88252 0 0 0
cpu0 66367 0 55457 16167661 14867 0 67928 0 0 0
cpu1 156028 200 63934 16074338 18177 0 7385 0 0 0
ctxt 332762667
btime 1754053669
processes 1197085
procs_running 1
procs_blocked 0
softirq 82482587 0 8357413 5 2059072 0 0 5088815 35665091 562 31311629"#;

        let proc_stat = ProcStat::from_str(stat_content).unwrap();

        println!("=== ProcStat 解析结果 ===");
        println!("CPU核心数: {}", proc_stat.cpu_times.len());

        println!("\n=== CPU时间统计 ===");
        for cpu_time in &proc_stat.cpu_times {
            println!("{}: user={}, nice={}, system={}, idle={}, iowait={}, irq={}, softirq={}, steal={}, guest={}, guest_nice={}",
                cpu_time.cpu_id,
                cpu_time.user, cpu_time.nice, cpu_time.system, cpu_time.idle,
                cpu_time.iowait, cpu_time.irq, cpu_time.softirq, cpu_time.steal,
                cpu_time.guest, cpu_time.guest_nice);
        }

        println!("\n=== 系统级统计 ===");
        println!("上下文切换: {}", proc_stat.system_stats.context_switches);
        println!("启动时间: {}", proc_stat.system_stats.boot_time);
        println!("进程总数: {}", proc_stat.system_stats.processes_total);
        println!("运行进程: {}", proc_stat.system_stats.procs_running);
        println!("阻塞进程: {}", proc_stat.system_stats.procs_blocked);

        println!("\n=== 软中断分类 ===");
        println!("HI: {}", proc_stat.softirq_breakdown.hi);
        println!("TIMER: {}", proc_stat.softirq_breakdown.timer);
        println!("NET_TX: {}", proc_stat.softirq_breakdown.net_tx);
        println!("NET_RX: {}", proc_stat.softirq_breakdown.net_rx);
        println!("BLOCK: {}", proc_stat.softirq_breakdown.block);
        println!("IRQ_POLL: {}", proc_stat.softirq_breakdown.irq_poll);
        println!("TASKLET: {}", proc_stat.softirq_breakdown.tasklet);
        println!("SCHED: {}", proc_stat.softirq_breakdown.sched);
        println!("HRTIMER: {}", proc_stat.softirq_breakdown.hrtimer);
        println!("RCU: {}", proc_stat.softirq_breakdown.rcu);

        println!("\n=== 中断统计 ===");
        println!("总中断数: {}", proc_stat.interrupts_total);
    }
    #[test]
    fn test_parse_real_proc_stat() {
        use std::fs;
        // 读取真实的 /proc/stat 文件
        let stat_content = match fs::read_to_string("/proc/stat") {
            Ok(content) => content,
            Err(e) => {
                eprintln!("无法读取 /proc/stat: {}", e);
                return;
            }
        };

        println!("=== 真实系统 /proc/stat 解析 ===");
        println!("文件大小: {} 字节", stat_content.len());
        println!("文件内容预览:");
        println!("{}", stat_content.lines().take(5).collect::<Vec<_>>().join("\n"));
        println!("...");

        match ProcStat::from_str(&stat_content) {
            Ok(proc_stat) => {
                println!("\n=== 解析成功 ===");
                println!("CPU核心数: {}", proc_stat.cpu_times.len());
                
                if !proc_stat.cpu_times.is_empty() {
                    println!("\n=== 第一个CPU统计 ===");
                    let first_cpu = &proc_stat.cpu_times[0];
                    println!("CPU: {}", first_cpu.cpu_id);
                    println!("用户态时间: {} ticks", first_cpu.user);
                    println!("系统态时间: {} ticks", first_cpu.system);
                    println!("空闲时间: {} ticks", first_cpu.idle);
                    println!("I/O等待: {} ticks", first_cpu.iowait);
                }

                if proc_stat.cpu_times.len() > 1 {
                    println!("\n=== 物理CPU统计 ===");
                    for (i, cpu_time) in proc_stat.cpu_times.iter().skip(1).take(3).enumerate() {
                        println!("CPU{}: user={}, system={}, idle={}", 
                            i, cpu_time.user, cpu_time.system, cpu_time.idle);
                    }
                    
                    if proc_stat.cpu_times.len() > 4 {
                        println!("... 还有 {} 个CPU核心", proc_stat.cpu_times.len() - 4);
                    }
                }

                println!("\n=== 系统级统计 ===");
                println!("上下文切换总数: {}", proc_stat.system_stats.context_switches);
                println!("系统启动时间: {} (Unix时间戳)", proc_stat.system_stats.boot_time);
                println!("启动时间转换: {}", {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let duration = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let uptime = duration.saturating_sub(proc_stat.system_stats.boot_time);
                    format!("{}天{}小时{}分钟", 
                        uptime / 86400, 
                        (uptime % 86400) / 3600, 
                        (uptime % 3600) / 60)
                });
                println!("进程总数: {}", proc_stat.system_stats.processes_total);
                println!("当前运行进程: {}", proc_stat.system_stats.procs_running);
                println!("当前阻塞进程: {}", proc_stat.system_stats.procs_blocked);

                println!("\n=== 软中断统计 ===");
                let softirq = &proc_stat.softirq_breakdown;
                let total_softirq = softirq.hi + softirq.timer + softirq.net_tx + 
                                  softirq.net_rx + softirq.block + softirq.irq_poll + 
                                  softirq.tasklet + softirq.sched + softirq.hrtimer + 
                                  softirq.rcu;
                println!("软中断总数: {}", total_softirq);
                println!("网络接收: {} ({:.1}%)", softirq.net_rx, 
                    softirq.net_rx as f64 / total_softirq as f64 * 100.0);
                println!("RCU: {} ({:.1}%)", softirq.rcu, 
                    softirq.rcu as f64 / total_softirq as f64 * 100.0);
                println!("定时器: {} ({:.1}%)", softirq.timer, 
                    softirq.timer as f64 / total_softirq as f64 * 100.0);

                println!("\n=== 中断统计 ===");
                println!("总中断数: {}", proc_stat.interrupts_total);

                // 计算CPU使用率（基于第一个CPU）
                if proc_stat.cpu_times.len() > 1 {
                    let cpu0 = &proc_stat.cpu_times[1]; // 跳过总CPU行
                    let total = cpu0.user + cpu0.nice + cpu0.system + cpu0.idle + 
                               cpu0.iowait + cpu0.irq + cpu0.softirq + cpu0.steal;
                    let usage = (cpu0.user + cpu0.nice + cpu0.system) as f64 / total as f64 * 100.0;
                    println!("CPU0使用率: {:.2}%", usage);
                }
            }
            Err(e) => {
                eprintln!("解析失败: {}", e);
            }
        }
    }

    #[test]
    fn test_empty_stat() {
        let empty = "";
        let proc_stat = ProcStat::from_str(empty).unwrap();
        assert!(proc_stat.cpu_times.is_empty());
        assert_eq!(proc_stat.system_stats.context_switches, 0);
    }
}
