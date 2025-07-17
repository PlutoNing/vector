use super::{default_all_processes, example_processes, FilterList, HostMetrics};
use std::ffi::OsStr;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, UpdateKind};
use vector_lib::configurable::configurable_component;
#[cfg(target_os = "linux")]
use vector_lib::metric_tags;

/// Options for the process metrics collector.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct ProcessConfig {
    /// Lists of process name patterns to include or exclude.
    #[serde(default = "default_all_processes")]
    #[configurable(metadata(docs::examples = "example_processes()"))]
    processes: FilterList,
}

const RUNTIME: &str = "process_runtime";
const CPU_USAGE: &str = "process_cpu_usage";
const MEMORY_USAGE: &str = "process_memory_usage";
const MEMORY_VIRTUAL_USAGE: &str = "process_memory_virtual_usage";

impl HostMetrics {
    pub async fn process_metrics(&mut self, output: &mut super::MetricsBuffer) {
        self.system.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::default()
                .with_memory()
                .with_cpu()
                .with_cmd(UpdateKind::OnlyIfNotSet),
        );
        output.name = "process";
        let sep = OsStr::new(" ");
        for (pid, process) in self.system.processes().iter().filter(|&(_, proc)| {
            self.config
                .process
                .processes
                .contains_str(proc.name().to_str())
        }) {
            let tags = || {
                metric_tags!(
                "pid" => pid.as_u32().to_string(),
                "name" => process.name().to_str().unwrap_or("unknown"),
                "command" => process.cmd().join(sep).to_str().unwrap_or(""))
            };
            output.gauge(CPU_USAGE, process.cpu_usage().into(), tags());
            output.gauge(MEMORY_USAGE, process.memory() as f64, tags());
            output.gauge(
                MEMORY_VIRTUAL_USAGE,
                process.virtual_memory() as f64,
                tags(),
            );
            output.counter(RUNTIME, process.run_time() as f64, tags());
        }
    }
}