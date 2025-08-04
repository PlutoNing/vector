use heim::memory::os::linux::MemoryExt;
use heim::units::information::byte;
use agent_lib::event::MetricTags;

use super::HostMetrics;

impl HostMetrics {
    pub async fn memory_metrics(&self, output: &mut super::MetricsBuffer) {
        output.name = "memory";
        match heim::memory::memory().await {
            Ok(memory) => {
                output.gauge(
                    "memory_total_bytes",
                    memory.total().get::<byte>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "memory_free_bytes",
                    memory.free().get::<byte>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "memory_available_bytes",
                    memory.available().get::<byte>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "memory_active_bytes",
                    memory.active().get::<byte>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "memory_buffers_bytes",
                    memory.buffers().get::<byte>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "memory_cached_bytes",
                    memory.cached().get::<byte>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "memory_shared_bytes",
                    memory.shared().get::<byte>() as f64,
                    MetricTags::default(),
                );
                output.gauge(
                    "memory_used_bytes",
                    memory.used().get::<byte>() as f64,
                    MetricTags::default(),
                );
            }
            Err(error) => {
                error!("Failed to load memory info: {}", error);
            }
        }
    }
}
