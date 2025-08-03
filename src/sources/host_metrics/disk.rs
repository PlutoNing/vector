use futures::StreamExt;
use heim::units::information::byte;
use agent_lib::configurable::configurable_component;
use agent_lib::metric_tags;

use super::{default_all_devices, example_devices, filter_result, FilterList, HostMetrics};

/* host metrics的disk相关的子config
disk:
  devices:
    includes: ["sda", "sdb"]
    excludes: ["loop*", "dm-*"]
 */
/// Options for the disk metrics collector.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct DiskConfig {
    /// Lists of device name patterns to include or exclude in gathering
    /// I/O utilize metrics.
    #[configurable(metadata(docs::examples = "example_devices()"))]
    #[serde(default = "default_all_devices")]
    devices: FilterList,
}

impl HostMetrics {
    pub async fn disk_metrics(&self, output: &mut super::MetricsBuffer) {
        match heim::disk::io_counters().await {
            Ok(counters) => {
                for counter in counters
                    .filter_map(|result| {
                        filter_result(result, "Failed to load/parse disk I/O data.")
                    })
                    .map(|counter| {
                        self.config
                            .disk
                            .devices
                            .contains_path(Some(counter.device_name().as_ref()))
                            .then_some(counter)
                    })
                    .filter_map(|counter| async { counter })
                    .collect::<Vec<_>>()
                    .await
                {
                    let tags = metric_tags! {
                        "device" => counter.device_name().to_string_lossy()
                    };
                    output.name = "disk";
                    output.counter(
                        "disk_read_bytes_total",
                        counter.read_bytes().get::<byte>() as f64,
                        tags.clone(),
                    );
                    output.counter(
                        "disk_reads_completed_total",
                        counter.read_count() as f64,
                        tags.clone(),
                    );
                    output.counter(
                        "disk_written_bytes_total",
                        counter.write_bytes().get::<byte>() as f64,
                        tags.clone(),
                    );
                    output.counter(
                        "disk_writes_completed_total",
                        counter.write_count() as f64,
                        tags,
                    );
                }
            }
            Err(error) => {
                error!("Failed to load disk I/O info: {}", error);
            }
        }
    }
}