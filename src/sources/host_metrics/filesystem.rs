use super::{default_all_devices, example_devices, filter_result, FilterList, HostMetrics};
use agent_lib::configurable::configurable_component;
use agent_lib::metric_tags;
use futures::StreamExt;
use heim::units::information::byte;
#[cfg(not(windows))]
use heim::units::ratio::ratio;

/// Options for the filesystem metrics collector.
#[configurable_component]
#[derive(Clone, Debug, Default)]
pub struct FilesystemConfig {
    /// Lists of device name patterns to include or exclude in gathering
    /// usage metrics.
    #[serde(default = "default_all_devices")]
    #[configurable(metadata(docs::examples = "example_devices()"))]
    devices: FilterList,

    /// Lists of filesystem name patterns to include or exclude in gathering
    /// usage metrics.
    #[serde(default = "default_all_devices")]
    #[configurable(metadata(docs::examples = "example_filesystems()"))]
    filesystems: FilterList,

    /// Lists of mount point path patterns to include or exclude in gathering
    /// usage metrics.
    #[serde(default = "default_all_devices")]
    #[configurable(metadata(docs::examples = "example_mountpoints()"))]
    mountpoints: FilterList,
}

fn example_filesystems() -> FilterList {
    FilterList {
        includes: Some(vec!["ntfs".try_into().unwrap()]),
        excludes: Some(vec!["ext*".try_into().unwrap()]),
    }
}

fn example_mountpoints() -> FilterList {
    FilterList {
        includes: Some(vec!["/home".try_into().unwrap()]),
        excludes: Some(vec!["/raid*".try_into().unwrap()]),
    }
}

impl HostMetrics {
    pub async fn filesystem_metrics(&self, output: &mut super::MetricsBuffer) {
        output.name = "filesystem";
        match heim::disk::partitions().await {
            Ok(partitions) => {
                for (partition, usage) in partitions
                    .filter_map(|result| {
                        filter_result(result, "Failed to load/parse partition data.")
                    })
                    // Filter on configured mountpoints
                    .map(|partition| {
                        self.config
                            .filesystem
                            .mountpoints
                            .contains_path(Some(partition.mount_point()))
                            .then_some(partition)
                    })
                    .filter_map(|partition| async { partition })
                    // Filter on configured devices
                    .map(|partition| {
                        self.config
                            .filesystem
                            .devices
                            .contains_path(partition.device().map(|d| d.as_ref()))
                            .then_some(partition)
                    })
                    .filter_map(|partition| async { partition })
                    // Filter on configured filesystems
                    .map(|partition| {
                        self.config
                            .filesystem
                            .filesystems
                            .contains_str(Some(partition.file_system().as_str()))
                            .then_some(partition)
                    })
                    .filter_map(|partition| async { partition })
                    // Load usage from the partition mount point
                    .filter_map(|partition| async {
                        heim::disk::usage(partition.mount_point())
                            .await
                            .map_err(|error| {
                                error!(
                                    "Failed to load partitions info for mount point {}: {}",
                                    partition.mount_point().to_str().unwrap_or("unknown"),
                                    error
                                );
                            })
                            .map(|usage| (partition, usage))
                            .ok()
                    })
                    .collect::<Vec<_>>()
                    .await
                {
                    let fs = partition.file_system();
                    let mut tags = metric_tags! {
                        "filesystem" => fs.as_str(),
                        "mountpoint" => partition.mount_point().to_string_lossy()
                    };
                    if let Some(device) = partition.device() {
                        tags.replace("device".into(), device.to_string_lossy().to_string());
                    }
                    output.gauge(
                        "filesystem_free_bytes",
                        usage.free().get::<byte>() as f64,
                        tags.clone(),
                    );
                    output.gauge(
                        "filesystem_total_bytes",
                        usage.total().get::<byte>() as f64,
                        tags.clone(),
                    );
                    output.gauge(
                        "filesystem_used_bytes",
                        usage.used().get::<byte>() as f64,
                        tags.clone(),
                    );
                    #[cfg(not(windows))]
                    output.gauge(
                        "filesystem_used_ratio",
                        usage.ratio().get::<ratio>() as f64,
                        tags,
                    );
                }
            }
            Err(error) => {
                error!("Failed to load partitions info: {}", error);
            }
        }
    }
}
