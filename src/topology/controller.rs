use std::sync::Arc;
use tokio::sync::{Mutex, MutexGuard};

use crate::{config, app::ShutdownError, topology::RunningTopology};

#[derive(Clone, Debug)]
pub struct SharedTopologyController(Arc<Mutex<TopologyController>>);

impl SharedTopologyController {
    pub fn new(inner: TopologyController) -> Self {
        Self(Arc::new(Mutex::new(inner)))
    }

    pub fn blocking_lock(&self) -> MutexGuard<TopologyController> {
        self.0.blocking_lock()
    }

    pub async fn lock(&self) -> MutexGuard<TopologyController> {
        self.0.lock().await
    }

    pub fn try_into_inner(self) -> Result<Mutex<TopologyController>, Self> {
        Arc::try_unwrap(self.0).map_err(Self)
    }
}

pub struct TopologyController {
    pub topology: RunningTopology,
    pub config_paths: Vec<config::ConfigPath>,
}

impl std::fmt::Debug for TopologyController {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopologyController")
            .field("config_paths", &self.config_paths)

            .finish()
    }
}

#[derive(Clone, Debug)]
pub enum ReloadOutcome {
    MissingApiKey,
    Success,
    RolledBack,
    FatalError(ShutdownError),
}

impl TopologyController {
    /* 调用 */
    pub async fn stop(self) {
        self.topology.stop().await;
    }
}
