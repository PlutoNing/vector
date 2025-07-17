//! Emits a heartbeat internal metric.
use std::time::{Duration, Instant};

use tokio::time::interval;

use crate::internal_events::Heartbeat;
/* heartbeat进程 */
/// Emits Heartbeat event every second.
pub async fn heartbeat() {
    let since = Instant::now();
    let mut interval = interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        emit!(Heartbeat { since });
    }
}
