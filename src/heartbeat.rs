//! Emits a heartbeat internal metric.
use std::time::{Duration, Instant};

use tokio::time::interval;
/* heartbeat进程 */
/// Emits heartbeat event every second.
pub async fn heartbeat() {
    let since = Instant::now();
    let mut interval = interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        debug!("heartbeat: uptime {} seconds", since.elapsed().as_secs());
    }
}
