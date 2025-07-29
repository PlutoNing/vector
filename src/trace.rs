#![allow(missing_docs)]

pub use tracing_tower::{InstrumentableService, InstrumentedService};

pub fn init(color: bool, _json: bool, levels: &str, _internal_log_rate_limit: u64) {
    tracing_subscriber::fmt()
        .with_ansi(color)
        .with_writer(std::io::stderr)
        .with_env_filter(levels)
        .init();
}
