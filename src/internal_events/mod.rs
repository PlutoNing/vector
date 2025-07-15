#![allow(missing_docs)]
pub mod prelude;

mod adaptive_concurrency;
mod aggregate;

#[cfg(feature = "api")]
mod api;
mod batch;
pub mod codecs;
mod common;
mod conditions;

#[cfg(feature = "transforms-impl-dedupe")]
mod dedupe;
#[cfg(feature = "sources-demo_logs")]
mod demo_logs;
#[cfg(feature = "sources-dnstap")]
mod dnstap;
mod encoding_transcode;
#[cfg(feature = "sources-exec")]
mod exec;
#[cfg(any(feature = "sources-file_descriptor", feature = "sources-stdin"))]
mod file_descriptor;
#[cfg(feature = "transforms-filter")]
mod filter;
mod heartbeat;
#[cfg(feature = "sources-host_metrics")]
mod host_metrics;
mod http;
pub mod http_client;
#[cfg(feature = "sources-utils-http-client")]
mod http_client_source;
#[cfg(feature = "sinks-influxdb")]
mod influxdb;
#[cfg(all(unix, feature = "sources-journald"))]
mod journald;
#[cfg(feature = "transforms-log_to_metric")]
mod log_to_metric;
#[cfg(feature = "sinks-loki")]
mod loki;
#[cfg(feature = "transforms-lua")]
mod lua;
#[cfg(feature = "transforms-metric_to_log")]
mod metric_to_log;
#[cfg(feature = "sources-mongodb_metrics")]
mod mongodb_metrics;
#[cfg(feature = "sources-nginx_metrics")]
mod nginx_metrics;
mod open;
mod parser;
#[cfg(feature = "sources-postgresql_metrics")]
mod postgresql_metrics;
mod process;
#[cfg(any(
    feature = "sources-prometheus-scrape",
    feature = "sources-prometheus-remote-write",
    feature = "sinks-prometheus"
))]
mod prometheus;
#[cfg(feature = "transforms-impl-reduce")]
mod reduce;
mod remap;
mod sample;
mod socket;
#[cfg(feature = "sinks-statsd")]
mod statsd_sink;
#[cfg(feature = "transforms-tag_cardinality_limit")]
mod tag_cardinality_limit;
mod tcp;
mod template;
#[cfg(feature = "transforms-throttle")]
mod throttle;
mod udp;
mod unix;
#[cfg(feature = "sinks-websocket")]
mod websocket;
#[cfg(feature = "sinks-websocket-server")]
mod websocket_server;
#[cfg(feature = "transforms-window")]
mod window;

#[cfg(any(
    feature = "sources-file",
    feature = "sinks-file",
))]
mod file;
mod windows;

#[cfg(feature = "sources-mongodb_metrics")]
pub(crate) use mongodb_metrics::*;

#[cfg(feature = "transforms-aggregate")]
pub(crate) use self::aggregate::*;
#[cfg(feature = "api")]
pub(crate) use self::api::*;
#[cfg(feature = "transforms-impl-dedupe")]
pub(crate) use self::dedupe::*;
#[cfg(feature = "sources-demo_logs")]
pub(crate) use self::demo_logs::*;
#[cfg(feature = "sources-dnstap")]
pub(crate) use self::dnstap::*;
#[cfg(feature = "sources-exec")]
pub(crate) use self::exec::*;
#[cfg(any(
    feature = "sources-file",
    feature = "sinks-file",
))]
pub(crate) use self::file::*;
#[cfg(any(feature = "sources-file_descriptor", feature = "sources-stdin"))]
pub(crate) use self::file_descriptor::*;
#[cfg(feature = "transforms-filter")]
pub(crate) use self::filter::*;
#[cfg(feature = "sources-host_metrics")]
pub(crate) use self::host_metrics::*;
#[cfg(feature = "sources-utils-http-client")]
pub(crate) use self::http_client_source::*;
#[cfg(feature = "sinks-influxdb")]
pub(crate) use self::influxdb::*;
#[cfg(all(unix, feature = "sources-journald"))]
pub(crate) use self::journald::*;
#[cfg(feature = "transforms-log_to_metric")]
pub(crate) use self::log_to_metric::*;
#[cfg(feature = "sinks-loki")]
pub(crate) use self::loki::*;
#[cfg(feature = "transforms-lua")]
pub(crate) use self::lua::*;
#[cfg(feature = "transforms-metric_to_log")]
pub(crate) use self::metric_to_log::*;
#[cfg(feature = "sources-nginx_metrics")]
pub(crate) use self::nginx_metrics::*;
#[allow(unused_imports)]
pub(crate) use self::parser::*;
#[cfg(feature = "sources-postgresql_metrics")]
pub(crate) use self::postgresql_metrics::*;
#[cfg(any(
    feature = "sources-prometheus-scrape",
    feature = "sources-prometheus-remote-write",
    feature = "sinks-prometheus"
))]
pub(crate) use self::prometheus::*;
#[cfg(feature = "transforms-impl-reduce")]
pub(crate) use self::reduce::*;
#[cfg(feature = "transforms-remap")]
pub(crate) use self::remap::*;
#[cfg(feature = "transforms-impl-sample")]
pub(crate) use self::sample::*;
#[cfg(feature = "sinks-statsd")]
pub(crate) use self::statsd_sink::*;
#[cfg(feature = "transforms-tag_cardinality_limit")]
pub(crate) use self::tag_cardinality_limit::*;
#[cfg(feature = "transforms-throttle")]
pub(crate) use self::throttle::*;
#[cfg(unix)]
pub(crate) use self::unix::*;
#[cfg(feature = "sinks-websocket")]
pub(crate) use self::websocket::*;
#[cfg(feature = "sinks-websocket-server")]
pub(crate) use self::websocket_server::*;
#[cfg(feature = "transforms-window")]
pub(crate) use self::window::*;
#[cfg(windows)]
pub(crate) use self::windows::*;
pub use self::{
    adaptive_concurrency::*, batch::*, common::*, conditions::*, encoding_transcode::*,
    heartbeat::*, http::*, open::*, process::*, socket::*, tcp::*, template::*, udp::*,
};
