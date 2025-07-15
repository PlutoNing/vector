#![allow(missing_docs)]
use snafu::Snafu;

#[cfg(feature = "sources-apache_metrics")]
pub mod apache_metrics;
#[cfg(feature = "sources-demo_logs")]
pub mod demo_logs;
#[cfg(feature = "sources-dnstap")]
pub mod dnstap;
#[cfg(feature = "sources-eventstoredb_metrics")]
pub mod eventstoredb_metrics;
#[cfg(feature = "sources-exec")]
pub mod exec;
#[cfg(feature = "sources-file")]
pub mod file;
#[cfg(any(
    feature = "sources-stdin",
    all(unix, feature = "sources-file_descriptor")
))]
pub mod file_descriptors;
#[cfg(feature = "sources-host_metrics")]
pub mod host_metrics;
#[cfg(feature = "sources-http_client")]
pub mod http_client;
#[cfg(feature = "sources-http_server")]
pub mod http_server;
#[cfg(feature = "sources-internal_logs")]
pub mod internal_logs;
#[cfg(feature = "sources-internal_metrics")]
pub mod internal_metrics;
#[cfg(all(unix, feature = "sources-journald"))]
pub mod journald;
#[cfg(feature = "sources-mongodb_metrics")]
pub mod mongodb_metrics;
#[cfg(feature = "sources-nginx_metrics")]
pub mod nginx_metrics;
#[cfg(feature = "sources-postgresql_metrics")]
pub mod postgresql_metrics;
#[cfg(any(
    feature = "sources-prometheus-scrape",
    feature = "sources-prometheus-remote-write",
    feature = "sources-prometheus-pushgateway"
))]
pub mod prometheus;
#[cfg(feature = "sources-redis")]
pub mod redis;
#[cfg(feature = "sources-socket")]
pub mod socket;
#[cfg(feature = "sources-splunk_hec")]
pub mod splunk_hec;
#[cfg(feature = "sources-static_metrics")]
pub mod static_metrics;
#[cfg(feature = "sources-statsd")]
pub mod statsd;
#[cfg(feature = "sources-syslog")]
pub mod syslog;
#[cfg(feature = "sources-vector")]
pub mod vector;

pub mod util;

pub use vector_lib::source::Source;

#[allow(dead_code)] // Easier than listing out all the features that use this
/// Common build errors
#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },
}
