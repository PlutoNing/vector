#![allow(missing_docs)]
use snafu::Snafu;

#[cfg(feature = "sources-demo_logs")]
pub mod demo_logs;
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
#[cfg(all(unix, feature = "sources-journald"))]
pub mod journald;
#[cfg(feature = "sources-socket")]
pub mod socket;
#[cfg(feature = "sources-static_metrics")]
pub mod static_metrics;
#[cfg(feature = "sources-syslog")]
pub mod syslog;

pub mod util;

pub use vector_lib::source::Source;

#[allow(dead_code)] // Easier than listing out all the features that use this
/// Common build errors
#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },
}
