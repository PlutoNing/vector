#![allow(missing_docs)]
use snafu::Snafu;

#[cfg(feature = "sources-host_metrics")]
pub mod host_metrics;

pub mod util;

pub use vector_lib::source::Source;

#[allow(dead_code)] // Easier than listing out all the features that use this
/// Common build errors
#[derive(Debug, Snafu)]
enum BuildError {
    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },
}
