#![allow(missing_docs)]

use snafu::Snafu;

pub mod prelude;
pub mod util;

#[cfg(feature = "sinks-console")]
pub mod console;
#[cfg(feature = "sinks-file")] 
pub mod file;
//pub mod sqlite;
pub use vector_lib::{config::Input, sink::VectorSink};

/// Common build errors
#[derive(Debug, Snafu)]
pub enum BuildError {
    #[snafu(display("Unable to resolve DNS for {:?}", address))]
    DnsFailure { address: String },

    #[snafu(display("Socket address problem: {}", source))]
    SocketAddressError { source: std::io::Error },

    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },

    #[snafu(display("HTTP request build error: {}", source))]
    HTTPRequestBuilderError { source: ::http::Error },
}
