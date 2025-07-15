#![allow(missing_docs)]
use futures::future::BoxFuture;
use snafu::Snafu;

pub mod prelude;
pub mod util;


#[cfg(feature = "sinks-appsignal")]
pub mod appsignal;
#[cfg(feature = "sinks-axiom")]
pub mod axiom;
#[cfg(feature = "sinks-azure_blob")]
pub mod azure_blob;
#[cfg(feature = "sinks-azure_blob")]
pub mod azure_common;
#[cfg(feature = "sinks-azure_monitor_logs")]
pub mod azure_monitor_logs;
#[cfg(feature = "sinks-blackhole")]
pub mod blackhole;
#[cfg(feature = "sinks-clickhouse")]
pub mod clickhouse;
#[cfg(feature = "sinks-console")]
pub mod console;
#[cfg(feature = "sinks-databend")]
pub mod databend;
#[cfg(feature = "sinks-file")]
pub mod file;
#[cfg(any(
    feature = "sinks-greptimedb_metrics",
    feature = "sinks-greptimedb_logs"
))]
pub mod greptimedb;
#[cfg(feature = "sinks-honeycomb")]
pub mod honeycomb;
#[cfg(feature = "sinks-http")]
pub mod http;
#[cfg(feature = "sinks-humio")]
pub mod humio;
#[cfg(any(feature = "sinks-influxdb", feature = "prometheus-integration-tests"))]
pub mod influxdb;
#[cfg(feature = "sinks-keep")]
pub mod keep;
#[cfg(feature = "sinks-loki")]
pub mod loki;
#[cfg(feature = "sinks-mezmo")]
pub mod mezmo;
#[cfg(feature = "sinks-mqtt")]
pub mod mqtt;
#[cfg(feature = "sinks-nats")]
pub mod nats;
#[cfg(feature = "sinks-new_relic")]
pub mod new_relic;
#[cfg(feature = "sinks-webhdfs")]
pub mod opendal_common;
#[cfg(feature = "sinks-opentelemetry")]
pub mod opentelemetry;
#[cfg(feature = "sinks-papertrail")]
pub mod papertrail;
#[cfg(feature = "sinks-postgres")]
pub mod postgres;
#[cfg(feature = "sinks-prometheus")]
pub mod prometheus;
#[cfg(feature = "sinks-pulsar")]
pub mod pulsar;
#[cfg(feature = "sinks-redis")]
pub mod redis;
#[cfg(feature = "sinks-socket")]
pub mod socket;
#[cfg(feature = "sinks-splunk_hec")]
pub mod splunk_hec;
#[cfg(feature = "sinks-statsd")]
pub mod statsd;
#[cfg(feature = "sinks-vector")]
pub mod vector;
#[cfg(feature = "sinks-webhdfs")]
pub mod webhdfs;
#[cfg(feature = "sinks-websocket")]
pub mod websocket;
#[cfg(feature = "sinks-websocket-server")]
pub mod websocket_server;

pub use vector_lib::{config::Input, sink::VectorSink};

pub type Healthcheck = BoxFuture<'static, crate::Result<()>>;

/// Common build errors
#[derive(Debug, Snafu)]
pub enum BuildError {
    #[snafu(display("Unable to resolve DNS for {:?}", address))]
    DnsFailure { address: String },
    #[snafu(display("DNS errored {}", source))]
    DnsError { source: crate::dns::DnsError },
    #[snafu(display("Socket address problem: {}", source))]
    SocketAddressError { source: std::io::Error },
    #[snafu(display("URI parse error: {}", source))]
    UriParseError { source: ::http::uri::InvalidUri },
    #[snafu(display("HTTP request build error: {}", source))]
    HTTPRequestBuilderError { source: ::http::Error },
}

/// Common healthcheck errors
#[derive(Debug, Snafu)]
pub enum HealthcheckError {
    #[snafu(display("Unexpected status: {}", status))]
    UnexpectedStatus { status: ::http::StatusCode },
}
