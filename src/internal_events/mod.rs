#![allow(missing_docs)]

mod adaptive_concurrency;
mod aggregate;

mod batch;
pub mod codecs;
mod common;
mod conditions;

#[cfg(feature = "sources-demo_logs")]
mod demo_logs;
mod encoding_transcode;
mod heartbeat;
#[cfg(feature = "sources-host_metrics")]
mod host_metrics;
mod http;
pub mod http_client;
mod open;
mod parser;
mod process;
#[cfg(feature = "transforms-impl-reduce")]
mod reduce;
mod remap;
mod sample;
mod socket;
#[cfg(feature = "transforms-tag_cardinality_limit")]
mod tag_cardinality_limit;
mod tcp;
mod template;
mod udp;
mod unix;

#[cfg(any(
    feature = "sinks-file",
))]
mod file;
mod windows;

#[cfg(feature = "sources-demo_logs")]
pub(crate) use self::demo_logs::*;
#[cfg(any(
    feature = "sinks-file",
))]
pub(crate) use self::file::*;
#[cfg(feature = "sources-host_metrics")]
pub(crate) use self::host_metrics::*;
#[allow(unused_imports)]
pub(crate) use self::parser::*;
#[cfg(feature = "transforms-impl-reduce")]
pub(crate) use self::reduce::*;
#[cfg(feature = "transforms-impl-sample")]
pub(crate) use self::sample::*;
#[cfg(feature = "transforms-tag_cardinality_limit")]
pub(crate) use self::tag_cardinality_limit::*;
#[cfg(unix)]
pub(crate) use self::unix::*;
#[cfg(windows)]
pub(crate) use self::windows::*;
pub use self::{
    adaptive_concurrency::*, batch::*, common::*, conditions::*, encoding_transcode::*,
    heartbeat::*, http::*, open::*, process::*, socket::*, tcp::*, template::*, udp::*,
};
