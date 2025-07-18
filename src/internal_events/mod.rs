#![allow(missing_docs)]

mod adaptive_concurrency;
mod aggregate;

mod batch;
pub mod codecs;
mod common;
mod conditions;

mod encoding_transcode;
mod heartbeat;
#[cfg(feature = "sources-host_metrics")]
mod host_metrics;
mod http;
pub mod http_client;
mod open;
mod parser;
mod process;

mod remap;
mod socket;

mod tcp;
mod template;
mod udp;
mod unix;

#[cfg(any(
    feature = "sinks-file",
))]
mod file;
mod windows;

#[cfg(any(
    feature = "sinks-file",
))]
pub(crate) use self::file::*;
#[cfg(feature = "sources-host_metrics")]
pub(crate) use self::host_metrics::*;
#[allow(unused_imports)]
pub(crate) use self::parser::*;
#[cfg(unix)]
pub(crate) use self::unix::*;
#[cfg(windows)]
pub(crate) use self::windows::*;
pub use self::{
    adaptive_concurrency::*, batch::*, common::*, conditions::*, encoding_transcode::*,
    heartbeat::*, http::*, open::*, process::*, socket::*, tcp::*, template::*, udp::*,
};
