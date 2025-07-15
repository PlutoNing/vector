//! Modules that are common between sources, transforms, and sinks.

#[cfg(any(feature = "transforms-log_to_metric"))]
pub(crate) mod expansion;

#[cfg(any(
    feature = "sources-utils-http-auth",
    feature = "sources-utils-http-error"
))]
pub mod http;
