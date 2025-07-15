//! Modules that are common between sources, transforms, and sinks.


#[cfg(any(feature = "sources-mqtt", feature = "sinks-mqtt",))]
/// Common MQTT configuration shared by MQTT components.
pub mod mqtt;

#[cfg(any(feature = "transforms-log_to_metric", feature = "sinks-loki"))]
pub(crate) mod expansion;

#[cfg(any(
    feature = "sources-utils-http-auth",
    feature = "sources-utils-http-error"
))]
pub mod http;
