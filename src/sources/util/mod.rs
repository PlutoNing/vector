#![allow(missing_docs)]
mod encoding_config;
#[cfg(any(
    feature = "sources-utils-http-auth",
    feature = "sources-utils-http-encoding",
    feature = "sources-utils-http-prelude"
))]
pub mod http;
pub mod multiline_config;
#[cfg(any(feature = "sources-utils-net-tcp", feature = "sources-utils-net-udp"))]
pub mod net;
#[cfg(all(
    unix,
    any(feature = "sources-socket", feature = "sources-utils-net-unix",)
))]
pub mod unix;
#[cfg(all(unix, feature = "sources-socket"))]
mod unix_datagram;
#[cfg(all(unix, feature = "sources-utils-net-unix"))]
mod unix_stream;
mod wrappers;

#[cfg(feature = "sources-file")]
pub use encoding_config::EncodingConfig;
pub use multiline_config::MultilineConfig;
#[cfg(all(
    unix,
    any(feature = "sources-socket", feature = "sources-utils-net-unix",)
))]
pub use unix::change_socket_permissions;
#[cfg(all(unix, feature = "sources-socket",))]
pub use unix_datagram::build_unix_datagram_source;
#[cfg(all(unix, feature = "sources-utils-net-unix",))]
pub use unix_stream::build_unix_stream_source;
pub use wrappers::{AfterRead, AfterReadExt};

#[cfg(any(
    feature = "sources-utils-http-encoding"
))]
pub use self::http::decode;
#[cfg(feature = "sources-utils-http-prelude")]
pub use self::http::HttpSource;