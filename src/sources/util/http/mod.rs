#[cfg(feature = "sources-utils-http-encoding")]
mod encoding;
#[cfg(any(
    feature = "sources-utils-http-headers"
))]
mod headers;
mod method;
#[cfg(feature = "sources-utils-http-prelude")]
mod prelude;
#[cfg(any(
    feature = "sources-utils-http-query"
))]
mod query;

#[cfg(feature = "sources-utils-http-encoding")]
pub use encoding::decode;
#[cfg(feature = "sources-utils-http-headers")]
pub use headers::add_headers;
pub use method::HttpMethod;
#[cfg(feature = "sources-utils-http-prelude")]
pub use prelude::HttpSource;
#[cfg(feature = "sources-utils-http-query")]
pub use query::add_query_parameters;
