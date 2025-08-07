//! This library includes common functionality relied upon by agent-core
//! and core-related crates (e.g. buffers).

#![deny(warnings)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(unreachable_pub)]
#![deny(unused_allocation)]
#![deny(unused_extern_crates)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]

#[cfg(feature = "btreemap")]
pub use vrl::btreemap;

pub mod byte_size_of;

pub mod json_size;

pub mod config;

#[cfg(feature = "conversion")]
pub use vrl::compiler::TimeZone;

#[cfg(feature = "encoding")]
pub mod encode_logfmt {
    pub use vrl::core::encode_logfmt::*;
}

pub mod conversion {
    pub use vrl::compiler::conversion::*;
}

pub mod finalization;

#[macro_use]
extern crate tracing;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;

pub type Result<T> = std::result::Result<T, Error>;
