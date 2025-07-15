#![allow(missing_docs)]
mod encoding_config;
pub mod multiline_config;
mod wrappers;

#[cfg(feature = "sources-file")]
pub use encoding_config::EncodingConfig;
pub use multiline_config::MultilineConfig;
pub use wrappers::{AfterRead, AfterReadExt};