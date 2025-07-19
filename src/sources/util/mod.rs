#![allow(missing_docs)]

pub mod multiline_config;
mod wrappers;

pub use multiline_config::MultilineConfig;
pub use wrappers::{AfterRead, AfterReadExt};