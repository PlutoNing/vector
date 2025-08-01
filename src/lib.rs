#![recursion_limit = "256"] // for async-stream
#![deny(unreachable_pub)]
#![deny(unused_extern_crates)]
#![deny(unused_allocation)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]
#![deny(warnings)]
#![cfg_attr(docsrs, feature(doc_cfg), deny(rustdoc::broken_intra_doc_links))]
#![allow(async_fn_in_trait)]
#![allow(clippy::approx_constant)]
#![allow(clippy::float_cmp)]
#![allow(clippy::match_wild_err_arm)]
#![allow(clippy::new_ret_no_self)]
#![allow(clippy::type_complexity)]
#![allow(clippy::unit_arg)]
#![deny(clippy::clone_on_ref_ptr)]
#![deny(clippy::trivially_copy_pass_by_ref)]
#![deny(clippy::disallowed_methods)] // [nursery] mark some functions as verboten
#![deny(clippy::missing_const_for_fn)] // [nursery] valuable to the optimizer, but may produce false positives

//! The main library to build
#[macro_use]
extern crate derivative;
#[macro_use]
extern crate tracing;
#[macro_use]
extern crate agent_lib;

pub use indoc::indoc;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[macro_use]
#[allow(unreachable_pub)]
pub mod config;

pub mod app;

#[allow(unreachable_pub)]
pub mod codecs;
pub mod common;
pub mod core;
pub mod buffers;
pub mod enrichment_tables;
#[allow(unreachable_pub)]
pub(crate) mod proto;
#[allow(unreachable_pub)]
pub mod sinks;
#[allow(unreachable_pub)]
pub mod sources;

pub mod template;

#[allow(unreachable_pub)]
pub mod topology;
#[allow(unreachable_pub)]
pub mod transforms;
pub mod internal_event;
pub use sources::SourceSender;
pub use agent_lib::{event, schema};
pub use agent_lib::{shutdown, Error, Result};

static APP_NAME_SLUG: std::sync::OnceLock<String> = std::sync::OnceLock::new();

pub fn get_app_name() -> &'static str {
    option_env!("VECTOR_APP_NAME").unwrap_or("scx_gent")
}

pub fn get_slugified_app_name() -> String {
    APP_NAME_SLUG
        .get_or_init(|| get_app_name().to_lowercase().replace(' ', "-"))
        .clone()
}


pub fn vector_version() -> impl std::fmt::Display {
    env!("CARGO_PKG_VERSION")
}

/// Returns a string containing full version information of the current build.
pub fn get_version() -> String {
    let pkg_version = vector_version();
    
    // 检查是否为调试构建
    let debug_info = if cfg!(debug_assertions) {
        " debug"
    } else {
        ""
    };
    
    format!("{} ({})", pkg_version, debug_info)
}


pub fn get_hostname() -> std::io::Result<String> {
    Ok(if let Ok(hostname) = std::env::var("VECTOR_HOSTNAME") {
        hostname.to_string()
    } else {
        hostname::get()?.to_string_lossy().into_owned()
    })
}

/// Spawn a task with the given name. The name is only used if
/// built with [`tokio_unstable`][tokio_unstable].
///
/// [tokio_unstable]: https://docs.rs/tokio/latest/tokio/#unstable-features
#[track_caller]
pub(crate) fn spawn_named<T>(
    task: impl std::future::Future<Output = T> + Send + 'static,
    _name: &str,
) -> tokio::task::JoinHandle<T>
where
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    return tokio::task::Builder::new()
        .name(_name)
        .spawn(task)
        .expect("tokio task should spawn");

    #[cfg(not(tokio_unstable))]
    tokio::spawn(task)
}

pub fn num_threads() -> usize {
    let count = match std::thread::available_parallelism() {
        Ok(count) => count,
        Err(error) => {
            warn!(message = "Failed to determine available parallelism for thread count, defaulting to 1.", %error);
            std::num::NonZeroUsize::new(1).unwrap()
        }
    };
    usize::from(count)
}
