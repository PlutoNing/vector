//! The agent Core Library
//!agent 核心库是构建 agent 所需的基础组件集合


#![deny(warnings)]
#![deny(clippy::all)]
#![deny(clippy::pedantic)]
#![deny(unreachable_pub)]
#![deny(unused_allocation)]
#![deny(unused_extern_crates)]
#![deny(unused_assignments)]
#![deny(unused_comparisons)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::default_trait_access)] // triggers on generated prost code
#![allow(clippy::float_cmp)]
#![allow(clippy::match_wildcard_for_single_variants)]
#![allow(clippy::module_name_repetitions)]
#![allow(clippy::must_use_candidate)] // many false positives in this package
#![allow(clippy::non_ascii_literal)] // using unicode literals is a-okay
#![allow(clippy::unnested_or_patterns)] // nightly-only feature as of 1.51.0
#![allow(clippy::type_complexity)] // long-types happen, especially in async code

pub mod config;
pub mod event;
pub mod metrics;
pub mod schema;
pub mod serde;
pub mod buffer;
pub mod transform;


use float_eq::FloatEq;

pub use event::EstimatedJsonEncodedSizeOf;
pub use config::metrics_expiration::PerMetricSetExpiration;
#[macro_use]
extern crate tracing;



pub(crate) use agent_common::{Error, Result};

pub(crate) fn float_eq(l_value: f64, r_value: f64) -> bool {
    (l_value.is_nan() && r_value.is_nan()) || l_value.eq_ulps(&r_value, &1)
}

// These macros aren't actually usable in lib crates without some `agent_lib` shenanigans.
// This test version won't be needed once all `InternalEvent`s implement `name()`.
#[cfg(feature = "test")]
#[macro_export]
macro_rules! emit {
    ($event:expr) => {
        agent_lib::internal_event::emit(agent_lib::internal_event::DefaultName {
            event: $event,
            name: stringify!($event),
        })
    };
}

#[cfg(not(feature = "test"))]
#[macro_export]
macro_rules! emit {
    ($event:expr) => {
        agent_lib::internal_event::emit($event)
    };
}

#[cfg(feature = "test")]
#[macro_export]
macro_rules! register {
    ($event:expr) => {
        agent_lib::internal_event::register(agent_lib::internal_event::DefaultName {
            event: $event,
            name: stringify!($event),
        })
    };
}
