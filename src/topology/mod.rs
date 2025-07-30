#![allow(missing_docs)]
//! Topology contains all topology based types.
//!
//! Topology is broken up into two main sections. The first
//! section contains all the main topology types include `Topology`
//! and the ability to start, stop and reload a config. The second
//! part contains config related items including config traits for
//! each type of component.

pub(super) use crate::core::fanout;
pub mod schema;

pub mod builder;
mod controller;
mod ready_arrays;
mod running;
mod task;


use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};

use futures::{Future, FutureExt};
use tokio::sync::mpsc;
use crate::buffers::topology::channel::{BufferReceiverStream, BufferSender};

pub use self::builder::TopologyPieces;
pub use self::controller::{ReloadOutcome, SharedTopologyController, TopologyController};
pub use self::running::{RunningTopology, ShutdownErrorReceiver};

use self::task::{TaskError, TaskResult};
use crate::{
    config::{Config, ConfigDiff},
    event::EventArray,
    app::ShutdownError,
};

type TaskHandle = tokio::task::JoinHandle<TaskResult>;

type BuiltBuffer = (
    BufferSender<EventArray>,
    Arc<Mutex<Option<BufferReceiverStream<EventArray>>>>,
);

async fn handle_errors(
    task: impl Future<Output = TaskResult>,
    abort_tx: mpsc::UnboundedSender<ShutdownError>,
    error: impl FnOnce(String) -> ShutdownError,
) -> TaskResult {
    AssertUnwindSafe(task)
        .catch_unwind()
        .await
        .map_err(|_| TaskError::Panicked)
        .and_then(|res| res)
        .map_err(|e| {
            error!("An error occurred that Vector couldn't handle: {}.", e);
            _ = abort_tx.send(error(e.to_string()));
            e
        })
}

/// If the closure returns false, then the element is removed
fn retain<T>(vec: &mut Vec<T>, mut retain_filter: impl FnMut(&mut T) -> bool) {
    let mut i = 0;
    while let Some(data) = vec.get_mut(i) {
        if retain_filter(data) {
            i += 1;
        } else {
            _ = vec.remove(i);
        }
    }
}
