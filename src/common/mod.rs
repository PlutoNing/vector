//! Modules that are common between sources, transforms, and sinks.
use futures::future::BoxFuture;

pub type Source = BoxFuture<'static, Result<(), ()>>;
