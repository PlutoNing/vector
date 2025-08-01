//! This module contains all our internal sink utilities
//!
//! All vector "sinks" are built around the `Sink` type which
//! we use to "push" events into. Within the different types of
//! vector "sinks" we need to support three main use cases:
//!
//! - Streaming sinks
//! - Single partition batching
//! - Multiple partition batching
//!
//! For each of these types this module provides one external type
//! that can be used within sinks. The simplest type being the `StreamSink`
//! type should be used when you do not want to batch events but you want
//! to _stream_ them to the downstream service. `BatchSink` and `PartitionBatchSink`
//! are similar in the sense that they both take some `tower::Service`, and `Batch`
//! and will provide full batching, and request dispatching based on
//! the settings passed.
//!
//! For more advanced use cases like HTTP based sinks, one should use the
//! `BatchedHttpSink` type, which is a wrapper for `BatchSink` and `HttpSink`.
//!
//! # Driving to completion
//!
//! Each sink utility provided here strictly follows the patterns described in
//! the `futures::Sink` docs. Each sink utility must be polled from a valid
//! tokio context.
//!
//! For service based sinks like `BatchSink` and `PartitionBatchSink` they also
//! must be polled within a valid tokio executor context. This is due to the fact
//! that they will spawn service requests to allow them to be driven independently
//! from the sink. A oneshot channel is used to tie them back into the sink to allow
//! it to notify the consumer that the request has succeeded.

// === StreamSink<Event> ===
pub use crate::core::sink::StreamSink;
