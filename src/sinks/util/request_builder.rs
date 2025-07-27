use std::{num::NonZeroUsize};

pub fn default_request_builder_concurrency_limit() -> NonZeroUsize {
    if let Some(limit) = std::env::var("VECTOR_EXPERIMENTAL_REQUEST_BUILDER_CONCURRENCY")
        .map(|value| value.parse::<NonZeroUsize>().ok())
        .ok()
        .flatten()
    {
        return limit;
    }

    crate::app::worker_threads().unwrap_or_else(|| NonZeroUsize::new(8).expect("static"))
}



/// Generalized interface for defining how a batch of events will incrementally be turned into requests.
///
/// As opposed to `RequestBuilder`, this trait provides the means to incrementally build requests
/// from a single batch of events, where all events in the batch may not fit into a single request.
/// This can be important for sinks where the underlying service has limitations on the size of a
/// request, or how many events may be present, necessitating a batch be split up into multiple requests.
///
/// While batches can be limited in size before being handed off to a request builder, we can't
/// always know in advance how large the encoded payload will be, which requires us to be able to
/// potentially split a batch into multiple requests.
pub trait IncrementalRequestBuilder<Input> {
    type Metadata;
    type Payload;
    type Request;
    type Error;

    /// Incrementally encodes the given input, potentially generating multiple payloads.
    fn encode_events_incremental(
        &mut self,
        input: Input,
    ) -> Vec<Result<(Self::Metadata, Self::Payload), Self::Error>>;

    /// Builds a request for the given metadata and payload.
    fn build_request(&mut self, metadata: Self::Metadata, payload: Self::Payload) -> Self::Request;
}
