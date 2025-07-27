use metrics::counter;
use vector_lib::internal_event::InternalEvent;
use vector_lib::internal_event::{error_stage, error_type};



#[derive(Debug)]
pub struct HostMetricsScrapeDetailError<E> {
    pub message: &'static str,
    pub error: E,
}

impl<E: std::fmt::Display> InternalEvent for HostMetricsScrapeDetailError<E> {
    fn emit(self) {
        error!(
            message = self.message,
            error = %self.error,
            error_type = error_type::READER_FAILED,
            stage = error_stage::RECEIVING,

        );

        counter!(
            "component_errors_total",
            "error_type" => error_type::READER_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}
