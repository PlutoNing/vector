use std::time::Duration;

use metrics::{counter, histogram, Histogram};
use vector_common::{
    internal_event::{error_type, InternalEvent},
    registered_event,
};

pub struct BufferReadError {
    pub error_code: &'static str,
    pub error: String,
}

impl InternalEvent for BufferReadError {
    fn emit(self) {
        error!(
            message = "Error encountered during buffer read.",
            error = %self.error,
            error_code = self.error_code,
            error_type = error_type::READER_FAILED,
            stage = "processing",

        );
        counter!(
            "buffer_errors_total", "error_code" => self.error_code,
            "error_type" => "reader_failed",
            "stage" => "processing",
        )
        .increment(1);
    }
}

registered_event! {
    BufferSendDuration {
        stage: usize,
    } => {
        send_duration: Histogram = histogram!("buffer_send_duration_seconds", "stage" => self.stage.to_string()),
    }

    fn emit(&self, duration: Duration) {
        self.send_duration.record(duration);
    }
}
