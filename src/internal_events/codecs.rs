use metrics::counter;
use vector_lib::internal_event::InternalEvent;
use vector_lib::internal_event::{error_stage, error_type};

#[derive(Debug)]
pub struct DecoderFramingError<E> {
    pub error: E,
}

impl<E: std::fmt::Display> InternalEvent for DecoderFramingError<E> {
    fn emit(self) {
        error!(
            message = "Failed framing bytes.",
            error = %self.error,
            error_code = "decoder_frame",
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::PROCESSING,

        );
        counter!(
            "component_errors_total",
            "error_code" => "decoder_frame",
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}

#[derive(Debug)]
pub struct DecoderDeserializeError<'a> {
    pub error: &'a crate::Error,
}

impl InternalEvent for DecoderDeserializeError<'_> {
    fn emit(self) {
        error!(
            message = "Failed deserializing frame.",
            error = %self.error,
            error_code = "decoder_deserialize",
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::PROCESSING,

        );
        counter!(
            "component_errors_total",
            "error_code" => "decoder_deserialize",
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}