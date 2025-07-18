use std::{error::Error, time::Duration};

use http::Response;
use metrics::{counter, histogram};
use vector_lib::internal_event::InternalEvent;
use vector_lib::{
    internal_event::{error_stage, error_type},
    json_size::JsonSize,
};

const HTTP_STATUS_LABEL: &str = "status";

#[derive(Debug)]
pub struct HttpServerRequestReceived;

impl InternalEvent for HttpServerRequestReceived {
    fn emit(self) {
        debug!(
            message = "Received HTTP request.",
            internal_log_rate_limit = true
        );
        counter!("http_server_requests_received_total").increment(1);
    }
}

#[derive(Debug)]
pub struct HttpServerResponseSent<'a, B> {
    pub response: &'a Response<B>,
    pub latency: Duration,
}

impl<B> InternalEvent for HttpServerResponseSent<'_, B> {
    fn emit(self) {
        let labels = &[(
            HTTP_STATUS_LABEL,
            self.response.status().as_u16().to_string(),
        )];
        counter!("http_server_responses_sent_total", labels).increment(1);
        histogram!("http_server_handler_duration_seconds", labels).record(self.latency);
    }
}

#[derive(Debug)]
pub struct HttpBytesReceived<'a> {
    pub byte_size: usize,
    pub http_path: &'a str,
    pub protocol: &'static str,
}

impl InternalEvent for HttpBytesReceived<'_> {
    fn emit(self) {
        trace!(
            message = "Bytes received.",
            byte_size = %self.byte_size,
            http_path = %self.http_path,
            protocol = %self.protocol
        );
        counter!(
            "component_received_bytes_total",
            "http_path" => self.http_path.to_string(),
            "protocol" => self.protocol,
        )
        .increment(self.byte_size as u64);
    }
}

#[derive(Debug)]
pub struct HttpEventsReceived<'a> {
    pub count: usize,
    pub byte_size: JsonSize,
    pub http_path: &'a str,
    pub protocol: &'static str,
}

impl InternalEvent for HttpEventsReceived<'_> {
    fn emit(self) {
        trace!(
            message = "Events received.",
            count = %self.count,
            byte_size = %self.byte_size,
            http_path = %self.http_path,
            protocol = %self.protocol,
        );

        histogram!("component_received_events_count").record(self.count as f64);
        counter!(
            "component_received_events_total",
            "http_path" => self.http_path.to_string(),
            "protocol" => self.protocol,
        )
        .increment(self.count as u64);
        counter!(
            "component_received_event_bytes_total",
            "http_path" => self.http_path.to_string(),
            "protocol" => self.protocol,
        )
        .increment(self.byte_size.get() as u64);
    }
}

#[derive(Debug)]
pub struct HttpDecompressError<'a> {
    pub error: &'a dyn Error,
    pub encoding: &'a str,
}

impl InternalEvent for HttpDecompressError<'_> {
    fn emit(self) {
        error!(
            message = "Failed decompressing payload.",
            error = %self.error,
            error_code = "failed_decompressing_payload",
            error_type = error_type::PARSER_FAILED,
            stage = error_stage::RECEIVING,
            encoding = %self.encoding,
            internal_log_rate_limit = true
        );
        counter!(
            "component_errors_total",
            "error_code" => "failed_decompressing_payload",
            "error_type" => error_type::PARSER_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}

pub struct HttpInternalError<'a> {
    pub message: &'a str,
}

impl InternalEvent for HttpInternalError<'_> {
    fn emit(self) {
        error!(
            message = %self.message,
            error_type = error_type::CONNECTION_FAILED,
            stage = error_stage::RECEIVING,
            internal_log_rate_limit = true
        );
        counter!(
            "component_errors_total",
            "error_type" => error_type::CONNECTION_FAILED,
            "stage" => error_stage::RECEIVING,
        )
        .increment(1);
    }
}
