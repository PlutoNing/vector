use std::{io::Error, path::Path};

use metrics::counter;
use vector_lib::internal_event::{
    error_stage, error_type, ComponentEventsDropped, InternalEvent, UNINTENTIONAL,
};

use crate::internal_events::SocketOutgoingConnectionError;

#[derive(Debug)]
pub struct UnixSocketConnectionEstablished<'a> {
    pub path: &'a std::path::Path,
}

impl InternalEvent for UnixSocketConnectionEstablished<'_> {
    fn emit(self) {
        debug!(message = "Connected.", path = ?self.path);
        counter!("connection_established_total", "mode" => "unix").increment(1);
    }
}

#[derive(Debug)]
pub struct UnixSocketOutgoingConnectionError<E> {
    pub error: E,
}

impl<E: std::error::Error> InternalEvent for UnixSocketOutgoingConnectionError<E> {
    fn emit(self) {
        // ## skip check-duplicate-events ##
        // ## skip check-validity-events ##
        emit!(SocketOutgoingConnectionError { error: self.error });
    }
}

#[derive(Debug)]
pub struct UnixSocketSendError<'a, E> {
    pub(crate) error: &'a E,
    pub path: &'a std::path::Path,
}

impl<E: std::fmt::Display> InternalEvent for UnixSocketSendError<'_, E> {
    fn emit(self) {
        let reason = "Unix socket send error.";
        error!(
            message = reason,
            error = %self.error,
            path = ?self.path,
            error_type = error_type::WRITER_FAILED,
            stage = error_stage::SENDING,

        );
        counter!(
            "component_errors_total",
            "error_type" => error_type::WRITER_FAILED,
            "stage" => error_stage::SENDING,
        )
        .increment(1);

        emit!(ComponentEventsDropped::<UNINTENTIONAL> { count: 1, reason });
    }
}

#[derive(Debug)]
pub struct UnixSendIncompleteError {
    pub data_size: usize,
    pub sent: usize,
}

impl InternalEvent for UnixSendIncompleteError {
    fn emit(self) {
        let reason = "Could not send all data in one Unix datagram.";
        error!(
            message = reason,
            data_size = self.data_size,
            sent = self.sent,
            dropped = self.data_size - self.sent,
            error_type = error_type::WRITER_FAILED,
            stage = error_stage::SENDING,
        );
        counter!(
            "component_errors_total",
            "error_type" => error_type::WRITER_FAILED,
            "stage" => error_stage::SENDING,
        )
        .increment(1);

        emit!(ComponentEventsDropped::<UNINTENTIONAL> { count: 1, reason });
    }
}

#[derive(Debug)]
pub struct UnixSocketFileDeleteError<'a> {
    pub path: &'a Path,
    pub error: Error,
}

impl InternalEvent for UnixSocketFileDeleteError<'_> {
    fn emit(self) {
        error!(
            message = "Failed in deleting unix socket file.",
            path = %self.path.display(),
            error = %self.error,
            error_code = "delete_socket_file",
            error_type = error_type::WRITER_FAILED,
            stage = error_stage::PROCESSING,

        );
        counter!(
            "component_errors_total",
            "error_code" => "delete_socket_file",
            "error_type" => error_type::WRITER_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}
