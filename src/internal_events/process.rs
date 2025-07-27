use metrics::counter;
use vector_lib::internal_event::InternalEvent;
use vector_lib::internal_event::{error_stage, error_type};

use crate::{config};

#[derive(Debug)]
pub struct VectorReloaded<'a> {
    pub config_paths: &'a [config::ConfigPath],
}

impl InternalEvent for VectorReloaded<'_> {
    fn emit(self) {
        info!(
            target: "vector",
            message = "Vector has reloaded.",
            path = ?self.config_paths
        );
        counter!("reloaded_total").increment(1);
    }
}

#[derive(Debug)]
pub struct VectorRecoveryError;

impl InternalEvent for VectorRecoveryError {
    fn emit(self) {
        error!(
            message = "Vector has failed to recover from a failed reload.",
            error_code = "recovery",
            error_type = error_type::CONFIGURATION_FAILED,
            stage = error_stage::PROCESSING,
        );
        counter!(
            "component_errors_total",
            "error_code" => "recovery",
            "error_type" => error_type::CONFIGURATION_FAILED,
            "stage" => error_stage::PROCESSING,
        )
        .increment(1);
    }
}
