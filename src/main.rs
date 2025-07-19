#![deny(warnings)]

extern crate vector;
use vector::{app::Application, extra_context::ExtraContext};

use std::process::ExitCode;
/* 项目主函数 */
#[cfg(unix)]
fn main() -> ExitCode {
    let exit_code = Application::run(ExtraContext::default())
        .code()/* 如果 Application::run 返回一个实现了 Termination trait 的类型，code()提取退出码 */
        .unwrap_or(exitcode::UNAVAILABLE) as u8;
    ExitCode::from(exit_code)
}

#[cfg(windows)]
pub fn main() -> ExitCode {
    // We need to be able to run vector in User Interactive mode. We first try
    // to run vector as a service. If we fail, we consider that we are in
    // interactive mode and then fallback to console mode.  See
    // https://docs.microsoft.com/en-us/dotnet/api/system.environment.userinteractive?redirectedfrom=MSDN&view=netcore-3.1#System_Environment_UserInteractive
    let exit_code = vector::vector_windows::run().unwrap_or_else(|_| {
        Application::run(ExtraContext::default())
            .code()
            .unwrap_or(exitcode::UNAVAILABLE)
    });
    ExitCode::from(exit_code as u8)
}
