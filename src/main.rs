#![deny(warnings)]

extern crate scx_agent;
use scx_agent::app::Application;

use std::process::ExitCode;
/* 项目主函数 */
#[cfg(unix)]
fn main() -> ExitCode {
    let exit_code = Application::run()
        .code()
        .unwrap_or(exitcode::UNAVAILABLE) as u8;
    ExitCode::from(exit_code)
}
