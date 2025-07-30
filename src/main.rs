#![deny(warnings)]

extern crate vector;
use vector::{app::Application};

use std::process::ExitCode;
/* 项目主函数 */
#[cfg(unix)]
fn main() -> ExitCode {
    let exit_code = Application::run()
        .code()/* 如果 Application::run 返回一个实现了 Termination trait 的类型，code()提取退出码 */
        .unwrap_or(exitcode::UNAVAILABLE) as u8;
    ExitCode::from(exit_code)
}
