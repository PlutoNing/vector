#![allow(missing_docs)]

use std::{num::NonZeroU64, path::PathBuf};

use clap::{ArgAction, CommandFactory, FromArgMatches, Parser};

#[cfg(windows)]
use crate::service;

use crate::{config, get_version};
use crate::{signal};
/* 程序选项 */
#[derive(Parser, Debug)] /* Parser是 clap 库提供的一个派生宏，用于自动生成命令行解析逻辑。 */
#[command(rename_all = "kebab-case")] /* 指定命令行选项的命名风格为 "kebab-case"，即选项名称中使用连字符分隔单词（例如，--some-option）。 */
pub struct Opts {
    #[command(flatten)]
    pub root: RootOpts,

    #[command(subcommand)] /* #[command(subcommand)] 表示 sub_command 是一个子命令，clap 会根据命令行输入解析出具体的子命令。 */
    pub sub_command: Option<SubCommand>,
}

impl Opts { /* 获取启动时的命令行选项,app.get_matches() 解析命令行参数，并返回一个 ArgMatches 实例。 Opts::from_arg_matches(...) 将 ArgMatches 转换为 Opts 实例。 */
    pub fn get_matches() -> Result<Self, clap::Error> {
        let version = get_version();/* Opts::command() 是 clap 提供的一个方法，用于创建一个命令行应用程序实例 */
        let app = Opts::command().version(version); /* .version(version) 设置应用程序的版本信息。 */
        Opts::from_arg_matches(&app.get_matches())/* app.get_matches() 解析命令行参数，并返回一个 ArgMatches 实例 */
    }

    pub const fn log_level(&self) -> &'static str {
        let (quiet_level, verbose_level) = 
       (self.root.quiet, self.root.verbose);
     
        match quiet_level {
            0 => match verbose_level {
                0 => "info",
                1 => "debug",
                2..=255 => "trace",
            },
            1 => "warn",
            2 => "error",
            3..=255 => "off",
        }
    }
}
/* 定义命令行选项 */
#[derive(Parser, Debug)]
#[command(rename_all = "kebab-case")]
pub struct RootOpts {
    /// Read configuration from one or more files. Wildcard paths are supported.
    /// File format is detected from the file name.
    /// If zero files are specified, the deprecated default config path
    /// `/etc/vector/vector.yaml` is targeted.
    #[arg(
        id = "config",
        short,
        long,
        env = "VECTOR_CONFIG",
        value_delimiter(',')
    )]
    pub config_paths: Vec<PathBuf>,

    /// Read configuration from files in one or more directories.
    /// File format is detected from the file name.
    ///
    /// Files not ending in .toml, .json, .yaml, or .yml will be ignored.
    #[arg(
        id = "config-dir",
        short = 'C',
        long,
        env = "VECTOR_CONFIG_DIR",
        value_delimiter(',')
    )]
    pub config_dirs: Vec<PathBuf>,

    /// Read configuration from one or more files. Wildcard paths are supported.
    /// TOML file format is expected.
    #[arg(
        id = "config-toml",
        long,
        env = "VECTOR_CONFIG_TOML",
        value_delimiter(',')
    )]
    pub config_paths_toml: Vec<PathBuf>,

    /// Read configuration from one or more files. Wildcard paths are supported.
    /// JSON file format is expected.
    #[arg(
        id = "config-json",
        long,
        env = "VECTOR_CONFIG_JSON",
        value_delimiter(',')
    )]
    pub config_paths_json: Vec<PathBuf>,

    /// Read configuration from one or more files. Wildcard paths are supported.
    /// YAML file format is expected.
    #[arg(
        id = "config-yaml",
        long,
        env = "VECTOR_CONFIG_YAML",
        value_delimiter(',')
    )]
    pub config_paths_yaml: Vec<PathBuf>,

    /// Exit on startup if any sinks fail healthchecks
    #[arg(short, long, env = "VECTOR_REQUIRE_HEALTHY")]
    pub require_healthy: Option<bool>,

    /// 线程数量
    #[arg(short, long, env = "VECTOR_THREADS")]
    pub threads: Option<usize>,

    /// Enable more detailed internal logging. Repeat to increase level. Overridden by `--quiet`.
    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,

    /// Reduce detail of internal logging. Repeat to reduce further. Overrides `--verbose`.
    #[arg(short, long, action = ArgAction::Count)]
    pub quiet: u8,

    /// Set the logging format
    #[arg(long, default_value = "text", env = "VECTOR_LOG_FORMAT")]
    pub log_format: LogFormat,



    /// Watch for changes in configuration file, and reload accordingly.
    #[arg(short, long, env = "VECTOR_WATCH_CONFIG")]
    pub watch_config: bool,

    /// Method for configuration watching.
    ///
    /// By default, `vector` uses recommended watcher for host OS
    /// - `inotify` for Linux-based systems.
    /// - `kqueue` for unix/macos
    /// - `ReadDirectoryChangesWatcher` for windows
    ///
    /// The `poll` watcher can be used in cases where `inotify` doesn't work, e.g., when attaching the configuration via NFS.
    #[arg(
        long,
        default_value = "recommended",
        env = "VECTOR_WATCH_CONFIG_METHOD"
    )]
    pub watch_config_method: WatchConfigMethod,

    /// Poll for changes in the configuration file at the given interval.
    ///
    /// This setting is only applicable if `Poll` is set in `--watch-config-method`.
    #[arg(
        long,
        env = "VECTOR_WATCH_CONFIG_POLL_INTERVAL_SECONDS",
        default_value = "30"
    )]
    pub watch_config_poll_interval_seconds: NonZeroU64,

    /// Set the internal log rate limit
    /// Note that traces are throttled by default unless tagged with `internal_log_rate_limit = false`.
    #[arg(
        short,
        long,
        env = "VECTOR_INTERNAL_LOG_RATE_LIMIT",
        default_value = "10"
    )]
    pub internal_log_rate_limit: u64,

    /// Set the duration in seconds to wait for graceful shutdown after SIGINT or SIGTERM are
    /// received. After the duration has passed, Vector will force shutdown. To never force
    /// shutdown, use `--no-graceful-shutdown-limit`.
    #[arg(
        long,
        default_value = "60",
        env = "VECTOR_GRACEFUL_SHUTDOWN_LIMIT_SECS",
        group = "graceful-shutdown-limit"
    )]
    pub graceful_shutdown_limit_secs: NonZeroU64,

    /// Never time out while waiting for graceful shutdown after SIGINT or SIGTERM received.
    /// This is useful when you would like for Vector to attempt to send data until terminated
    /// by a SIGKILL. Overrides/cannot be set with `--graceful-shutdown-limit-secs`.
    #[arg(
        long,
        default_value = "false",
        env = "VECTOR_NO_GRACEFUL_SHUTDOWN_LIMIT",
        group = "graceful-shutdown-limit"
    )]
    pub no_graceful_shutdown_limit: bool,

    /// Set runtime allocation tracing
    #[cfg(feature = "allocation-tracing")]
    #[arg(long, env = "ALLOCATION_TRACING", default_value = "false")]
    pub allocation_tracing: bool,

    /// Set allocation tracing reporting rate in milliseconds.
    #[cfg(feature = "allocation-tracing")]
    #[arg(
        long,
        env = "ALLOCATION_TRACING_REPORTING_INTERVAL_MS",
        default_value = "5000"
    )]
    pub allocation_tracing_reporting_interval_ms: u64,



    /// Allow the configuration to run without any components. This is useful for loading in an
    /// empty stub config that will later be replaced with actual components. Note that this is
    /// likely not useful without also watching for config file changes as described in
    /// `--watch-config`.
    #[arg(long, env = "VECTOR_ALLOW_EMPTY_CONFIG", default_value = "false")]
    pub allow_empty_config: bool,
}
/* 实现opt的方法 */
impl RootOpts {
    /// Return a list of config paths with the associated formats.
    pub fn config_paths_with_formats(&self) -> Vec<config::ConfigPath> {
        config::merge_path_lists(vec![
            (&self.config_paths, None),
            (&self.config_paths_toml, Some(config::Format::Toml)),
            (&self.config_paths_json, Some(config::Format::Json)),
            (&self.config_paths_yaml, Some(config::Format::Yaml)),
        ])
        .map(|(path, hint)| config::ConfigPath::File(path, hint))
        .chain(
            self.config_dirs
                .iter()
                .map(|dir| config::ConfigPath::Dir(dir.to_path_buf())),
        )
        .collect()
    }
    /* 启动时初始化root opt */
    pub fn init_global(&self) {
        crate::metrics::init_global().expect("metrics initialization failed");
    }
}

#[derive(Parser, Debug)]
#[command(rename_all = "kebab-case")]
pub enum SubCommand {
    /// Output a provided Vector configuration file/dir as a single JSON object, useful for checking in to version control.
    #[command(hide = true)]
    Config(config::Opts),

    /// Manage the vector service.
    #[cfg(windows)]
    Service(service::Opts),

    /// Vector Remap Language CLI
    Vrl(vrl::cli::Opts),
}

impl SubCommand {
    pub async fn execute(
        &self,
        _signals: signal::SignalPair,
        _color: bool,
    ) -> exitcode::ExitCode {
        match self {
            Self::Config(c) => config::cmd(c),
            #[cfg(windows)]
            Self::Service(s) => service::cmd(s),
            Self::Vrl(s) => {
                let mut functions = vrl::stdlib::all();
                functions.extend(vector_vrl_functions::all());
                vrl::cli::cmd::cmd(s, functions)
            }
        }
    }
}



#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum WatchConfigMethod {
    /// Recommended watcher for the current OS, usually `inotify` for Linux-based systems.
    Recommended,
    /// Poll-based watcher, typically used for watching files on EFS/NFS-like network storage systems.
    /// The interval is determined by  [`RootOpts::watch_config_poll_interval_seconds`].
    Poll,
}

pub fn handle_config_errors(errors: Vec<String>) -> exitcode::ExitCode {
    for error in errors {
        error!(message = "Configuration error.", %error);
    }

    exitcode::CONFIG
}
