#![allow(missing_docs)]
/// heart beat
use std::time::Instant;
use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    process::ExitStatus,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

/// heartbeat end
/// opt start
use clap::{ArgAction, CommandFactory, FromArgMatches, Parser};
use tokio::time::interval;

use super::config::ComponentKey;
use crate::get_version;
/// signal end
use exitcode::ExitCode;
use futures::StreamExt;
/// opts end
/// signal start
use snafu::Snafu;
use tokio::runtime::Runtime;
use tokio::runtime::{self};
use tokio::sync::broadcast::{channel, error::RecvError, Receiver, Sender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::Stream;

use crate::{
    config::{self, Config, ConfigPath},
    topology::{
        RunningTopology, SharedTopologyController, ShutdownErrorReceiver, TopologyController,
    },
};

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
use tokio::runtime::Handle;

static WORKER_THREADS: AtomicUsize = AtomicUsize::new(0);

pub fn worker_threads() -> Option<NonZeroUsize> {
    NonZeroUsize::new(WORKER_THREADS.load(Ordering::Relaxed))
}
/// Refer to [`crate::cli::watchConfigMethod`] for details.
pub enum WatcherConfig {
    /// Recommended watcher for the current OS.
    RecommendedWatcher,
    /// A poll-based watcher that checks for file changes at regular intervals.
    PollWatcher(u64),
}
/// signal start
pub type ShutdownTx = Sender<()>;
pub type SignalTx = Sender<SignalTo>;
pub type SignalRx = Receiver<SignalTo>;

#[derive(Debug, Clone)]
/// agent用于驱动拓扑和关闭事件的控制消息。
#[allow(clippy::large_enum_variant)] // discovered during Rust upgrade to 1.57; just allowing for now since we did previously
pub enum SignalTo {
    /// Signal to shutdown process.
    Shutdown(Option<ShutdownError>),
    /// Shutdown process immediately.
    Quit,
}

impl PartialEq for SignalTo {
    fn eq(&self, other: &Self) -> bool {
        use SignalTo::*;

        match (self, other) {
            (Shutdown(a), Shutdown(b)) => a == b,
            (Quit, Quit) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, Snafu, PartialEq, Eq)]
pub enum ShutdownError {
    // For future work: It would be nice if we could keep the actual errors in here, but
    // `crate::Error` doesn't implement `Clone`, and adding `DynClone` for errors is tricky.
    #[snafu(display("The API failed to start: {error}"))]
    ApiFailed { error: String },
    #[snafu(display("Reload failed, and then failed to restore the previous config"))]
    ReloadFailedToRestore,
    #[snafu(display(r#"The task for source "{key}" died during execution: {error}"#))]
    SourceAborted { key: ComponentKey, error: String },
    #[snafu(display(r#"The task for transform "{key}" died during execution: {error}"#))]
    TransformAborted { key: ComponentKey, error: String },
    #[snafu(display(r#"The task for sink "{key}" died during execution: {error}"#))]
    SinkAborted { key: ComponentKey, error: String },
}
/* - __接收操作系统信号__（SIGINT, SIGTERM等）

- __管理优雅关闭流程__

- __协调组件生命周期__
 */
/// Convenience struct for app setup handling.
pub struct SignalPair {
    pub handler: SignalHandler,
    pub receiver: SignalRx,
}

impl SignalPair {
    pub fn new(runtime: &Runtime) -> Self {
        let (handler, receiver) = SignalHandler::new();
        let signals = os_signals(runtime);
        handler.forever(runtime, signals);
        Self { handler, receiver }
    }
}

/// SignalHandler is a general `ControlTo` message receiver and transmitter. It's used by
/// OS signals and providers to surface control events to the root of the application.
pub struct SignalHandler {
    tx: SignalTx,
    shutdown_txs: Vec<ShutdownTx>,
}

impl SignalHandler {
    /* 生成一个receiver(往这里发信号)和handler(接受,进行对应的处理) */
    /// Create a new signal handler with space for 128 control messages at a time, to
    /// ensure the channel doesn't overflow and drop signals.
    fn new() -> (Self, SignalRx) {
        let (tx, rx) = channel(128);
        let handler = Self {
            tx,
            shutdown_txs: vec![],
        };

        (handler, rx)
    }

    /// Clones the transmitter.
    pub fn clone_tx(&self) -> SignalTx {
        self.tx.clone()
    }

    /// Subscribe to the stream, and return a new receiver.
    pub fn subscribe(&self) -> SignalRx {
        self.tx.subscribe()
    }

    /// Takes a stream who's elements are convertible to `SignalTo`, and spawns a permanent
    /// task for transmitting to the receiver.
    fn forever<T, S>(&self, runtime: &Runtime, stream: S)
    where
        T: Into<SignalTo> + Send + Sync,
        S: Stream<Item = T> + 'static + Send,
    {
        let tx = self.tx.clone();

        runtime.spawn(async move {
            tokio::pin!(stream);

            while let Some(value) = stream.next().await {
                if tx.send(value.into()).is_err() {
                    error!(message = "Couldn't send signal.");
                    break;
                }
            }
        });
    }

    /// Takes a stream, sending to the underlying signal receiver. Returns a broadcast tx
    /// channel which can be used by the caller to either subscribe to cancellation, or trigger
    /// it. Useful for providers that may need to do both.
    pub fn add<T, S>(&mut self, stream: S)
    where
        T: Into<SignalTo> + Send,
        S: Stream<Item = T> + 'static + Send,
    {
        let (shutdown_tx, mut shutdown_rx) = channel::<()>(2);
        let tx = self.tx.clone();

        self.shutdown_txs.push(shutdown_tx);

        tokio::spawn(async move {
            tokio::pin!(stream);

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.recv() => break,
                    Some(value) = stream.next() => {
                        if tx.send(value.into()).is_err() {
                            error!(message = "Couldn't send signal.");
                            break;
                        }
                    }
                    else => {
                        error!(message = "Underlying stream is closed.");
                        break;
                    }
                }
            }
        });
    }

    /// Shutdown active signal handlers.
    pub fn clear(&mut self) {
        for shutdown_tx in self.shutdown_txs.drain(..) {
            // An error just means the channel was already shut down; safe to ignore.
            _ = shutdown_tx.send(());
        }
    }
}

/// Signals from OS/user.
#[cfg(unix)]
fn os_signals(runtime: &Runtime) -> impl Stream<Item = SignalTo> {
    use tokio::signal::unix::{signal, SignalKind};

    // The `signal` function must be run within the context of a Tokio runtime.
    runtime.block_on(async {
        let mut sigint = signal(SignalKind::interrupt()).expect("Failed to set up SIGINT handler.");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to set up SIGTERM handler.");
        let mut sigquit = signal(SignalKind::quit()).expect("Failed to set up SIGQUIT handler.");
        let mut sighup = signal(SignalKind::hangup()).expect("Failed to set up SIGHUP handler.");

        async_stream::stream! {
            loop {
                let signal = tokio::select! {
                    _ = sigint.recv() => {
                        info!(message = "Signal received.", signal = "SIGINT");
                        SignalTo::Shutdown(None)
                    },
                    _ = sigterm.recv() => {
                        info!(message = "Signal received.", signal = "SIGTERM");
                        SignalTo::Shutdown(None)
                    } ,
                    _ = sigquit.recv() => {
                        info!(message = "Signal received.", signal = "SIGQUIT");
                        SignalTo::Quit
                    },
                    _ = sighup.recv() => {
                        info!(message = "Signal received.", signal = "SIGHUP");
                        SignalTo::Shutdown(None)
                    },
                };
                yield signal;
            }
        }
    })
}

/// signal end

/* 定义app的config */
pub struct ApplicationConfig {
    pub config_paths: Vec<config::ConfigPath>, /* 配置文件路径 */
    pub topology: RunningTopology,             /* 构建的拓扑 */
    pub graceful_crash_receiver: ShutdownErrorReceiver,
    pub internal_topologies: Vec<RunningTopology>,
}

/* app本体? */
pub struct Application {
    pub root_opts: RootOpts,
    pub config: ApplicationConfig,
    pub signals: SignalPair,
}

pub fn handle_config_errors(errors: Vec<String>) -> exitcode::ExitCode {
    for error in errors {
        error!(message = "Configuration error.", %error);
    }

    exitcode::CONFIG
}
/// opts start

/* 程序选项 */
#[derive(Parser, Debug)] /* Parser是 clap 库提供的一个派生宏，用于自动生成命令行解析逻辑。 */
#[command(rename_all = "kebab-case")] /* 指定命令行选项的命名风格为 "kebab-case"，即选项名称中使用连字符分隔单词（例如，--some-option）。 */
pub struct Opts {
    #[command(flatten)]
    pub root: RootOpts,
}

impl Opts {
    /* 获取启动时的命令行选项,app.get_matches() 解析命令行参数，并返回一个 ArgMatches 实例。 Opts::from_arg_matches(...) 将 ArgMatches 转换为 Opts 实例。 */
    pub fn get_matches() -> Result<Self, clap::Error> {
        let version = get_version(); /* Opts::command() 是 clap 提供的一个方法，用于创建一个命令行应用程序实例 */
        let app = Opts::command().version(version); /* .version(version) 设置应用程序的版本信息。 */
        Opts::from_arg_matches(&app.get_matches()) /* app.get_matches() 解析命令行参数，并返回一个 ArgMatches 实例 */
    }

    pub const fn log_level(&self) -> &'static str {
        let (quiet_level, verbose_level) = (self.root.quiet, self.root.verbose);

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
    #[arg(id = "config", short, long, env = "agent_config", value_delimiter(','))]
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

    /// 线程数量
    #[arg(short, long, env = "VECTOR_THREADS")]
    pub threads: Option<usize>,

    /// Enable more detailed internal logging. Repeat to increase level. Overridden by `--quiet`.
    #[arg(short, long, action = ArgAction::Count)]
    pub verbose: u8,

    /// Reduce detail of internal logging. Repeat to reduce further. Overrides `--verbose`.
    #[arg(short, long, action = ArgAction::Count)]
    pub quiet: u8,

    /// Set the internal log rate limit
    /// Note that traces are throttled by default unless tagged with `internal_log_rate_limit = false`.
    #[arg(
        short,
        long,
        env = "VECTOR_INTERNAL_LOG_RATE_LIMIT",
        default_value = "10"
    )]
    pub internal_log_rate_limit: u64,

    /// 设置收到SIGINT或SIGTERM后优雅关闭的等待秒数。超过此时间后agent将强制关闭。
    /// 要永不强制关闭，使用--no-graceful-shutdown-limit。
    #[arg(
        long,
        default_value = "60",
        env = "VECTOR_GRACEFUL_SHUTDOWN_LIMIT_SECS",
        group = "graceful-shutdown-limit"
    )]
    pub graceful_shutdown_limit_secs: NonZeroU64,

    /// 收到SIGINT或SIGTERM后等待优雅关闭时永不超时。适用于希望agent持续尝试发送数据直到被SIGKILL终止的场景。
    /// 会覆盖--graceful-shutdown-limit-secs且不能与其同时设置。
    #[arg(
        long,
        default_value = "false",
        env = "VECTOR_NO_GRACEFUL_SHUTDOWN_LIMIT",
        group = "graceful-shutdown-limit"
    )]
    pub no_graceful_shutdown_limit: bool,

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
}

/// opts end

impl ApplicationConfig {
    pub async fn from_opts(
        opts: &RootOpts,
        signal_handler: &mut SignalHandler,
    ) -> Result<Self, ExitCode> {
        /* 从prepare_from_opts来到这里 */
        let config_paths = opts.config_paths_with_formats();

        let graceful_shutdown_duration = (!opts.no_graceful_shutdown_limit)
            .then(|| Duration::from_secs(u64::from(opts.graceful_shutdown_limit_secs)));

        /* 加载配置 20250717152148 */
        let config = load_configs(
            &config_paths,
            opts.allow_empty_config,
            graceful_shutdown_duration,
            signal_handler,
        )
        .await?;

        Self::from_config(config_paths, config).await
    }
    /*  */
    pub async fn from_config(
        config_paths: Vec<ConfigPath>, /* 配置文件路径 */
        config: Config,                /* 读取,解析好的config结构体 */
    ) -> Result<Self, ExitCode> {
        /* 解析好的config下一步来到这， 生成拓扑(包含source, sink等的东西) */
        let (topology, graceful_crash_receiver) = RunningTopology::start_init_validated(config)
            .await
            .ok_or(exitcode::CONFIG)?;

        Ok(Self {
            config_paths,
            topology,
            graceful_crash_receiver,
            internal_topologies: Vec::new(),
        })
    }

    pub async fn add_internal_config(&mut self, config: Config) -> Result<(), ExitCode> {
        let Some((topology, _)) = RunningTopology::start_init_validated(config).await else {
            return Err(exitcode::CONFIG);
        };
        self.internal_topologies.push(topology);
        Ok(())
    }
}

/* heartbeat进程 */
/// Emits heartbeat event every second.
pub async fn heartbeat() {
    let since = Instant::now();
    let mut interval = interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        debug!("heartbeat: uptime {} seconds", since.elapsed().as_secs());
    }
}

/* 运行程序 */
impl Application {
    pub fn run() -> ExitStatus {
        let opts_res = Opts::get_matches();
        let opts = match opts_res {
            Ok(opts) => opts,
            Err(error) => {
                let _ = error.print();
                std::process::exit(exitcode::USAGE);
            }
        };
        let res1 = Self::prepare_from_opts(opts);
        let res = match res1 {
            Ok((runtime, app)) => {
                let start_res = app.start(runtime.handle());
                match start_res {
                    Ok(app) => Ok((runtime, app)),
                    Err(code) => Err(code),
                }
            }
            Err(code) => Err(code),
        };
        let (runtime, app) = match res {
            Ok((runtime, app)) => (runtime, app),
            Err(code) => std::process::exit(code),
        };

        runtime.block_on(app.run())
    }

    pub fn prepare_from_opts(opts: Opts) -> Result<(Runtime, Self), ExitCode> {
        if let Err(_) = metrics::set_global_recorder(metrics::NoopRecorder) {
            // 如果已经初始化过，忽略错误
            warn!("Metrics recorder already initialized");
        }
        init_logging(true, opts.log_level(), opts.root.internal_log_rate_limit);

        let runtime = build_runtime(opts.root.threads, "vector-worker")?;

        let mut signals = SignalPair::new(&runtime);

        let config = runtime.block_on(ApplicationConfig::from_opts(
            &opts.root,
            &mut signals.handler,
        ))?;

        Ok((
            runtime,
            Self {
                root_opts: opts.root,
                config,
                signals,
            },
        ))
    }
    /* 开启app? */
    pub fn start(self, handle: &Handle) -> Result<StartedApplication, ExitCode> {
        // Any internal_logs sources will have grabbed a copy of the
        // early buffer by this point and set up a subscriber.

        info!("Agent has started.");
        handle.spawn(heartbeat());
        let db_path = "/tmp/scx_agent-2025-08-04.db";
        let socket_path = "/tmp/scx_agent/agent_rpc.sock";

        handle.spawn(async move {
            if let Err(e) = crate::core::rpc_service::QueryServiceImpl::start_server(
                db_path,
                "events",
                socket_path,
            )
            .await
            {
                error!("RPC server error: {}", e);
            }
        });
        let Self {
            root_opts,
            config,
            signals,
        } = self;

        let topology_controller = SharedTopologyController::new(TopologyController {
            topology: config.topology,                 /* 构建的拓扑 */
            config_paths: config.config_paths.clone(), /* 配置文件路径 */
        });

        Ok(StartedApplication {
            config_paths: config.config_paths,
            internal_topologies: config.internal_topologies,
            graceful_crash_receiver: config.graceful_crash_receiver,
            signals,
            topology_controller,
            allow_empty_config: root_opts.allow_empty_config,
        })
    }
}

pub struct StartedApplication {
    pub config_paths: Vec<ConfigPath>,
    pub internal_topologies: Vec<RunningTopology>,
    pub graceful_crash_receiver: ShutdownErrorReceiver,
    pub signals: SignalPair,
    pub topology_controller: SharedTopologyController,
    pub allow_empty_config: bool,
}

impl StartedApplication {
    /* 初始化好config, 已经相关source, sink进程后调用 */
    pub async fn run(self) -> ExitStatus {
        self.main().await.shutdown().await
    }
    /* app的主函数? */
    pub async fn main(self) -> FinishedApplication {
        let Self {
            config_paths,
            graceful_crash_receiver,
            signals,
            topology_controller,
            internal_topologies,
            allow_empty_config,
        } = self;

        let mut graceful_crash = UnboundedReceiverStream::new(graceful_crash_receiver);

        let mut signal_handler = signals.handler;
        let mut signal_rx = signals.receiver;

        let signal = loop {
            tokio::select! {
                signal = signal_rx.recv() => if let Some(signal) = handle_signal(
                    signal,
                    &topology_controller,
                    &config_paths,
                    &mut signal_handler,
                    allow_empty_config,
                ).await {
                    break signal;
                },
                // Trigger graceful shutdown if a component crashed, or all sources have ended.
                error = graceful_crash.next() => break SignalTo::Shutdown(error),
                else => unreachable!("Signal streams never end"),
            }
        };

        FinishedApplication {
            signal,
            signal_rx,
            topology_controller,
            internal_topologies,
        }
    }
}

async fn handle_signal(
    signal: Result<SignalTo, RecvError>,
    _topology_controller: &SharedTopologyController,
    _config_paths: &[ConfigPath],
    _signal_handler: &mut SignalHandler,
    _allow_empty_config: bool,
) -> Option<SignalTo> {
    match signal {
        Err(RecvError::Lagged(amt)) => {
            warn!("Overflow, dropped {} signals.", amt);
            None
        }
        Err(RecvError::Closed) => Some(SignalTo::Shutdown(None)),
        Ok(signal) => Some(signal),
    }
}

pub struct FinishedApplication {
    pub signal: SignalTo,
    pub signal_rx: SignalRx,
    pub topology_controller: SharedTopologyController,
    pub internal_topologies: Vec<RunningTopology>,
}

impl FinishedApplication {
    pub async fn shutdown(self) -> ExitStatus {
        let FinishedApplication {
            signal,
            signal_rx,
            topology_controller,
            internal_topologies,
        } = self;

        // At this point, we'll have the only reference to the shared topology controller and can
        // safely remove it from the wrapper to shut down the topology.
        let topology_controller = topology_controller
            .try_into_inner()
            .expect("fail to unwrap topology controller")
            .into_inner();

        let status = match signal {
            SignalTo::Shutdown(_) => Self::stop(topology_controller, signal_rx).await,
            SignalTo::Quit => Self::quit(),
        };

        for topology in internal_topologies {
            topology.stop().await;
        }

        status
    }

    async fn stop(topology_controller: TopologyController, mut signal_rx: SignalRx) -> ExitStatus {
        info!("Agent has stopped.");
        tokio::select! {
            _ = topology_controller.stop() => ExitStatus::from_raw({
                info!("stop app");
                #[cfg(unix)]
                exitcode::OK
            }), // Graceful shutdown finished
            _ = signal_rx.recv() => Self::quit(),
        }
    }

    fn quit() -> ExitStatus {
        // It is highly unlikely that this event will exit from topology.
        info!("Agent has quit.");
        ExitStatus::from_raw({
            #[cfg(unix)]
            exitcode::OK
        })
    }
}

fn get_log_levels(default: &str) -> String {
    std::env::var("VECTOR_LOG")
        .or_else(|_| {
            std::env::var("LOG").inspect(|_log| {
                warn!(
                    message =
                        "DEPRECATED: Use of $LOG is deprecated. Please use $VECTOR_LOG instead."
                );
            })
        })
        .unwrap_or_else(|_| default.into())
}
/* 构建一个多线程的运行时 20250717150927 */
pub fn build_runtime(threads: Option<usize>, thread_name: &str) -> Result<Runtime, ExitCode> {
    let mut rt_builder = runtime::Builder::new_multi_thread();
    rt_builder.max_blocking_threads(20_000);
    rt_builder.enable_all().thread_name(thread_name);

    let threads = threads.unwrap_or_else(crate::num_threads);
    if threads == 0 {
        error!("The `threads` argument must be greater or equal to 1.");
        return Err(exitcode::CONFIG);
    }
    WORKER_THREADS
        .compare_exchange(0, threads, Ordering::Acquire, Ordering::Relaxed)
        .unwrap_or_else(|_| panic!("double thread initialization"));
    rt_builder.worker_threads(threads);

    debug!(messaged = "Building runtime.", worker_threads = threads);
    Ok(rt_builder.build().expect("Unable to create async runtime"))
}
/*运行后来到这里, 加载配置  20250717152203, 读取配置文件, 解析, 存入config结构体 */
pub async fn load_configs(
    config_paths: &[ConfigPath], /* 里面是搜索的可能的conf路径? */
    allow_empty_config: bool,
    graceful_shutdown_duration: Option<Duration>,
    signal_handler: &mut SignalHandler,
) -> Result<Config, ExitCode> {
    let config_paths = config::process_paths(config_paths).ok_or(exitcode::CONFIG)?;
    /* config_paths是搜索的配置文件路径列表 */
    let watched_paths = config_paths
        .iter()
        .map(<&PathBuf>::from)
        .collect::<Vec<_>>();

    info!(
        message = "Loading configs.",
        paths = ?watched_paths
    );
    /* 20250717153620  secrets是什么? */
    let mut config = config::load_from_paths_with_provider_and_secrets(
        &config_paths,
        signal_handler,
        allow_empty_config,
    )
    .await
    .map_err(handle_config_errors)?;
    /* 刚刚处理了config */

    config::init_log_schema(config.global.log_schema.clone(), true); /* 这里是初始化这两个字段 */

    config.graceful_shutdown_duration = graceful_shutdown_duration;

    Ok(config)
}

pub fn init_logging(_color: bool, log_level: &str, _rate: u64) {
    let level = get_log_levels(log_level);
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::new(log_level);

    fmt().with_env_filter(filter).init();

    info!("日志系统已初始化");
    info!(message = "Log level is enabled.", level = ?level);
}

pub fn watcher_config(_interval: NonZeroU64) -> WatcherConfig {
    WatcherConfig::RecommendedWatcher
}
