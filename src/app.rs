#![allow(missing_docs)]
use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    process::ExitStatus,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
/// heart beat
use std::time::{Instant};

use tokio::time::interval;
/// 
use exitcode::ExitCode;
use futures::StreamExt;
use tokio::runtime::{self, Runtime};
use tokio::sync::broadcast::error::RecvError;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::{
    cli::{Opts, RootOpts},
    config::{self, Config, ConfigPath},
    signal::{SignalHandler, SignalPair, SignalRx, SignalTo},
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

impl ApplicationConfig {
    /* vector::app::ApplicationConfig::from_opts vector::app::Application::prepare_from_opts vector::app::Application::prepare vector::app::Application::prepare_start vector::app::Application::run vector::main*/
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
        /* 解析好的config下一步来到这， 看来是生成什么拓扑(包含source, sink等的东西) */
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
        let (runtime, app) = Self::prepare_start().unwrap_or_else(|code| std::process::exit(code));

        runtime.block_on(app.run())
    }
    /* 运行调用层次: main->run->prepare_start */
    pub fn prepare_start() -> Result<(Runtime, StartedApplication), ExitCode> {
        /* 返回一个 Result 类型，表示操作的结果。成功时返回一个包含 Runtime 和 StartedApplication 的元组，失败时返回一个 ExitCode */
        Self::prepare() /* prepare 方法返回一个 Result<(Runtime, Application), ExitCode> */
            .and_then(|(runtime, app)| app.start(runtime.handle()).map(|app| (runtime, app)))
        /* and_then 是 Result 类型的方法，用于链式处理成功的结果。如果 prepare 方法成功，and_then 会接收一个包含 runtime 和 app 的元组，并执行闭包中的代码。 */
    }
    /*main->run->prepare_start->prepare ; 返回rt和app, 然后app.start(runtime.handle()).map(|app| (runtime, app)) */
    pub fn prepare() -> Result<(Runtime, Self), ExitCode> {
        let opts = Opts::get_matches().map_err(|error| {
            /* opts 是一个类型为 Opts 的变量。它通常用于存储从命令行解析得到的选项和参数 */
            // Printing to stdout/err can itself fail; ignore it.
            _ = error.print();
            exitcode::USAGE
        })?;
        /* 程序刚运行时一直来到这里 */
        Self::prepare_from_opts(opts)
    }
    /* main->run->prepare_start->prepare->prepare_from_opts */
    pub fn prepare_from_opts(opts: Opts) -> Result<(Runtime, Self), ExitCode> {
        opts.root.init_global();

        init_logging(
            true,
            opts.log_level(),
            opts.root.internal_log_rate_limit,
        );

        /* 构建rt */
        let runtime = build_runtime(opts.root.threads, "vector-worker")?;

        // Signal handler for OS and provider messages.
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

        info!("Vector has started.");
        handle.spawn(heartbeat());

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
        info!("Vector has stopped.");
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
        info!("Vector has quit.");
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

    /* log schema是什么 */
    config::init_log_schema(config.global.log_schema.clone(), true); /* 这里是初始化这两个字段 */

    config.graceful_shutdown_duration = graceful_shutdown_duration;

    Ok(config)
}

pub fn init_logging(_color: bool, log_level: &str, _rate: u64) {
    let level = get_log_levels(log_level);
    debug!(message = "Internal log rate limit configured.",);
    info!(message = "Log level is enabled.", level = ?level);
}

pub fn watcher_config(
    _interval: NonZeroU64,
) -> WatcherConfig {
        WatcherConfig::RecommendedWatcher

}
