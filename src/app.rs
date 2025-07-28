#![allow(missing_docs)]
use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
    process::ExitStatus,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use exitcode::ExitCode;
use futures::StreamExt;
use tokio::runtime::{self, Runtime};
use tokio::sync::{broadcast::error::RecvError, MutexGuard};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::extra_context::ExtraContext;
use crate::{
    cli::{handle_config_errors, LogFormat, Opts, RootOpts, WatchConfigMethod},
    config::{self, Config, ConfigPath},
    heartbeat,
    signal::{SignalHandler, SignalPair, SignalRx, SignalTo},
    topology::{
        ReloadOutcome, RunningTopology, SharedTopologyController, ShutdownErrorReceiver,
        TopologyController,
    },
    trace,
};

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(windows)]
use std::os::windows::process::ExitStatusExt;
use tokio::runtime::Handle;

static WORKER_THREADS: AtomicUsize = AtomicUsize::new(0);

pub fn worker_threads() -> Option<NonZeroUsize> {
    NonZeroUsize::new(WORKER_THREADS.load(Ordering::Relaxed))
}

/* 定义app的config */
pub struct ApplicationConfig {
    pub config_paths: Vec<config::ConfigPath>, /* 配置文件路径 */
    pub topology: RunningTopology, /* 构建的拓扑 */
    pub graceful_crash_receiver: ShutdownErrorReceiver,
    pub internal_topologies: Vec<RunningTopology>,
    pub extra_context: ExtraContext,
}

/* app本体? */
pub struct Application {
    pub root_opts: RootOpts,
    pub config: ApplicationConfig,
    pub signals: SignalPair,
}

impl ApplicationConfig { /* vector::app::ApplicationConfig::from_opts vector::app::Application::prepare_from_opts vector::app::Application::prepare vector::app::Application::prepare_start vector::app::Application::run vector::main*/
    pub async fn from_opts(
        opts: &RootOpts,
        signal_handler: &mut SignalHandler,
        extra_context: ExtraContext,
    ) -> Result<Self, ExitCode> { /* 从prepare_from_opts来到这里 */
        let config_paths = opts.config_paths_with_formats();

        let graceful_shutdown_duration = (!opts.no_graceful_shutdown_limit)
            .then(|| Duration::from_secs(u64::from(opts.graceful_shutdown_limit_secs)));

        let watcher_conf = if opts.watch_config {
            Some(watcher_config(
                opts.watch_config_method,
                opts.watch_config_poll_interval_seconds,
            ))
        } else {
            None
        };
/* 加载配置 20250717152148 */
        let config = load_configs(
            &config_paths,
            watcher_conf,

            opts.allow_empty_config,
            graceful_shutdown_duration,
            signal_handler,
        )
        .await?;

        Self::from_config(config_paths, config, extra_context).await
    }
/*  */
    pub async fn from_config(
        config_paths: Vec<ConfigPath>, /* 配置文件路径 */
        config: Config, /* 读取,解析好的config结构体 */
        extra_context: ExtraContext,
    ) -> Result<Self, ExitCode> {
/* 解析好的config下一步来到这， 看来是生成什么拓扑(包含source, sink等的东西) */
        let (topology, graceful_crash_receiver) =
            RunningTopology::start_init_validated(config, extra_context.clone())
                .await
                .ok_or(exitcode::CONFIG)?;

        Ok(Self {
            config_paths,
            topology,
            graceful_crash_receiver,
            internal_topologies: Vec::new(),
            extra_context,
        })
    }

    pub async fn add_internal_config(
        &mut self,
        config: Config,
        extra_context: ExtraContext,
    ) -> Result<(), ExitCode> {
        let Some((topology, _)) =
            RunningTopology::start_init_validated(config, extra_context).await
        else {
            return Err(exitcode::CONFIG);
        };
        self.internal_topologies.push(topology);
        Ok(())
    }

}
/* 运行程序 */
impl Application {
    pub fn run(extra_context: ExtraContext) -> ExitStatus {
        let (runtime, app) =
            Self::prepare_start(extra_context).unwrap_or_else(|code| std::process::exit(code));

        runtime.block_on(app.run())
    }
/* 运行调用层次: main->run->prepare_start */
    pub fn prepare_start(
        extra_context: ExtraContext,
    ) -> Result<(Runtime, StartedApplication), ExitCode> { /* 返回一个 Result 类型，表示操作的结果。成功时返回一个包含 Runtime 和 StartedApplication 的元组，失败时返回一个 ExitCode */
        Self::prepare(extra_context)/* prepare 方法返回一个 Result<(Runtime, Application), ExitCode> */
            .and_then(|(runtime, app)| app.start(runtime.handle()).map(|app| (runtime, app)))/* and_then 是 Result 类型的方法，用于链式处理成功的结果。如果 prepare 方法成功，and_then 会接收一个包含 runtime 和 app 的元组，并执行闭包中的代码。 */
    }
    /*main->run->prepare_start->prepare ; 返回rt和app, 然后app.start(runtime.handle()).map(|app| (runtime, app)) */
    pub fn prepare(extra_context: ExtraContext) -> Result<(Runtime, Self), ExitCode> {
        let opts = Opts::get_matches().map_err(|error| { /* opts 是一个类型为 Opts 的变量。它通常用于存储从命令行解析得到的选项和参数 */
            // Printing to stdout/err can itself fail; ignore it.
            _ = error.print();
            exitcode::USAGE
        })?;
/* 程序刚运行时一直来到这里 */
        Self::prepare_from_opts(opts, extra_context)
    }
    /* main->run->prepare_start->prepare->prepare_from_opts */
    pub fn prepare_from_opts(
        opts: Opts,
        extra_context: ExtraContext,
    ) -> Result<(Runtime, Self), ExitCode> {
        opts.root.init_global();

        init_logging(
            true,
            opts.root.log_format,
            opts.log_level(),
            opts.root.internal_log_rate_limit,
        );





/* 构建rt */
        let runtime = build_runtime(opts.root.threads, "vector-worker")?;

        // Signal handler for OS and provider messages.
        let mut signals = SignalPair::new(&runtime);

        if let Some(sub_command) = &opts.sub_command { /* 这里不一定执行 */
            return Err(runtime.block_on(sub_command.execute(signals, true)));
        }

        let config = runtime.block_on(ApplicationConfig::from_opts(
            &opts.root,
            &mut signals.handler,
            extra_context,
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
        crate::trace::stop_early_buffering();

        info!("Vector has started.");
        handle.spawn(heartbeat::heartbeat());

        let Self {
            root_opts,
            config,
            signals,
        } = self;

        let topology_controller = SharedTopologyController::new(TopologyController {
            topology: config.topology, /* 构建的拓扑 */
            config_paths: config.config_paths.clone(), /* 配置文件路径 */

            extra_context: config.extra_context,
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

impl StartedApplication { /* 初始化好config, 已经相关source, sink进程后调用 */
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
            let has_sources = !topology_controller.lock().await.topology.config.is_empty();
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
                _ = TopologyController::sources_finished(topology_controller.clone()), if has_sources => {
                    info!("All sources have finished.");
                    break SignalTo::Shutdown(None)
                } ,
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
    topology_controller: &SharedTopologyController,
    config_paths: &[ConfigPath],
    signal_handler: &mut SignalHandler,
    allow_empty_config: bool,
) -> Option<SignalTo> {
    match signal {
        Ok(SignalTo::ReloadComponents(components_to_reload)) => {
            let mut topology_controller = topology_controller.lock().await;
            topology_controller
                .topology
                .extend_reload_set(components_to_reload);

            // Reload paths
            if let Some(paths) = config::process_paths(config_paths) {
                topology_controller.config_paths = paths;
            }

            // Reload config
            let new_config = config::load_from_paths_with_provider_and_secrets(
                &topology_controller.config_paths,
                signal_handler,
                allow_empty_config,
            )
            .await;

            reload_config_from_result(topology_controller, new_config).await
        }
        Ok(SignalTo::ReloadFromConfigBuilder(config_builder)) => {
            let topology_controller = topology_controller.lock().await;
            reload_config_from_result(topology_controller, config_builder.build()).await
        }
        Ok(SignalTo::ReloadFromDisk) => {
            let mut topology_controller = topology_controller.lock().await;

            // Reload paths
            if let Some(paths) = config::process_paths(config_paths) {
                topology_controller.config_paths = paths;
            }

            // Reload config
            let new_config = config::load_from_paths_with_provider_and_secrets(
                &topology_controller.config_paths,
                signal_handler,
                allow_empty_config,
            )
            .await;

            reload_config_from_result(topology_controller, new_config).await
        }
        Err(RecvError::Lagged(amt)) => {
            warn!("Overflow, dropped {} signals.", amt);
            None
        }
        Err(RecvError::Closed) => Some(SignalTo::Shutdown(None)),
        Ok(signal) => Some(signal),
    }
}

async fn reload_config_from_result(
    mut topology_controller: MutexGuard<'_, TopologyController>,
    config: Result<Config, Vec<String>>,
) -> Option<SignalTo> {
    match config {
        Ok(new_config) => match topology_controller.reload(new_config).await {
            ReloadOutcome::FatalError(error) => Some(SignalTo::Shutdown(Some(error))),
            _ => None,
        },
        Err(errors) => {
            handle_config_errors(errors);
            error!("Failed to load config files, reload aborted.");

            None
        }
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
            _ => unreachable!(),
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
                #[cfg(windows)]
                {
                    exitcode::OK as u32
                }
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
            #[cfg(windows)]
            {
                exitcode::UNAVAILABLE as u32
            }
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
    _watcher_conf: Option<config::watcher::WatcherConfig>,

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
    config::init_log_schema(config.global.log_schema.clone(), true);/* 这里是初始化这两个字段 */

    config.graceful_shutdown_duration = graceful_shutdown_duration;

    Ok(config)
}

pub fn init_logging(color: bool, format: LogFormat, log_level: &str, rate: u64) {
    let level = get_log_levels(log_level);
    let json = match format {
        LogFormat::Text => false,
        LogFormat::Json => true,
    };

    trace::init(color, json, &level, rate);
    debug!(message = "Internal log rate limit configured.",);
    info!(message = "Log level is enabled.", level = ?level);
}

pub fn watcher_config(
    method: WatchConfigMethod,
    interval: NonZeroU64,
) -> config::watcher::WatcherConfig {
    match method {
        WatchConfigMethod::Recommended => config::watcher::WatcherConfig::RecommendedWatcher,
        WatchConfigMethod::Poll => config::watcher::WatcherConfig::PollWatcher(interval.into()),
    }
}
