use std::{
    collections::HashMap,
    future::ready,

    sync::{Arc, LazyLock, Mutex},

};

use futures::{StreamExt, TryStreamExt};
use futures_util::stream::FuturesUnordered;

use stream_cancel::{StreamExt as StreamCancelExt, Trigger, Tripwire};
use tokio::{
    select,
    sync::{mpsc::UnboundedSender, oneshot},

};
use tracing::Instrument;

use vector_lib::internal_event::{
    CountByteSize, InternalEventHandle as _,
};

use vector_lib::{
    buffers::{
        topology::{
            channel::{BufferSender},
        },
        BufferType,
    },

    EstimatedJsonEncodedSizeOf,
};

use super::{
    fanout::{self, Fanout},
    schema,
    task::{Task, TaskOutput},
    BuiltBuffer, ConfigDiff,
};
use crate::{
    config::{
        ComponentKey, Config, DataType, EnrichmentTableConfig, Inputs, OutputId,
        ProxyConfig, SinkContext, SourceContext,
    },
    event::{EventArray, EventContainer},
    internal_events::EventsReceived,
    shutdown::SourceShutdownCoordinator,
    source_sender::{SourceSenderItem, CHUNK_SIZE},
    spawn_named,
    topology::task::TaskError,
    SourceSender,
};

static ENRICHMENT_TABLES: LazyLock<vector_lib::enrichment::TableRegistry> =
    LazyLock::new(vector_lib::enrichment::TableRegistry::default);

pub(crate) static SOURCE_SENDER_BUFFER_SIZE: LazyLock<usize> =
    LazyLock::new(|| *TRANSFORM_CONCURRENCY_LIMIT * CHUNK_SIZE);

static TRANSFORM_CONCURRENCY_LIMIT: LazyLock<usize> = LazyLock::new(|| {
    crate::app::worker_threads()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or_else(crate::num_threads)
});

const INTERNAL_SOURCES: [&str; 2] = ["internal_logs", "internal_metrics"];

struct Builder<'a> {
    config: &'a super::Config,/* 现在的新config */
    diff: &'a ConfigDiff,/* 新config带来的diff */
    shutdown_coordinator: SourceShutdownCoordinator, /*  */
    errors: Vec<String>,
    outputs: HashMap<OutputId, UnboundedSender<fanout::ControlMessage>>, /*  */
    tasks: HashMap<ComponentKey, Task>, /* 好像是source或者output的一个task, sink的也在这 */
    buffers: HashMap<ComponentKey, BuiltBuffer>,
    inputs: HashMap<ComponentKey, (BufferSender<EventArray>, Inputs<OutputId>)>,/* buffer的tx */
    detach_triggers: HashMap<ComponentKey, Trigger>,
}
/* 基于config构建拓扑 */
impl<'a> Builder<'a> {
    fn new(
        config: &'a super::Config,
        diff: &'a ConfigDiff,
        buffers: HashMap<ComponentKey, BuiltBuffer>,
    ) -> Self {
        Self {
            config,
            diff,
            buffers,
            shutdown_coordinator: SourceShutdownCoordinator::default(),
            errors: vec![],
            outputs: HashMap::new(),
            tasks: HashMap::new(),
            inputs: HashMap::new(),
            detach_triggers: HashMap::new(),
        }
    }
/* 构建一个拓扑,（config, diff已经初始化在了self） */
    /// Builds the new pieces of the topology found in `self.diff`.
    async fn build(mut self) -> Result<TopologyPieces, Vec<String>> {
        let enrichment_tables = self.load_enrichment_tables().await;/* 一般是空的 */
        let source_tasks = self.build_sources(enrichment_tables).await; /* 构建source */
        self.build_transforms(enrichment_tables).await; /* 好像一般也是空的 */
        self.build_sinks(enrichment_tables).await; /* 构建sink */

        // We should have all the data for the enrichment tables loaded now, so switch them over to
        // readonly.
        enrichment_tables.finish_load();

        if self.errors.is_empty() {
            Ok(TopologyPieces {
                inputs: self.inputs,
                outputs: Self::finalize_outputs(self.outputs),
                tasks: self.tasks,
                source_tasks,
                shutdown_coordinator: self.shutdown_coordinator,
                detach_triggers: self.detach_triggers,
            })
        } else {
            Err(self.errors)
        }
    }

    fn finalize_outputs(
        outputs: HashMap<OutputId, UnboundedSender<fanout::ControlMessage>>,
    ) -> HashMap<ComponentKey, HashMap<Option<String>, UnboundedSender<fanout::ControlMessage>>>
    {
        let mut finalized_outputs = HashMap::new();
        for (id, output) in outputs {
            let entry = finalized_outputs
                .entry(id.component)
                .or_insert_with(HashMap::new);
            entry.insert(id.port, output);
        }

        finalized_outputs
    }
/* build之前加载enrichment表 */
    /// Loads, or reloads the enrichment tables.
    /// The tables are stored in the `ENRICHMENT_TABLES` global variable.
    async fn load_enrichment_tables(&mut self) -> &'static vector_lib::enrichment::TableRegistry {
        let mut enrichment_tables = HashMap::new();
/* 'tables: 是一个标签，标记了这个 for 循环 */
        // Build enrichment tables
        'tables: for (name, table_outer) in self.config.enrichment_tables.iter() {/* 这里的循环一般是空的 */
            let table_name = name.to_string();
            if ENRICHMENT_TABLES.needs_reload(&table_name) {
                let indexes = if !self.diff.enrichment_tables.is_added(name) {
                    // If this is an existing enrichment table, we need to store the indexes to reapply
                    // them again post load.
                    Some(ENRICHMENT_TABLES.index_fields(&table_name))
                } else {
                    None
                };

                let mut table = match table_outer.inner.build(&self.config.global).await {
                    Ok(table) => table,
                    Err(error) => {
                        self.errors
                            .push(format!("Enrichment Table \"{}\": {}", name, error));
                        continue;
                    }
                };

                if let Some(indexes) = indexes {
                    for (case, index) in indexes {
                        match table
                            .add_index(case, &index.iter().map(|s| s.as_ref()).collect::<Vec<_>>())
                        {
                            Ok(_) => (),
                            Err(error) => {
                                // If there is an error adding an index we do not want to use the reloaded
                                // data, the previously loaded data will still need to be used.
                                // Just report the error and continue.
                                error!(message = "Unable to add index to reloaded enrichment table.",
                                    table = ?name.to_string(),
                                    %error);
                                continue 'tables;
                            }
                        }
                    }
                }

                enrichment_tables.insert(table_name, table);
            }
        }

        ENRICHMENT_TABLES.load(enrichment_tables);

        &ENRICHMENT_TABLES
    }
/* 构建拓扑过程中调用,  */
    async fn build_sources(
        &mut self,
        enrichment_tables: &vector_lib::enrichment::TableRegistry,/* 可能是空的 */
    ) -> HashMap<ComponentKey, Task> {
        let mut source_tasks = HashMap::new();

        let table_sources = self
            .config
            .enrichment_tables
            .iter()
            .filter_map(|(key, table)| table.as_source(key))
            .collect::<Vec<_>>();
        for (key, source) in self
            .config
            .sources()
            .filter(|(key, _)| self.diff.sources.contains_new(key))
            .chain(
                table_sources
                    .iter()
                    .map(|(key, sink)| (key, sink))
                    .filter(|(key, _)| self.diff.enrichment_tables.contains_new(key)),
            )
        {
            debug!(component = %key, "Building new source.");

            let typetag = source.inner.get_component_name();
            /* 20250717201231 里面可能是SourceOutput*/
            let source_outputs = source.inner.outputs(self.config.schema.log_namespace());

            let span = error_span!(
                "source",
                component_kind = "source",
                component_id = %key.id(),
                component_type = %source.inner.get_component_name(),
            );
            let _entered_span = span.enter();

            let task_name = format!(
                ">> {} ({}, pump) >>",
                source.inner.get_component_name(),
                key.id()
            );

            let mut builder = SourceSender::builder().with_buffer(*SOURCE_SENDER_BUFFER_SIZE);
            let mut pumps = Vec::new();
            let mut controls = HashMap::new();
            let mut schema_definitions = HashMap::with_capacity(source_outputs.len());

            for output in source_outputs.into_iter() {/* key可能就是my_source_id */
                let mut rx = builder.add_source_output(output.clone(), key.clone());
/* 把builder的self.default_output设置为tx, 返回rx */
                let (mut fanout, control) = Fanout::new();
                let source_type = source.inner.get_component_name();
                let source = Arc::new(key.clone());
/* 这就开始导出source了? */
                let pump = async move {
                    debug!("Source pump starting.");

                    while let Some(SourceSenderItem {
                        events: mut array,
                        send_reference,
                    }) = rx.next().await/* 从rx读取 */
                    { /* 如果读取到了 */
                        array.set_output_id(&source);
                        array.set_source_type(source_type);
                        fanout
                            .send(array, Some(send_reference))
                            .await
                            .map_err(|e| {
                                debug!("Source pump finished with an error.");
                                TaskError::wrapped(e)
                            })?;
                    }

                    debug!("Source pump finished normally.");
                    Ok(TaskOutput::Source)
                };
/* 把span塞到pump里面，push进去 */
                pumps.push(pump.instrument(span.clone()));
                controls.insert(
                    OutputId {
                        component: key.clone(),
                        port: output.port.clone(),
                    },
                    control,
                );

                let port = output.port.clone();
                if let Some(definition) = output.schema_definition(self.config.schema.enabled) {
                    schema_definitions.insert(port, definition);
                }
            }

            let (pump_error_tx, mut pump_error_rx) = oneshot::channel();
            let pump = async move {
                debug!("Source pump supervisor starting.");

                // Spawn all of the per-output pumps and then await their completion.
                //
                // If any of the pumps complete with an error, or panic/are cancelled, we return
                // immediately.
                let mut handles = FuturesUnordered::new();
                for pump in pumps {
                    handles.push(spawn_named(pump, task_name.as_ref()));
                }

                let mut had_pump_error = false;
                while let Some(output) = handles.try_next().await? {
                    if let Err(e) = output {
                        // Immediately send the error to the source's wrapper future, but ignore any
                        // errors during the send, since nested errors wouldn't make any sense here.
                        _ = pump_error_tx.send(e);
                        had_pump_error = true;
                        break;
                    }
                }

                if had_pump_error {
                    debug!("Source pump supervisor task finished with an error.");
                } else {
                    debug!("Source pump supervisor task finished normally.");
                }
                Ok(TaskOutput::Source)
            };
            let pump = Task::new(key.clone(), typetag, pump);
            /* 包含了self.default_output的一个source */
            let pipeline = builder.build();

            let (shutdown_signal, force_shutdown_tripwire) = self
                .shutdown_coordinator
                .register_source(key, INTERNAL_SOURCES.contains(&typetag));

            let context = SourceContext {
                key: key.clone(), /* source的id */
                globals: self.config.global.clone(),
                enrichment_tables: enrichment_tables.clone(),
                shutdown: shutdown_signal, /* self.shutdown_coordinator里面那个 */
                out: pipeline, /* 是self.default_output那个tx */
                proxy: ProxyConfig::merge_with_env(&self.config.global.proxy, &source.proxy),
                schema_definitions,
                schema: self.config.schema,
            };/* impl SourceConfig for HostMetricsConfig */
            let source = source.inner.build(context).await;
            let server = match source {
                Err(error) => {
                    self.errors.push(format!("Source \"{}\": {}", key, error));
                    continue;
                }
                Ok(server) => server,
            };

            // Build a wrapper future that drives the actual source future, but returns early if we've
            // been signalled to forcefully shutdown, or if the source pump encounters an error.
            //
            // The forceful shutdown will only resolve if the source itself doesn't shutdown gracefully
            // within the allotted time window. This can occur normally for certain sources, like stdin,
            // where the I/O is blocking (in a separate thread) and won't wake up to check if it's time
            // to shutdown unless some input is given.
            let server = async move {
                debug!("Source starting.");
/* 这里是开始获取指标的逻辑 */
                let mut result = select! {
                    biased;

                    // We've been told that we must forcefully shut down.
                    _ = force_shutdown_tripwire => Ok(()),

                    // The source pump encountered an error, which we're now bubbling up here to stop
                    // the source as well, since the source running makes no sense without the pump.
                    //
                    // We only match receiving a message, not the error of the sender being dropped,
                    // just to keep things simpler.
                    Ok(e) = &mut pump_error_rx => Err(e),

                    // The source finished normally.
                    result = server => result.map_err(|_| TaskError::Opaque),
                };

                // Even though we already tried to receive any pump task error above, we may have exited
                // on the source itself returning an error due to task scheduling, where the pump task
                // encountered an error, sent it over the oneshot, but we were polling the source
                // already and hit an error trying to send to the now-shutdown pump task.
                //
                // Since the error from the source is opaque at the moment (i.e. `()`), we try a final
                // time to see if the pump task encountered an error, using _that_ instead if so, to
                // propagate the true error that caused the source to have to stop.
                if let Ok(e) = pump_error_rx.try_recv() {
                    result = Err(e);
                }

                match result {
                    Ok(()) => {
                        debug!("Source finished normally.");
                        Ok(TaskOutput::Source)
                    }
                    Err(e) => {
                        debug!("Source finished with an error.");
                        Err(e)
                    }
                }
            };
            let server = Task::new(key.clone(), typetag, server);

            self.outputs.extend(controls);
            self.tasks.insert(key.clone(), pump); /* 这是两个task */
            source_tasks.insert(key.clone(), server);
        }

        source_tasks
    }
/* 构建完config之后, 构建enrichment, source,然后基于enrichment构建transform */
    async fn build_transforms(
        &mut self,
        _enrichment_tables: &vector_lib::enrichment::TableRegistry,
    ) {/* 有调用 */

    }
/* 获取config之后, 构建source, transform, sinks, 这里构建sinks */
    async fn build_sinks(&mut self, enrichment_tables: &vector_lib::enrichment::TableRegistry) {
        let table_sinks = self
            .config
            .enrichment_tables
            .iter()
            .filter_map(|(key, table)| table.as_sink(key))
            .collect::<Vec<_>>();
        for (key, sink) in self
            .config
            .sinks()
            .filter(|(key, _)| self.diff.sinks.contains_new(key))
            .chain(
                table_sinks
                    .iter()
                    .map(|(key, sink)| (key, sink))
                    .filter(|(key, _)| self.diff.enrichment_tables.contains_new(key)),
            )
        {
            debug!(component = %key, "Building new sink.");

            let sink_inputs = &sink.inputs;
            /* 可能是Console */
            let typetag = sink.inner.get_component_name();
            let input_type = sink.inner.input().data_type(); /*  */

            let span = error_span!(
                "sink",
                component_kind = "sink",
                component_id = %key.id(),
                component_type = %sink.inner.get_component_name(),
            );
            let _entered_span = span.enter();

            // At this point, we've validated that all transforms are valid, including any
            // transform that mutates the schema provided by their sources. We can now validate the
            // schema expectations of each individual sink.
            if let Err(mut err) = schema::validate_sink_expectations(
                key,
                sink,
                self.config,
                enrichment_tables.clone(),
            ) {
                self.errors.append(&mut err);
            };
/* buffer的tx rx */
            let (tx, rx) = if let Some(buffer) = self.buffers.remove(key) {
                buffer
            } else {
                let buffer_type = match sink.buffer.stages().first().expect("cant ever be empty") {
                    BufferType::Memory { .. } => "memory",
                };
                let buffer_span = error_span!("sink", buffer_type);
                let buffer = sink
                    .buffer
                    .build(
                        self.config.global.data_dir.clone(),
                        key.to_string(),
                        buffer_span,
                    )
                    .await; /* 返回的是buffer的tx和rx */
                match buffer {
                    Err(error) => {
                        self.errors.push(format!("Sink \"{}\": {}", key, error));
                        continue;
                    }/* buffer的tx rx */
                    Ok((tx, rx)) => (tx, Arc::new(Mutex::new(Some(rx.into_stream())))),
                }
            };

            let cx = SinkContext {
                globals: self.config.global.clone(),
                enrichment_tables: enrichment_tables.clone(),
                proxy: ProxyConfig::merge_with_env(&self.config.global.proxy, sink.proxy()),
                schema: self.config.schema,
                app_name: crate::get_app_name().to_string(),
                app_name_slug: crate::get_slugified_app_name(),
            };
            /* 这里的sink就是具体的sink了, 比如到console */
            let sink = match sink.inner.build(cx).await {
                Err(error) => {
                    self.errors.push(format!("Sink \"{}\": {}", key, error));
                    continue;
                }
                Ok(built) => built,
            };

            let (trigger, tripwire) = Tripwire::new();

            let _component_key = key.clone();
            let sink = async move {
                debug!("Sink starting.");

                // Why is this Arc<Mutex<Option<_>>> needed you ask.
                // In case when this function build_pieces errors
                // this future won't be run so this rx won't be taken
                // which will enable us to reuse rx to rebuild
                // old configuration by passing this Arc<Mutex<Option<_>>>
                // yet again.   buffer的rx
                let rx = rx
                    .lock()
                    .unwrap()
                    .take()
                    .expect("Task started but input has been taken.");

                let mut rx = rx;

                let events_received = register!(EventsReceived);
                sink.run(
                    rx.by_ref()
                        .filter(|events: &EventArray| ready(filter_events_type(events, input_type)))
                        .inspect(|events| {
                            events_received.emit(CountByteSize(
                                events.len(),
                                events.estimated_json_encoded_size_of(),
                            ))
                        })
                        .take_until_if(tripwire),
                )
                .await
                .map(|_| {
                    debug!("Sink finished normally.");
                    TaskOutput::Sink(rx)
                })
                .map_err(|_| {
                    debug!("Sink finished with an error.");
                    TaskError::Opaque
                })
            };
            /* 像是开启一个task, 运行上面这个siink */
            let task = Task::new(key.clone(), typetag, sink);



/* key是sink的id */
            self.inputs.insert(key.clone(), (tx, sink_inputs.clone()));
            self.tasks.insert(key.clone(), task); /* 把sink task塞进去 */
            self.detach_triggers.insert(key.clone(), trigger);
        }
    }
}

pub struct TopologyPieces {
    pub(super) inputs: HashMap<ComponentKey, (BufferSender<EventArray>, Inputs<OutputId>)>, /* 里面是buffer的tx */
    pub(crate) outputs: HashMap<ComponentKey, HashMap<Option<String>, fanout::ControlChannel>>, /*  */
    pub(super) tasks: HashMap<ComponentKey, Task>, /* source，output,sink等的task */
    pub(crate) source_tasks: HashMap<ComponentKey, Task>, /* source的task */
    pub(crate) shutdown_coordinator: SourceShutdownCoordinator, /*  */
    pub(crate) detach_triggers: HashMap<ComponentKey, Trigger>, /*  */
}
/* 构建拓扑的方法 */
impl TopologyPieces {/*  */
    pub async fn build_or_log_errors(
        config: &Config,/* 解析出的config */
        diff: &ConfigDiff,/* 这个config的产生的变化 */
        buffers: HashMap<ComponentKey, BuiltBuffer>,
    ) -> Option<Self> {/* 构建各种东西, source, sink等等 */
        match TopologyPieces::build(config, diff, buffers).await {
            Err(errors) => {
                for error in errors {
                    error!(message = "Configuration error.", %error);
                }
                None
            }
            Ok(new_pieces) => Some(new_pieces),
        }
    }

    /// Builds only the new pieces, and doesn't check their topology.
    pub async fn build(
        config: &super::Config, /* 新config */
        diff: &ConfigDiff, /* 新config产生的diff */
        buffers: HashMap<ComponentKey, BuiltBuffer>,
    ) -> Result<Self, Vec<String>> {
        Builder::new(config, diff, buffers)
            .build()
            .await
    }
}

const fn filter_events_type(events: &EventArray, data_type: DataType) -> bool {
    match events {
        EventArray::Logs(_) => data_type.contains(DataType::Log),
        EventArray::Metrics(_) => data_type.contains(DataType::Metric),
        EventArray::Traces(_) => data_type.contains(DataType::Trace),
    }
}