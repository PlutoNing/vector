use std::{path::Path, time::Duration};

use indexmap::IndexMap;
use vector_lib::config::GlobalOptions;
use vector_lib::configurable::configurable_component;

use crate::{enrichment_tables::EnrichmentTables};

use super::{
    compiler, schema, BoxedSink, BoxedSource, BoxedTransform, ComponentKey, Config,
    EnrichmentTableOuter, SinkOuter, SourceOuter, 
    TransformOuter,
};

/// A complete Vector configuration.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct ConfigBuilder {
    #[serde(flatten)]
    pub global: GlobalOptions,

    #[configurable(derived)]
    #[configurable(metadata(docs::hidden))]
    #[serde(default)]
    pub schema: schema::Options,

    /// All configured enrichment tables.
    #[serde(default)]
    pub enrichment_tables: IndexMap<ComponentKey, EnrichmentTableOuter<String>>,

    /// All configured sources.
    #[serde(default)]
    pub sources: IndexMap<ComponentKey, SourceOuter>,

    /// All configured sinks.
    #[serde(default)]
    pub sinks: IndexMap<ComponentKey, SinkOuter<String>>,

    /// All configured transforms.
    #[serde(default)]
    pub transforms: IndexMap<ComponentKey, TransformOuter<String>>,

    /// The duration in seconds to wait for graceful shutdown after SIGINT or SIGTERM are received.
    /// After the duration has passed, Vector will force shutdown. Default value is 60 seconds. This
    /// value can be set using a [cli arg](crate::cli::RootOpts::graceful_shutdown_limit_secs).
    #[serde(default, skip)]
    #[doc(hidden)]
    pub graceful_shutdown_duration: Option<Duration>,

    /// Allow the configuration to be empty, resulting in a topology with no components.
    #[serde(default, skip)]
    #[doc(hidden)]
    pub allow_empty: bool,
}

impl From<Config> for ConfigBuilder {
    fn from(config: Config) -> Self {
        let Config {
            global,
            schema,
            enrichment_tables,
            sources,
            sinks,
            transforms,
            graceful_shutdown_duration,
        } = config;

        let transforms = transforms
            .into_iter()
            .map(|(key, transform)| (key, transform.map_inputs(ToString::to_string)))
            .collect();

        let sinks = sinks
            .into_iter()
            .map(|(key, sink)| (key, sink.map_inputs(ToString::to_string)))
            .collect();

        let enrichment_tables = enrichment_tables
            .into_iter()
            .map(|(key, table)| (key, table.map_inputs(ToString::to_string)))
            .collect();

        ConfigBuilder {
            global,
            schema,
            enrichment_tables,
            sources,
            sinks,
            transforms,
            graceful_shutdown_duration,
            allow_empty: false,
        }
    }
}

impl ConfigBuilder {
    pub fn build(self) -> Result<Config, Vec<String>> {
        let (config, warnings) = self.build_with_warnings()?;

        for warning in warnings {
            warn!("{}", warning);
        }

        Ok(config)
    }
    /* 构建一个config返回 */
    pub fn build_with_warnings(self) -> Result<(Config, Vec<String>), Vec<String>> {
        compiler::compile(self)
    }

    pub fn add_enrichment_table<K: Into<String>, E: Into<EnrichmentTables>>(
        &mut self,
        key: K,
        inputs: &[&str],
        enrichment_table: E,
    ) {
        let inputs = inputs
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        self.enrichment_tables.insert(
            ComponentKey::from(key.into()),
            EnrichmentTableOuter::new(inputs, enrichment_table),
        );
    }

    pub fn add_source<K: Into<String>, S: Into<BoxedSource>>(&mut self, key: K, source: S) {
        self.sources
            .insert(ComponentKey::from(key.into()), SourceOuter::new(source));
    }

    pub fn add_sink<K: Into<String>, S: Into<BoxedSink>>(
        &mut self,
        key: K,
        inputs: &[&str],
        sink: S,
    ) {
        let inputs = inputs
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let sink = SinkOuter::new(inputs, sink);
        self.add_sink_outer(key, sink);
    }

    pub fn add_sink_outer<K: Into<String>>(&mut self, key: K, sink: SinkOuter<String>) {
        self.sinks.insert(ComponentKey::from(key.into()), sink);
    }

    // For some feature sets, no transforms are compiled, which leads to no callers using this
    // method, and in turn, annoying errors about unused variables.
    pub fn add_transform(
        &mut self,
        key: impl Into<String>,
        inputs: &[&str],
        transform: impl Into<BoxedTransform>,
    ) {
        let inputs = inputs
            .iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>();
        let transform = TransformOuter::new(inputs, transform);

        self.transforms
            .insert(ComponentKey::from(key.into()), transform);
    }

    pub fn set_data_dir(&mut self, path: &Path) {
        self.global.data_dir = Some(path.to_owned());
    }
/* 处理配置文件选项? 把配置项附加到自己的各个配置成员里面. with是新配置文件 */
    pub fn append(&mut self, with: Self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        match self.global.merge(with.global) {
            Err(errs) => errors.extend(errs),
            Ok(new_global) => self.global = new_global,
        }
        info!("global: {:?}", self.global);

        self.schema.append(with.schema, &mut errors);
        self.schema.log_namespace = self.schema.log_namespace.or(with.schema.log_namespace);
        info!("schema: {:?}", self.schema);

        with.enrichment_tables.keys().for_each(|k| {
            if self.enrichment_tables.contains_key(k) {
                errors.push(format!("duplicate enrichment_table name found: {}", k));
            }
        });
        info!("enrichment_tables: {:?}", self.enrichment_tables);

        with.sources.keys().for_each(|k| {
            if self.sources.contains_key(k) {
                errors.push(format!("duplicate source id found: {}", k));
            }
        });
        info!("sources: {:?}", self.sources);

        with.sinks.keys().for_each(|k| {
            info!("with.sinks.key {:?}", k);
            if self.sinks.contains_key(k) {
                errors.push(format!("duplicate sink id found: {}", k));
            }
        });
        info!("sinks: {:?}", self.sinks);
        self.sinks.keys().for_each(|k|{
            info!("self.sinks.key {:?}", k);
        });






        if !errors.is_empty() {
            return Err(errors);
        }

        self.enrichment_tables.extend(with.enrichment_tables);
        self.sources.extend(with.sources);
        self.sinks.extend(with.sinks);
        info!("sinks: {:?}", self.sinks);


        Ok(())
    }
}
