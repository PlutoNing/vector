#![allow(missing_docs)]
use std::{
    collections::{HashMap, HashSet},
    fmt::{self, Display, Formatter},
    fs,
    hash::Hash,
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

use indexmap::IndexMap;
use serde::Serialize;

pub use vector_lib::config::{
    DataType, GlobalOptions, Input, LogNamespace,
    SourceOutput, TransformOutput, WildcardMatching,
};
pub use vector_lib::configurable::component::{
    GenerateConfig, SinkDescription, TransformDescription,
};

mod builder;
mod cmd;
mod compiler;
mod diff;
pub mod dot_graph;
mod enrichment_table;
pub mod format;
mod graph;
mod loading;
pub mod provider;
pub mod schema;
mod sink;
mod source;
mod transform;
mod vars;
pub mod watcher;

pub use builder::ConfigBuilder;
pub use cmd::{cmd, Opts};
pub use diff::ConfigDiff;
pub use enrichment_table::{EnrichmentTableConfig, EnrichmentTableOuter};
pub use format::{Format, FormatHint};
pub use loading::{
    load, load_builder_from_paths, load_from_paths, load_from_paths_with_provider_and_secrets,
    load_from_str, load_source_from_paths, merge_path_lists, process_paths,
    CONFIG_PATHS,
};
pub use sink::{BoxedSink, SinkConfig, SinkContext, SinkOuter};
pub use source::{BoxedSource, SourceConfig, SourceContext, SourceOuter};
pub use transform::{
    get_transform_output_ids, BoxedTransform, TransformConfig, TransformContext, TransformOuter,
};
pub use vars::{interpolate, ENVIRONMENT_VARIABLE_INTERPOLATION_REGEX};
pub use vector_lib::{
    config::{
        init_log_schema, log_schema, proxy::ProxyConfig, ComponentKey,
        LogSchema, OutputId,
    },
    id::Inputs,
};

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub struct ComponentConfig {
    pub config_paths: Vec<PathBuf>,
    pub component_key: ComponentKey,
}

impl ComponentConfig {
    pub fn new(config_paths: Vec<PathBuf>, component_key: ComponentKey) -> Self {
        let canonicalized_paths = config_paths
            .into_iter()
            .filter_map(|p| fs::canonicalize(p).ok())
            .collect();

        Self {
            config_paths: canonicalized_paths,
            component_key,
        }
    }

    pub fn contains(&self, config_paths: &[PathBuf]) -> Option<ComponentKey> {
        if config_paths.iter().any(|p| self.config_paths.contains(p)) {
            return Some(self.component_key.clone());
        }
        None
    }
}
/* 表示一个配置文件的路径 */
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum ConfigPath {
    File(PathBuf, FormatHint),
    Dir(PathBuf),
}

impl<'a> From<&'a ConfigPath> for &'a PathBuf {
    fn from(config_path: &'a ConfigPath) -> &'a PathBuf {
        match config_path {
            ConfigPath::File(path, _) => path,
            ConfigPath::Dir(path) => path,
        }
    }
}

impl ConfigPath {
    pub const fn as_dir(&self) -> Option<&PathBuf> {
        match self {
            Self::Dir(path) => Some(path),
            _ => None,
        }
    }
}
/* 最终构建的config? */
#[derive(Debug, Default, Serialize)]
pub struct Config {
    pub schema: schema::Options,
    pub global: GlobalOptions,
    sources: IndexMap<ComponentKey, SourceOuter>,
    sinks: IndexMap<ComponentKey, SinkOuter<OutputId>>,
    transforms: IndexMap<ComponentKey, TransformOuter<OutputId>>,
    pub enrichment_tables: IndexMap<ComponentKey, EnrichmentTableOuter<OutputId>>,
    pub graceful_shutdown_duration: Option<Duration>,
}

impl Config {
    pub fn builder() -> builder::ConfigBuilder {
        Default::default()
    }

    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
/* 获取source源 */
    pub fn sources(&self) -> impl Iterator<Item = (&ComponentKey, &SourceOuter)> {
        self.sources.iter()
    }

    pub fn source(&self, id: &ComponentKey) -> Option<&SourceOuter> {
        self.sources.get(id)
    }

    pub fn transforms(&self) -> impl Iterator<Item = (&ComponentKey, &TransformOuter<OutputId>)> {
        self.transforms.iter()
    }

    pub fn transform(&self, id: &ComponentKey) -> Option<&TransformOuter<OutputId>> {
        self.transforms.get(id)
    }

    pub fn sinks(&self) -> impl Iterator<Item = (&ComponentKey, &SinkOuter<OutputId>)> {
        self.sinks.iter()
    }

    pub fn sink(&self, id: &ComponentKey) -> Option<&SinkOuter<OutputId>> {
        self.sinks.get(id)
    }

    pub fn enrichment_tables(
        &self,
    ) -> impl Iterator<Item = (&ComponentKey, &EnrichmentTableOuter<OutputId>)> {
        self.enrichment_tables.iter()
    }

    pub fn enrichment_table(&self, id: &ComponentKey) -> Option<&EnrichmentTableOuter<OutputId>> {
        self.enrichment_tables.get(id)
    }

    pub fn inputs_for_node(&self, id: &ComponentKey) -> Option<&[OutputId]> {
        self.transforms
            .get(id)
            .map(|t| &t.inputs[..])
            .or_else(|| self.sinks.get(id).map(|s| &s.inputs[..]))
            .or_else(|| self.enrichment_tables.get(id).map(|s| &s.inputs[..]))
    }
}

/// Unique thing, like port, of which only one owner can be.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub enum Resource {
    Port(SocketAddr, Protocol),
    SystemFdOffset(usize),
    Fd(u32),
    DiskBuffer(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Copy)]
pub enum Protocol {
    Tcp,
    Udp,
}

impl Resource {
    pub const fn tcp(addr: SocketAddr) -> Self {
        Self::Port(addr, Protocol::Tcp)
    }

    pub const fn udp(addr: SocketAddr) -> Self {
        Self::Port(addr, Protocol::Udp)
    }
/* 检查配置项的source和sink有没有冲突 */
    /// From given components returns all that have a resource conflict with any other component.
    pub fn conflicts<K: Eq + Hash + Clone>(
        components: impl IntoIterator<Item = (K, Vec<Resource>)>,
    ) -> HashMap<Resource, HashSet<K>> {
        let mut resource_map = HashMap::<Resource, HashSet<K>>::new();
        let mut unspecified = Vec::new();

        // Find equality based conflicts
        for (key, resources) in components {
            for resource in resources {
                if let Resource::Port(address, protocol) = &resource {
                    if address.ip().is_unspecified() {
                        unspecified.push((key.clone(), *address, *protocol));
                    }
                }

                resource_map
                    .entry(resource)
                    .or_default()
                    .insert(key.clone());
            }
        }

        // Port with unspecified address will bind to all network interfaces
        // so we have to check for all Port resources if they share the same
        // port.
        for (key, address0, protocol0) in unspecified {
            for (resource, components) in resource_map.iter_mut() {
                if let Resource::Port(address, protocol) = resource {
                    // IP addresses can either be v4 or v6.
                    // Therefore we check if the ip version matches, the port matches and if the protocol (TCP/UDP) matches
                    // when checking for equality.
                    if &address0 == address && &protocol0 == protocol {
                        components.insert(key.clone());
                    }
                }
            }
        }

        resource_map.retain(|_, components| components.len() > 1);

        resource_map
    }
}

impl Display for Protocol {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Protocol::Udp => write!(fmt, "udp"),
            Protocol::Tcp => write!(fmt, "tcp"),
        }
    }
}

impl Display for Resource {
    fn fmt(&self, fmt: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Resource::Port(address, protocol) => write!(fmt, "{} {}", protocol, address),
            Resource::SystemFdOffset(offset) => write!(fmt, "systemd {}th socket", offset + 1),
            Resource::Fd(fd) => write!(fmt, "file descriptor: {}", fd),
            Resource::DiskBuffer(name) => write!(fmt, "disk buffer {:?}", name),
        }
    }
}