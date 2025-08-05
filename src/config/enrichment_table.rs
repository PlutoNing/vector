use enum_dispatch::enum_dispatch;
use serde::Serialize;
use crate::core::global_options::GlobalOptions;
use agent_lib::configurable::{configurable_component, Configurable, NamedComponent, ToValue};
use agent_lib::config::ComponentKey;
use crate::common::Inputs;
use crate::enrichment_tables::EnrichmentTables;

use super::dot_graph::GraphConfig;
use super::{SinkConfig, SinkOuter, SourceConfig, SourceOuter};

/// Fully resolved enrichment table component.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct EnrichmentTableOuter<T>
where
    T: Configurable + Serialize + 'static + ToValue + Clone,
{
    #[serde(flatten)]
    pub inner: EnrichmentTables,
    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "agent_lib::config::is_default")]
    pub graph: GraphConfig,
    #[configurable(derived)]
    #[serde(
        default = "Inputs::<T>::default",
        skip_serializing_if = "Inputs::is_empty"
    )]
    pub inputs: Inputs<T>,
}

impl<T> EnrichmentTableOuter<T>
where
    T: Configurable + Serialize + 'static + ToValue + Clone,
{
    pub fn new<I, IET>(inputs: I, inner: IET) -> Self
    where
        I: IntoIterator<Item = T>,
        IET: Into<EnrichmentTables>,
    {
        Self {
            inner: inner.into(),
            graph: Default::default(),
            inputs: Inputs::from_iter(inputs),
        }
    }

// 组件目前的设计方式是它们严格匹配其中一个角色（source、transform、sink、enrichment table）。
// 由于"memory" enrichment table 的特殊需求，它需要同时承担2个角色（sink和enrichment table）。
// 为了减少这个非常特殊需求的影响，现在任何enrichment table都可以选择性地映射为一个sink，
// 但这只适用于"memory" enrichment table，因为其他表不会有"sink_config"存在。
// 这也不是理想的方案，因为`SinkOuter`本不打算表示实际的配置，
// 它应该只是用于反序列化的配置表示。
// 在未来，如果出现更多这样的组件，最好将这些"Outer"组件限制为仅用于反序列化，
// 并以更细粒度的方式构建组件和拓扑结构，每个组件都有用于输入的"模块"（使其作为sink有效）、
// 用于健康检查、用于提供输出等。
    pub fn as_sink(&self, default_key: &ComponentKey) -> Option<(ComponentKey, SinkOuter<T>)> {
        self.inner.sink_config(default_key).map(|(key, sink)| {
            (
                key,
                SinkOuter {
                    graph: self.graph.clone(),
                    inputs: self.inputs.clone(),
                    inner: sink,
                },
            )
        })
    }

    pub fn as_source(&self, default_key: &ComponentKey) -> Option<(ComponentKey, SourceOuter)> {
        self.inner.source_config(default_key).map(|(key, source)| {
            (
                key,
                SourceOuter {
                    graph: self.graph.clone(),
                    inner: source,
                },
            )
        })
    }

    pub(super) fn map_inputs<U>(self, f: impl Fn(&T) -> U) -> EnrichmentTableOuter<U>
    where
        U: Configurable + Serialize + 'static + ToValue + Clone,
    {
        let inputs = self.inputs.iter().map(f).collect::<Vec<_>>();
        self.with_inputs(inputs)
    }

    pub(crate) fn with_inputs<I, U>(self, inputs: I) -> EnrichmentTableOuter<U>
    where
        I: IntoIterator<Item = U>,
        U: Configurable + Serialize + 'static + ToValue + Clone,
    {
        EnrichmentTableOuter {
            inputs: Inputs::from_iter(inputs),
            inner: self.inner,
            graph: self.graph,
        }
    }
}

/// Generalized interface for describing and building enrichment table components.
#[enum_dispatch]
pub trait EnrichmentTableConfig: NamedComponent + core::fmt::Debug + Send + Sync {
    /// Builds the enrichment table with the given globals.
    ///
    /// If the enrichment table is built successfully, `Ok(...)` is returned containing the
    /// enrichment table.
    ///
    /// # Errors
    ///
    /// If an error occurs while building the enrichment table, an error variant explaining the
    /// issue is returned.
    async fn build(
        &self,
        globals: &GlobalOptions,
    ) -> crate::Result<Box<dyn crate::enrichment_tables::enrichment::Table + Send + Sync>>;

    fn sink_config(
        &self,
        _default_key: &ComponentKey,
    ) -> Option<(ComponentKey, Box<dyn SinkConfig>)> {
        None
    }

    fn source_config(
        &self,
        _default_key: &ComponentKey,
    ) -> Option<(ComponentKey, Box<dyn SourceConfig>)> {
        None
    }
}
