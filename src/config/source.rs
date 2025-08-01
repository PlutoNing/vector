use std::cell::RefCell;
use std::collections::HashMap;

use crate::common::Source;
use crate::core::global_options::GlobalOptions;
use agent_config::{Configurable, GenerateError, Metadata, NamedComponent};
use agent_config_common::attributes::CustomAttribute;
use agent_config_common::schema::{SchemaGenerator, SchemaObject};
use agent_config_macros::configurable_component;
use agent_lib::config::{LogNamespace, SourceOutput};
use async_trait::async_trait;
use dyn_clone::DynClone;

use super::{dot_graph::GraphConfig, schema, ComponentKey, ProxyConfig, Resource};
use crate::common::ShutdownSignal;
use crate::SourceSender;
/* 使用 Box 装箱一个实现了 SourceConfig 特征的动态对象。这种用法允许在运行时决定具体的 SourceConfig 实现。 */
pub type BoxedSource = Box<dyn SourceConfig>;

impl Configurable for BoxedSource {
    fn referenceable_name() -> Option<&'static str> {
        Some("vector::sources::Sources")
    }

    fn metadata() -> Metadata {
        let mut metadata = Metadata::default();
        metadata.set_description("Configurable sources in Vector.");
        metadata.add_custom_attribute(CustomAttribute::kv("docs::enum_tagging", "internal"));
        metadata.add_custom_attribute(CustomAttribute::kv("docs::enum_tag_field", "type"));
        metadata
    }

    fn generate_schema(gen: &RefCell<SchemaGenerator>) -> Result<SchemaObject, GenerateError> {
        agent_lib::configurable::component::SourceDescription::generate_schemas(gen)
    }
}

impl<T: SourceConfig + 'static> From<T> for BoxedSource {
    fn from(value: T) -> Self {
        Box::new(value)
    }
}
/* config里面对一个source的定义 */
/// Fully resolved source component.
#[configurable_component]
#[configurable(metadata(docs::component_base_type = "source"))]
#[derive(Clone, Debug)]
pub struct SourceOuter {
    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "agent_lib::serde::is_default")]
    pub proxy: ProxyConfig,

    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "agent_lib::serde::is_default")]
    pub graph: GraphConfig,

    #[configurable(metadata(docs::hidden))]
    #[serde(flatten)]
    pub(crate) inner: BoxedSource,
}

impl SourceOuter {
    pub(crate) fn new<I: Into<BoxedSource>>(inner: I) -> Self {
        Self {
            proxy: Default::default(),
            graph: Default::default(),
            inner: inner.into(),
        }
    }
}
/* 每个source都要实现这个接口 */
/// Generalized interface for describing and building source components.
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait SourceConfig: DynClone + NamedComponent + core::fmt::Debug + Send + Sync {
    /// Builds the source with the given context.
    ///
    /// If the source is built successfully, `Ok(...)` is returned containing the source.
    ///
    /// # Errors
    ///
    /// If an error occurs while building the source, an error variant explaining the issue is
    /// returned.
    async fn build(&self, cx: SourceContext) -> crate::Result<Source>;

    /// Gets the list of outputs exposed by this source.
    fn outputs(&self, global_log_namespace: LogNamespace) -> Vec<SourceOutput>;

    /// Gets the list of resources, if any, used by this source.
    ///
    /// Resources represent dependencies -- network ports, file descriptors, and so on -- that
    /// cannot be shared between components at runtime. This ensures that components can not be
    /// configured in a way that would deadlock the spawning of a topology, and as well, allows
    /// Vector to determine the correct order for rebuilding a topology during configuration reload
    /// when resources must first be reclaimed before being reassigned, and so on.
    fn resources(&self) -> Vec<Resource> {
        Vec::new()
    }
}

dyn_clone::clone_trait_object!(SourceConfig);
/* 表示一个source */
pub struct SourceContext {
    pub key: ComponentKey, /*   source的id  */
    pub globals: GlobalOptions,
    pub enrichment_tables: crate::enrichment_tables::enrichment::TableRegistry,
    pub shutdown: ShutdownSignal, /* self.shutdown_coordinator里面那个 */
    pub out: SourceSender,        /*  是self.default_output那个tx*/
    pub proxy: ProxyConfig,
    pub schema: schema::Options,

    /// Tracks the schema IDs assigned to schemas exposed by the source.
    ///
    /// Given a source can expose multiple [`SourceOutput`] channels, the ID is tied to the identifier of
    /// that `SourceOutput`.
    pub schema_definitions: HashMap<Option<String>, schema::Definition>,
}

impl SourceContext {
    /// Gets the log namespacing to use. The passed in value is from the source itself
    /// and will override any global default if it's set.
    pub fn log_namespace(&self, namespace: Option<bool>) -> LogNamespace {
        namespace
            .or(self.schema.log_namespace)
            .unwrap_or(false)
            .into()
    }
}
