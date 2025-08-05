use crate::common::Inputs;
use crate::core::global_options::GlobalOptions;
use crate::core::sink::VectorSink;
use agent_lib::config::Input;
use agent_lib::configurable::attributes::CustomAttribute;
use agent_lib::configurable::schema::{SchemaGenerator, SchemaObject};
use agent_lib::configurable::{
    configurable_component, Configurable, GenerateError, Metadata, NamedComponent,
};
use async_trait::async_trait;
use dyn_clone::DynClone;
use serde::Serialize;
use std::cell::RefCell;
use std::path::PathBuf;

use super::{dot_graph::GraphConfig, schema, ComponentKey, Resource};
pub type BoxedSink = Box<dyn SinkConfig>;

impl Configurable for BoxedSink {
    fn referenceable_name() -> Option<&'static str> {
        Some("vector::sinks::Sinks")
    }

    fn metadata() -> Metadata {
        let mut metadata = Metadata::default();
        metadata.set_description("Configurable sinks in Vector.");
        metadata.add_custom_attribute(CustomAttribute::kv("docs::enum_tagging", "internal"));
        metadata.add_custom_attribute(CustomAttribute::kv("docs::enum_tag_field", "type"));
        metadata
    }

    fn generate_schema(gen: &RefCell<SchemaGenerator>) -> Result<SchemaObject, GenerateError> {
        agent_lib::configurable::component::SinkDescription::generate_schemas(gen)
    }
}

impl<T: SinkConfig + 'static> From<T> for BoxedSink {
    fn from(value: T) -> Self {
        Box::new(value)
    }
}
/* 用来构建一个sink */
/// Fully resolved sink component.
#[configurable_component]
#[configurable(metadata(docs::component_base_type = "sink"))]
#[derive(Clone, Debug)]
pub struct SinkOuter<T>
where
    T: Configurable + Serialize + 'static,
{
    #[configurable(derived)]
    #[serde(default, skip_serializing_if = "agent_lib::config::is_default")]
    pub graph: GraphConfig,

    #[configurable(derived)]
    pub inputs: Inputs<T>,

    #[serde(flatten)]
    #[configurable(metadata(docs::hidden))]
    pub inner: BoxedSink, /* 比如可能是个file sink config */
}

impl<T> SinkOuter<T>
where
    T: Configurable + Serialize,
{
    pub fn new<I, IS>(inputs: I, inner: IS) -> SinkOuter<T>
    where
        I: IntoIterator<Item = T>,
        IS: Into<BoxedSink>,
    {
        SinkOuter {
            inputs: Inputs::from_iter(inputs),
            inner: inner.into(),
            graph: Default::default(),
        }
    }

    pub fn resources(&self, _id: &ComponentKey) -> Vec<Resource> {
        let resources = self.inner.resources();
        resources
    }

    pub(super) fn map_inputs<U>(self, f: impl Fn(&T) -> U) -> SinkOuter<U>
    where
        U: Configurable + Serialize,
    {
        let inputs = self.inputs.iter().map(f).collect::<Vec<_>>();
        self.with_inputs(inputs)
    }

    pub(crate) fn with_inputs<I, U>(self, inputs: I) -> SinkOuter<U>
    where
        I: IntoIterator<Item = U>,
        U: Configurable + Serialize,
    {
        SinkOuter {
            inputs: Inputs::from_iter(inputs),
            inner: self.inner,
            graph: self.graph,
        }
    }
}

/* 一个sink的实现(到文件,到console,到db,或者到远程)需要实现这个接口 */
/// Generalized interface for describing and building sink components.
#[async_trait]
#[typetag::serde(tag = "type")]
pub trait SinkConfig: DynClone + NamedComponent + core::fmt::Debug + Send + Sync {
    /// Builds the sink with the given context.
    ///
    /// If the sink is built successfully, `Ok(...)` is returned containing the sink and the sink's
    ///
    ///
    /// # Errors
    ///
    /// If an error occurs while building the sink, an error variant explaining the issue is
    /// returned.
    async fn build(&self, cx: SinkContext) -> crate::Result<VectorSink>;

    /// Gets the input configuration for this sink.
    fn input(&self) -> Input;

    /// Gets the files to watch to trigger reload
    fn files_to_watch(&self) -> Vec<&PathBuf> {
        Vec::new()
    }

    /// Gets the list of resources, if any, used by this sink.
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

dyn_clone::clone_trait_object!(SinkConfig);

#[derive(Clone)]
pub struct SinkContext {
    pub globals: GlobalOptions,
    pub enrichment_tables: crate::enrichment_tables::enrichment::TableRegistry,
    pub schema: schema::Options,
    pub app_name: String,
    pub app_name_slug: String,
}

impl Default for SinkContext {
    fn default() -> Self {
        Self {
            globals: Default::default(),
            enrichment_tables: Default::default(),
            schema: Default::default(),
            app_name: crate::get_app_name().to_string(),
            app_name_slug: crate::get_slugified_app_name(),
        }
    }
}

impl SinkContext {
    pub const fn globals(&self) -> &GlobalOptions {
        &self.globals
    }
}
