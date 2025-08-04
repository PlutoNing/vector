use std::{
    fmt,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
    slice,
};

use serde::{de, Deserialize, Deserializer, Serialize};
use snafu::{ResultExt, Snafu};
use tracing::Span;
use agent_common::{config::ComponentKey, finalization::Finalizable};
use agent_config::configurable_component;
use crate::buffers::topology::{
        builder::{TopologyBuilder, TopologyError},
        channel::{BufferReceiver, BufferSender},
    };
use crate::buffers::variants::{MemoryBuffer};
use agent_lib::config::{Bufferable,WhenFull,};
#[derive(Debug, Snafu)]
pub enum BufferBuildError {
    #[snafu(display("the configured buffer type requires `data_dir` be specified"))]
    RequiresDataDir,
    #[snafu(display("error occurred when building buffer: {}", source))]
    FailedToBuildTopology { source: TopologyError },
    #[snafu(display("`max_events` must be greater than zero"))]
    InvalidMaxEvents,
}

#[derive(Deserialize, Serialize)]
enum BufferTypeKind {
    #[serde(rename = "memory")]
    Memory,
}

const ALL_FIELDS: [&str; 4] = ["type", "max_events", "max_size", "when_full"];

struct BufferTypeVisitor;

impl BufferTypeVisitor {
    fn visit_map_impl<'de, A>(mut map: A) -> Result<BufferType, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        let mut kind: Option<BufferTypeKind> = None;
        let mut max_events: Option<NonZeroUsize> = None;
        let mut max_size: Option<NonZeroU64> = None;
        let mut when_full: Option<WhenFull> = None;
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "type" => {
                    if kind.is_some() {
                        return Err(de::Error::duplicate_field("type"));
                    }
                    kind = Some(map.next_value()?);
                }
                "max_events" => {
                    if max_events.is_some() {
                        return Err(de::Error::duplicate_field("max_events"));
                    }
                    max_events = Some(map.next_value()?);
                }
                "max_size" => {
                    if max_size.is_some() {
                        return Err(de::Error::duplicate_field("max_size"));
                    }
                    max_size = Some(map.next_value()?);
                }
                "when_full" => {
                    if when_full.is_some() {
                        return Err(de::Error::duplicate_field("when_full"));
                    }
                    when_full = Some(map.next_value()?);
                }
                other => {
                    return Err(de::Error::unknown_field(other, &ALL_FIELDS));
                }
            }
        }
        let kind = kind.unwrap_or(BufferTypeKind::Memory);
        let when_full = when_full.unwrap_or_default();
        match kind {
            BufferTypeKind::Memory => {
                if max_size.is_some() {
                    return Err(de::Error::unknown_field(
                        "max_size",
                        &["type", "max_events", "when_full"],
                    ));
                }
                Ok(BufferType::Memory {
                    max_events: max_events.unwrap_or_else(memory_buffer_default_max_events),
                    when_full,
                })
            }
        }
    }
}

impl<'de> de::Visitor<'de> for BufferTypeVisitor {
    type Value = BufferType;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("enum BufferType")
    }

    fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
    where
        A: de::MapAccess<'de>,
    {
        BufferTypeVisitor::visit_map_impl(map)
    }
}

impl<'de> Deserialize<'de> for BufferType {
    fn deserialize<D>(deserializer: D) -> Result<BufferType, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(BufferTypeVisitor)
    }
}

pub const fn memory_buffer_default_max_events() -> NonZeroUsize {
    unsafe { NonZeroUsize::new_unchecked(500) }
}

/// Disk usage configuration for disk-backed buffers.
#[derive(Debug)]
pub struct DiskUsage {
    id: ComponentKey,
    data_dir: PathBuf,
    max_size: NonZeroU64,
}

impl DiskUsage {
    /// Creates a new `DiskUsage` with the given usage configuration.
    pub fn new(id: ComponentKey, data_dir: PathBuf, max_size: NonZeroU64) -> Self {
        Self {
            id,
            data_dir,
            max_size,
        }
    }

    /// Gets the component key for the component this buffer is attached to.
    pub fn id(&self) -> &ComponentKey {
        &self.id
    }

    /// Gets the maximum size, in bytes, that this buffer can consume on disk.
    pub fn max_size(&self) -> u64 {
        self.max_size.get()
    }

    /// Gets the data directory path that this buffer will store its files on disk.
    pub fn data_dir(&self) -> &Path {
        self.data_dir.as_path()
    }
}

/// A specific type of buffer stage.
#[configurable_component(no_deser)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case", tag = "type")]
#[configurable(metadata(docs::enum_tag_description = "The type of buffer to use."))]
pub enum BufferType {
    /// 由tokio提供的内存通道支持的缓冲区。性能更高但持久性较差，如果agent强制重启或崩溃数据会丢失
    #[configurable(title = "Events are buffered in memory.")]
    #[serde(rename = "memory")]
    Memory {
        /// The maximum number of events allowed in the buffer.
        #[serde(default = "memory_buffer_default_max_events")]
        max_events: NonZeroUsize,

        #[configurable(derived)]
        #[serde(default)]
        when_full: WhenFull,
    },
}
/* 指内存,硬盘什么的 */
impl BufferType {
    /// Gets the metadata around disk usage by the buffer, if supported.
    ///
    /// For buffer types that write to disk, `Some(value)` is returned with their usage metadata,
    /// such as maximum size and data directory path.
    ///
    /// Otherwise, `None` is returned.
    pub fn disk_usage(
        &self,
        global_data_dir: Option<PathBuf>,
        _id: &ComponentKey,
    ) -> Option<DiskUsage> {
        // All disk-backed buffers require the global data directory to be specified, and
        // non-disk-backed buffers do not require it to be set... so if it's not set here, we ignore
        // it because either:
        // - it's a non-disk-backed buffer, in which case we can just ignore, or
        // - this method is being called at a point before we actually check that a global data
        //   directory is specified because we have a disk buffer present
        //
        // Since we're not able to emit/surface errors about a lack of a global data directory from
        // where this method is called, we simply return `None` to let it reach the code that _does_
        // emit/surface those errors... and once those errors are fixed, this code can return valid
        // disk usage information, which will then be validated and emit any errors for _that_
        // aspect.
        match global_data_dir {
            None => None,
            Some(_global_data_dir) => match self {
                Self::Memory { .. } => None,
            },
        }
    }

    /// 把自己这个类型的buffer,创建一个,加入topology builder
    pub fn add_to_builder<T>(
        &self,
        builder: &mut TopologyBuilder<T>,
        _data_dir: Option<PathBuf>,
        _id: String,
    ) -> Result<(), BufferBuildError>
    where
        T: Bufferable + Clone + Finalizable,
    {
        match *self {
            BufferType::Memory {
                when_full,
                max_events,
            } => {/* 新建一个mem buffer塞进去 */
                builder.stage(MemoryBuffer::new(max_events), when_full);
            }
        }

        Ok(())
    }
}

/// agent中用于配置缓冲区（buffer）行为的枚举类型，定义数据在sink组件中的缓冲方式
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(untagged)]
#[configurable(
    title = "Configures the buffering behavior for this sink.",
    description = r#"More information about the individual buffer types, and buffer behavior, can be found in the
[Buffering Model][buffering_model] section.

[buffering_model]: /docs/architecture/buffering-model/"#
)]
pub enum BufferConfig {
    /// A single stage buffer topology.
    Single(BufferType),

    /// A chained buffer topology.
    Chained(Vec<BufferType>),
}

/* 默认是基于内存的 */
impl Default for BufferConfig {
    fn default() -> Self {
        Self::Single(BufferType::Memory {
            max_events: memory_buffer_default_max_events(),
            when_full: WhenFull::default(),
        })
    }
}

impl BufferConfig {
    /// 返回内部的buffer type
    pub fn stages(&self) -> &[BufferType] {
        match self {
            Self::Single(stage) => slice::from_ref(stage),/* file sink这里 */
            Self::Chained(stages) => stages.as_slice(),
        }
    }

    /// Builds the buffer components represented by this configuration.
    ///
    /// The caller gets back a `Sink` and `Stream` implementation that represent a way to push items
    /// into the buffer, as well as pop items out of the buffer, respectively.
    ///
    /// # Errors
    ///
    /// If the buffer is configured with anything other than a single stage, an error variant will
    /// be thrown.
    ///
    /// If a disk buffer stage is configured and the data directory provided is `None`, an error
    /// variant will be thrown.
    #[allow(clippy::needless_pass_by_value)]
    pub async fn build<T>(
        &self,
        data_dir: Option<PathBuf>,
        buffer_id: String,
        span: Span,
    ) -> Result<(BufferSender<T>, BufferReceiver<T>), BufferBuildError>
    where
        T: Bufferable + Clone + Finalizable,
    {
        let mut builder = TopologyBuilder::default();
        for stage in self.stages() {
            stage.add_to_builder(&mut builder, data_dir.clone(), buffer_id.clone())?;
        }

        builder
            .build(buffer_id, span)
            .await
            .context(FailedToBuildTopologySnafu)
    }
}