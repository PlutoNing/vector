use std::sync::{LazyLock, OnceLock};

/// =================== for OptionalTargetPath
use vrl::owned_value_path;
use vrl::path::PathPrefix;

use agent_config::configurable_component;
use vrl::path::PathParseError;
/// =======================for OptionalTargetPath
use vrl::path::{OwnedTargetPath, OwnedValuePath};

static LOG_SCHEMA: OnceLock<LogSchema> = OnceLock::new();
static LOG_SCHEMA_DEFAULT: LazyLock<LogSchema> = LazyLock::new(LogSchema::default);

const MESSAGE: &str = "message";
const TIMESTAMP: &str = "timestamp";
const HOST: &str = "host";
const SOURCE_TYPE: &str = "source_type";
const METADATA: &str = "metadata";

/// Loads Log Schema from configurations and sets global schema. Once this is
/// done, configurations can be correctly loaded using configured log schema
/// defaults.
///
/// # Errors
///
/// This function will fail if the `builder` fails.
///
/// # Panics
///
/// If deny is set, will panic if schema has already been set.
pub fn init_log_schema(log_schema: LogSchema, deny_if_set: bool) {
    assert!(
        !(LOG_SCHEMA.set(log_schema).is_err() && deny_if_set),
        "Couldn't set schema"
    );
}

/// Components should use global `LogSchema` returned by this function.  The
/// returned value can differ from `LogSchema::default()` which is unchanging.
pub fn log_schema() -> &'static LogSchema {
    LOG_SCHEMA.get().unwrap_or(&LOG_SCHEMA_DEFAULT)
}
/// optionalTargetPath impl
#[configurable_component]
#[derive(Debug, Clone, PartialEq, Eq, Default, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "proptest", derive(proptest_derive::Arbitrary))]
#[serde(try_from = "String", into = "String")]
/// An optional path that deserializes an empty string to `None`.
pub struct OptionalTargetPath {
    pub path: Option<OwnedTargetPath>,
}

impl OptionalTargetPath {
    pub fn none() -> Self {
        Self { path: None }
    }

    pub fn event(path: &str) -> Self {
        Self {
            path: Some(OwnedTargetPath {
                prefix: PathPrefix::Event,
                path: owned_value_path!(path),
            }),
        }
    }

    pub fn from(prefix: PathPrefix, path: Option<OwnedValuePath>) -> Self {
        Self {
            path: path.map(|path| OwnedTargetPath { prefix, path }),
        }
    }

    pub fn as_ref(&self) -> Option<&OwnedTargetPath> {
        self.path.as_ref()
    }
}

impl TryFrom<String> for OptionalTargetPath {
    type Error = PathParseError;

    fn try_from(src: String) -> Result<Self, Self::Error> {
        if src.is_empty() {
            Ok(Self { path: None })
        } else {
            OwnedTargetPath::try_from(src).map(|path| Self { path: Some(path) })
        }
    }
}

impl From<OptionalTargetPath> for String {
    fn from(optional_path: OptionalTargetPath) -> Self {
        match optional_path.path {
            Some(path) => String::from(path),
            None => String::new(),
        }
    }
}

impl From<OwnedTargetPath> for OptionalTargetPath {
    fn from(path: OwnedTargetPath) -> Self {
        Self { path: Some(path) }
    }
}

/* 日志模式定义了agent处理日志事件的标准字段映射。它包含五个核心字段：message_key存储原始日志内容，
timestamp_key记录事件时间，host_key标识来源主机，source_type_key标记创建事件的源类型，
metadata_key保存agent生成的元数据。这些字段名称可配置，默认使用标准名称。系统支持全局模式设置和
配置合并，确保所有组件以统一方式处理日志事件。 */
/// Log schema.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
/* #[derive(...)] 是一个派生宏，用于自动为结构体实现一些常用的特征（traits） */
#[serde(default)] /* 是 Serde 库的一个属性，用于序列化和反序列化。它指定在反序列化时，如果某个字段缺失，则使用默认值。 */
pub struct LogSchema {
    /// The name of the event field to treat as the event message.
    ///
    /// This would be the field that holds the raw message, such as a raw log line.
    #[serde(default = "LogSchema::default_message_key")]
    message_key: OptionalTargetPath,

    /// The name of the event field to treat as the event timestamp.
    #[serde(default = "LogSchema::default_timestamp_key")]
    timestamp_key: OptionalTargetPath,

    /// The name of the event field to treat as the host which sent the message.
    ///
    /// This field will generally represent a real host, or container, that generated the message,
    /// but is somewhat source-dependent.
    #[serde(default = "LogSchema::default_host_key")]
    host_key: OptionalTargetPath,

    /* 日志模式定义事件字段的标准位置。source_type_key字段由创建事件的agent源设置，
    用于标识事件来源类型。默认值为"source_type"，可配置为其他路径。系统支持全局模式设置和配置合并，
    确保所有组件统一处理日志事件。 */
    /// The name of the event field to set the source identifier in.
    #[serde(default = "LogSchema::default_source_type_key")]
    source_type_key: OptionalTargetPath,

    /* metadata_key字段由agent设置，用于保存事件特定的元数据，比如remap转换在遇到错误或中止时添加的注释。 */
    /// The name of the event field to set the event metadata in.
    #[serde(default = "LogSchema::default_metadata_key")]
    metadata_key: OptionalTargetPath,
}

impl Default for LogSchema {
    fn default() -> Self {
        LogSchema {
            message_key: Self::default_message_key(),
            timestamp_key: Self::default_timestamp_key(),
            host_key: Self::default_host_key(),
            source_type_key: Self::default_source_type_key(),
            metadata_key: Self::default_metadata_key(),
        }
    }
}

impl LogSchema {
    fn default_message_key() -> OptionalTargetPath {
        OptionalTargetPath::event(MESSAGE)
    }

    fn default_timestamp_key() -> OptionalTargetPath {
        OptionalTargetPath::event(TIMESTAMP)
    }

    fn default_host_key() -> OptionalTargetPath {
        OptionalTargetPath::event(HOST)
    }

    fn default_source_type_key() -> OptionalTargetPath {
        OptionalTargetPath::event(SOURCE_TYPE)
    }

    fn default_metadata_key() -> OptionalTargetPath {
        OptionalTargetPath::event(METADATA)
    }

    pub fn message_key(&self) -> Option<&OwnedValuePath> {
        self.message_key.path.as_ref().map(|key| &key.path)
    }

    /// Returns an `OwnedTargetPath` of the message key.
    /// This parses the path and will panic if it is invalid.
    ///
    /// This should only be used where the result will either be cached,
    /// or performance isn't critical, since this requires memory allocation.
    ///
    /// # Panics
    ///
    /// Panics if the path in `self.message_key` is invalid.
    pub fn owned_message_path(&self) -> OwnedTargetPath {
        self.message_key
            .path
            .as_ref()
            .expect("valid message key")
            .clone()
    }

    pub fn timestamp_key(&self) -> Option<&OwnedValuePath> {
        self.timestamp_key.as_ref().map(|key| &key.path)
    }

    pub fn host_key(&self) -> Option<&OwnedValuePath> {
        self.host_key.as_ref().map(|key| &key.path)
    }

    pub fn source_type_key(&self) -> Option<&OwnedValuePath> {
        self.source_type_key.as_ref().map(|key| &key.path)
    }

    pub fn metadata_key(&self) -> Option<&OwnedValuePath> {
        self.metadata_key.as_ref().map(|key| &key.path)
    }

    pub fn message_key_target_path(&self) -> Option<&OwnedTargetPath> {
        self.message_key.as_ref()
    }

    pub fn timestamp_key_target_path(&self) -> Option<&OwnedTargetPath> {
        self.timestamp_key.as_ref()
    }

    pub fn host_key_target_path(&self) -> Option<&OwnedTargetPath> {
        self.host_key.as_ref()
    }

    pub fn source_type_key_target_path(&self) -> Option<&OwnedTargetPath> {
        self.source_type_key.as_ref()
    }

    pub fn metadata_key_target_path(&self) -> Option<&OwnedTargetPath> {
        self.metadata_key.as_ref()
    }

    pub fn set_message_key(&mut self, path: Option<OwnedTargetPath>) {
        self.message_key = OptionalTargetPath { path };
    }

    pub fn set_timestamp_key(&mut self, path: Option<OwnedTargetPath>) {
        self.timestamp_key = OptionalTargetPath { path };
    }

    pub fn set_host_key(&mut self, path: Option<OwnedTargetPath>) {
        self.host_key = OptionalTargetPath { path };
    }

    pub fn set_source_type_key(&mut self, path: Option<OwnedTargetPath>) {
        self.source_type_key = OptionalTargetPath { path };
    }

    pub fn set_metadata_key(&mut self, path: Option<OwnedTargetPath>) {
        self.metadata_key = OptionalTargetPath { path };
    }

    /// Merge two `LogSchema` instances together.
    ///
    /// # Errors
    ///
    /// This function will fail when the `LogSchema` to be merged contains
    /// conflicting keys.
    pub fn merge(&mut self, other: &LogSchema) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if *other != *LOG_SCHEMA_DEFAULT {
            // If the set value is the default, override it. If it's already overridden, error.
            if self.host_key() != LOG_SCHEMA_DEFAULT.host_key()
                && self.host_key() != other.host_key()
            {
                errors.push("conflicting values for 'log_schema.host_key' found".to_owned());
            } else {
                self.set_host_key(other.host_key_target_path().cloned());
            }
            if self.message_key() != LOG_SCHEMA_DEFAULT.message_key()
                && self.message_key() != other.message_key()
            {
                errors.push("conflicting values for 'log_schema.message_key' found".to_owned());
            } else {
                self.set_message_key(other.message_key_target_path().cloned());
            }
            if self.timestamp_key() != LOG_SCHEMA_DEFAULT.timestamp_key()
                && self.timestamp_key() != other.timestamp_key()
            {
                errors.push("conflicting values for 'log_schema.timestamp_key' found".to_owned());
            } else {
                self.set_timestamp_key(other.timestamp_key_target_path().cloned());
            }
            if self.source_type_key() != LOG_SCHEMA_DEFAULT.source_type_key()
                && self.source_type_key() != other.source_type_key()
            {
                errors.push("conflicting values for 'log_schema.source_type_key' found".to_owned());
            } else {
                self.set_source_type_key(other.source_type_key_target_path().cloned());
            }
            if self.metadata_key() != LOG_SCHEMA_DEFAULT.metadata_key()
                && self.metadata_key() != other.metadata_key()
            {
                errors.push("conflicting values for 'log_schema.metadata_key' found".to_owned());
            } else {
                self.set_metadata_key(other.metadata_key_target_path().cloned());
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}
