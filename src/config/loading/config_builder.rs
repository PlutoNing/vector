use std::{io::Read};

use indexmap::IndexMap;
use toml::value::Table;

use super::{deserialize_table, loader, prepare_input};
use super::{ComponentHint, Process};
use crate::config::{
    ComponentKey, ConfigBuilder, EnrichmentTableOuter, SinkOuter, SourceOuter,
    TransformOuter,
};

pub struct ConfigBuilderLoader {
    builder: ConfigBuilder,
}
/* 一个解析配置文件内容的? */
impl ConfigBuilderLoader {
    pub fn new() -> Self {
        Self {
            builder: ConfigBuilder::default(),
        }
    }
}

impl Process for ConfigBuilderLoader {
    /// 解析配置文件内容, 替换掉环境变量
    fn prepare<R: Read>(&mut self, input: R) -> Result<String, Vec<String>> {
        let prepared_input = prepare_input(input)?;/* prepared_input:替换掉环境变量的配置文件内容 */
        Ok(prepared_input)
    }
    /* table应该是反序列化之后的配置文件内容, hint可能是none */
    /// Merge a TOML `Table` with a `ConfigBuilder`. Component types extend specific keys.
    fn merge(&mut self, table: Table, hint: Option<ComponentHint>) -> Result<(), Vec<String>> {
        match hint {
            /* 这里switch的是config的成员 */
            Some(ComponentHint::Source) => {
                self.builder.sources.extend(deserialize_table::<
                    IndexMap<ComponentKey, SourceOuter>,
                >(table)?);
            }
            Some(ComponentHint::Sink) => {
                self.builder.sinks.extend(
                    deserialize_table::<IndexMap<ComponentKey, SinkOuter<_>>>(table)?,
                );
            }
            Some(ComponentHint::Transform) => {
                self.builder.transforms.extend(deserialize_table::<
                    IndexMap<ComponentKey, TransformOuter<_>>,
                >(table)?);
            }
            Some(ComponentHint::EnrichmentTable) => {
                self.builder.enrichment_tables.extend(deserialize_table::<
                    IndexMap<ComponentKey, EnrichmentTableOuter<_>>,
                >(table)?);
            }
            /* 涉及反序列化, 编解码等走到这里? 20250719155859什么时候走上面 */
            None => { /* 把解析出的配置项吸收到自己config里面 */
                self.builder.append(deserialize_table(table)?)?;
            }
        };

        Ok(())
    }
}

impl loader::Loader<ConfigBuilder> for ConfigBuilderLoader {
    /// Returns the resulting `ConfigBuilder`.
    fn take(self) -> ConfigBuilder {
        self.builder
    }
}
