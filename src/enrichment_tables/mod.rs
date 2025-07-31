//! Functionality to handle enrichment tables.
use enum_dispatch::enum_dispatch;
use vector_lib::configurable::configurable_component;
pub use crate::enrichment_tables::enrichment::{Condition, IndexHandle, Table};

use crate::config::{
    ComponentKey, EnrichmentTableConfig, GenerateConfig, SinkConfig, SourceConfig,
};
use crate::core::global_options::GlobalOptions;
/// doc
pub mod file;
/// doc
pub mod enrichment;

/// Configuration options for an [enrichment table](https://vector.dev/docs/reference/glossary/#enrichment-tables) to be used in a
/// [`remap`](https://vector.dev/docs/reference/configuration/transforms/remap/) transform. Currently supported are:
///
/// * [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) files
/// * [MaxMind](https://www.maxmind.com/en/home) databases
/// * In-memory storage
///
/// For the lookup in the enrichment tables to be as performant as possible, the data is indexed according
/// to the fields that are used in the search. Note that indices can only be created for fields for which an
/// exact match is used in the condition. For range searches, an index isn't used and the enrichment table
/// drops back to a sequential scan of the data. A sequential scan shouldn't impact performance
/// significantly provided that there are only a few possible rows returned by the exact matches in the
/// condition. We don't recommend using a condition that uses only date range searches.
///
///
#[configurable_component(global_option("enrichment_tables"))]
#[derive(Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
#[enum_dispatch(EnrichmentTableConfig)]
#[configurable(metadata(
    docs::enum_tag_description = "enrichment table type",
    docs::common = false,
    docs::required = false,
))]
pub enum EnrichmentTables {
    /// Exposes data from a static file as an enrichment table.
    File(file::FileConfig),
}

impl GenerateConfig for EnrichmentTables {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::File(file::FileConfig {
            file: file::FileSettings {
                path: "path/to/file".into(),
                encoding: file::Encoding::default(),
            },
            schema: Default::default(),
        }))
        .unwrap()
    }
}
