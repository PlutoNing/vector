use std::path::PathBuf;

use clap::Parser;
use serde_json::Value;

use super::{load_builder_from_paths, load_source_from_paths, process_paths, ConfigBuilder};
use crate::app::handle_config_errors;
use crate::config;

#[derive(Parser, Debug, Clone)]
#[command(rename_all = "kebab-case")]
pub struct Opts {
    /// Pretty print JSON
    #[arg(short, long)]
    pretty: bool,

    /// Include default values where missing from config
    #[arg(short, long)]
    include_defaults: bool,

    /// Read configuration from one or more files. Wildcard paths are supported.
    /// File format is detected from the file name.
    /// If zero files are specified, the deprecated default config path
    /// `/etc/vector/vector.yaml` is targeted.
    #[arg(
        id = "config",
        short,
        long,
        env = "VECTOR_CONFIG",
        value_delimiter(',')
    )]
    paths: Vec<PathBuf>,

    /// Vector config files in TOML format.
    #[arg(id = "config-toml", long, value_delimiter(','))]
    paths_toml: Vec<PathBuf>,

    /// Vector config files in JSON format.
    #[arg(id = "config-json", long, value_delimiter(','))]
    paths_json: Vec<PathBuf>,

    /// Vector config files in YAML format.
    #[arg(id = "config-yaml", long, value_delimiter(','))]
    paths_yaml: Vec<PathBuf>,

    /// Read configuration from files in one or more directories.
    /// File format is detected from the file name.
    ///
    /// Files not ending in .toml, .json, .yaml, or .yml will be ignored.
    #[arg(
        id = "config-dir",
        short = 'C',
        long,
        env = "VECTOR_CONFIG_DIR",
        value_delimiter(',')
    )]
    pub config_dirs: Vec<PathBuf>,
}

impl Opts {
    fn paths_with_formats(&self) -> Vec<config::ConfigPath> {
        config::merge_path_lists(vec![
            (&self.paths, None),
            (&self.paths_toml, Some(config::Format::Toml)),
            (&self.paths_json, Some(config::Format::Json)),
            (&self.paths_yaml, Some(config::Format::Yaml)),
        ])
        .map(|(path, hint)| config::ConfigPath::File(path, hint))
        .chain(
            self.config_dirs
                .iter()
                .map(|dir| config::ConfigPath::Dir(dir.to_path_buf())),
        )
        .collect()
    }
}

/// Helper to merge JSON. Handles objects and array concatenation.
fn merge_json(a: &mut Value, b: Value) {
    match (a, b) {
        (Value::Object(ref mut a), Value::Object(b)) => {
            for (k, v) in b {
                merge_json(a.entry(k).or_insert(Value::Null), v);
            }
        }
        (a, b) => {
            *a = b;
        }
    }
}

/// Helper to sort array values.
fn sort_json_array_values(json: &mut Value) {
    match json {
        Value::Array(ref mut arr) => {
            for v in arr.iter_mut() {
                sort_json_array_values(v);
            }

            // Since `Value` does not have a native ordering, we first convert
            // to string, sort, and then convert back to `Value`.
            //
            // Practically speaking, there should not be config options that mix
            // many JSON types in a single array. This is mainly to sort fields
            // like component inputs.
            let mut a = arr
                .iter()
                .map(|v| serde_json::to_string(v).unwrap())
                .collect::<Vec<_>>();
            a.sort();
            *arr = a
                .iter()
                .map(|v| serde_json::from_str(v.as_str()).unwrap())
                .collect::<Vec<_>>();
        }
        Value::Object(ref mut json) => {
            for (_, v) in json {
                sort_json_array_values(v);
            }
        }
        _ => {}
    }
}

/// Convert a raw user config to a JSON string
fn serialize_to_json(
    source: toml::value::Table,
    source_builder: &ConfigBuilder,
    include_defaults: bool,
    pretty_print: bool,
) -> serde_json::Result<String> {
    // Convert table to JSON
    let mut source_json = serde_json::to_value(source)
        .expect("should serialize config source to JSON. Please report.");

    // If a user has requested default fields, we'll serialize a `ConfigBuilder`. Otherwise,
    // we'll serialize the raw user provided config (without interpolated env vars, to preserve
    // the original source).
    if include_defaults {
        // For security, we don't want environment variables to be interpolated in the final
        // output, but we *do* want defaults. To work around this, we'll serialize `ConfigBuilder`
        // to JSON, and merge in the raw config which will contain the pre-interpolated strings.
        let mut builder = serde_json::to_value(source_builder)
            .expect("should serialize ConfigBuilder to JSON. Please report.");

        merge_json(&mut builder, source_json);

        source_json = builder
    }

    sort_json_array_values(&mut source_json);

    // Get a JSON string. This will either be pretty printed or (default) minified.
    if pretty_print {
        serde_json::to_string_pretty(&source_json)
    } else {
        serde_json::to_string(&source_json)
    }
}

/// Function used by the `vector config` subcommand for outputting a normalized configuration.
/// The purpose of this func is to combine user configuration after processing all paths,
/// Pipelines expansions, etc. The JSON result of this serialization can itself be used as a config,
/// which also makes it useful for version control or treating as a singular unit of configuration.
pub fn cmd(opts: &Opts) -> exitcode::ExitCode {
    let paths = opts.paths_with_formats();
    // Start by serializing to a `ConfigBuilder`. This will leverage validation in config
    // builder fields which we'll use to error out if required.
    let (paths, builder) = match process_paths(&paths) {
        Some(paths) => match load_builder_from_paths(&paths) {
            Ok(builder) => (paths, builder),
            Err(errs) => return handle_config_errors(errs),
        },
        None => return exitcode::CONFIG,
    };

    // Load source TOML.
    let source = match load_source_from_paths(&paths) {
        Ok(map) => map,
        Err(errs) => return handle_config_errors(errs),
    };

    let json = serialize_to_json(source, &builder, opts.include_defaults, opts.pretty);

    #[allow(clippy::print_stdout)]
    {
        println!("{}", json.expect("config should be serializable"));
    }

    exitcode::OK
}