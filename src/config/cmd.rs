use std::path::PathBuf;

use clap::Parser;



#[derive(Parser, Debug, Clone)]
#[command(rename_all = "kebab-case")]
pub struct Opts {
    /// Pretty print JSON
    #[arg(short, long)]
    pretty: bool,

    /// Include default values where missing from config
    #[arg(short, long)]
    include_defaults: bool,

    #[arg(
        id = "config",
        short,
        long,
        env = "agent_config",
        value_delimiter(',')
    )]
    paths: Vec<PathBuf>,

    /// config files in TOML format.
    #[arg(id = "config-toml", long, value_delimiter(','))]
    paths_toml: Vec<PathBuf>,

    /// config files in JSON format.
    #[arg(id = "config-json", long, value_delimiter(','))]
    paths_json: Vec<PathBuf>,

    /// config files in YAML format.
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