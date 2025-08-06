mod loader;


use loader::process::Process;
pub use loader::*;
use std::{
    collections::HashMap,
    fmt::Debug,
    fs::File,
    path::{Path, PathBuf},
};
use toml::Table;



use super::{builder::ConfigBuilder, format, Config, ConfigPath, Format, FormatHint};
use std::io::Read;

pub struct ConfigBuilderLoader {
    builder: ConfigBuilder,
}

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
        let prepared_input = prepare_input(input)?; /* prepared_input:替换掉环境变量的配置文件内容 */
        Ok(prepared_input)
    }
    /// Merge a TOML `Table` with a `configBuilder`. Component types extend specific keys.
    fn merge(&mut self, table: Table) -> Result<(), Vec<String>> {
        self.builder.append(deserialize_table(table)?)?;
        Ok(())
    }
}

impl loader::Loader<ConfigBuilder> for ConfigBuilderLoader {
    /// Returns the resulting `configBuilder`.
    fn take(self) -> ConfigBuilder {
        self.builder
    }
}

pub(super) fn component_name<P: AsRef<Path> + Debug>(path: P) -> Result<String, Vec<String>> {
    path.as_ref()
        .file_stem()
        .and_then(|name| name.to_str())
        .map(|name| name.to_string())
        .ok_or_else(|| vec![format!("Couldn't get component name for file: {:?}", path)])
}

pub(super) fn open_file<P: AsRef<Path> + Debug>(path: P) -> Option<File> {
    match File::open(&path) {
        Ok(f) => Some(f),
        Err(error) => {
            if let std::io::ErrorKind::NotFound = error.kind() {
                error!(message = "Config file not found in path.", ?path);
                None
            } else {
                error!(message = "Error opening config file.", %error, ?path);
                None
            }
        }
    }
}

/// Merge the paths coming from different cli flags with different formats into
/// a unified list of paths with formats.
pub fn merge_path_lists(
    path_lists: Vec<(&[PathBuf], FormatHint)>,
) -> impl Iterator<Item = (PathBuf, FormatHint)> + '_ {
    path_lists
        .into_iter()
        .flat_map(|(paths, format)| paths.iter().cloned().map(move |path| (path, format)))
}

/// 简单处理配置文件路径
pub fn process_paths(config_paths: &[ConfigPath]) -> Option<Vec<ConfigPath>> {
    let starting_paths = if !config_paths.is_empty() {
        config_paths.to_owned()
    } else {
        default_config_paths()
    };
    let config_path = starting_paths.first()?;
    let path: &PathBuf = config_path.into();
    if !path.exists() {
        error!(message = "Config file not found.", path = ?path);
        std::process::exit(exitcode::CONFIG);
    }
    let paths = vec![config_path.clone()];

    Some(paths)
}

/// 从配置文件路径加载配置
pub async fn load_from_paths_with_provider_and_secrets(
    config_paths: &[ConfigPath], /* 配置文件列表 */
    signal_handler: &mut crate::app::SignalHandler,
    allow_empty: bool,
) -> Result<Config, Vec<String>> {
    let mut builder = load_builder_from_paths(config_paths)?;
    /* 现在builder包含了配置项 */
    builder.allow_empty = allow_empty;

    signal_handler.clear();

    /* 这里构建一个config */
    let (new_config, build_warnings) = builder.build_with_warnings()?;

    for warning in build_warnings {
        warn!("{}", warning);
    }

    Ok(new_config)
}

/// 处理配置文件
fn loader_from_paths<T, L>(mut loader: L, config_paths: &[ConfigPath]) -> Result<T, Vec<String>>
where
    T: serde::de::DeserializeOwned,
    L: Loader<T> + Process,
{
    let mut errors = Vec::new();

    for config_path in config_paths {
        if let ConfigPath::File(path, format_hint) = config_path {
            let format = format_hint
                .or_else(|| Format::from_path(path).ok())
                .unwrap_or_default();

            if let Err(errs) = loader.load_from_file(path, format) {
                errors.extend(errs);
            }
        }
    }

    if errors.is_empty() {
        Ok(loader.take())
    } else {
        Err(errors)
    }
}
/// Uses `configBuilderLoader` to process `ConfigPaths`, deserializing to a `configBuilder`.
pub fn load_builder_from_paths(config_paths: &[ConfigPath]) -> Result<ConfigBuilder, Vec<String>> {
    loader_from_paths(ConfigBuilderLoader::new(), config_paths)
}

use std::sync::LazyLock;

use regex::{Captures, Regex};

pub static ENVIRONMENT_VARIABLE_INTERPOLATION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?x)
        \$\$|
        \$([[:word:].]+)|
        \$\{([[:word:].]+)(?:(:?-|:?\?)([^}]*))?\}",
    )
    .unwrap()
});
/* str是配置文件内容 ,在给定的字符串中替换环境变量占位符*/
/// Result<interpolated config, errors>
pub fn interpolate(input: &str, vars: &HashMap<String, String>) -> Result<String, Vec<String>> {
    let mut errors = Vec::new();

    let interpolated = ENVIRONMENT_VARIABLE_INTERPOLATION_REGEX
        .replace_all(input, |caps: &Captures<'_>| {
            let flags = caps.get(3).map(|m| m.as_str()).unwrap_or_default();
            let def_or_err = caps.get(4).map(|m| m.as_str()).unwrap_or_default();
            caps.get(1)
                .or_else(|| caps.get(2))
                .map(|m| m.as_str())
                .map(|name| {
                    let val = vars.get(name).map(|v| v.as_str());
                    match flags {
                        ":-" => match val {
                            Some(v) if !v.is_empty() => v,
                            _ => def_or_err,
                        },
                        "-" => val.unwrap_or(def_or_err),
                        ":?" => match val {
                            Some(v) if !v.is_empty() => v,
                            _ => {
                                errors.push(format!(
                                    "Non-empty environment variable required in config. name = {name:?}, error = {def_or_err:?}",
                                ));
                                ""
                            },
                        }
                        "?" => val.unwrap_or_else(|| {
                            errors.push(format!(
                                "Missing environment variable required in config. name = {name:?}, error = {def_or_err:?}",
                            ));
                            ""
                        }),
                        _ => val.unwrap_or_else(|| {
                            errors.push(format!(
                                "Missing environment variable in config. name = {name:?}",
                            ));
                            ""
                        }),
                    }
                })
                .unwrap_or("$")
                .to_string()
        })
        .into_owned();

    if errors.is_empty() {
        Ok(interpolated)
    } else {
        Err(errors)
    }
}

pub fn prepare_input<R: std::io::Read>(mut input: R) -> Result<String, Vec<String>> {
    let mut source_string = String::new();
    input
        .read_to_string(&mut source_string)
        .map_err(|e| vec![e.to_string()])?;
    /* source string就是配置文件内容 */
    let mut vars = std::env::vars().collect::<HashMap<_, _>>();
    if !vars.contains_key("HOSTNAME") {
        if let Ok(hostname) = crate::get_hostname() {
            vars.insert("HOSTNAME".into(), hostname);
        }
    }
    interpolate(&source_string, &vars)
}

pub fn load<R: std::io::Read, T>(input: R, format: Format) -> Result<T, Vec<String>>
where
    T: serde::de::DeserializeOwned,
{
    let with_vars = prepare_input(input)?;

    format::deserialize(&with_vars, format)
}
/* 默认的配置文件路径 */
#[cfg(not(windows))]
fn default_path() -> PathBuf {
    "/etc/scx_agent/config.yaml".into()
}
/* 获取默认的配置文件路径 */
fn default_config_paths() -> Vec<ConfigPath> {
    #[cfg(not(windows))]
    let default_path = default_path();

    vec![ConfigPath::File(default_path, Some(Format::Yaml))]
}
