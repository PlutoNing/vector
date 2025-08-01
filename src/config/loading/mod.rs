mod config_builder;
mod loader;
mod source;

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{File, ReadDir},
    path::{Path, PathBuf},
    sync::Mutex,
};

use config_builder::ConfigBuilderLoader;
use glob::glob;
use loader::process::Process;
pub use loader::*;
pub use source::*;

use super::{
    builder::ConfigBuilder, format, vars, Config, ConfigPath, Format, FormatHint,
};


pub static CONFIG_PATHS: Mutex<Vec<ConfigPath>> = Mutex::new(Vec::new());

pub(super) fn read_dir<P: AsRef<Path> + Debug>(path: P) -> Result<ReadDir, Vec<String>> {
    path.as_ref()
        .read_dir()
        .map_err(|err| vec![format!("Could not read config dir: {:?}, {}.", path, err)])
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

/// Expand a list of paths (potentially containing glob patterns) into real
/// config paths, replacing it with the default paths when empty.
pub fn process_paths(config_paths: &[ConfigPath]) -> Option<Vec<ConfigPath>> {
    let starting_paths = if !config_paths.is_empty() {
        config_paths.to_owned()
    } else {
        default_config_paths() /* 不额外指定的话,走这里 */
    };

    let mut paths = Vec::new();

    for config_path in &starting_paths {
        let config_pattern: &PathBuf = config_path.into(); /* config_pattern可以看见路径了 */

        let matches: Vec<PathBuf> = match glob(config_pattern.to_str().expect("No ability to glob"))
        {
            Ok(glob_paths) => glob_paths.filter_map(Result::ok).collect(),
            Err(err) => {
                error!(message = "Failed to read glob pattern.", path = ?config_pattern, error = ?err);
                return None;
            }
        };

        if matches.is_empty() {
            error!(message = "Config file not found in path.", path = ?config_pattern);
            std::process::exit(exitcode::CONFIG);
        }

        match config_path {
            ConfigPath::File(_, format) => {
                for path in matches {
                    paths.push(ConfigPath::File(path, *format));
                }
            }
            ConfigPath::Dir(_) => {
                for path in matches {
                    paths.push(ConfigPath::Dir(path))
                }
            }
        }
    }

    paths.sort();
    paths.dedup();
    // Ignore poison error and let the current main thread continue running to do the cleanup.
    drop(
        CONFIG_PATHS
            .lock()
            .map(|mut guard| guard.clone_from(&paths)),
    );
/* paths里面就是找到的配置文件路径, 比如/etc/scx_agent/config.yaml */
    Some(paths)
}

pub fn load_from_paths(config_paths: &[ConfigPath]) -> Result<Config, Vec<String>> {
    let builder = load_builder_from_paths(config_paths)?;
    let (config, build_warnings) = builder.build_with_warnings()?;

    for warning in build_warnings {
        warn!("{}", warning);
    }

    Ok(config)
}

/// Loads a configuration from paths. Handle secret replacement and if a provider is present
/// in the builder, the config is used as bootstrapping for a remote source. Otherwise,
/// provider instantiation is skipped.
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
/* loader一个解析内容的loader?  config_paths配置文件路径  */
/// Iterators over `ConfigPaths`, and processes a file/dir according to a provided `Loader`.
fn loader_from_paths<T, L>(mut loader: L, config_paths: &[ConfigPath]) -> Result<T, Vec<String>>
where
    T: serde::de::DeserializeOwned,
    L: Loader<T> + Process,
{
    let mut errors = Vec::new();

    for config_path in config_paths {
        match config_path {
            ConfigPath::File(path, format_hint) => { /* 一般都是文件, 都是这个路径 */
                match loader.load_from_file( /* 取出配置文件内容,解析,反序列化,读取配置项,吸收进来 */
                    path,
                    format_hint
                        .or_else(move || Format::from_path(&path).ok())
                        .unwrap_or_default(),
                ) {/* 这里是解析的结果 */
                    Ok(()) => {}
                    Err(errs) => errors.extend(errs),
                };
            }
            ConfigPath::Dir(path) => {
                match loader.load_from_dir(path) {
                    Ok(()) => {}
                    Err(errs) => errors.extend(errs),
                };
            }
        }
    }

    if errors.is_empty() {
        Ok(loader.take())
    } else {
        Err(errors)
    }
}
/* 应该是加载配置文件内容了, 最终把每个选项分类加载到config里面 */
/// Uses `ConfigBuilderLoader` to process `ConfigPaths`, deserializing to a `ConfigBuilder`.
pub fn load_builder_from_paths(config_paths: &[ConfigPath]) -> Result<ConfigBuilder, Vec<String>> {
    loader_from_paths(ConfigBuilderLoader::new(), config_paths)
}

/// Uses `SourceLoader` to process `ConfigPaths`, deserializing to a toml `SourceMap`.
pub fn load_source_from_paths(
    config_paths: &[ConfigPath],
) -> Result<toml::value::Table, Vec<String>> {
    loader_from_paths(SourceLoader::new(), config_paths)
}


pub fn load_from_str(input: &str, format: Format) -> Result<Config, Vec<String>> {
    let builder = load_from_inputs(std::iter::once((input.as_bytes(), format)))?;
    let (config, build_warnings) = builder.build_with_warnings()?;

    for warning in build_warnings {
        warn!("{}", warning);
    }

    Ok(config)
}

fn load_from_inputs(
    inputs: impl IntoIterator<Item = (impl std::io::Read, Format)>,
) -> Result<ConfigBuilder, Vec<String>> {
    let mut config = Config::builder();
    let mut errors = Vec::new();

    for (input, format) in inputs {
        if let Err(errs) = load(input, format).and_then(|n| config.append(n)) {
            // TODO: add back paths
            errors.extend(errs.iter().map(|e| e.to_string()));
        }
    }

    if errors.is_empty() {
        Ok(config)
    } else {
        Err(errors)
    }
}
/* 20250717155121 ,返回替换掉环境变量的配置文件内容字符串  */
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
    vars::interpolate(&source_string, &vars)
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