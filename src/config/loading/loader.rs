use std::path::Path;

use toml::value::{Table, Value};

use super::{component_name, open_file, Format};
use crate::config::format;

// The loader traits are split into two parts -- an internal `process` mod, that contains
// functionality for processing files/folders, and a `Loader<T>` trait, that provides a public
// interface getting a `T` from a file/folder. The private mod is available to implementors
// within the loading mod, but does not form part of the public interface. This is useful
// because there are numerous internal functions for dealing with (non)recursive loading that
// rely on `&self` but don't need overriding and would be confusingly named in a public API.
pub(super) mod process {
    use std::io::Read;

    use super::*;

    /// This trait contains methods that deserialize files/folders. There are a few methods
    /// in here with subtly different names that can be hidden from public view, hence why
    /// this is nested in a private mod.
    pub trait Process {
        /// Prepares input for serialization. This can be a useful step to Interpolate
        /// environment variables or perform some other pre-processing on the input.
        fn prepare<R: Read>(&mut self, input: R) -> Result<String, Vec<String>>;
        /* 加载配置文件内容 */
        /// Calls into the `prepare` method, and deserializes a `Read` to a `T`.
        fn load<R: std::io::Read, T>(&mut self, input: R, format: Format) -> Result<T, Vec<String>>
        where
            T: serde::de::DeserializeOwned,
        {
            let value = self.prepare(input)?;
            /* value是配置文件内容, 调用对应格式的反序列化器? */
            format::deserialize(&value, format)
        }

        /* 加载配置文件内容, 返回反序列化之后的 */
        /// Loads and deserializes a file into a TOML `Table`.
        fn load_file(
            &mut self,
            path: &Path,    /* 文件路径 */
            format: Format, /* 比如说YAML */
        ) -> Result<Option<(String, Table)>, Vec<String>> {
            if let (Ok(name), Some(file)) = (component_name(path), open_file(path)) {
                self.load(file, format).map(|value| Some((name, value))) /* 返回的是反序列化之后的配置内容 */
            } else {
                Ok(None)
            }
        }

        /// Merge a provided TOML `Table` in an implementation-specific way. Contains an
        /// optional component hint, which may affect how components are merged. Takes a `&mut self`
        /// with the intention of merging an inner value that can be `take`n by a `Loader`.
        fn merge(&mut self, table: Table) -> Result<(), Vec<String>>;
    }
}

/// `Loader` represents the public part of the loading interface. Includes methods for loading
/// from a file or folder, and accessing the final deserialized `T` value via the `take` method.
pub trait Loader<T>: process::Process
where
    T: serde::de::DeserializeOwned,
{
    /// Consumes Self, and returns the final, deserialized `T`.
    fn take(self) -> T;

    /// Deserializes a file with the provided format, and makes the result available via `take`.
    /// Returns a vector of non-fatal warnings on success, or a vector of error strings on failure.
    fn load_from_file(&mut self, path: &Path, format: Format) -> Result<(), Vec<String>> {
        if let Some((_, table)) = self.load_file(path, format)? {
            /* table里面为解析好反序列好的内容 */
            self.merge(table)?;
            Ok(())
        } else {
            Ok(())
        }
    }
}

/// Deserialize a TOML `Table` into a `T`.
pub(super) fn deserialize_table<T: serde::de::DeserializeOwned>(
    table: Table,
) -> Result<T, Vec<String>> {
    Value::Table(table)
        .try_into()
        .map_err(|e| vec![e.to_string()])
}
