use std::{fs::DirBuilder, path::PathBuf, time::Duration};

use snafu::{ResultExt, Snafu};
use agent_lib::TimeZone;
use agent_config::{configurable_component, impl_generate_config_from_default};


use agent_lib::{ config::LogSchema};
pub fn default_data_dir() -> Option<PathBuf> {
    Some(PathBuf::from("/var/lib/vector/"))
}
#[derive(Debug, Snafu)]
pub(crate) enum DataDirError {
    #[snafu(display("data_dir option required, but not given here or globally"))]
    MissingDataDir,
    #[snafu(display("data_dir {:?} does not exist", data_dir))]
    DoesNotExist { data_dir: PathBuf },
    #[snafu(display("data_dir {:?} is not writable", data_dir))]
    NotWritable { data_dir: PathBuf },
    #[snafu(display(
        "Could not create subdirectory {:?} inside of data dir {:?}: {}",
        subdir,
        data_dir,
        source
    ))]
    CouldNotCreate {
        subdir: PathBuf,
        data_dir: PathBuf,
        source: std::io::Error,
    },
}

/// Specifies the wildcard matching mode, relaxed allows configurations where wildcard doesn not match any existing inputs
#[configurable_component]
#[derive(Clone, Debug, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WildcardMatching {
    /// Strict matching (must match at least one existing input)
    #[default]
    Strict,

    /// Relaxed matching (must match 0 or more inputs)
    Relaxed,
}
#[inline]
pub fn is_default<E: Default + PartialEq>(e: &E) -> bool {
    e == &E::default()
}
/// Global configuration options.
//
// If this is modified, make sure those changes are reflected in the `configBuilder::append`
// function!
#[configurable_component(global_option("global_option"))]
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GlobalOptions {
    /// The directory used for persisting Vector state data.
    ///
    /// This is the directory where Vector will store any state data, such as disk buffers, file
    /// checkpoints, and more.
    ///
    /// Vector must have write permissions to this directory.
    #[serde(default = "default_data_dir")]
    #[configurable(metadata(docs::common = false))]
    pub data_dir: Option<PathBuf>,

    /// Set wildcard matching mode for inputs
    ///
    /// Setting this to "relaxed" allows configurations with wildcards that do not match any inputs
    /// to be accepted without causing an error.
    #[serde(skip_serializing_if = "is_default")]
    #[configurable(metadata(docs::common = false, docs::required = false))]
    pub wildcard_matching: Option<WildcardMatching>,

    /// Default log schema for all events.
    ///
    /// This is used if a component does not have its own specific log schema. All events use a log
    /// schema, whether or not the default is used, to assign event fields on incoming events.
    #[serde(default, skip_serializing_if = "is_default")]
    #[configurable(metadata(docs::common = false, docs::required = false))]
    pub log_schema: LogSchema,

    /// The name of the time zone to apply to timestamp conversions that do not contain an explicit time zone.
    ///
    /// The time zone name may be any name in the [TZ database][tzdb] or `local` to indicate system
    /// local time.
    ///
    /// Note that in Vector/VRL all timestamps are represented in UTC.
    ///
    /// [tzdb]: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    #[serde(default, skip_serializing_if = "is_default")]
    #[configurable(metadata(docs::common = false))]
    pub timezone: Option<TimeZone>,

    /// The amount of time, in seconds, that internal metrics will persist after having not been
    /// updated before they expire and are removed.
    ///
    /// Deprecated: use expire_metrics_secs instead
    #[configurable(deprecated)]
    #[serde(default, skip_serializing_if = "is_default")]
    #[configurable(metadata(docs::hidden))]
    pub expire_metrics: Option<Duration>,

    /// The amount of time, in seconds, that internal metrics will persist after having not been
    /// updated before they expire and are removed.
    ///
    /// Set this to a value larger than your `internal_metrics` scrape interval (default 5 minutes)
    /// so metrics live long enough to be emitted and captured.
    #[serde(skip_serializing_if = "is_default")]
    #[configurable(metadata(docs::common = false, docs::required = false))]
    pub expire_metrics_secs: Option<f64>,
}

impl_generate_config_from_default!(GlobalOptions);

impl GlobalOptions {
    /// Resolve the `data_dir` option in either the global or local config, and
    /// validate that it exists and is writable.
    ///
    /// # Errors
    ///
    /// Function will error if it is unable to make data directory.
    pub fn resolve_and_validate_data_dir(
        &self,
        local_data_dir: Option<&PathBuf>,
    ) -> crate::Result<PathBuf> {
        let data_dir = local_data_dir
            .or(self.data_dir.as_ref())
            .ok_or(DataDirError::MissingDataDir)
            .map_err(Box::new)?
            .clone();
        if !data_dir.exists() {
            return Err(DataDirError::DoesNotExist { data_dir }.into());
        }
        let readonly = std::fs::metadata(&data_dir)
            .map(|meta| meta.permissions().readonly())
            .unwrap_or(true);
        if readonly {
            return Err(DataDirError::NotWritable { data_dir }.into());
        }
        Ok(data_dir)
    }

    /// Resolve the `data_dir` option using `resolve_and_validate_data_dir` and
    /// then ensure a named subdirectory exists.
    ///
    /// # Errors
    ///
    /// Function will error if it is unable to make data subdirectory.
    pub fn resolve_and_make_data_subdir(
        &self,
        local: Option<&PathBuf>,
        subdir: &str,
    ) -> crate::Result<PathBuf> {
        let data_dir = self.resolve_and_validate_data_dir(local)?;

        let mut data_subdir = data_dir.clone();
        data_subdir.push(subdir);

        DirBuilder::new()
            .recursive(true)
            .create(&data_subdir)
            .with_context(|_| CouldNotCreateSnafu { subdir, data_dir })?;
        Ok(data_subdir)
    }

    /// Merge a second global configuration into self, and return the new merged data.
    ///
    /// # Errors
    ///
    /// Returns a list of textual errors if there is a merge conflict between the two global
    /// configs.
    pub fn merge(&self, with: Self) -> Result<Self, Vec<String>> {
        let mut errors = Vec::new();

        if conflicts(
            self.wildcard_matching.as_ref(),
            with.wildcard_matching.as_ref(),
        ) {
            errors.push("conflicting values for 'wildcard_matching' found".to_owned());
        }


        if conflicts(self.timezone.as_ref(), with.timezone.as_ref()) {
            errors.push("conflicting values for 'timezone' found".to_owned());
        }

        if conflicts(self.expire_metrics.as_ref(), with.expire_metrics.as_ref()) {
            errors.push("conflicting values for 'expire_metrics' found".to_owned());
        }

        if conflicts(
            self.expire_metrics_secs.as_ref(),
            with.expire_metrics_secs.as_ref(),
        ) {
            errors.push("conflicting values for 'expire_metrics_secs' found".to_owned());
        }

        let data_dir = if self.data_dir.is_none() || self.data_dir == default_data_dir() {
            with.data_dir
        } else if with.data_dir != default_data_dir() && self.data_dir != with.data_dir {
            // If two configs both set 'data_dir' and have conflicting values
            // we consider this an error.
            errors.push("conflicting values for 'data_dir' found".to_owned());
            None
        } else {
            self.data_dir.clone()
        };

        // If the user has multiple config files, we must *merge* log schemas
        // until we meet a conflict, then we are allowed to error.
        let mut log_schema = self.log_schema.clone();
        if let Err(merge_errors) = log_schema.merge(&with.log_schema) {
            errors.extend(merge_errors);
        }

        if errors.is_empty() {
            Ok(Self {
                data_dir,
                wildcard_matching: self.wildcard_matching.or(with.wildcard_matching),
                log_schema,
                timezone: self.timezone.or(with.timezone),
                expire_metrics: self.expire_metrics.or(with.expire_metrics),
                expire_metrics_secs: self.expire_metrics_secs.or(with.expire_metrics_secs),
            })
        } else {
            Err(errors)
        }
    }

    /// Get the configured time zone, using "local" time if none is set.
    pub fn timezone(&self) -> TimeZone {
        self.timezone.unwrap_or(TimeZone::Local)
    }
}

fn conflicts<T: PartialEq>(this: Option<&T>, that: Option<&T>) -> bool {
    matches!((this, that), (Some(this), Some(that)) if this != that)
}