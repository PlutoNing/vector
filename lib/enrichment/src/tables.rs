//! The Enrichment `TableRegistry` manages the collection of `Table`s loaded
//! into Vector. Enrichment Tables go through two stages.
//!
//! ## 1. Writing
//!
//! The tables are loaded. There are two elements that need loading. The first
//! is the actual data. This is loaded at config load time, the actual loading
//! is performed by the implementation of the `EnrichmentTable` trait. Next, the
//! tables are passed through Vectors `Transform` components, particularly the
//! `Remap` transform. These Transforms are able to determine which fields we
//! will want to lookup whilst Vector is running. They can notify the tables of
//! these fields so that the data can be indexed.
//!
//! During this phase, the data is loaded within a single thread, so can be
//! loaded directly into a `HashMap`.
//!
//! ## 2. Reading
//!
//! Once all the data has been loaded we can move to the next stage. This is
//! signified by calling the `finish_load` method. At this point all the data is
//! swapped into the `ArcSwap` of the `tables` field. `ArcSwap` provides
//! lock-free read-only access to the data. From this point on we have fast,
//! efficient read-only access and can no longer add indexes or otherwise mutate
//! the data.
//!
//! This data within the `ArcSwap` is accessed through the `tableSearch`
//! struct. Any transform that needs access to this can call
//! `TableRegistry::as_readonly`. This returns a cheaply clonable struct that
//! implements `vrl:EnrichmentTableSearch` through with the enrichment tables
//! can be searched.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;


use super::{IndexHandle, Table};
use crate::Case;

/// A hashmap of name => implementation of an enrichment table.
type TableMap = HashMap<String, Box<dyn Table + Send + Sync>>;

#[derive(Clone, Default)]
pub struct TableRegistry {
    loading: Arc<Mutex<Option<TableMap>>>,
    tables: Arc<ArcSwap<Option<TableMap>>>,
}

/// Pessimistic Eq implementation for caching purposes
impl PartialEq for TableRegistry {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.tables, &other.tables) && Arc::ptr_eq(&self.loading, &other.loading)
            || self.tables.load().is_none()
                && other.tables.load().is_none()
                && self.loading.lock().expect("lock poison").is_none()
                && other.loading.lock().expect("lock poison").is_none()
    }
}
impl Eq for TableRegistry {}

impl TableRegistry {
    /// Load the given Enrichment Tables into the registry. This can be new tables
    /// loaded from the config, or tables that need to be reloaded because the
    /// underlying data has changed.
    ///
    /// If there are no tables currently loaded into the registry, this is a
    /// simple operation, we simply load the tables into the `loading` field.
    ///
    /// If there are tables that have already been loaded things get a bit more
    /// complicated. This can occur when the config is reloaded. Vector will be
    /// currently running and transforming events, thus the tables loaded into
    /// the `tables` field could be in active use. Since there is no lock
    /// against these tables, we cannot mutate this list. We do need to have a
    /// full list of tables in the `loading` field since there may be some
    /// transforms that will need to add indexes to these tables during the
    /// reload.
    ///
    /// Our only option is to clone the data that is in `tables` and move it
    /// into the `loading` field so it can be mutated. This could be a
    /// potentially expensive operation. For the period whilst the config is
    /// reloading we could potentially have double the enrichment data loaded
    /// into memory.
    ///
    /// Once loading is complete, the data is swapped out of `loading` and we
    /// return to a single copy of the tables.
    ///
    /// # Panics
    ///
    /// Panics if the Mutex is poisoned.
    pub fn load(&self, mut tables: TableMap) {
        let mut loading = self.loading.lock().unwrap();
        let existing = self.tables.load();
        if let Some(existing) = &**existing {
            // We already have some tables
            let extend = existing
                .iter()
                .filter(|(key, _)| !tables.contains_key(*key))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<HashMap<_, _>>();

            tables.extend(extend);
        }
        match *loading {
            None => *loading = Some(tables),
            Some(ref mut loading) => loading.extend(tables),
        }
    }

    /// Swap the data out of the `HashTable` into the `ArcSwap`.
    ///
    /// From this point we can no longer add indexes to the tables, but are now
    /// allowed to read the data.
    ///
    /// # Panics
    ///
    /// Panics if the Mutex is poisoned.
    pub fn finish_load(&self) {
        let mut tables_lock = self.loading.lock().unwrap();
        let tables = tables_lock.take();
        self.tables.swap(Arc::new(tables));
    }

    /// Return a list of the available tables that we can write to.
    ///
    /// This only works in the writing stage and will acquire a lock to retrieve
    /// the tables.
    ///
    /// # Panics
    ///
    /// Panics if the Mutex is poisoned.
    pub fn table_ids(&self) -> Vec<String> {
        let locked = self.loading.lock().unwrap();
        match *locked {
            Some(ref tables) => tables.iter().map(|(key, _)| key.clone()).collect(),
            None => Vec::new(),
        }
    }

    /// Adds an index to the given Enrichment Table.
    ///
    /// If we are in the reading stage, this function will error.
    ///
    /// # Panics
    ///
    /// Panics if the Mutex is poisoned.
    pub fn add_index(
        &mut self,
        table: &str,
        case: Case,
        fields: &[&str],
    ) -> Result<IndexHandle, String> {
        let mut locked = self.loading.lock().unwrap();

        match *locked {
            None => Err("finish_load has been called".to_string()),
            Some(ref mut tables) => match tables.get_mut(table) {
                None => Err(format!("table '{}' not loaded", table)),
                Some(table) => table.add_index(case, fields),
            },
        }
    }



    /// Returns the indexes that have been applied to the given table.
    /// If the table is reloaded we need these to reapply them to the new reloaded tables.
    pub fn index_fields(&self, table: &str) -> Vec<(Case, Vec<String>)> {
        match &**self.tables.load() {
            Some(tables) => tables
                .get(table)
                .map(|table| table.index_fields())
                .unwrap_or_default(),
            None => Vec::new(),
        }
    }

    /// Checks if the table needs reloading.
    /// If in doubt (the table isn't in our list) we return true.
    pub fn needs_reload(&self, table: &str) -> bool {
        match &**self.tables.load() {
            Some(tables) => tables
                .get(table)
                .map(|table| table.needs_reload())
                .unwrap_or(true),
            None => true,
        }
    }
}
