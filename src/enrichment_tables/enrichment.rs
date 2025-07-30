#![deny(warnings)]

use dyn_clone::DynClone;
use vrl::value::{ObjectMap, Value};

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
/// A hashmap of name => implementation of an enrichment table.
type TableMap = HashMap<String, Box<dyn Table + Send + Sync>>;
/// doc
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
/// doc
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct IndexHandle(pub usize);
/// doc
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Condition<'a> {
    /// Condition exactly matches the field value.
    Equals {
        /// doc
        field: &'a str,
        /// doc
        value: Value,
    },
    /// The date in the field is between from and to (inclusive).
    BetweenDates {
        /// doc
        field: &'a str,
        /// doc
        from: chrono::DateTime<chrono::Utc>,
        /// doc
        to: chrono::DateTime<chrono::Utc>,
    },
    /// The date in the field is greater than or equal to `from`.
    FromDate {
        /// doc
        field: &'a str,
        /// doc
        from: chrono::DateTime<chrono::Utc>,
    },
    /// The date in the field is less than or equal to `to`.
    ToDate {
        /// doc
        field: &'a str,
        /// doc
        to: chrono::DateTime<chrono::Utc>,
    },
}
/// doc
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Case {
    /// doc
    Sensitive,
    /// doc
    Insensitive,
}

/// Enrichment tables represent additional data sources that can be used to enrich the event data
/// passing through Vector.
pub trait Table: DynClone {
    /// Search the enrichment table data with the given condition.
    /// All conditions must match (AND).
    ///
    /// # Errors
    /// Errors if no rows, or more than 1 row is found.
    fn find_table_row<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&[String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<ObjectMap, String>;

    /// Search the enrichment table data with the given condition.
    /// All conditions must match (AND).
    /// Can return multiple matched records
    fn find_table_rows<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&[String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<Vec<ObjectMap>, String>;

    /// Hints to the enrichment table what data is going to be searched to allow it to index the
    /// data in advance.
    ///
    /// # Errors
    /// Errors if the fields are not in the table.
    fn add_index(&mut self, case: Case, fields: &[&str]) -> Result<IndexHandle, String>;

    /// Returns a list of the field names that are in each index
    fn index_fields(&self) -> Vec<(Case, Vec<String>)>;

    /// Returns true if the underlying data has changed and the table needs reloading.
    fn needs_reload(&self) -> bool;
}

dyn_clone::clone_trait_object!(Table);
