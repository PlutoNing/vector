//! ExtraContext is used for passing extra data to Vector's components when Vector is used as a library.
use std::{
    any::{Any, TypeId},
    collections::HashMap,
    marker::{Send, Sync},
    sync::Arc,
};
/* 这个结构体作为主程序run的参数 */
/// Structure containing any extra data.
/// The data is held in an [`Arc`] so is cheap to clone.
#[derive(Clone, Default)] /* Default 是 Rust 标准库中的一个 trait，用于为类型提供一个默认值 */
pub struct ExtraContext(Arc<HashMap<TypeId, ContextItem>>);

type ContextItem = Box<dyn Any + Send + Sync>;

impl ExtraContext {
    /// Create a new `ExtraContext` that contains the single passed in value.
    pub fn single_value<T: Any + Send + Sync>(value: T) -> Self {
        [Box::new(value) as _].into_iter().collect()
    }

    /// Get an object from the context.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.0
            .get(&TypeId::of::<T>())
            .and_then(|t| t.downcast_ref())
    }

    /// Get an object from the context, if it doesn't exist return the default.
    pub fn get_or_default<T: Clone + Default + 'static>(&self) -> T {
        self.get().cloned().unwrap_or_default()
    }
}

impl FromIterator<ContextItem> for ExtraContext {
    fn from_iter<T: IntoIterator<Item = ContextItem>>(iter: T) -> Self {
        Self(Arc::new(
            iter.into_iter()
                .map(|item| ((*item).type_id(), item))
                .collect(),
        ))
    }
}
