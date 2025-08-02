//! Expiring Hash Map and related types. See [`ExpiringHashMap`].
#![warn(missing_docs)]

use std::{
    borrow::Borrow,
    collections::HashMap,
    fmt,
    hash::Hash,
    time::{Duration, Instant},
};

use futures::StreamExt;
use tokio_util::time::{delay_queue, DelayQueue};

/// An expired item, holding the value and the key with an expiration information.
pub type ExpiredItem<K, V> = (V, delay_queue::Expired<K>);
/* 是一个带过期时间的哈希映射，结合了 HashMap 和 DelayQueue 来实现键值对的自动过期清理。 */
/// A [`HashMap`] that maintains deadlines for the keys via a [`DelayQueue`].
pub struct ExpiringHashMap<K, V> {
    map: HashMap<K, (V, delay_queue::Key)>,
    expiration_queue: DelayQueue<K>,
}

impl<K, V> Unpin for ExpiringHashMap<K, V> {}

impl<K, V> ExpiringHashMap<K, V>
where
    K: Eq + Hash + Clone,
{
    /// Insert a new key with a TTL.
    pub fn insert(&mut self, key: K, value: V, ttl: Duration) {
        let delay_queue_key = self.expiration_queue.insert(key.clone(), ttl);
        self.map.insert(key, (value, delay_queue_key));
    }

    /// Insert a new value with a deadline.
    pub fn insert_at(&mut self, key: K, value: V, deadline: Instant) {
        let delay_queue_key = self
            .expiration_queue
            .insert_at(key.clone(), deadline.into());
        self.map.insert(key, (value, delay_queue_key));
    }

    /// Get a reference to the value by key.
    pub fn get<Q>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        self.map.get(k).map(|(v, _)| v)
    }

    /// Get a mut reference to the value by key.
    pub fn get_mut<Q>(&mut self, k: &Q) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        self.map.get_mut(k).map(|&mut (ref mut v, _)| v)
    }

    /* 实现了"访问即续期"的语义，文件句柄管理、连接池等场景 */
    /// 重置已存在键的过期时间，并返回该键对应值的可变引用
    pub fn reset_at<Q>(&mut self, k: &Q, when: Instant) -> Option<&mut V>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let (value, delay_queue_key) = self.map.get_mut(k)?;
        self.expiration_queue.reset_at(delay_queue_key, when.into());
        Some(value)
    }

    /// Reset the key if it exists, returning the value and the expiration
    /// information.
    pub fn remove<Q>(&mut self, k: &Q) -> Option<ExpiredItem<K, V>>
    where
        K: Borrow<Q>,
        Q: ?Sized + Hash + Eq,
    {
        let (value, expiration_queue_key) = self.map.remove(k)?;
        let expired = self.expiration_queue.remove(&expiration_queue_key);
        Some((value, expired))
    }

    /// Return an iterator over keys and values of ExpiringHashMap. Useful for
    /// processing all values in ExpiringHashMap irrespective of expiration. This
    /// may be required for processing shutdown or other operations.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&K, &mut V)> {
        self.map.iter_mut().map(|(k, (v, _delayed_key))| (k, v))
    }

    /// Check whether the [`ExpiringHashMap`] is empty.
    /// If it's empty, the `next_expired` function immediately resolves to
    /// [`None`]. Be aware that this may cause a spinlock behaviour if the
    /// `next_expired` is polled in a loop while [`ExpiringHashMap`] is empty.
    /// See [`ExpiringHashMap::next_expired`] for more info.
    pub fn is_empty(&self) -> bool {
        self.expiration_queue.is_empty()
    }

    /// Returns the number of elements in the map.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// doc
    pub async fn next_expired(&mut self) -> Option<ExpiredItem<K, V>> {
        self.expiration_queue.next().await.map(|key| {
            let (value, _) = self.map.remove(key.get_ref()).unwrap();
            (value, key)
        })
    }
}

impl<K, V> Default for ExpiringHashMap<K, V>
where
    K: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            expiration_queue: DelayQueue::new(),
        }
    }
}

impl<K, V> fmt::Debug for ExpiringHashMap<K, V>
where
    K: Eq + Hash + Clone,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpiringHashMap").finish()
    }
}
