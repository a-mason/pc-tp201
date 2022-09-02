use std::{collections::HashMap, hash::Hash};

pub struct KvStore<K, V>
where
    K: Eq + Hash,
    V: ToOwned<Owned = V>,
{
    inner_map: HashMap<K, V>,
}

impl<K, V> Default for KvStore<K, V>
where
    K: Eq + Hash,
    V: ToOwned<Owned = V>,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> KvStore<K, V>
where
    K: Eq + Hash,
    V: ToOwned<Owned = V>,
{
    pub fn new() -> KvStore<K, V> {
        KvStore {
            inner_map: HashMap::new(),
        }
    }
    pub fn set(&mut self, key: K, val: V) -> Option<V> {
        self.inner_map.insert(key, val)
    }
    pub fn get(&mut self, key: K) -> Option<V> {
        self.inner_map.get(&key).map(|v| v.to_owned())
    }
    pub fn remove(&mut self, key: K) -> Option<V> {
        self.inner_map.remove(&key)
    }
}
