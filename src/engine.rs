use crate::Result;

pub trait KvsEngine<K, V>: Clone + Send + 'static {
    fn set(&self, key: K, value: V) -> Result<()>;
    fn get(&self, key: K) -> Result<Option<V>>;
    fn remove(&self, key: K) -> Result<()>;
}

pub mod sled;
pub mod store;
