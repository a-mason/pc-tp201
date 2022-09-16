use std::fs::OpenOptions;
use std::io::Write;
use std::{collections::HashMap, hash::Hash};
use std::path::{Path};

use serde::{Serialize, Deserialize};
use serde_json::StreamDeserializer;

#[derive(Debug)]
pub enum KvError {
    SerializationError(String),
    IOError(String),
    Other,
}

impl From<serde_json::Error> for KvError {
    fn from(serde_err: serde_json::Error) -> Self {
        KvError::SerializationError(serde_err.to_string())
    }
}

impl From<std::io::Error> for KvError {
    fn from(io_err: std::io::Error) -> Self {
        KvError::IOError(io_err.to_string())
    }
}

pub trait Key: Eq + Hash + Serialize + for<'de> Deserialize<'de>{}
pub trait Value: Serialize + for<'de> Deserialize<'de> {}

#[derive(Serialize, Deserialize, Debug)]
pub struct KV<K, V> {
    key: K,
    value: V,
}


impl <K,V> KV<K, V>
where
    K: Key,
    V: Value,
{
    pub fn new(key: K, value: V) -> Self {
        return KV {
            key,
            value,
        }
    }
}

pub type Result<T> = std::result::Result<T, KvError>;

pub struct KvStore<'a, K, V> {
    inner_map: HashMap<K, V>,
    path_to_file: &'a Path,
}

impl<'a, K, V> KvStore<'a, K, V>
where
    K: Key,
    V: Value,
{
    pub fn new(file_path: &Path) -> KvStore<'a, K, V> {
        KvStore {
            inner_map: HashMap::new(),
            path_to_file: file_path,
        }
    }
    pub fn set(&mut self, key: K, val: V) -> Result<Option<V>> {
        let kv = KV::new(key, val);
        let serialized = serde_json::to_string(&kv)?;
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.path_to_file)
            .unwrap();
        file.write_all(serialized.as_bytes())?;
        Ok(Some(val))
    }
    pub fn get(&mut self, key: K) -> Result<Option<V>> {
        Ok(self.inner_map.get(&key).map(|v| v.clone()))
    }
    pub fn remove(&mut self, key: K) -> Result<Option<V>> {
        Ok(self.inner_map.remove(&key))
    }
    pub fn open(path: &Path) -> Result<KvStore<'a, K, V>> {
        if !path.exists() {
            Err(KvError::SerializationError(format!("Path does not exist {}", path.to_str().unwrap())))
        } else {
            let file = OpenOptions::new()
                .read(true)
                .open(path)?;
            let deserialized: StreamDeserializer<serde_json::de::IoRead<std::fs::File>, KV<K, V>> = serde_json::Deserializer::from_reader(file).into_iter();
            let store = KvStore::new(path);
            for kv in deserialized {
                let offset = deserialized.byte_offset();
                store.inner_map.insert(kv.unwrap().key, kv.unwrap().value);
            }
            Ok(store)
        }
    }
}
