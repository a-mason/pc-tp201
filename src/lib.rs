use std::fmt::Display;
use std::fmt::Debug;
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::{Write, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::{collections::HashMap, hash::Hash};
use std::path::{Path};

use serde::{Serialize, Deserialize};
use serde_json::StreamDeserializer;

#[derive(Debug)]
pub enum KvError {
    SerializationError(String),
    IOError(String),
    NonExistantKey,
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

pub trait Key: Debug + Display + Clone + Eq + Hash + Serialize + for<'de> Deserialize<'de>{}
pub trait Value: Debug + Display + Clone + Serialize + for<'de> Deserialize<'de> {}

impl Key for String {}
impl Value for String {}

#[derive(Serialize, Deserialize, Debug)]
pub struct KV<K, V> {
    key: K,
    value: V,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum KVCommand<K, V> {
    Set(KV<K, V>),
    Rm(K)
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

pub struct KvStore<K, V> {
    inner_map: HashMap<K, (usize, usize)>,
    path_to_file: Box<PathBuf>,
    total_bytes: usize,
    phantom: PhantomData<V>
}

impl<K, V> KvStore<K, V>
where
    K: Key,
    V: Value,
{
    fn new(dir_path: &Path) -> Result<KvStore<K, V>> {
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)?;
        }
        let db_file_path = dir_path.join("db");

        if !db_file_path.exists() {
            fs::File::create(db_file_path.clone())?;
        }
        Ok(KvStore {
            inner_map: HashMap::new(),
            path_to_file: Box::new(db_file_path),
            total_bytes: 0,
            phantom: PhantomData,
        })
    }
    pub fn set(&mut self, key: K, val: V) -> Result<Option<V>>
    where
        V: Value,
    {
        let kv = KV::new(key.clone(), val.clone());
        let serialized = serde_json::to_string(&KVCommand::Set(kv))?;
        let buf = serialized.as_bytes();
        self.inner_map.insert(key, (self.total_bytes, buf.len()));
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(self.path_to_file.as_ref())
            .unwrap();
        file.write_all(buf)?;
        self.total_bytes += buf.len();
        Ok(Some(val))
    }
    pub fn get(&mut self, key: K) -> Result<Option<V>>
    where
        V: Value,
    {
        if let Some(offset) = self.inner_map.get(&key) {
            let mut file = OpenOptions::new()
                .read(true)
                .open(self.path_to_file.as_ref())?;
            file.seek(SeekFrom::Start(offset.0 as u64))?;
            let mut buf = vec![0u8; offset.1];
            file.read_exact(&mut buf)?;
            match serde_json::from_slice(&buf)? {
                KVCommand::Set(kv) => {
                    let _hidden: K =  kv.key;
                    Ok(Some(kv.value))
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
    pub fn remove(&mut self, key: K) -> Result<Option<V>>
    where
        V: Value,
    {
        if let Ok(Some(val)) = self.get(key.clone()) {
            let serialized = serde_json::to_string(&KVCommand::Rm::<K, V>(key.clone()))?;
            let buf = serialized.as_bytes();
            self.inner_map.insert(key, (self.total_bytes, buf.len()));
            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(self.path_to_file.as_ref())
                .unwrap();
            file.write_all(serialized.as_bytes())?;
            self.total_bytes += buf.len();
            Ok(Some(val))
        } else {
            Err(KvError::NonExistantKey)
        }
    }
    pub fn open(path: &Path) -> Result<KvStore<K, V>>
    where
        V: Value
    {
        let mut store = KvStore::new(path)?;
        let mut next_offset = 0;
        let file = OpenOptions::new().read(true).open(store.path_to_file.as_ref())?;
        store.total_bytes = file.metadata()?.len() as usize;
        let mut deserialized: StreamDeserializer<serde_json::de::IoRead<std::fs::File>, KVCommand<K, V>> = serde_json::Deserializer::from_reader(file).into_iter();
        while let Some(deser) = deserialized.next() {
            let size = deserialized.byte_offset() - next_offset;
            match deser.unwrap() {
                KVCommand::Set(kv) => {
                    store.inner_map.insert(kv.key, (next_offset, size));
                },
                KVCommand::Rm(key) => {
                    store.inner_map.insert(key, (next_offset, size));
                },
            }
            next_offset += size;
        }
        Ok(store)
    }
}
