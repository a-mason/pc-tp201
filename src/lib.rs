use std::fmt::Display;
use std::fmt::Debug;
use std::fs;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::{Write, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use std::{collections::HashMap, hash::Hash};
use std::path::{Path};

use serde::{Serialize, Deserialize};
use serde_json::StreamDeserializer;

#[derive(Debug)]
pub enum KvError {
    FileListEmpty,
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

struct ValueData {
    size: usize,
    offset: usize,
    file_path: PathBuf,
}

pub struct KvStore<K, V>
where
    K: Key,
    V: Value,
{
    dir_path: Box<PathBuf>,
    inner_map: HashMap<K, ValueData>,
    files: Vec<PathBuf>,
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
        let mut files = vec![];
        for file in fs::read_dir(dir_path)? {
            files.push(file.unwrap().path());
        }
        if files.is_empty() {
            let path = dir_path.join(format!(
                "{}.kvs",
                SystemTime::now().duration_since(UNIX_EPOCH).expect("time went backwards").as_nanos().to_string())
            );
            fs::File::create(&path)?;
            files.push(path);
        }
        Ok(KvStore {
            dir_path: Box::new(dir_path.to_owned()),
            inner_map: HashMap::new(),
            files,
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
        let file_path = self.files.last().ok_or(KvError::FileListEmpty)?;
        self.inner_map.insert(key, ValueData { offset: self.total_bytes, size: buf.len(), file_path: file_path.clone() });
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(file_path)
            .unwrap();
        file.write_all(buf)?;
        self.total_bytes += buf.len();
        Ok(Some(val))
    }
    pub fn get(&mut self, key: K) -> Result<Option<V>>
    where
        V: Value,
    {
        if let Some(value_data) = self.inner_map.get(&key) {
            let mut file = OpenOptions::new()
                .read(true)
                .open(value_data.file_path.clone() )?;
            file.seek(SeekFrom::Start(value_data.offset as u64))?;
            let mut buf = vec![0u8; value_data.size];
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
            let file_path = self.files.last().ok_or(KvError::FileListEmpty)?;
            self.inner_map.insert(key, ValueData { offset: self.total_bytes, size: buf.len(), file_path: file_path.clone()  });
            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .open(file_path)
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
        for file_path in &store.files {
            let file = OpenOptions::new().read(true).open(file_path.clone())?;
            store.total_bytes = file.metadata()?.len() as usize;
            let mut deserialized: StreamDeserializer<serde_json::de::IoRead<std::fs::File>, KVCommand<K, V>> = serde_json::Deserializer::from_reader(file).into_iter();
            while let Some(deser) = deserialized.next() {
                let size = deserialized.byte_offset() - next_offset;
                let value_data = ValueData {
                    offset: next_offset,
                    size,
                    file_path: file_path.clone(),
                };
                match deser.unwrap() {
                    KVCommand::Set(kv) => {
                        store.inner_map.insert(kv.key, value_data);
                    },
                    KVCommand::Rm(key) => {
                        store.inner_map.insert(key, value_data);
                    },
                }
                next_offset += size;
            }
        }
        Ok(store)
    }

    pub fn compact_files(&mut self, cleanup_inner_map: bool) -> Result<()> {
        for file in self.files.clone().iter() {
            self.compact_file(file.as_path(), cleanup_inner_map)?;
        }
        return Ok(());
    }

    fn compact_file(&mut self, path: &Path, cleanup_inner_map: bool) -> Result<()> {
        let file = OpenOptions::new().read(true).open(path.clone())?;
        let mut set_map = HashMap::new();
        let mut rm_vec = vec![];
        let mut deserialized: StreamDeserializer<serde_json::de::IoRead<std::fs::File>, KVCommand<K, V>> = serde_json::Deserializer::from_reader(file).into_iter();
        while let Some(deser) = deserialized.next() {
            match deser.unwrap() {
                KVCommand::Set(kv) => {
                    set_map.insert(kv.key, kv.value);
                },
                KVCommand::Rm(k)  => {
                    rm_vec.push(k);
                }
            }
        }
        let compacted_path = self.dir_path.join(format!(
            "{}.kvs",
            SystemTime::now().duration_since(UNIX_EPOCH).expect("time went backwards").as_nanos().to_string())
        );
        let compacted_file = fs::File::create(&compacted_path)?;
        let mut next_offset = 0;
        for entry in set_map {
            let size = deserialized.byte_offset() - next_offset;
            let value_data = ValueData {
                offset: next_offset,
                size,
                file_path: compacted_path.clone(),
            };
            serde_json::to_writer(&compacted_file, &KVCommand::Set(KV::new(entry.0.clone(), entry.1)))?;
            if cleanup_inner_map {
                if let Some(v_data) = self.inner_map.get(&entry.0) {
                    if v_data.file_path == path {
                        self.inner_map.insert(entry.0, value_data);
                    }
                }
            }
            next_offset += size;
        }
        if cleanup_inner_map {
            for key in rm_vec {
                if let Some(v_data) = self.inner_map.get(&key) {
                    if v_data.file_path == path {
                        self.inner_map.remove(&key);
                    }
                }
            }
        }
        self.files.retain(|f| f != &path );
        self.files.push(compacted_path);
        Ok(())
    }
}
