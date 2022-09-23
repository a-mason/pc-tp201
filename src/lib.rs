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
use serde_cbor::StreamDeserializer;

#[derive(Debug)]
pub enum KvError {
    FileListEmpty,
    SerializationError(String),
    IOError(String),
    NonExistantKey,
    Other,
}

impl From<serde_cbor::Error> for KvError {
    fn from(serde_err: serde_cbor::Error) -> Self {
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
pub enum KVCommand<K, V> {
    Set((K, V)),
    Rm(K)
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
    dir_path: PathBuf,
    inner_map: HashMap<K, ValueData>,
    files: Vec<PathBuf>,
    bytes_in_last_file: usize,
    phantom: PhantomData<V>
}

impl<K, V> KvStore<K, V>
where
    K: Key,
    V: Value,
{
    fn alloc_new_file(dir_path: &Path) -> Result<PathBuf> {
        let path = dir_path.join(format!(
            "{}.kvs",
            SystemTime::now().duration_since(UNIX_EPOCH).expect("time went backwards").as_nanos().to_string())
        );
        fs::File::create(&path)?;
        Ok(path)
    }

    fn alloc_new_file_if_needed(&mut self) -> Result<()> {
        if self.bytes_in_last_file > 100000 {
            println!("New file being allocated");
            if self.files.len() > 10 {
                println!("Compacting files");
                self.compact_files()?;
                return Ok(());
            }
            println!("New file being allocated");
            let new_path = KvStore::<K,V>::alloc_new_file(&self.dir_path)?;
            self.files.push(new_path);
            self.bytes_in_last_file = 0;
        }
        Ok(())
    }

    fn new(dir_path: &Path) -> Result<KvStore<K, V>> {
        if !dir_path.exists() {
            fs::create_dir_all(dir_path)?;
        }
        let mut files = vec![];
        for file in fs::read_dir(dir_path)? {
            files.push(file.unwrap().path());
        }
        if files.is_empty() {
            files.push(KvStore::<K,V>::alloc_new_file(dir_path)?);
        }
        Ok(KvStore {
            dir_path: dir_path.to_owned(),
            inner_map: HashMap::new(),
            files,
            bytes_in_last_file: 0,
            phantom: PhantomData,
        })
    }

    fn write_command(&mut self, command: &KVCommand<K, V>, key: K) -> Result<()> {
        let serialized = serde_cbor::to_vec(command)?;
        let file_path = self.files.last().ok_or(KvError::FileListEmpty)?;
        self.inner_map.insert(key, ValueData { offset: self.bytes_in_last_file, size: serialized.len(), file_path: file_path.clone() });
        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(file_path)
            .unwrap();
        file.write_all(&serialized)?;
        self.bytes_in_last_file += serialized.len();
        self.alloc_new_file_if_needed()?;
        Ok(())
    }

    pub fn set(&mut self, key: K, val: V) -> Result<Option<V>> {
        self.write_command(&KVCommand::Set((key.clone(), val.clone())), key)?;
        Ok(Some(val))
    }
    pub fn get(&mut self, key: K) -> Result<Option<V>> {
        if let Some(value_data) = self.inner_map.get(&key) {
            let mut file = OpenOptions::new()
                .read(true)
                .open(value_data.file_path.clone())?;
            file.seek(SeekFrom::Start(value_data.offset as u64))?;
            let mut buf = vec![0u8; value_data.size];
            file.read_exact(&mut buf)?;
            match serde_cbor::from_slice(&buf)? {
                KVCommand::Set(kv) => {
                    let _key: K = kv.0;
                    Ok(Some(kv.1))
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
    pub fn remove(&mut self, key: K) -> Result<Option<V>> {
        if let Ok(Some(val)) = self.get(key.clone()) {
            self.write_command(&KVCommand::Rm::<K, V>(key.clone()), key)?;
            Ok(Some(val))
        } else {
            Err(KvError::NonExistantKey)
        }
    }
    pub fn open(db_path: &Path) -> Result<KvStore<K, V>> {
        let mut store = KvStore::new(db_path)?;
        for file_path in &store.files {
            let mut next_offset = 0;
            let file_bytes = fs::read(file_path)?;
            store.bytes_in_last_file = file_bytes.len();
            let mut deserialized: StreamDeserializer<_, KVCommand<K, V>> = serde_cbor::Deserializer::from_slice(&file_bytes).into_iter();
            while let Some(deser) = deserialized.next() {
                let size = deserialized.byte_offset() - next_offset;
                let value_data = ValueData {
                    offset: next_offset,
                    size,
                    file_path: file_path.clone(),
                };
                match deser.unwrap() {
                    KVCommand::Set(kv) => {
                        store.inner_map.insert(kv.0, value_data);
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

    pub fn compact_files(&mut self) -> Result<()> {
        let mut set_map = HashMap::new();
        let file_paths_to_delete = self.files.clone();
        for file_path in &self.files {
            let file = OpenOptions::new().read(true).open(file_path)?;
            let mut deserialized: StreamDeserializer<serde_cbor::de::IoRead<std::fs::File>, KVCommand<K, V>> = serde_cbor::Deserializer::from_reader(file).into_iter();
            while let Some(deser) = deserialized.next() {
                match deser.unwrap() {
                    KVCommand::Set(kv) => {
                        set_map.insert(kv.0, Some(kv.1));
                    },
                    KVCommand::Rm(k)  => {
                        set_map.insert(k, None);
                    }
                }
            }
        }
        let compacted_path = KvStore::<K,V>::alloc_new_file(&self.dir_path)?;
        let mut compacted_file = fs::File::create(&compacted_path)?;
        let mut next_offset = 0;
        // println!("Compacting map: {:?} to file: {:?}", set_map, compacted_path);
        for entry in &set_map {
            match entry.1 {
                Some(v) => {
                    let serialized = serde_cbor::to_vec(&KVCommand::Set((entry.0, v)))?;
                    let value_data = ValueData {
                        offset: next_offset,
                        size: serialized.len(),
                        file_path: compacted_path.clone(),
                    };
                    self.inner_map.insert(entry.0.clone(), value_data);
                    compacted_file.write_all(&serialized)?;
                    next_offset += serialized.len();
                },
                None => {
                    self.inner_map.remove(entry.0);
                }
            }
        }
        self.files = vec![compacted_path];
        self.bytes_in_last_file = next_offset;
        for file in &file_paths_to_delete {
            fs::remove_file(file)?;
        }
        Ok(())
    }
}

impl <K, V> Drop for KvStore<K, V>
where
    K: Key,
    V: Value,
{
    fn drop(&mut self) {
        self.compact_files().expect("Could not compact files on drop");
    }
}
