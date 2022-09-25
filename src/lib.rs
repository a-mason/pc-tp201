use std::{
    fs::{self, OpenOptions},
    path::Path,
};

use crate::store::Result;
use protocol::{KvError, KvsEngineType};

pub fn parse_kv_config(db_path: &Path, engine: Option<KvsEngineType>) -> Result<KvsEngineType> {
    if !db_path.exists() {
        fs::create_dir_all(&db_path)?;
    }
    let config_file_path = db_path.join("config.info");
    if config_file_path.exists() {
        let previous_config: KvsEngineType = serde_json::from_reader(
            OpenOptions::new()
                .write(false)
                .read(true)
                .open(&config_file_path)
                .unwrap(),
        )?;
        match engine {
            Some(e) => {
                if previous_config != e {
                    return Err(KvError::WrongEngine);
                }
            }
            _ => {}
        };
        Ok(previous_config)
    } else {
        let new_config_file = std::fs::File::create(&config_file_path)?;
        let new_engine = engine.unwrap_or(KvsEngineType::Kvs);
        serde_json::to_writer(new_config_file, &new_engine)?;
        Ok(new_engine)
    }
}

pub trait KvsEngine<K, V> {
    fn set(&mut self, key: K, value: V) -> Result<()>;
    fn get(&mut self, key: K) -> Result<Option<V>>;
    fn remove(&mut self, key: K) -> Result<()>;
}

pub mod protocol {
    use crate::Result;
    use clap::ArgEnum;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug)]
    pub enum KvRequest<K, V> {
        Set((K, V)),
        Rm(K),
        Get(K),
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct KvResponse<V> {
        pub value: Result<Option<V>>,
    }

    #[derive(Debug, ArgEnum, Clone, Serialize, Deserialize, PartialEq)]
    pub enum KvsEngineType {
        Sled,
        Kvs,
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum KvError {
        FileListEmpty,
        WrongEngine,
        SerializationError(String),
        IOError(String),
        NonExistantKey,
        Other,
    }
}

pub mod store {
    use std::fmt::Debug;
    use std::fmt::Display;
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::Read;
    use std::io::{Seek, SeekFrom, Write};
    use std::marker::PhantomData;
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use std::time::UNIX_EPOCH;
    use std::{collections::HashMap, hash::Hash};

    use serde::{Deserialize, Serialize};
    use serde_json::StreamDeserializer;

    use crate::protocol::KvError;
    use crate::KvsEngine;

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

    pub trait Key:
        Debug + Display + Clone + Eq + Hash + Serialize + for<'de> Deserialize<'de>
    {
    }
    pub trait Value: Debug + Display + Clone + Serialize + for<'de> Deserialize<'de> {}

    impl Key for String {}
    impl Value for String {}

    #[derive(Serialize, Deserialize, Debug)]
    enum KvRecord<K, V> {
        Set((K, V)),
        Rm(K),
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
        phantom: PhantomData<V>,
    }

    impl<K, V> KvsEngine<K, V> for KvStore<K, V>
    where
        K: Key,
        V: Value,
    {
        fn set(&mut self, key: K, val: V) -> Result<()> {
            self.write_command(&KvRecord::Set((key.clone(), val.clone())), key)?;
            Ok(())
        }
        fn get(&mut self, key: K) -> Result<Option<V>> {
            if let Some(value_data) = self.inner_map.get(&key) {
                let mut file = OpenOptions::new()
                    .read(true)
                    .open(value_data.file_path.clone())?;
                file.seek(SeekFrom::Start(value_data.offset as u64))?;
                let mut buf = vec![0u8; value_data.size];
                file.read_exact(&mut buf)?;
                match serde_json::from_slice(&buf)? {
                    KvRecord::Set(kv) => {
                        let _key: K = kv.0;
                        Ok(Some(kv.1))
                    }
                    _ => Ok(None),
                }
            } else {
                Ok(None)
            }
        }
        fn remove(&mut self, key: K) -> Result<()> {
            if let Ok(Some(_val)) = self.get(key.clone()) {
                self.write_command(&KvRecord::Rm::<K, V>(key.clone()), key)?;
                Ok(())
            } else {
                Err(KvError::NonExistantKey)
            }
        }
    }

    impl<K, V> KvStore<K, V>
    where
        K: Key,
        V: Value,
    {
        fn alloc_new_file(dir_path: &Path) -> Result<PathBuf> {
            let path = dir_path.join(format!(
                "{}.kvs",
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("time went backwards")
                    .as_nanos()
                    .to_string()
            ));
            fs::File::create(&path)?;
            Ok(path)
        }

        fn alloc_new_file_if_needed(&mut self) -> Result<()> {
            if self.bytes_in_last_file > 100000 {
                if self.files.len() > 10 {
                    self.compact_files()?;
                    return Ok(());
                }
                let new_path = KvStore::<K, V>::alloc_new_file(&self.dir_path)?;
                self.files.push(new_path);
                self.bytes_in_last_file = 0;
            }
            Ok(())
        }

        fn new(db_path: &Path) -> Result<KvStore<K, V>> {
            if !db_path.exists() {
                fs::create_dir_all(&db_path)?;
            }
            let mut files = vec![];
            let mut bytes_in_last_file: usize = 0;
            for file in fs::read_dir(&db_path)? {
                let file = file.unwrap();
                if file
                    .path()
                    .extension()
                    .map(|osstr| (*osstr).to_str().map(|str| str == "kvs").unwrap_or(false))
                    .unwrap_or(false)
                {
                    files.push(file.path());
                    bytes_in_last_file = usize::try_from(file.metadata()?.len()).unwrap();
                }
            }
            if files.is_empty() {
                files.push(KvStore::<K, V>::alloc_new_file(&db_path)?);
            }
            Ok(KvStore {
                dir_path: db_path.into(),
                inner_map: HashMap::new(),
                files,
                bytes_in_last_file,
                phantom: PhantomData,
            })
        }

        fn write_command(&mut self, command: &KvRecord<K, V>, key: K) -> Result<()> {
            let serialized = serde_json::to_vec(command)?;
            let file_path = self.files.last().ok_or(KvError::FileListEmpty)?;
            self.inner_map.insert(
                key,
                ValueData {
                    offset: self.bytes_in_last_file,
                    size: serialized.len(),
                    file_path: file_path.clone(),
                },
            );
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

        pub fn open(db_path: &Path) -> Result<KvStore<K, V>> {
            let mut store = KvStore::new(db_path)?;
            for file_path in &store.files {
                let mut next_offset = 0;
                let file_bytes = fs::read(file_path)?;
                let mut deserialized: StreamDeserializer<_, KvRecord<K, V>> =
                    serde_json::Deserializer::from_slice(&file_bytes).into_iter();
                while let Some(deser) = deserialized.next() {
                    let size = deserialized.byte_offset() - next_offset;
                    let value_data = ValueData {
                        offset: next_offset,
                        size,
                        file_path: file_path.clone(),
                    };
                    match deser.unwrap() {
                        KvRecord::Set(kv) => {
                            store.inner_map.insert(kv.0, value_data);
                        }
                        KvRecord::Rm(key) => {
                            store.inner_map.insert(key, value_data);
                        }
                    }
                    next_offset += size;
                }
            }
            Ok(store)
        }

        fn compact_files(&mut self) -> Result<()> {
            let mut set_map = HashMap::new();
            for file_path in &self.files {
                let file = OpenOptions::new().read(true).open(file_path)?;
                let mut deserialized: StreamDeserializer<
                    serde_json::de::IoRead<std::fs::File>,
                    KvRecord<K, V>,
                > = serde_json::Deserializer::from_reader(file).into_iter();
                while let Some(deser) = deserialized.next() {
                    match deser.unwrap() {
                        KvRecord::Set(kv) => {
                            set_map.insert(kv.0, Some(kv.1));
                        }
                        KvRecord::Rm(k) => {
                            set_map.insert(k, None);
                        }
                    }
                }
            }
            let compacted_path = KvStore::<K, V>::alloc_new_file(&self.dir_path)?;
            let mut compacted_file = fs::File::create(&compacted_path)?;
            let mut next_offset = 0;
            for entry in &set_map {
                match entry.1 {
                    Some(v) => {
                        let serialized = serde_json::to_vec(&KvRecord::Set((entry.0, v)))?;
                        let value_data = ValueData {
                            offset: next_offset,
                            size: serialized.len(),
                            file_path: compacted_path.clone(),
                        };
                        self.inner_map.insert(entry.0.clone(), value_data);
                        compacted_file.write_all(&serialized)?;
                        next_offset += serialized.len();
                    }
                    None => {
                        self.inner_map.remove(entry.0);
                    }
                }
            }
            for file in &self.files {
                fs::remove_file(file)?;
            }
            self.files = vec![compacted_path];
            self.bytes_in_last_file = next_offset;
            Ok(())
        }
    }

    impl<K, V> Drop for KvStore<K, V>
    where
        K: Key,
        V: Value,
    {
        fn drop(&mut self) {
            self.compact_files()
                .expect("Could not compact files on drop");
        }
    }
}

pub mod sled {
    use std::path::Path;

    use crate::{protocol::KvError, KvsEngine, Result};
    use ::sled::Db;

    pub struct SledKvsEngine {
        db: Db,
    }

    impl SledKvsEngine {
        pub fn new(db_dir: &Path) -> Result<SledKvsEngine> {
            Ok(SledKvsEngine {
                db: sled::open(db_dir)?,
            })
        }
    }

    impl From<sled::Error> for KvError {
        fn from(sled_err: sled::Error) -> Self {
            KvError::IOError(sled_err.to_string())
        }
    }

    impl KvsEngine<String, String> for SledKvsEngine {
        fn set(&mut self, key: String, value: String) -> Result<()> {
            self.db.insert(key.as_bytes(), value.as_bytes())?;
            self.db.flush()?;
            Ok(())
        }
        fn get(&mut self, key: String) -> Result<Option<String>> {
            Ok(self
                .db
                .get(key.as_bytes())
                .map(|op| op.map(|v| String::from_utf8(v.to_vec()).unwrap()))?)
        }
        fn remove(&mut self, key: String) -> Result<()> {
            match self.db.remove(key.as_bytes())? {
                Some(_v) => {
                    self.db.flush()?;
                    Ok(())
                }
                None => Err(KvError::NonExistantKey),
            }
        }
    }
    impl Drop for SledKvsEngine {
        fn drop(&mut self) {
            self.db
                .flush()
                .expect("Error flushing database when dropped");
        }
    }
}
