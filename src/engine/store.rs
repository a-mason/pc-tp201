use std::fmt::Debug;
use std::fmt::Display;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::hash::Hash;
use std::io;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Write;
use std::marker::PhantomData;
use std::os::unix::prelude::FileExt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};

use super::super::KvsError;
use super::KvsEngine;
use super::Result;
pub trait Key:
    Debug + Display + Clone + Eq + Hash + Serialize + for<'de> Deserialize<'de> + Send + 'static
{
}
pub trait Value:
    Debug + Display + Clone + Serialize + for<'de> Deserialize<'de> + Send + 'static
{
}

impl Key for String {}
impl Value for String {}

#[derive(Serialize, Deserialize, Debug)]
enum KvRecord<K, V> {
    Set((K, V)),
    Rm(K),
}

struct ValueData {
    size: usize,
    offset: u64,
}

struct BufWriterWithPosition<T: Write> {
    buf_writer: BufWriter<T>,
    path: PathBuf,
    position: u64,
}

fn get_new_file_path(dir_path: &Path) -> PathBuf {
    dir_path.join(format!(
        "{}.kvs",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos()
            .to_string()
    ))
}

pub struct KvStore<K, V>
where
    K: Key,
    V: Value,
{
    path: Arc<PathBuf>,
    writer: Arc<Mutex<BufWriterWithPosition<File>>>,
    reader: Arc<File>,
    index: Arc<DashMap<K, ValueData>>,
    uncompressed_bytes: AtomicU64,
    phantom: PhantomData<V>,
}

impl <K,V> Clone for KvStore<K, V>
where
    K: Key,
    V: Value,
{
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            writer: self.writer.clone(),
            reader: self.reader.clone(),
            index: self.index.clone(),
            uncompressed_bytes: AtomicU64::new(self.uncompressed_bytes.load(Ordering::SeqCst)),
            phantom: self.phantom.clone()
        }
    }
}

impl<K, V> KvsEngine<K, V> for KvStore<K, V>
where
    K: Key + Sync,
    V: Value,
{
    fn set(&self, key: K, val: V) -> Result<()> {
        let serialized = rmp_serde::to_vec(&KvRecord::Set((key.clone(), val)))?;
        let mut writer = self.writer.lock()?;
        let value_data = ValueData {
            offset: writer.position,
            size: serialized.len(),
        };
        writer.buf_writer.write_all(&serialized)?;
        writer.buf_writer.flush()?;
        writer.position += serialized.len() as u64;
        if let Some(previous_value) = self.index.insert(key, value_data) {
            self.uncompressed_bytes.fetch_add(previous_value.size as u64, Ordering::SeqCst);
        }
        Ok(())
    }
    fn get(&self, key: K) -> Result<Option<V>> {
        if let Some(value_data) = self.index.get(&key) {
            let mut buf = vec![0u8; value_data.size];
            self.reader.read_exact_at(&mut buf, value_data.offset)?;
            match rmp_serde::from_slice(&buf)? {
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
    fn remove(&self, key: K) -> Result<()> {
        let mut writer = self.writer.lock()?;
        if let Some(previous_value) = self.index.remove(&key) {
            let serialized = rmp_serde::to_vec(&KvRecord::<K, V>::Rm(key.clone()))?;
            let value_data = ValueData {
                offset: writer.position,
                size: serialized.len(),
            };
            writer.buf_writer.write_all(&serialized)?;
            writer.buf_writer.flush()?;
            writer.position += serialized.len() as u64;
            self.uncompressed_bytes.fetch_add((previous_value.1.size + value_data.size) as u64, Ordering::SeqCst);
            Ok(())
        } else {
            Err(KvsError::NonExistantKey)
        }
    }
}

impl From<rmp_serde::decode::Error> for KvsError {
    fn from(serde_err: rmp_serde::decode::Error) -> Self {
        KvsError::SerializationError(serde_err.to_string())
    }
}

impl From<rmp_serde::encode::Error> for KvsError {
    fn from(serde_err: rmp_serde::encode::Error) -> Self {
        KvsError::SerializationError(serde_err.to_string())
    }
}

impl<T> From<PoisonError<T>> for KvsError {
    fn from(poison_error: PoisonError<T>) -> Self {
        KvsError::SerializationError(poison_error.to_string())
    }
}

impl<K, V> KvStore<K, V>
where
    K: Key,
    V: Value,
{
    fn compress_dir_files(db_path: &Path) -> Result<PathBuf> {
        if !db_path.exists() {
            fs::create_dir_all(&db_path)?;
        }
        let mut files_in_dir = fs::read_dir(&db_path)?;
        let path = files_in_dir
            .next()
            .map(|f| f.unwrap().path())
            .unwrap_or(get_new_file_path(db_path));
        let mut final_file = fs::OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&path)?;
        for file in files_in_dir {
            let file = file.unwrap();
            if file
                .path()
                .extension()
                .map(|osstr| (*osstr).to_str().map(|str| str == "kvs").unwrap_or(false))
                .unwrap_or(false)
            {
                let mut to_copy = fs::OpenOptions::new().read(true).open(file.path())?;
                io::copy(&mut final_file, &mut to_copy)?;
            }
        }
        Ok(path)
    }

    fn deserialize_file(
        file_path: &PathBuf,
        mut f: impl FnMut(KvRecord<K, V>, ValueData) -> Result<()>,
    ) -> Result<()> {
        let file = fs::read(file_path)?;
        let mut deserializer = rmp_serde::Deserializer::new(Cursor::new(&file));
        let mut position: u64 = 0;
        while position < file.len() as u64 {
            let deserialized: KvRecord<K, V> = serde::Deserialize::deserialize(&mut deserializer)?;
            let new_position = rmp_serde::decode::Deserializer::position(&deserializer);
            let value_data = ValueData {
                offset: position,
                size: (new_position - position) as usize,
            };
            f(deserialized, value_data)?;
            position = new_position;
        }
        Ok(())
    }

    pub fn open(db_path: &Path) -> Result<KvStore<K, V>> {
        let file_path = KvStore::<K, V>::compress_dir_files(db_path)?;
        let index = Arc::new(DashMap::new());
        KvStore::deserialize_file(&file_path, |deserialized: KvRecord<K, V>, value_data| {
            Ok(match deserialized {
                KvRecord::Set(kv) => {
                    index.insert(kv.0, value_data);
                }
                KvRecord::Rm(key) => {
                    index.insert(key, value_data);
                }
            })
        })?;
        let write_buf = OpenOptions::new()
            .write(true)
            .append(true)
            .open(&file_path)?;
        Ok(KvStore {
            path: Arc::new(db_path.to_path_buf()),
            index,
            reader: Arc::new(OpenOptions::new().read(true).open(&file_path)?),
            writer: Arc::new(Mutex::new(BufWriterWithPosition {
                path: file_path,
                position: (write_buf.metadata()?.len()),
                buf_writer: BufWriter::new(write_buf),
            })),
            uncompressed_bytes: AtomicU64::new(0),
            phantom: PhantomData,
        })
    }

    fn compact_file(&self) -> Result<()> {
        // let new_path = get_new_file_path(&self.path);
        // let mut new_file = fs::File::create(&new_path)?;
        // let writer = self.writer.lock()?;
        // let new_map = DashMap::new();
        // KvStore::deserialize_file(&writer.path,
        //     |deserialized: KvRecord<K, V>, value_data| match &deserialized {
        //         KvRecord::Set((key, _value)) => {
        //             if let Some(entry) = self.index.get(&key) {
        //                 if entry.offset == value_data.offset {
        //                     let serialized = rmp_serde::to_vec(&deserialized)?;
        //                     new_file.write_all(&serialized)?;
        //                     let value_data = ValueData {
        //                         offset: new_file.metadata()?.len(),
        //                         size: serialized.len(),
        //                     };
        //                     new_map.insert(entry.key().clone(), value_data);
        //                 }
        //             }
        //             Ok(())
        //         }
        //         KvRecord::Rm(_key) => Ok(())
        //     },
        // )?;
        // writer.buf_writer = BufWriterWithPosition {
        //     path: new_path,
        //     buf_writer: BufWriter::new(new_file),
        //     position: new_file.metadata()?.len()
        // };
        // self.index = new_map;
        // self.uncompressed_bytes = 0;
        // self.reader = OpenOptions::new().read(true).open(&new_path)?;
        Ok(())
    }

        // TODO: reimplement
        // let inner = self.inner.read()?;
        // let mut set_map = HashMap::new();
        // KvStore::deserialize_files(
        //     &[
        //         inner.inactive_files.as_slice(),
        //         vec![inner.active_file.clone()].as_slice(),
        //     ]
        //     .concat(),
        //     |deserialized: KvRecord<K, V>, _| match deserialized {
        //         KvRecord::Set(kv) => {
        //             set_map.insert(kv.0, Some(kv.1));
        //         }
        //         KvRecord::Rm(k) => {
        //             set_map.insert(k, None);
        //         }
        //     },
        // )?;
        // drop(inner);
        // let mut inner = self.inner.write()?;
        // let compacted_path = KvStore::<K, V>::alloc_new_file(&inner.dir_path)?;
        // let mut compacted_file = fs::File::create(&compacted_path)?;
        // let mut next_offset = 0;
        // for entry in &set_map {
        //     match entry.1 {
        //         Some(v) => {
        //             let serialized = rmp_serde::to_vec(&KvRecord::Set((entry.0, v)))?;
        //             let value_data = ValueData {
        //                 offset: next_offset,
        //                 size: serialized.len(),
        //                 file_path: compacted_path.clone(),
        //             };
        //             inner.key_map.insert(entry.0.clone(), value_data);
        //             compacted_file.write_all(&serialized)?;
        //             next_offset += serialized.len() as u64;
        //         }
        //         None => {
        //             inner.key_map.remove(entry.0);
        //         }
        //     }
        // }
        // for file in &inner.inactive_files {
        //     fs::remove_file(file)?;
        // }
        // fs::remove_file(&inner.active_file)?;
        // inner.active_file = compacted_path;
        // inner.inactive_files = vec![];
        // inner.bytes_in_last_file = next_offset;
        // Ok(())
}

// impl<K, V> Drop for KvStore<K, V>
// where
//     K: Key,
//     V: Value,
// {
//     fn drop(&mut self) {
//         self.compact_files()
//             .expect("Could not compact files on drop");
//     }
// }
