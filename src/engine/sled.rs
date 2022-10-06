use std::path::Path;

use sled::Db;

use super::super::KvsError;
use super::{KvsEngine, Result};

#[derive(Clone)]
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

impl From<sled::Error> for KvsError {
    fn from(sled_err: sled::Error) -> Self {
        KvsError::IOError(sled_err.to_string())
    }
}

impl KvsEngine<String, String> for SledKvsEngine {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self
            .db
            .get(key.as_bytes())
            .map(|op| op.map(|v| String::from_utf8(v.to_vec()).unwrap()))?)
    }
    fn remove(&self, key: String) -> Result<()> {
        match self.db.remove(key.as_bytes())? {
            Some(_v) => {
                self.db.flush()?;
                Ok(())
            }
            None => Err(KvsError::NonExistantKey),
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
