use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, KvsError>;

#[derive(Debug, Serialize, Deserialize)]
pub enum KvsError {
    FileListEmpty,
    WrongEngine,
    SerializationError(String),
    IOError(String),
    NonExistantKey,
    ThreadPoolBuildError(String),
    Other,
}

impl From<serde_json::Error> for KvsError {
    fn from(serde_err: serde_json::Error) -> Self {
        KvsError::SerializationError(serde_err.to_string())
    }
}

impl From<std::io::Error> for KvsError {
    fn from(io_err: std::io::Error) -> Self {
        KvsError::IOError(io_err.to_string())
    }
}

impl From<rayon::ThreadPoolBuildError> for KvsError {
    fn from(rayon_err: rayon::ThreadPoolBuildError) -> Self {
        KvsError::IOError(rayon_err.to_string())
    }
}

pub mod protocol {
    use crate::Result;
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
}

pub mod engine;
pub mod thread_pool;
