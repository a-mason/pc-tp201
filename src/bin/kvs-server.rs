use clap::clap_derive::ArgEnum;
use clap::Parser;
use kvs::{KvsEngine, KvsError, Result};
use log::*;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    path::Path,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, ArgEnum, PartialEq, Serialize, Deserialize)]
pub enum KvsEngineType {
    Sled,
    Kvs,
}

#[derive(Debug, Parser)] // requires `derive` feature
#[clap(author, version, about, long_about = None)]
struct KvServerArgs {
    #[clap(short, long, value_parser, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
    #[clap(short, long, value_enum)]
    engine: Option<KvsEngineType>,
    // #[clap(short = 'v', long, parse(from_occurrences))]
    // verbose: usize,
}

fn parse_kv_config(db_path: &Path, engine: Option<KvsEngineType>) -> Result<KvsEngineType> {
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
                    return Err(KvsError::WrongEngine);
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

fn main() -> kvs::Result<()> {
    stderrlog::new()
        .module(module_path!())
        .verbosity(4)
        .init()
        .unwrap();
    warn!("version: {}", VERSION);

    let args = KvServerArgs::parse();

    info!("configuration: {:?}", args);

    let path = Path::new("./db");

    let engine = parse_kv_config(path, args.engine)?;

    info!("final engine: {:?}", engine);

    let mut store: Box<dyn KvsEngine<String, String>> = match engine {
        KvsEngineType::Kvs => Box::new(kvs::store::KvStore::open(path)?),
        KvsEngineType::Sled => Box::new(kvs::sled::SledKvsEngine::new(&path.join("sled"))?), // Need to implement Sled Engine
    };

    let listener = TcpListener::bind(args.addr)?;

    for stream in listener.incoming() {
        match stream {
            Ok(mut s) => {
                let deserialized: kvs::protocol::KvRequest<String, String> =
                    serde_json::from_reader(&s)?;
                info!("Got from stream: {:?}", deserialized);
                let result = match deserialized {
                    kvs::protocol::KvRequest::Set(kv) => store.set(kv.0, kv.1).map(|_| None),
                    kvs::protocol::KvRequest::Get(k) => store.get(k),
                    kvs::protocol::KvRequest::Rm(k) => store.remove(k).map(|_| None),
                };
                debug!("Response from store: {:?}", result);
                serde_json::to_writer(&s, &kvs::protocol::KvResponse { value: result })?;
                s.write(b"\n\n")?;
                drop(s);
            }
            Err(e) => {
                warn!("Errored in stream: {}", e);
            }
        }
    }

    Ok(())
}
