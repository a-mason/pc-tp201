use std::{net::{SocketAddr, Ipv4Addr, IpAddr, TcpListener}, io::{Write}, path::Path};
use kvs::protocol::{KvsEngineType};
use kvs::{KvsEngine};
use log::*;
use clap::{Parser};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Parser)] // requires `derive` feature
#[clap(author, version, about, long_about = None)]
struct KvServerArgs {
  #[clap(short, long, value_parser, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
  addr: SocketAddr,
  #[clap(short, long, value_enum)]
  engine: Option<kvs::protocol::KvsEngineType>,

  // #[clap(short = 'v', long, parse(from_occurrences))]
  // verbose: usize,
}

fn main() -> kvs::store::Result<()> {
  stderrlog::new()
  .module(module_path!())
  .verbosity(4)
  .init()
  .unwrap();
  warn!("version: {}", VERSION);

  let args = KvServerArgs::parse();

  info!("configuration: {:?}", args);

  let path = Path::new("./");

  let engine = kvs::parse_kv_config(path, args.engine)?;

  info!("final engine: {:?}", engine);


  let mut store = match engine {
    KvsEngineType::Kvs => kvs::store::KvStore::open(path)?,
    KvsEngineType::Sled => kvs::store::KvStore::open(path)? // Need to implement Sled Engine
  };

  let listener = TcpListener::bind(args.addr)?;

  for stream in listener.incoming() {
    match stream {
      Ok(mut s) => {
        let deserialized: kvs::protocol::KvRequest<String, String> = serde_json::from_reader(&s)?;
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
      },
      Err(e) => {
        warn!("Errored in stream: {}", e);
      }
    }
  }

  Ok(())
}
