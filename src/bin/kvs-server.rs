use std::{net::{SocketAddr, Ipv4Addr, IpAddr, TcpListener}, io::{Write}, path::Path};
use log::*;
use clap::{Parser};
use kvs::{KvsEngine, Result};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Parser)] // requires `derive` feature
#[clap(author, version, about, long_about = None)]
struct KvServerArgs {
  #[clap(short, long, value_parser, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
  addr: SocketAddr,
  #[clap(short, long, value_enum, default_value_t = KvsEngine::Kvs)]
  engine: KvsEngine,

  #[clap(short = 'v', long, parse(from_occurrences))]
  verbose: usize,
}


fn main() -> Result<()> {
  let args = KvServerArgs::parse();

  let mut store = kvs::KvStore::open(Path::new("./"))?;

  stderrlog::new()
  .module(module_path!())
  .verbosity(args.verbose)
  .init()
  .unwrap();

  warn!("version: {}, configuration: {:?}", VERSION, args);

  let listener = TcpListener::bind(args.addr)?;

  for stream in listener.incoming() {
    match stream {
      Ok(mut s) => {
        let deserialized: kvs::KvCommand<String, String> = serde_json::from_reader(&s)?;
        info!("Got from stream: {:?}", deserialized);
        let response = match deserialized {
          kvs::KvCommand::Set(kv) => store.set(kv.0, kv.1),
          kvs::KvCommand::Get(k) => store.get(k),
          kvs::KvCommand::Rm(k) => store.remove(k)
        }.unwrap();
        match response {
          Some(val) => s.write_all(val.as_bytes()),
          None => s.write_all(b"none")
        }?;
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
