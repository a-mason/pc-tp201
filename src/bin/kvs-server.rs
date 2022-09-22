use std::{net::{SocketAddr, Ipv4Addr, IpAddr, TcpListener}, io::{Read, Write}};
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
        let mut message = String::new();
        let bytes_read = s.read_to_string(&mut message)?;
        info!("Got {} bytes on stream: {}", bytes_read, message);
        match s.write_all(message.as_bytes()) {
          Ok(()) => {
            debug!("response successfully sent");
          },
          Err(e) => {
            warn!("Could not send response: {:?}", e);
          }
        }
        drop(s);
      },
      Err(e) => {
        warn!("Errored in stream: {}", e);
      }
    }
  }

  Ok(())
}
