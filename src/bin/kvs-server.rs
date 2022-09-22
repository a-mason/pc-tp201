use std::net::{SocketAddr, Ipv4Addr, IpAddr};
use log::*;
use clap::{Parser};
use kvs::KvsEngine;

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


fn main() {
  let args = KvServerArgs::parse();

  stderrlog::new()
  .module(module_path!())
  .verbosity(args.verbose)
  .init()
  .unwrap();
  
  debug!("{:?}", args);

  info!("version: {}", VERSION);
}
