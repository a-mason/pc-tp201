use std::{path, net::{SocketAddr, IpAddr, Ipv4Addr}};

use clap::{Parser, Subcommand, Args};
use kvs::{Result, KvStore};

#[derive(Debug, Args)]
struct SetCommand {
    /// key to set the value for
    key: String,

    /// value to set for the key
    value: String,
}

#[derive(Debug, Args)]
struct GetCommand {
    /// key to get the value for
    key: String,
}

#[derive(Debug, Args)]
struct RmCommand {
    /// key to delete the value for
    key: String,
}

#[derive(Debug, Subcommand)]
enum Method {
    Set(SetCommand),
    Get(GetCommand),
    Rm(RmCommand),
}

#[derive(Debug, Parser)] // requires `derive` feature
#[clap(author, version, about, long_about = None)]
struct KvClientArgs {
    /// method to call on the database
    #[clap(subcommand)]
    method: Method,

    /// address to connect to the server
    #[clap(short, long, value_parser, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
}

fn main() -> Result<()> {
    let args = KvClientArgs::parse();
    let mut store: KvStore<String, String> = KvStore::open(path::Path::new("./"))?;

    match args.method {
        Method::Set(command) => store.set(command.key, command.value),
        Method::Get(command) => {
            let response = store.get(command.key)?;
            match &response {
                Some(val) => { println!("{}", val); },
                None => { println!("Key not found"); }
            }
            Ok(response)
        }
        Method::Rm(command) => {
            let response = store.remove(command.key);
            if Result::is_err(&response) {
                println!("Key not found");
            }
            response
        }
    }?;
    Ok(())
}
