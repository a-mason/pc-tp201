use std::{path, net::{SocketAddr, IpAddr, Ipv4Addr, TcpStream, Shutdown}, io::{Write, Read}};
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

fn make_request(args: KvClientArgs, stream: &mut TcpStream) -> Result<Option<String>> {
    stream.write_all(format!("{:?}", args.method).as_bytes())?;
    stream.shutdown(Shutdown::Write)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    println!("response from server: {}", response);
    Ok(Some(response))
}

fn main() -> Result<()> {
    let args = KvClientArgs::parse();

    let mut stream = TcpStream::connect(args.addr)?;

    match make_request(args, &mut stream) {
        Ok(optional) => {
            match optional {
                Some(val) => {
                    println!("{}", val);
                },
                None => {
                    println!("Key not found!");
                }
            }
        },
        Err(e) => {
            println!("{:?}", e);
        }
    }
    Ok(())
}
