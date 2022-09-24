use std::{net::{SocketAddr, IpAddr, Ipv4Addr, TcpStream, Shutdown}, io::{Write, Read}};
use clap::{Parser, Subcommand, Args};
use kvs::{Result, KvCommand};

#[derive(Debug, Args)]
struct SetArgs {
    /// key to set the value for
    key: String,

    /// value to set for the key
    value: String,
}

#[derive(Debug, Args)]
struct GetArgs {
    /// key to get the value for
    key: String,
}

#[derive(Debug, Args)]
struct RmArgs {
    /// key to delete the value for
    key: String,
}

#[derive(Debug, Subcommand)]
enum Method {
    Set(SetArgs),
    Get(GetArgs),
    Rm(RmArgs),
}

impl From<Method> for KvCommand<String, String> {
    fn from(m: Method) -> Self {
        match m {
            Method::Set(set_args) => KvCommand::Set((set_args.key, set_args.value)),
            Method::Get(set_args) => KvCommand::Get(set_args.key),
            Method::Rm(set_args) => KvCommand::Rm(set_args.key),
        }
    }
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

fn make_request(command: KvCommand<String, String>, mut stream: TcpStream) -> Result<Option<String>> {
    serde_json::to_writer(&mut stream, &command)?;
    stream.write(b"\n\n")?;
    stream.shutdown(Shutdown::Write)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    println!("response from server: {}", response);
    Ok(Some(response))
}

fn main() -> Result<()> {
    let args = KvClientArgs::parse();

    let stream = TcpStream::connect(args.addr)?;

    let server_command: KvCommand<String, String> = args.method.into();

    match make_request(server_command, stream) {
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
