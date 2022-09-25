use clap::{Args, Parser, Subcommand};
use kvs::protocol::{KvError, KvRequest, KvResponse};
use std::{
    io::Write,
    net::{IpAddr, Ipv4Addr, Shutdown, SocketAddr, TcpStream},
};

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

impl From<Method> for KvRequest<String, String> {
    fn from(m: Method) -> Self {
        match m {
            Method::Set(set_args) => KvRequest::Set((set_args.key, set_args.value)),
            Method::Get(set_args) => KvRequest::Get(set_args.key),
            Method::Rm(set_args) => KvRequest::Rm(set_args.key),
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

fn make_request(
    command: &KvRequest<String, String>,
    mut stream: TcpStream,
) -> kvs::store::Result<KvResponse<String>> {
    serde_json::to_writer(&mut stream, command)?;
    stream.write(b"\n\n")?;
    stream.shutdown(Shutdown::Write)?;
    let response: KvResponse<String> = serde_json::from_reader(&stream)?;
    Ok(response)
}

fn main() -> kvs::store::Result<()> {
    let args = KvClientArgs::parse();

    let stream = TcpStream::connect(args.addr)?;

    let server_command: KvRequest<String, String> = args.method.into();

    match make_request(&server_command, stream)?.value {
        Ok(optional_value) => match optional_value {
            Some(val) => {
                println!("{}", val);
                Ok(())
            }
            None => {
                match server_command {
                    KvRequest::Get(_k) => {
                        println!("Key not found!");
                    }
                    _ => {}
                };
                Ok(())
            }
        },
        Err(e) => {
            match e {
                KvError::NonExistantKey => {
                    eprintln!("Key not found!");
                }
                _ => {
                    eprintln!("{:?}", e);
                }
            };
            Err(e)
        }
    }
}
