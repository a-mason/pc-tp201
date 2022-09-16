use clap::{Parser, Subcommand, Args};
use kvs::Result;

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
enum KvMethod {
    Set(SetCommand),
    Get(GetCommand),
    Rm(RmCommand),
}

#[derive(Debug, Parser)] // requires `derive` feature
#[clap(author, version, about, long_about = None)]
struct KvArgs {
    #[clap(subcommand)]
    method: KvMethod,
}

fn main() -> Result<()> {
    let args = KvArgs::parse();
    println!("{:?}", args.method);
    Ok(())
}
