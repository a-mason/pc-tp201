use std::path;

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
    let mut store: KvStore<String, String> = KvStore::open(path::Path::new("./"))?;

    match args.method {
        KvMethod::Set(command) => store.set(command.key, command.value),
        KvMethod::Get(command) => {
            let response = store.get(command.key)?;
            match &response {
                Some(val) => { println!("{}", val); },
                None => { println!("Key not found"); }
            }
            Ok(response)
        }
        KvMethod::Rm(command) => {
            let response = store.remove(command.key);
            if Result::is_err(&response) {
                println!("Key not found");
            }
            response
        }
    }?;
    Ok(())
}
