use clap::{arg, Parser, ValueEnum};
// use kvdb::KvStore;
use kvdb::defs::Result;
use kvdb::KvStore;

#[derive(Parser, Debug)]
#[clap(author, version, about = "in memory key value store", long_about = None)]
struct Args {
    /// Command to run
    #[clap(arg_enum, value_parser)]
    command: Commands,
    /// Key to operate on
    #[clap(value_parser)]
    key: String,

    /// Optional value, used when set command is invoked
    #[clap(value_parser)]
    value: Option<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Commands {
    Get,
    Set,
    Rm,
}

fn main() -> Result<()> {
    let args: Args = Args::parse();

    let mut store = KvStore::open(std::env::current_dir().expect("error getting current dir"))?;

    let result: Option<String> = match args.command {
        Commands::Get => {
            if args.value.is_some() {
                panic!("extra arguments");
            }
            store
                .get(args.key)
                .map(|r| r.or(Some(String::from("Key not found"))))
        }
        Commands::Set => {
            store.set(args.key, args.value.unwrap())?;
            Ok(None)
        }
        Commands::Rm => {
            store.remove(args.key)?;
            Ok(None)
        }
    }?;

    if let Some(res) = result {
        println!("{}", res);
    };

    Ok(())
}
