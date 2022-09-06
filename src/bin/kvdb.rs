use clap::{Parser, ValueEnum};
// use kvdb::KvStore;

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

fn main() {
    let _: Args = Args::parse();

    // let mut store = KvStore::new();

    panic!("unimplemented");
    // let result = match args.command {
    //     Commands::Get => store.get(args.key),
    //     Commands::Set => {
    //         store.set(args.key, args.value.unwrap());
    //         None
    //     } ,
    //     Commands::Rm => {
    //         store.remove(args.key);
    //         None
    //     },
    // };
}
