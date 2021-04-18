use std::{io, path::PathBuf};
use structopt::StructOpt;

mod indexer;

#[derive(Debug, StructOpt)]
#[structopt(name = "tetsu-anidb", about = "Providing anime stuff")]
pub(crate) struct Arguments {
    /// Path to SQLite database
    #[structopt(short)]
    database: Option<PathBuf>,

    /// Action to perform
    #[structopt(subcommand)]
    action: Action,
}

#[derive(Debug, StructOpt)]
enum Action {
    /// Add all media files in a folder to the index
    Index {
        /// Folder to index
        path: PathBuf,
    },

    /// Get information about a file that was previously indexed
    Query {
        /// File to query
        path: PathBuf,
    },
}

#[tokio::main]
async fn main() -> io::Result<()> {
    pretty_env_logger::init();

    let args = Arguments::from_args();
    dbg!(&args);

    let db_path = args.database.unwrap_or_else(|| PathBuf::from(std::env::var_os("HOME").unwrap()).join(".tetsu-index.db"));

    match args.action {
        Action::Index { path } => {
            indexer::index(&path, &db_path).await;
        }
        Action::Query { path: _ } => unimplemented!()
    }

    Ok(())
}
