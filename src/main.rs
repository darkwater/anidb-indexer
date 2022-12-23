use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

mod indexer;

#[derive(Debug, Parser)]
#[clap(author, version, about)]
pub(crate) struct Args {
    /// Path to SQLite database
    #[clap(short, long)]
    database: Option<PathBuf>,

    /// Action to perform
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, Parser)]
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
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let args = Args::parse();

    let db_path = args.database.unwrap_or_else(|| {
        PathBuf::from(std::env::var_os("HOME").unwrap()).join(".tetsu-index.db")
    });

    match args.action {
        Action::Index { path } => {
            indexer::index(&path, &db_path).await?;
        }
        Action::Query { path: _ } => unimplemented!(),
    }

    Ok(())
}
