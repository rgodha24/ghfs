use std::path::PathBuf;

use clap::{Parser, Subcommand};
use ghfs_types::RepoKey;

#[derive(Parser)]
#[command(
    name = "ghfs",
    about = "GitHub Filesystem - mount GitHub repos locally"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Mount the filesystem
    Mount {
        /// The mountpoint directory
        mountpoint: PathBuf,
    },
    /// Unmount the filesystem
    Unmount {
        /// The mountpoint directory to unmount
        mountpoint: PathBuf,
    },
    /// Check dependencies
    Doctor,
    /// Pre-cache a repository
    Prefetch {
        /// Repository in owner/repo format
        repo: RepoKey,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Mount { mountpoint } => {
            println!("Mounting at {}", mountpoint.display());
        }
        Commands::Unmount { mountpoint } => {
            println!("Unmounting {}", mountpoint.display());
        }
        Commands::Doctor => {
            println!("Running diagnostics...");
        }
        Commands::Prefetch { repo } => {
            println!("Prefetching {}", repo);
        }
    }
}
