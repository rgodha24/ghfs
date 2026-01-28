use std::path::PathBuf;

pub mod cache;
pub mod fs;
pub mod types;

use clap::{Parser, Subcommand};
use crate::fs::GhFs;
use crate::types::RepoKey;

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
    /// Force refresh a repository (ignores staleness)
    Refresh {
        /// Repository in owner/repo format
        repo: RepoKey,
    },
}

fn main() {
    env_logger::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Mount { mountpoint } => {
            // Create mountpoint directory if it doesn't exist
            if !mountpoint.exists() {
                if let Err(e) = std::fs::create_dir_all(&mountpoint) {
                    eprintln!("Failed to create mountpoint directory: {}", e);
                    std::process::exit(1);
                }
                println!("Created mountpoint directory: {}", mountpoint.display());
            }

            // Check FUSE availability before mounting
            #[cfg(target_os = "linux")]
            let fuse_available = std::path::Path::new("/dev/fuse").exists();
            #[cfg(target_os = "macos")]
            let fuse_available = std::path::Path::new("/Library/Filesystems/macfuse.fs").exists();
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            let fuse_available = false;

            if !fuse_available {
                eprintln!("Error: FUSE is not available on this system.");
                #[cfg(target_os = "linux")]
                eprintln!(
                    "Please install FUSE: sudo apt install fuse3 (Debian/Ubuntu) or sudo dnf install fuse3 (Fedora)"
                );
                #[cfg(target_os = "macos")]
                eprintln!("Please install macFUSE: https://osxfuse.github.io/");
                std::process::exit(1);
            }

            println!("Mounting ghfs at {}", mountpoint.display());
            let fs = GhFs::with_default_cache();
            if let Err(e) = fs.mount(&mountpoint) {
                eprintln!("Mount failed: {}", e);
                eprintln!(
                    "Hint: Make sure the mountpoint is not already in use and you have necessary permissions."
                );
                std::process::exit(1);
            }
        }
        Commands::Unmount { mountpoint } => {
            println!("Unmounting {}", mountpoint.display());

            // Use fusermount -u on Linux, umount on macOS
            #[cfg(target_os = "linux")]
            let status = std::process::Command::new("fusermount")
                .args(["-u", &mountpoint.to_string_lossy()])
                .status();

            #[cfg(target_os = "macos")]
            let status = std::process::Command::new("umount")
                .arg(&mountpoint)
                .status();

            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            let status: Result<std::process::ExitStatus, std::io::Error> = Err(
                std::io::Error::new(std::io::ErrorKind::Unsupported, "Unsupported platform"),
            );

            match status {
                Ok(s) if s.success() => println!("Unmounted successfully"),
                Ok(s) => {
                    eprintln!("Unmount failed with exit code: {:?}", s.code());
                    std::process::exit(1);
                }
                Err(e) => {
                    eprintln!("Failed to run unmount command: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Commands::Doctor => {
            println!("GHFS System Check\n");

            // Check git
            let git_ok = std::process::Command::new("git")
                .args(["--version"])
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false);
            println!(
                "[{}] git: {}",
                if git_ok { "OK" } else { "FAIL" },
                if git_ok { "available" } else { "not found" }
            );

            // Check FUSE
            #[cfg(target_os = "linux")]
            let fuse_ok = std::path::Path::new("/dev/fuse").exists();
            #[cfg(target_os = "macos")]
            let fuse_ok = std::path::Path::new("/Library/Filesystems/macfuse.fs").exists();
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            let fuse_ok = false;

            println!(
                "[{}] FUSE: {}",
                if fuse_ok { "OK" } else { "FAIL" },
                if fuse_ok {
                    "available"
                } else {
                    "not found (install FUSE)"
                }
            );

            // Check cache directory
            let cache_dir = dirs::cache_dir().map(|p| p.join("ghfs"));
            let cache_ok = cache_dir.as_ref().map(|p| p.exists()).unwrap_or(false);
            println!(
                "[{}] Cache dir: {}",
                if cache_ok { "OK" } else { "INFO" },
                cache_dir
                    .as_ref()
                    .map(|p| p.display().to_string())
                    .unwrap_or("unknown".into())
            );

            if !git_ok || !fuse_ok {
                std::process::exit(1);
            }
        }
        Commands::Prefetch { repo } => {
            println!("Prefetching {}...", repo);

            let paths = crate::cache::CachePaths::default();
            let cache = crate::cache::RepoCache::new(paths);

            match cache.ensure_current(&repo) {
                Ok(generation) => {
                    println!("Cached at: {}", generation.path.display());
                    println!("Commit: {}", generation.commit);
                }
                Err(e) => {
                    eprintln!("Failed to prefetch: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Commands::Refresh { repo } => {
            println!("Refreshing {}...", repo);

            let paths = crate::cache::CachePaths::default();
            let cache = crate::cache::RepoCache::new(paths);

            match cache.force_refresh(&repo) {
                Ok(generation) => {
                    println!("Refreshed at: {}", generation.path.display());
                    println!("Commit: {}", generation.commit);
                    println!("Generation: {}", generation.generation);
                }
                Err(e) => {
                    eprintln!("Failed to refresh: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }
}
