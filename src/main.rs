pub mod cache;
pub mod cli;
pub mod daemon;
pub mod fs;
pub mod protocol;
pub mod types;

use clap::{Parser, Subcommand};

use crate::cli::Client;
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
    /// Start the daemon and mount the filesystem
    Daemon,

    /// Stop the running daemon
    Stop,

    /// Show daemon status
    Status,

    /// Force sync a repository
    Sync {
        /// Repository in owner/repo format
        repo: String,
    },

    /// Watch a repository (sync more frequently)
    Watch {
        /// Repository in owner/repo format
        repo: String,
    },

    /// Unwatch a repository
    Unwatch {
        /// Repository in owner/repo format
        repo: String,
    },

    /// List known repositories
    List,

    /// Check dependencies
    Doctor,
}

fn main() {
    env_logger::init();

    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Daemon => cmd_daemon(),
        Commands::Stop => cmd_stop(),
        Commands::Status => cmd_status(),
        Commands::Sync { repo } => cmd_sync(&repo),
        Commands::Watch { repo } => cmd_watch(&repo),
        Commands::Unwatch { repo } => cmd_unwatch(&repo),
        Commands::List => cmd_list(),
        Commands::Doctor => cmd_doctor(),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        if e.to_string().contains("not running") {
            eprintln!();
            eprintln!("Hint: Start the daemon with: ghfs daemon");
        }
        std::process::exit(1);
    }
}

fn cmd_daemon() -> Result<(), Box<dyn std::error::Error>> {
    daemon::start()?;
    Ok(())
}

fn cmd_stop() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect()?;
    client.stop()?;
    println!("Daemon stopped");
    Ok(())
}

fn cmd_status() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect()?;
    let status = client.status()?;

    println!("GHFS Daemon Status");
    println!(
        "  Running:     {}",
        if status.running { "yes" } else { "no" }
    );
    println!("  Mount point: {}", status.mount_point);
    println!("  Repos:       {}", status.repo_count);
    println!("  Uptime:      {}", format_duration(status.uptime_secs));

    Ok(())
}

fn cmd_sync(repo: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Validate repo format first
    let _: RepoKey = repo
        .parse()
        .map_err(|e| format!("Invalid repo format: {}", e))?;

    println!("Syncing {}...", repo);

    let mut client = Client::connect()?;
    let result = client.sync(repo)?;

    println!("Synced successfully");
    println!("  Generation: {}", result.generation);
    println!(
        "  Commit:     {}",
        if result.commit.len() > 12 {
            &result.commit[..12]
        } else {
            &result.commit
        }
    );

    Ok(())
}

fn cmd_watch(repo: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _: RepoKey = repo
        .parse()
        .map_err(|e| format!("Invalid repo format: {}", e))?;

    let mut client = Client::connect()?;
    client.watch(repo)?;

    println!("Watching {}", repo);

    Ok(())
}

fn cmd_unwatch(repo: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _: RepoKey = repo
        .parse()
        .map_err(|e| format!("Invalid repo format: {}", e))?;

    let mut client = Client::connect()?;
    client.unwatch(repo)?;

    println!("Unwatched {}", repo);

    Ok(())
}

fn cmd_list() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect()?;
    let result = client.list()?;

    if result.repos.is_empty() {
        println!("No repositories cached");
        return Ok(());
    }

    println!(
        "{:<40} {:>8} {:>12} {:>15}",
        "REPO", "PRIORITY", "GENERATION", "LAST SYNC"
    );
    println!("{}", "-".repeat(80));

    for repo in result.repos {
        let repo_name = format!("{}/{}", repo.owner, repo.repo);
        let priority = if repo.priority > 0 {
            format!("{} (watched)", repo.priority)
        } else {
            "-".to_string()
        };
        let generation = repo
            .generation
            .map(|g| g.to_string())
            .unwrap_or_else(|| "-".to_string());
        let last_sync = repo.last_sync.unwrap_or_else(|| "never".to_string());

        println!(
            "{:<40} {:>8} {:>12} {:>15}",
            repo_name, priority, generation, last_sync
        );
    }

    Ok(())
}

fn cmd_doctor() -> Result<(), Box<dyn std::error::Error>> {
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

    // Check if daemon is running
    let daemon_running = daemon::is_daemon_running();
    println!(
        "[{}] Daemon: {}",
        if daemon_running { "OK" } else { "INFO" },
        if daemon_running {
            "running"
        } else {
            "not running"
        }
    );

    // Check mount point
    let mount_point = std::path::Path::new(daemon::MOUNT_POINT);
    let mount_ok = mount_point.exists();
    println!(
        "[{}] Mount point: {}",
        if mount_ok { "OK" } else { "INFO" },
        daemon::MOUNT_POINT
    );

    if !git_ok || !fuse_ok {
        std::process::exit(1);
    }

    Ok(())
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}
