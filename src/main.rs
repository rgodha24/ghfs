pub mod cache;
pub mod cli;
pub mod daemon;
pub mod fs;
pub mod protocol;
pub mod types;

use clap::{Parser, Subcommand};

use crate::cli::{Client, ClientError, run_tui};
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
    Stop {
        /// Kill processes with files open under the mount before stopping
        #[arg(short, long)]
        force: bool,
    },

    /// Show daemon status
    Status,

    /// Open interactive TUI
    Tui,

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
        Commands::Stop { force } => cmd_stop(force),
        Commands::Status => cmd_status(),
        Commands::Tui => cmd_tui(),
        Commands::Sync { repo } => cmd_sync(&repo),
        Commands::Watch { repo } => cmd_watch(&repo),
        Commands::Unwatch { repo } => cmd_unwatch(&repo),
        Commands::List => cmd_list(),
        Commands::Doctor => cmd_doctor(),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        if let Some(ClientError::NotRunning) = e.downcast_ref::<ClientError>() {
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

fn cmd_stop(force: bool) -> Result<(), Box<dyn std::error::Error>> {
    if force {
        // Find processes with files open under the mount and kill them
        let pids = find_open_file_pids(daemon::MOUNT_POINT);
        if !pids.is_empty() {
            println!("Killing {} process(es) with open files...", pids.len());
            for pid in &pids {
                let _ = std::process::Command::new("kill")
                    .args(["-9", &pid.to_string()])
                    .status();
            }
            // Give processes time to die
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    let mut client = Client::connect()?;
    client.stop()?;
    println!("Daemon stopped");
    Ok(())
}

/// Find PIDs of processes with files open under the given path.
fn find_open_file_pids(target_path: &str) -> Vec<u32> {
    use std::fs;

    let mut pids = Vec::new();

    let Ok(proc_dir) = fs::read_dir("/proc") else {
        return pids;
    };

    for entry in proc_dir.flatten() {
        let pid_str = entry.file_name();
        let pid_str = pid_str.to_string_lossy();

        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };

        // Skip our own process
        if pid == std::process::id() {
            continue;
        }

        let fd_dir = format!("/proc/{}/fd", pid);
        let Ok(fds) = fs::read_dir(&fd_dir) else {
            continue;
        };

        for fd_entry in fds.flatten() {
            if let Ok(link_target) = fs::read_link(fd_entry.path()) {
                if link_target.to_string_lossy().starts_with(target_path) {
                    pids.push(pid);
                    break;
                }
            }
        }
    }

    pids
}

fn cmd_status() -> Result<(), Box<dyn std::error::Error>> {
    cli::print_status()
}

fn cmd_tui() -> Result<(), Box<dyn std::error::Error>> {
    match Client::connect() {
        Ok(_) => run_tui(),
        Err(ClientError::NotRunning) => {
            println!("Daemon is not running");
            Ok(())
        }
        Err(err) => Err(Box::new(err)),
    }
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
        "{:<40} {:>8} {:>12} {:>15} {:>15}",
        "REPO", "PRIORITY", "GENERATION", "LAST SYNC", "LAST ACCESS"
    );
    println!("{}", "-".repeat(96));

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
        let last_access = repo.last_access.unwrap_or_else(|| "never".to_string());

        println!(
            "{:<40} {:>8} {:>12} {:>15} {:>15}",
            repo_name, priority, generation, last_sync, last_access
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
