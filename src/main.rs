pub mod cache;
pub mod cli;
pub mod daemon;
pub mod fs;
pub mod protocol;
pub mod service;
pub mod types;

use clap::{Parser, Subcommand};

use crate::cli::{Client, ClientError, run_tui};
use crate::types::RepoKey;

fn command_in_path(name: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| {
            std::env::split_paths(&paths)
                .map(|dir| dir.join(name))
                .any(|candidate| candidate.is_file())
        })
        .unwrap_or(false)
}

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
    /// Start the daemon in the foreground (used by service managers)
    Daemon,

    /// Manage the background service
    Service {
        #[command(subcommand)]
        action: ServiceAction,
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

    /// Fetch full git history for a repository
    #[command(name = "unshallow")]
    Unshallow {
        /// Repository in owner/repo format
        repo: String,
    },

    /// Convert a repository back to shallow clone (depth=1)
    #[command(name = "reshallow")]
    Reshallow {
        /// Repository in owner/repo format
        repo: String,
    },

    /// List known repositories
    List,

    /// Check dependencies
    Doctor,
}

#[derive(Subcommand)]
enum ServiceAction {
    /// Install and start the daemon as a system service
    Install {
        /// Install but don't start immediately
        #[arg(long)]
        no_start: bool,
    },

    /// Stop and remove the daemon service
    Uninstall,

    /// Start the installed service
    Start,

    /// Stop the installed service
    Stop {
        /// Kill processes with files open under the mount before stopping
        #[arg(short, long)]
        force: bool,
    },

    /// Restart the installed service (picks up new binary after update)
    Restart,

    /// Show service status
    Status,

    /// Tail daemon logs
    Logs,
}

fn main() {
    env_logger::init();

    let cli = Cli::parse();

    let result = match cli.command {
        Commands::Daemon => cmd_daemon(),
        Commands::Service { action } => cmd_service(action),
        Commands::Status => cmd_status(),
        Commands::Tui => cmd_tui(),
        Commands::Sync { repo } => cmd_sync(&repo),
        Commands::Watch { repo } => cmd_watch(&repo),
        Commands::Unwatch { repo } => cmd_unwatch(&repo),
        Commands::Unshallow { repo } => cmd_unshallow(&repo),
        Commands::Reshallow { repo } => cmd_reshallow(&repo),
        Commands::List => cmd_list(),
        Commands::Doctor => cmd_doctor(),
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        if let Some(ClientError::NotRunning) = e.downcast_ref::<ClientError>() {
            eprintln!();
            eprintln!("Hint: Start the daemon with: ghfs service start");
        }
        std::process::exit(1);
    }
}

fn cmd_daemon() -> Result<(), Box<dyn std::error::Error>> {
    daemon::start()?;
    Ok(())
}

fn cmd_service(action: ServiceAction) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ServiceAction::Install { no_start } => service::install(no_start)?,
        ServiceAction::Uninstall => service::uninstall()?,
        ServiceAction::Start => service::start()?,
        ServiceAction::Stop { force } => service::stop(force)?,
        ServiceAction::Restart => service::restart()?,
        ServiceAction::Status => service::status()?,
        ServiceAction::Logs => service::logs()?,
    }

    Ok(())
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

fn cmd_unshallow(repo: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _: RepoKey = repo
        .parse()
        .map_err(|e| format!("Invalid repo format: {}", e))?;

    println!("Fetching full history for {}...", repo);

    let mut client = Client::connect()?;
    let result = client.unshallow(repo)?;

    println!("Unshallowed successfully");
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

fn cmd_reshallow(repo: &str) -> Result<(), Box<dyn std::error::Error>> {
    let _: RepoKey = repo
        .parse()
        .map_err(|e| format!("Invalid repo format: {}", e))?;

    println!("Converting {} to shallow clone...", repo);

    let mut client = Client::connect()?;
    let result = client.reshallow(repo)?;

    println!("Reshallowed successfully");
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

fn cmd_list() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = Client::connect()?;
    let result = client.list()?;

    if result.repos.is_empty() {
        println!("No repositories cached");
        return Ok(());
    }

    println!(
        "{:<40} {:>12} {:>12} {:>15} {:>15}",
        "REPO", "PRIORITY", "GENERATION", "LAST SYNC", "LAST ACCESS"
    );
    println!("{}", "-".repeat(100));

    for repo in result.repos {
        let repo_name = format!("{}/{}", repo.owner, repo.repo);
        let priority = if repo.priority > 0 {
            format!("{} (watched)", repo.priority)
        } else {
            "-".to_string()
        };
        let generation = match (repo.generation, repo.shallow) {
            (Some(_), Some(true)) => "shallow".to_string(),
            (Some(g), Some(false)) => g.to_string(),
            (Some(g), None) => g.to_string(),
            (None, _) => "-".to_string(),
        };
        let last_sync = repo.last_sync.unwrap_or_else(|| "never".to_string());
        let last_access = repo.last_access.unwrap_or_else(|| "never".to_string());

        println!(
            "{:<40} {:>12} {:>12} {:>15} {:>15}",
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

    #[cfg(target_os = "linux")]
    let (backend_ok, backend_label, backend_detail) = {
        let dev_fuse_ok = std::path::Path::new("/dev/fuse").exists();
        let fuse_helper_ok = command_in_path("fusermount3") || command_in_path("fusermount");
        let ok = dev_fuse_ok && fuse_helper_ok;
        let detail = if ok {
            "available (/dev/fuse + fusermount helper)".to_string()
        } else if !dev_fuse_ok && !fuse_helper_ok {
            "missing /dev/fuse and fusermount helper (install/enable FUSE)".to_string()
        } else if !dev_fuse_ok {
            "missing /dev/fuse (install/enable FUSE kernel support)".to_string()
        } else {
            "missing fusermount helper (install FUSE userspace tools, usually fuse3)".to_string()
        };
        (ok, "FUSE backend", detail)
    };

    #[cfg(target_os = "macos")]
    let (backend_ok, backend_label, backend_detail) = {
        let mount_nfs_ok = std::process::Command::new("mount_nfs")
            .arg("-h")
            .output()
            .is_ok();

        let detail = if mount_nfs_ok {
            "mount_nfs available".to_string()
        } else {
            "mount_nfs not found (install/enable macOS NFS client tools)".to_string()
        };

        (mount_nfs_ok, "NFS backend", detail)
    };

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    let (backend_ok, backend_label, backend_detail) =
        (false, "Backend", "unsupported platform".to_string());

    println!(
        "[{}] {}: {}",
        if backend_ok { "OK" } else { "FAIL" },
        backend_label,
        backend_detail
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

    match service::installation_status() {
        Ok(install) if install.installed => {
            println!(
                "[OK] Service: installed ({})",
                install.backend.installed_kind()
            );
        }
        Ok(_) => {
            println!("[INFO] Service: not installed (run 'ghfs service install')");
        }
        Err(service::ServiceError::UnsupportedPlatform)
        | Err(service::ServiceError::BackendUnavailable(_)) => {
            println!("[INFO] Service: unsupported on this platform");
        }
        Err(err) => {
            println!("[INFO] Service: check failed ({})", err);
        }
    }

    // Check mount point
    let mount_point = daemon::mount_point();
    let mount_ok = mount_point.exists();
    println!(
        "[{}] Mount point: {}",
        if mount_ok { "OK" } else { "INFO" },
        mount_point.display()
    );

    if !git_ok || !backend_ok {
        std::process::exit(1);
    }

    Ok(())
}
