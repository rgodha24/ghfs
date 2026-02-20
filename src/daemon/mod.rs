//! Daemon module for background sync operations.

mod backfill;
mod scheduler;
mod socket;
pub mod state;
mod worker;

pub use scheduler::SchedulerHandle;
pub use socket::{SocketServerHandle, socket_path};
pub use state::{RepoState, State};
pub use worker::{WorkerHandle, WorkerRequest};

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use thiserror::Error;

use crate::cache::{CachePaths, ManagedCache};
use crate::fs::GhFs;

/// Default mount point on Linux.
#[cfg(target_os = "linux")]
pub const DEFAULT_MOUNT_POINT: &str = "/mnt/github";

/// Default mount point on macOS.
#[cfg(target_os = "macos")]
pub const DEFAULT_MOUNT_POINT: &str = "/tmp/ghfs";

/// Default mount point for other platforms.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub const DEFAULT_MOUNT_POINT: &str = "/tmp/ghfs";

/// Resolve the mount point, allowing override via GHFS_MOUNT_POINT.
pub fn mount_point() -> PathBuf {
    if let Some(custom) = std::env::var_os("GHFS_MOUNT_POINT") {
        return PathBuf::from(custom);
    }

    PathBuf::from(DEFAULT_MOUNT_POINT)
}

/// Errors that can occur when running the daemon.
#[derive(Error, Debug)]
pub enum DaemonError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("Mount failed: {0}")]
    Mount(std::io::Error),

    #[error("Daemon is already running")]
    AlreadyRunning,
}

/// The GHFS daemon.
pub struct Daemon {
    cache_paths: CachePaths,
    mount_point: PathBuf,
    state: Arc<State>,
    shutdown: Arc<AtomicBool>,
}

/// Spawn a detached thread to unmount the active filesystem backend.
pub(crate) fn spawn_unmount(mount_point: String) {
    std::thread::spawn(move || {
        // Small delay to allow any response to be sent first.
        std::thread::sleep(std::time::Duration::from_millis(100));

        #[cfg(target_os = "linux")]
        let status = std::process::Command::new("fusermount")
            .args(["-u", &mount_point])
            .status();

        #[cfg(target_os = "macos")]
        let status = {
            let first = std::process::Command::new("diskutil")
                .args(["unmount", &mount_point])
                .status();

            match first {
                Ok(exit) if exit.success() => Ok(exit),
                _ => {
                    let second = std::process::Command::new("diskutil")
                        .args(["unmount", "force", &mount_point])
                        .status();

                    match second {
                        Ok(exit) if exit.success() => Ok(exit),
                        _ => std::process::Command::new("umount")
                            .arg(&mount_point)
                            .status(),
                    }
                }
            }
        };

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        let status: Result<std::process::ExitStatus, std::io::Error> = Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Unsupported platform",
        ));

        match status {
            Ok(s) if s.success() => {}
            Ok(s) => log::warn!("Unmount command failed with exit code: {:?}", s.code()),
            Err(e) => log::warn!("Failed to run unmount command: {}", e),
        }
    });
}

#[cfg(target_os = "linux")]
fn try_unmount_linux(mount_point: &str) -> bool {
    if std::process::Command::new("fusermount3")
        .args(["-u", mount_point])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
    {
        return true;
    }

    if std::process::Command::new("fusermount")
        .args(["-u", mount_point])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
    {
        return true;
    }

    std::process::Command::new("umount")
        .args(["-l", mount_point])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn ensure_mount_point_ready(mount_point: &std::path::Path) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        if let Err(err) = std::fs::read_dir(mount_point) {
            if err.raw_os_error() == Some(libc::ENOTCONN) {
                log::warn!(
                    "Mount point {} appears disconnected; attempting cleanup",
                    mount_point.display()
                );

                let mount_point_str = mount_point.to_string_lossy();
                let cleaned = try_unmount_linux(&mount_point_str);
                if !cleaned {
                    log::warn!(
                        "Failed to unmount disconnected mount at {}",
                        mount_point.display()
                    );
                }
            }
        }
    }

    match std::fs::create_dir_all(mount_point) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err),
    }
}

impl Daemon {
    /// Create a new daemon instance.
    pub fn new() -> Result<Self, DaemonError> {
        let cache_paths = CachePaths::default();
        let mount_point = mount_point();

        // Ensure cache directories exist
        std::fs::create_dir_all(cache_paths.mirrors_dir())?;
        std::fs::create_dir_all(cache_paths.worktrees_dir())?;
        std::fs::create_dir_all(cache_paths.locks_dir())?;

        // Open state database
        let db_path = cache_paths.root().join("ghfs.db");
        let state = State::open(&db_path)?;
        state.init()?;
        backfill::backfill_cache_state(&state, &cache_paths);

        Ok(Self {
            cache_paths,
            mount_point,
            state: Arc::new(state),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Run the daemon (blocks until shutdown).
    pub fn run(self) -> Result<(), DaemonError> {
        log::info!("Starting ghfs daemon");
        log::info!("Mount point: {}", self.mount_point.display());
        log::info!("Cache: {}", self.cache_paths.root().display());
        log::info!("Socket: {}", socket_path().display());

        // Ensure mount point exists and recover from disconnected stale mounts.
        ensure_mount_point_ready(&self.mount_point)?;

        // Create managed cache for the worker
        let managed_cache = ManagedCache::new(self.cache_paths.clone(), Arc::clone(&self.state));

        // Spawn worker thread
        let worker = Arc::new(WorkerHandle::spawn(managed_cache));
        log::info!("Worker thread started");

        // Spawn socket server
        let _socket_server = SocketServerHandle::spawn(
            Arc::clone(&self.state),
            Arc::clone(&worker),
            self.cache_paths.clone(),
            self.mount_point.to_string_lossy().to_string(),
            Arc::clone(&self.shutdown),
        )?;
        log::info!("Socket server started");

        // Spawn scheduler
        let _scheduler = SchedulerHandle::spawn(
            Arc::clone(&self.state),
            worker.sender(),
            Arc::clone(&self.shutdown),
        );
        log::info!("Scheduler started");

        // Setup signal handler for graceful shutdown
        let mount_point = self.mount_point.to_string_lossy().to_string();
        let shutdown = Arc::clone(&self.shutdown);
        ctrlc::set_handler(move || {
            log::info!("Received shutdown signal");
            shutdown.store(true, Ordering::SeqCst);
            spawn_unmount(mount_point.clone());
        })
        .expect("failed to set signal handler");

        // Create and mount filesystem backend.
        let fs = GhFs::new(Arc::clone(&worker), self.cache_paths.clone());

        #[cfg(target_os = "linux")]
        log::info!("Mounting Linux FUSE filesystem");

        #[cfg(target_os = "macos")]
        log::info!("Mounting macOS NFS filesystem");

        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        log::info!("Mounting filesystem backend");

        // This blocks until unmount
        if let Err(e) = fs.mount(&self.mount_point) {
            log::error!("Mount failed: {}", e);
            return Err(DaemonError::Mount(e));
        }

        log::info!("Filesystem unmounted, shutting down");

        // Shutdown is triggered by unmount or signal
        // Threads will clean up via their Drop impls

        Ok(())
    }
}

/// Check if a daemon is already running.
pub fn is_daemon_running() -> bool {
    let path = socket_path();
    if !path.exists() {
        return false;
    }

    // Try to connect to see if it's alive
    std::os::unix::net::UnixStream::connect(&path).is_ok()
}

/// Get the PID file path.
fn pid_file_path() -> PathBuf {
    socket_path().with_extension("pid")
}

/// Write PID file.
fn write_pid_file() -> std::io::Result<()> {
    let pid = std::process::id();
    std::fs::write(pid_file_path(), pid.to_string())
}

/// Remove PID file.
fn remove_pid_file() {
    let _ = std::fs::remove_file(pid_file_path());
}

/// Start the daemon (blocks until shutdown).
pub fn start() -> Result<(), DaemonError> {
    if is_daemon_running() {
        return Err(DaemonError::AlreadyRunning);
    }

    // Create daemon first, then write PID file on success
    let daemon = Daemon::new()?;

    write_pid_file()?;

    let result = daemon.run();

    remove_pid_file();

    result
}
