//! Daemon module for background sync operations.

mod scheduler;
mod socket;
pub mod state;
mod worker;

pub use scheduler::SchedulerHandle;
pub use socket::{socket_path, SocketServerHandle};
pub use state::{RepoState, State};
pub use worker::{WorkerHandle, WorkerRequest};

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use thiserror::Error;

use crate::cache::{CachePaths, ManagedCache};
use crate::fs::GhFs;

/// The mount point (hardcoded for now)
pub const MOUNT_POINT: &str = "/mnt/github";

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

impl Daemon {
    /// Create a new daemon instance.
    pub fn new() -> Result<Self, DaemonError> {
        let cache_paths = CachePaths::default();
        let mount_point = PathBuf::from(MOUNT_POINT);

        // Ensure cache directories exist
        std::fs::create_dir_all(cache_paths.mirrors_dir())?;
        std::fs::create_dir_all(cache_paths.worktrees_dir())?;
        std::fs::create_dir_all(cache_paths.locks_dir())?;

        // Open state database
        let db_path = cache_paths.root().join("ghfs.db");
        let state = State::open(&db_path)?;
        state.init()?;

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

        // Ensure mount point exists
        if !self.mount_point.exists() {
            std::fs::create_dir_all(&self.mount_point)?;
        }

        // Create managed cache for the worker
        let managed_cache = ManagedCache::new(self.cache_paths.clone(), Arc::clone(&self.state));

        // Spawn worker thread
        let worker = Arc::new(WorkerHandle::spawn(managed_cache));
        log::info!("Worker thread started");

        // Spawn socket server
        let _socket_server = SocketServerHandle::spawn(
            Arc::clone(&self.state),
            Arc::clone(&worker),
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
        let shutdown = Arc::clone(&self.shutdown);
        ctrlc::set_handler(move || {
            log::info!("Received shutdown signal");
            shutdown.store(true, Ordering::SeqCst);
        })
        .expect("failed to set signal handler");

        // Create and mount FUSE filesystem
        let fs = GhFs::new(Arc::clone(&worker), self.cache_paths.clone());

        log::info!("Mounting FUSE filesystem");

        // This blocks until unmount
        if let Err(e) = fs.mount(&self.mount_point) {
            log::error!("FUSE mount failed: {}", e);
            return Err(DaemonError::Mount(e));
        }

        log::info!("FUSE unmounted, shutting down");

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

    write_pid_file()?;

    let result = Daemon::new()?.run();

    remove_pid_file();

    result
}

/// Stop a running daemon by sending the Stop command.
pub fn stop() -> Result<(), Box<dyn std::error::Error>> {
    use crate::cli::Client;
    use crate::protocol::Request;

    let mut client = Client::connect()?;
    client.call(Request::Stop)?;

    Ok(())
}
