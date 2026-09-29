//! The ghfs daemon: one tokio runtime running the filesystem frontend, the
//! CLI socket, and background maintenance.

mod maintenance;
mod socket;

pub use socket::socket_path;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use thiserror::Error;
use tokio::sync::watch;

use crate::git::GitCli;
use crate::paths::CachePaths;
use crate::repos::Repos;
use crate::vfs::Vfs;

#[cfg(target_os = "linux")]
pub const DEFAULT_MOUNT_POINT: &str = "/mnt/github";
#[cfg(not(target_os = "linux"))]
pub const DEFAULT_MOUNT_POINT: &str = "/tmp/ghfs";

/// Mount point, overridable with `GHFS_MOUNT_POINT`.
pub fn mount_point() -> PathBuf {
    std::env::var_os("GHFS_MOUNT_POINT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_MOUNT_POINT))
}

#[derive(Error, Debug)]
pub enum DaemonError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Mount failed: {0}")]
    Mount(std::io::Error),
    #[error("Daemon is already running")]
    AlreadyRunning,
}

/// Whether a daemon is answering on the socket.
pub fn is_daemon_running() -> bool {
    std::os::unix::net::UnixStream::connect(socket_path()).is_ok()
}

fn pid_file_path() -> PathBuf {
    socket_path().with_extension("pid")
}

/// Run the daemon in the foreground until a signal, `ghfs service stop`, or an
/// external unmount.
pub fn start() -> Result<(), DaemonError> {
    if is_daemon_running() {
        return Err(DaemonError::AlreadyRunning);
    }
    raise_fd_limit();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("ghfs")
        .build()?;
    std::fs::write(pid_file_path(), std::process::id().to_string())?;
    let result = runtime.block_on(run());
    let _ = std::fs::remove_file(pid_file_path());
    let _ = std::fs::remove_file(socket_path());
    // Don't wait on stuck blocking tasks (e.g. a git subprocess) at exit.
    runtime.shutdown_timeout(Duration::from_secs(2));
    result
}

/// A file server holds many pack files, cached blobs, git pipes and client
/// sockets at once; macOS's default soft limit of 256 is far too low.
pub fn raise_fd_limit() {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return;
    }
    // macOS rejects values above OPEN_MAX even when the hard limit is higher.
    let target = if cfg!(target_os = "macos") {
        lim.rlim_max.min(10240)
    } else {
        lim.rlim_max
    };
    if lim.rlim_cur < target {
        let raised = libc::rlimit {
            rlim_cur: target,
            rlim_max: lim.rlim_max,
        };
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } != 0 {
            log::warn!("could not raise the open file limit from {}", lim.rlim_cur);
        }
    }
}

async fn run() -> Result<(), DaemonError> {
    let paths = CachePaths::from_env();
    let mount_point = mount_point();
    log::info!("Starting ghfs daemon {}", env!("CARGO_PKG_VERSION"));
    log::info!("Mount point: {}", mount_point.display());
    log::info!("Cache: {}", paths.root().display());

    paths.create_dirs()?;
    // Old mirrors can be many GB; deleting them must not delay the mount.
    let legacy = paths.clone();
    tokio::task::spawn_blocking(move || {
        let removed = legacy.remove_legacy();
        if !removed.is_empty() {
            log::info!("Removed legacy cache entries: {}", removed.join(", "));
        }
    });

    let repos = Repos::new(paths, GitCli::new());
    let vfs = Vfs::new(repos.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let listener = socket::bind()?;
    let socket_task = tokio::spawn(socket::serve(
        listener,
        Arc::new(socket::Context {
            repos: repos.clone(),
            started: Instant::now(),
            mount_point: mount_point.to_string_lossy().into_owned(),
            shutdown: shutdown_tx.clone(),
        }),
    ));
    let maintenance_task = tokio::spawn(maintenance::run(repos.clone()));
    tokio::spawn(forward_signals(shutdown_tx.clone()));

    prepare_mount_point(&mount_point).await?;
    let result = mount_and_wait(vfs, &mount_point, shutdown_rx).await;

    socket_task.abort();
    maintenance_task.abort();
    log::info!("ghfs daemon stopped");
    result
}

async fn forward_signals(shutdown: watch::Sender<bool>) {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        log::error!("failed to install signal handlers");
        return;
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    log::info!("Received shutdown signal");
    let _ = shutdown.send(true);
}

async fn wait_for(mut rx: watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

/// Create the mount point and clear a dead mount left by a crashed daemon.
async fn prepare_mount_point(mount_point: &Path) -> std::io::Result<()> {
    let probe = mount_point.to_path_buf();
    let stale = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || std::fs::read_dir(&probe).err()),
    )
    .await;
    let dead = match stale {
        // Hung: a mount whose server is gone.
        Err(_) => true,
        Ok(Ok(Some(err))) => err.raw_os_error() == Some(libc::ENOTCONN),
        _ => false,
    } || leftover_nfs_mount(mount_point).await;
    if dead {
        log::warn!("{} is a dead mount; unmounting", mount_point.display());
        force_unmount(mount_point).await;
    }
    tokio::fs::create_dir_all(mount_point).await
}

/// No daemon is running (we checked the socket), so an NFS mount still here
/// belongs to a dead one, even if it happens to fail fast instead of hanging.
#[cfg(target_os = "macos")]
async fn leftover_nfs_mount(mount_point: &Path) -> bool {
    let probe = mount_point.to_path_buf();
    tokio::task::spawn_blocking(move || crate::nfs::is_mounted(&probe))
        .await
        .unwrap_or(false)
}

#[cfg(not(target_os = "macos"))]
async fn leftover_nfs_mount(_mount_point: &Path) -> bool {
    false
}

async fn force_unmount(mount_point: &Path) {
    #[cfg(target_os = "linux")]
    for (cmd, args) in [
        ("fusermount3", &["-uz"][..]),
        ("fusermount", &["-uz"][..]),
        ("umount", &["-l"][..]),
    ] {
        let ok = tokio::process::Command::new(cmd)
            .args(args)
            .arg(mount_point)
            .status()
            .await
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            return;
        }
    }
    #[cfg(target_os = "macos")]
    if let Err(e) = crate::nfs::unmount(mount_point).await {
        log::warn!("failed to clear stale mount: {e}");
    }
}

#[cfg(target_os = "linux")]
async fn mount_and_wait(
    vfs: Arc<Vfs>,
    mount_point: &Path,
    shutdown: watch::Receiver<bool>,
) -> Result<(), DaemonError> {
    let fs = crate::fuse::FuseFs::new(vfs, tokio::runtime::Handle::current());
    let session = fs.mount(mount_point).map_err(DaemonError::Mount)?;
    log::info!("Mounted FUSE filesystem at {}", mount_point.display());
    loop {
        tokio::select! {
            _ = wait_for(shutdown.clone()) => break,
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                if session.guard.is_finished() {
                    log::info!("Filesystem was unmounted externally");
                    break;
                }
            }
        }
    }
    // Dropping the session unmounts and joins the FUSE thread.
    tokio::task::spawn_blocking(move || drop(session))
        .await
        .map_err(|e| DaemonError::Io(std::io::Error::other(e)))?;
    Ok(())
}

#[cfg(target_os = "macos")]
async fn mount_and_wait(
    vfs: Arc<Vfs>,
    mount_point: &Path,
    shutdown: watch::Receiver<bool>,
) -> Result<(), DaemonError> {
    let (listener, port) = crate::nfs::bind().await?;
    let server = tokio::spawn(crate::nfs::NfsServer::new(vfs).serve(listener));
    let mount_point = tokio::fs::canonicalize(mount_point).await?;
    if let Err(e) = crate::nfs::mount(port, &mount_point).await {
        server.abort();
        return Err(DaemonError::Mount(e));
    }
    log::info!(
        "Mounted NFS filesystem at {} (port {port})",
        mount_point.display()
    );
    loop {
        tokio::select! {
            _ = wait_for(shutdown.clone()) => break,
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                let mp = mount_point.clone();
                let mounted = tokio::task::spawn_blocking(move || crate::nfs::is_mounted(&mp))
                    .await
                    .unwrap_or(true);
                if !mounted {
                    log::info!("Filesystem was unmounted externally");
                    server.abort();
                    return Ok(());
                }
            }
        }
    }
    // Unmount while the server is still up so the kernel can talk to it.
    if let Err(e) = crate::nfs::unmount(&mount_point).await {
        log::warn!("unmount failed: {e}");
    }
    server.abort();
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
async fn mount_and_wait(
    _vfs: Arc<Vfs>,
    _mount_point: &Path,
    _shutdown: watch::Receiver<bool>,
) -> Result<(), DaemonError> {
    Err(DaemonError::Mount(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "unsupported platform",
    )))
}
