//! Unix socket server for handling JSON-RPC requests from the CLI.

use std::io::BufReader;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Instant;

use crate::daemon::state::State;
use crate::daemon::worker::WorkerHandle;
use crate::protocol::{
    ListResult, RepoInfo, Request, Response, RpcError, RpcErrorResponse, RpcResponse, StatusResult,
    SyncResult, read_request, write_message,
};
use crate::types::RepoKey;

/// Get the socket path for the daemon.
///
/// Uses `$XDG_RUNTIME_DIR/ghfs.sock` on Linux.
/// Falls back to `/tmp/ghfs-$UID.sock`.
pub fn socket_path() -> PathBuf {
    if let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(runtime_dir).join("ghfs.sock");
    }

    let uid = unsafe { libc::getuid() };
    PathBuf::from(format!("/tmp/ghfs-{}.sock", uid))
}

/// Context shared by request handlers.
pub struct Context {
    pub state: Arc<State>,
    pub worker: Arc<WorkerHandle>,
    pub start_time: Instant,
    pub mount_point: String,
}

/// Handle a single JSON-RPC request.
fn handle_request(ctx: &Context, request: Request) -> Result<Response, RpcError> {
    match request {
        Request::Status => {
            let repos = ctx
                .state
                .list_repos()
                .map_err(|e| RpcError::internal(e.to_string()))?;

            Ok(Response::Status(StatusResult {
                running: true,
                mount_point: ctx.mount_point.clone(),
                repo_count: repos.len(),
                uptime_secs: ctx.start_time.elapsed().as_secs(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                pid: std::process::id(),
                pending_syncs: vec![], // TODO: track pending syncs in worker
            }))
        }

        Request::Sync { repo } => {
            let key: RepoKey = repo
                .parse()
                .map_err(|e| RpcError::invalid_params(format!("invalid repo: {}", e)))?;

            let gen_ref = ctx
                .worker
                .sync(key)
                .map_err(|e| RpcError::internal(e.to_string()))?;

            Ok(Response::Sync(SyncResult {
                generation: gen_ref.generation.as_u64(),
                commit: gen_ref.commit,
            }))
        }

        Request::Watch { repo } => {
            let key: RepoKey = repo
                .parse()
                .map_err(|e| RpcError::invalid_params(format!("invalid repo: {}", e)))?;

            ctx.state
                .get_or_create_repo(&key)
                .map_err(|e| RpcError::internal(e.to_string()))?;
            ctx.state
                .set_priority(&key, 10)
                .map_err(|e| RpcError::internal(e.to_string()))?;

            Ok(Response::Ok(()))
        }

        Request::Unwatch { repo } => {
            let key: RepoKey = repo
                .parse()
                .map_err(|e| RpcError::invalid_params(format!("invalid repo: {}", e)))?;

            ctx.state
                .set_priority(&key, 0)
                .map_err(|e| RpcError::internal(e.to_string()))?;

            Ok(Response::Ok(()))
        }

        Request::List => {
            let repos = ctx
                .state
                .list_repos_with_stats()
                .map_err(|e| RpcError::internal(e.to_string()))?;

            let infos: Vec<RepoInfo> = repos
                .into_iter()
                .map(|r| RepoInfo {
                    owner: r.owner,
                    repo: r.repo,
                    priority: r.priority,
                    generation: r.current_generation,
                    commit: r.head_commit,
                    last_sync: r.last_sync_at.map(format_timestamp),
                    last_access: r.last_access_at.map(format_timestamp),
                    generation_count: r.generation_count,
                    commit_count: r.commit_count,
                    total_size_bytes: r.total_size_bytes,
                })
                .collect();

            Ok(Response::List(ListResult { repos: infos }))
        }

        Request::Stop => {
            // Trigger unmount to unblock the FUSE event loop.
            super::spawn_unmount(ctx.mount_point.clone());
            Ok(Response::Ok(()))
        }
    }
}

/// Format a Unix timestamp as a human-readable relative time.
fn format_timestamp(ts: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    let diff = now - ts;
    if diff < 0 {
        return format!("in {}s", -diff);
    }
    if diff < 60 {
        format!("{}s ago", diff)
    } else if diff < 3600 {
        format!("{}m ago", diff / 60)
    } else if diff < 86400 {
        format!("{}h ago", diff / 3600)
    } else {
        format!("{}d ago", diff / 86400)
    }
}

/// Handle a connected client, reading requests and writing responses.
fn handle_client(ctx: &Context, stream: UnixStream) {
    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;

    loop {
        let rpc_req = match read_request(&mut reader) {
            Ok(Some(req)) => req,
            Ok(None) => break, // Client disconnected
            Err(e) => {
                log::warn!("Failed to read request: {}", e);
                break;
            }
        };

        let id = rpc_req.id.clone();

        match handle_request(ctx, rpc_req.request) {
            Ok(result) => {
                let response = RpcResponse::new(result, id);
                if let Err(e) = write_message(&mut writer, &response) {
                    log::warn!("Failed to write response: {}", e);
                    break;
                }
            }
            Err(error) => {
                let response = RpcErrorResponse::new(error, id);
                if let Err(e) = write_message(&mut writer, &response) {
                    log::warn!("Failed to write error response: {}", e);
                    break;
                }
            }
        }
    }
}

/// Handle for managing the socket server thread.
pub struct SocketServerHandle {
    thread: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl SocketServerHandle {
    /// Spawn the socket server thread.
    pub fn spawn(
        state: Arc<State>,
        worker: Arc<WorkerHandle>,
        mount_point: String,
        shutdown: Arc<AtomicBool>,
    ) -> std::io::Result<Self> {
        let path = socket_path();

        // Remove stale socket file
        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        let listener = UnixListener::bind(&path)?;
        log::info!("Socket server listening on {}", path.display());

        let ctx = Arc::new(Context {
            state,
            worker,
            start_time: Instant::now(),
            mount_point,
        });

        let shutdown_clone = shutdown.clone();
        let thread = thread::Builder::new()
            .name("ghfs-socket".to_string())
            .spawn(move || {
                for stream in listener.incoming() {
                    if shutdown_clone.load(Ordering::SeqCst) {
                        break;
                    }

                    match stream {
                        Ok(stream) => {
                            let ctx = Arc::clone(&ctx);
                            thread::spawn(move || handle_client(&ctx, stream));
                        }
                        Err(e) => {
                            if shutdown_clone.load(Ordering::SeqCst) {
                                break;
                            }
                            log::warn!("Failed to accept connection: {}", e);
                        }
                    }
                }

                // Cleanup socket file
                let _ = std::fs::remove_file(socket_path());
                log::info!("Socket server stopped");
            })
            .expect("failed to spawn socket thread");

        Ok(Self {
            thread: Some(thread),
            shutdown,
        })
    }

    /// Signal shutdown and wait for thread to exit.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);

        // Connect to the socket to unblock accept()
        let _ = UnixStream::connect(socket_path());

        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for SocketServerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
