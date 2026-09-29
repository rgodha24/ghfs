//! JSON-RPC over a Unix socket for the CLI (`ghfs status`, `ghfs sync`, ...).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

use crate::protocol::{
    ListResult, RepoInfo, Request, Response, RpcError, RpcErrorResponse, RpcRequest, RpcResponse,
    StatusResult, SyncResult, VersionResult,
};
use crate::repos::{RepoError, Repos};
use crate::types::RepoKey;

/// `$GHFS_SOCKET`, else `$XDG_RUNTIME_DIR/ghfs.sock`, else `/tmp/ghfs-$UID.sock`.
pub fn socket_path() -> PathBuf {
    if let Some(path) = std::env::var_os("GHFS_SOCKET") {
        return PathBuf::from(path);
    }
    if let Some(runtime_dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(runtime_dir).join("ghfs.sock");
    }
    let uid = unsafe { libc::getuid() };
    PathBuf::from(format!("/tmp/ghfs-{uid}.sock"))
}

pub struct Context {
    pub repos: Arc<Repos>,
    pub started: Instant,
    pub mount_point: String,
    pub shutdown: watch::Sender<bool>,
}

pub fn bind() -> std::io::Result<UnixListener> {
    let path = socket_path();
    if path.exists() {
        std::fs::remove_file(&path)?;
    }
    let listener = UnixListener::bind(&path)?;
    log::info!("Socket listening on {}", path.display());
    Ok(listener)
}

pub async fn serve(listener: UnixListener, ctx: Arc<Context>) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(e) = client(stream, ctx).await {
                        log::debug!("socket client error: {e}");
                    }
                });
            }
            Err(e) => {
                log::warn!("socket accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
}

async fn client(stream: UnixStream, ctx: Arc<Context>) -> std::io::Result<()> {
    let (rd, mut wr) = stream.into_split();
    let mut lines = BufReader::new(rd).lines();
    while let Some(line) = lines.next_line().await? {
        let request: RpcRequest = serde_json::from_str(&line)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let id = request.id.clone();
        let json = match handle(&ctx, request.request).await {
            Ok(result) => serde_json::to_string(&RpcResponse::new(result, id)),
            Err(error) => serde_json::to_string(&RpcErrorResponse::new(error, id)),
        }
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        wr.write_all(json.as_bytes()).await?;
        wr.write_all(b"\n").await?;
        wr.flush().await?;
    }
    Ok(())
}

async fn handle(ctx: &Context, request: Request) -> Result<Response, RpcError> {
    match request {
        Request::Status => Ok(Response::Status(StatusResult {
            running: true,
            mount_point: ctx.mount_point.clone(),
            repo_count: ctx.repos.paths().cached_repos().len(),
            uptime_secs: ctx.started.elapsed().as_secs(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            pid: std::process::id(),
            pending_syncs: ctx.repos.activity(),
        })),
        Request::Sync { repo } => {
            let key: RepoKey = repo
                .parse()
                .map_err(|e| RpcError::invalid_params(format!("invalid repo: {e}")))?;
            let handle = ctx.repos.get(&key);
            let refs = handle.refresh_head().await.map_err(rpc_error)?;
            let head = refs
                .head
                .ok_or_else(|| RpcError::not_found(format!("{key} has no HEAD")))?;
            handle.ensure_commit(head).await.map_err(rpc_error)?;
            Ok(Response::Sync(SyncResult {
                commit: head.to_string(),
            }))
        }
        Request::Gc => Ok(Response::Gc(super::maintenance::gc(&ctx.repos).await)),
        Request::List => {
            let mut repos = Vec::new();
            for key in ctx.repos.paths().cached_repos() {
                let handle = ctx.repos.get(&key);
                let dir = handle.dir().to_path_buf();
                let commit = handle.cached_head_commit().map(|c| c.to_string());
                let size = tokio::task::spawn_blocking(move || dir_size(&dir))
                    .await
                    .unwrap_or(0);
                repos.push(RepoInfo {
                    owner: key.owner.to_string(),
                    repo: key.repo.to_string(),
                    commit,
                    last_sync: None,
                    last_access: None,
                    total_size_bytes: size,
                });
            }
            Ok(Response::List(ListResult { repos }))
        }
        Request::Version => Ok(Response::Version(VersionResult {
            version: env!("CARGO_PKG_VERSION").to_string(),
            pid: std::process::id(),
        })),
        Request::Stop => {
            let _ = ctx.shutdown.send(true);
            Ok(Response::Ok(()))
        }
    }
}

fn rpc_error(e: RepoError) -> RpcError {
    match e {
        RepoError::NotFound(_) => RpcError::not_found(e.to_string()),
        RepoError::Failed(_) => RpcError::internal(e.to_string()),
    }
}

pub fn dir_size(dir: &std::path::Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .map(|e| match e.file_type() {
            Ok(t) if t.is_dir() => dir_size(&e.path()),
            Ok(_) => e.metadata().map(|m| m.len()).unwrap_or(0),
            Err(_) => 0,
        })
        .sum()
}
