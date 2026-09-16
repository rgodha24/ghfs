//! Background worker thread for git operations.
//!
//! Offloads network operations (blobless clone/fetch, ref resolution) from
//! the mount backend thread so filesystem operations don't block on the
//! promisor. Operates entirely against the [`crate::store::Store`].

use crossbeam_channel::{Receiver, Sender, bounded};
use std::sync::Arc;
use std::sync::mpsc as oneshot;
use std::thread::{self, JoinHandle};

use crate::cache::NegativeCache;
use crate::daemon::State;
use crate::store::{Store, StoreError};
use crate::types::RepoKey;

/// Requests the worker can handle.
pub enum WorkerRequest {
    /// Ensure the repo mirror exists and resolve the default-branch (HEAD)
    /// commit. Returns the commit OID hex string.
    Materialize {
        repo: RepoKey,
        reply: oneshot::Sender<Result<String, StoreError>>,
    },

    /// Ensure the repo mirror exists and resolve an arbitrary ref selector to
    /// a commit OID hex string.
    Resolve {
        repo: RepoKey,
        selector: String,
        reply: oneshot::Sender<Result<String, StoreError>>,
    },

    /// Background refresh (fire and forget): re-fetch the mirror's refs.
    Refresh { repo: RepoKey },

    /// Force sync (from CLI): re-fetch and return the HEAD commit.
    Sync {
        repo: RepoKey,
        reply: oneshot::Sender<Result<String, StoreError>>,
    },

    /// Shutdown the worker.
    Shutdown,
}

/// Background worker that processes git operations.
pub struct Worker {
    receiver: Receiver<WorkerRequest>,
    store: Store,
    state: Arc<State>,
    negative_cache: NegativeCache,
}

impl Worker {
    pub fn new(receiver: Receiver<WorkerRequest>, store: Store, state: Arc<State>) -> Self {
        Self {
            receiver,
            store,
            state,
            negative_cache: NegativeCache::new(),
        }
    }

    /// Persist the HEAD commit and sync time for a repo. Failures are logged
    /// but never surfaced: the mirror is the source of truth, the db is
    /// bookkeeping for `ghfs status`, the scheduler, and gc.
    fn record_sync(&self, repo: &RepoKey, commit: &str) {
        if let Err(e) = self.state.record_sync(repo, commit) {
            log::warn!("Failed to record sync for {repo}: {e}");
        }
    }

    fn record_access(&self, repo: &RepoKey) {
        if let Err(e) = self.state.touch_access(repo) {
            log::warn!("Failed to record access for {repo}: {e}");
        }
    }

    /// Resolve HEAD after a refresh and persist it.
    fn record_refreshed_head(&self, repo: &RepoKey) {
        match self.store.resolve_head(repo) {
            Ok(oid) => self.record_sync(repo, &oid.to_string()),
            Err(e) => log::warn!("Failed to resolve HEAD after refresh for {repo}: {e}"),
        }
    }

    /// Run the worker loop (blocks until Shutdown).
    pub fn run(self) {
        log::info!("Worker thread started");
        loop {
            match self.receiver.recv() {
                Ok(WorkerRequest::Materialize { repo, reply }) => {
                    if self.negative_cache.contains(&repo) {
                        let _ = reply.send(Err(StoreError::RepoNotFound(repo.to_string())));
                        continue;
                    }
                    let result = self.store.resolve_head(&repo).map(|oid| oid.to_string());
                    match &result {
                        Ok(commit) => {
                            self.record_access(&repo);
                            self.record_sync(&repo, commit);
                        }
                        Err(StoreError::Git(crate::store::GitError::CloneError(_))) => {
                            if self.negative_cache.insert_if_not_exists(&repo) {
                                // confirmed not found; error already returned
                            }
                        }
                        Err(_) => {}
                    }
                    let _ = reply.send(result);
                }
                Ok(WorkerRequest::Resolve {
                    repo,
                    selector,
                    reply,
                }) => {
                    let result = self
                        .store
                        .resolve_revision(&repo, &selector)
                        .map(|oid| oid.to_string());
                    if result.is_ok() {
                        self.record_access(&repo);
                    }
                    let _ = reply.send(result);
                }
                Ok(WorkerRequest::Refresh { repo }) => {
                    if self.negative_cache.contains(&repo) {
                        continue;
                    }
                    match self.store.refresh(&repo) {
                        Ok(()) => self.record_refreshed_head(&repo),
                        Err(e) => log::warn!("Background refresh failed for {repo}: {e}"),
                    }
                }
                Ok(WorkerRequest::Sync { repo, reply }) => {
                    let result = match self.store.refresh(&repo) {
                        Ok(()) => self.store.resolve_head(&repo).map(|oid| oid.to_string()),
                        Err(e) => Err(e),
                    };
                    if let Ok(commit) = &result {
                        self.record_sync(&repo, commit);
                    }
                    let _ = reply.send(result);
                }
                Ok(WorkerRequest::Shutdown) => {
                    log::info!("Worker thread shutting down");
                    break;
                }
                Err(_) => {
                    log::info!("Worker channel closed, exiting");
                    break;
                }
            }
        }
    }
}

/// Handle for managing the worker thread from the main daemon.
pub struct WorkerHandle {
    sender: Sender<WorkerRequest>,
    thread: Option<JoinHandle<()>>,
}

impl WorkerHandle {
    /// Spawn the worker thread.
    pub fn spawn(store: Store, state: Arc<State>) -> Self {
        let (sender, receiver) = bounded(100);
        let worker = Worker::new(receiver, store, state);
        let thread = thread::Builder::new()
            .name("ghfs-worker".to_string())
            .spawn(move || worker.run())
            .expect("failed to spawn worker thread");
        Self {
            sender,
            thread: Some(thread),
        }
    }

    pub fn sender(&self) -> Sender<WorkerRequest> {
        self.sender.clone()
    }

    /// Ensure the mirror exists and resolve HEAD.
    pub fn materialize(&self, repo: RepoKey) -> Result<String, StoreError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Materialize { repo, reply: tx })
            .map_err(|_| StoreError::LockFailed)?;
        rx.recv().map_err(|_| StoreError::LockFailed)?
    }

    /// Ensure the mirror exists and resolve a ref selector.
    pub fn resolve(&self, repo: RepoKey, selector: String) -> Result<String, StoreError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Resolve {
                repo,
                selector,
                reply: tx,
            })
            .map_err(|_| StoreError::LockFailed)?;
        rx.recv().map_err(|_| StoreError::LockFailed)?
    }

    pub fn refresh(&self, repo: RepoKey) {
        let _ = self.sender.send(WorkerRequest::Refresh { repo });
    }

    /// Force refresh and return the HEAD commit.
    pub fn sync(&self, repo: RepoKey) -> Result<String, StoreError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Sync { repo, reply: tx })
            .map_err(|_| StoreError::LockFailed)?;
        rx.recv().map_err(|_| StoreError::LockFailed)?
    }

    pub fn shutdown(&mut self) {
        let _ = self.sender.send(WorkerRequest::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
