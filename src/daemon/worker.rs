//! Background worker thread for git operations.
//!
//! Offloads network operations (blobless clone/fetch, ref resolution) from
//! the mount backend thread so filesystem operations don't block on the
//! promisor. Operates entirely against the [`crate::store::Store`].

use crossbeam_channel::{Receiver, Sender, bounded};
use std::sync::mpsc as oneshot;
use std::thread::{self, JoinHandle};

use crate::cache::NegativeCache;
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
    negative_cache: NegativeCache,
}

impl Worker {
    pub fn new(receiver: Receiver<WorkerRequest>, store: Store) -> Self {
        Self {
            receiver,
            store,
            negative_cache: NegativeCache::new(),
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
                    if let Err(StoreError::Git(crate::store::GitError::CloneError(_))) = &result {
                        if self.negative_cache.insert_if_not_exists(&repo) {
                            // confirmed not found; error already returned
                        }
                    }
                    let _ = reply.send(result);
                }
                Ok(WorkerRequest::Resolve {
                    repo,
                    selector,
                    reply,
                }) => {
                    let _ = reply.send(
                        self.store
                            .resolve_revision(&repo, &selector)
                            .map(|oid| oid.to_string()),
                    );
                }
                Ok(WorkerRequest::Refresh { repo }) => {
                    if self.negative_cache.contains(&repo) {
                        continue;
                    }
                    if let Err(e) = self.store.refresh(&repo) {
                        log::warn!("Background refresh failed for {repo}: {e}");
                    }
                }
                Ok(WorkerRequest::Sync { repo, reply }) => {
                    let _ = reply.send(match self.store.refresh(&repo) {
                        Ok(()) => self.store.resolve_head(&repo).map(|oid| oid.to_string()),
                        Err(e) => Err(e),
                    });
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
    pub fn spawn(store: Store) -> Self {
        let (sender, receiver) = bounded(100);
        let worker = Worker::new(receiver, store);
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
