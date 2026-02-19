//! Background worker thread for git operations.
//!
//! This module provides a worker thread that handles git operations (clone, fetch,
//! worktree creation) off the mount backend thread to avoid blocking filesystem operations.

use crossbeam_channel::{Receiver, Sender, bounded};
use std::sync::mpsc as oneshot;
use std::thread::{self, JoinHandle};

use crate::cache::{CacheError, GenerationRef, ManagedCache, NegativeCache};
use crate::types::RepoKey;

/// Requests the worker can handle.
pub enum WorkerRequest {
    /// Materialize a repo (blocking - mount backend needs the result).
    Materialize {
        repo: RepoKey,
        reply: oneshot::Sender<Result<GenerationRef, CacheError>>,
    },

    /// Refresh a repo in background (fire and forget).
    Refresh { repo: RepoKey },

    /// Force sync (from CLI, needs result for response).
    Sync {
        repo: RepoKey,
        reply: oneshot::Sender<Result<GenerationRef, CacheError>>,
    },

    /// Unshallow a repo (fetch full history).
    Unshallow {
        repo: RepoKey,
        reply: oneshot::Sender<Result<GenerationRef, CacheError>>,
    },

    /// Reshallow a repo (convert back to depth=1).
    Reshallow {
        repo: RepoKey,
        reply: oneshot::Sender<Result<GenerationRef, CacheError>>,
    },

    /// Shutdown the worker.
    Shutdown,
}

/// Background worker that processes git operations.
pub struct Worker {
    receiver: Receiver<WorkerRequest>,
    cache: ManagedCache,
    /// Cache of repos that are known not to exist.
    negative_cache: NegativeCache,
}

impl Worker {
    /// Create a new worker with the given receiver and cache.
    pub fn new(receiver: Receiver<WorkerRequest>, cache: ManagedCache) -> Self {
        Self {
            receiver,
            cache,
            negative_cache: NegativeCache::new(),
        }
    }

    /// Run the worker loop (blocks until Shutdown).
    pub fn run(self) {
        log::info!("Worker thread started");

        loop {
            match self.receiver.recv() {
                Ok(WorkerRequest::Materialize { repo, reply }) => {
                    // Check negative cache first
                    if self.negative_cache.contains(&repo) {
                        log::debug!("Repo {} is in negative cache, skipping", repo);
                        let _ = reply.send(Err(CacheError::RepoNotFound(repo.to_string())));
                        continue;
                    }

                    log::debug!("Materializing repo: {}", repo);
                    let result = self.cache.ensure_current(&repo);

                    // If materialization failed, check if we should add to negative cache
                    if let Err(ref e) = result {
                        log::debug!("Materialization failed for {}: {}", repo, e);
                        // Only check for not-found on git errors (clone failures)
                        if matches!(e, CacheError::Git(_)) {
                            self.negative_cache.insert_if_not_exists(&repo);
                        }
                    }

                    let _ = reply.send(result);
                }
                Ok(WorkerRequest::Refresh { repo }) => {
                    // Skip refresh for repos in negative cache
                    if self.negative_cache.contains(&repo) {
                        log::debug!("Repo {} is in negative cache, skipping refresh", repo);
                        continue;
                    }

                    log::debug!("Background refresh for repo: {}", repo);
                    if let Err(e) = self.cache.ensure_current(&repo) {
                        log::warn!("Background refresh failed for {}: {}", repo, e);
                    }
                }
                Ok(WorkerRequest::Sync { repo, reply }) => {
                    log::debug!("Force sync for repo: {}", repo);
                    let result = self.cache.force_refresh(&repo);
                    let _ = reply.send(result);
                }
                Ok(WorkerRequest::Unshallow { repo, reply }) => {
                    log::debug!("Unshallow for repo: {}", repo);
                    let result = self.cache.unshallow(&repo);
                    let _ = reply.send(result);
                }
                Ok(WorkerRequest::Reshallow { repo, reply }) => {
                    log::debug!("Reshallow for repo: {}", repo);
                    let result = self.cache.reshallow(&repo);
                    let _ = reply.send(result);
                }
                Ok(WorkerRequest::Shutdown) => {
                    log::info!("Worker thread shutting down");
                    break;
                }
                Err(_) => {
                    // Channel closed, exit
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
    pub fn spawn(cache: ManagedCache) -> Self {
        let (sender, receiver) = bounded(100); // Buffer up to 100 requests

        let worker = Worker::new(receiver, cache);
        let thread = thread::Builder::new()
            .name("ghfs-worker".to_string())
            .spawn(move || worker.run())
            .expect("failed to spawn worker thread");

        Self {
            sender,
            thread: Some(thread),
        }
    }

    /// Get a clone of the sender for submitting work.
    pub fn sender(&self) -> Sender<WorkerRequest> {
        self.sender.clone()
    }

    /// Request materialization (blocking until complete).
    pub fn materialize(&self, repo: RepoKey) -> Result<GenerationRef, CacheError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Materialize { repo, reply: tx })
            .map_err(|_| CacheError::LockFailed)?;
        rx.recv().map_err(|_| CacheError::LockFailed)?
    }

    /// Request background refresh (non-blocking).
    pub fn refresh(&self, repo: RepoKey) {
        let _ = self.sender.send(WorkerRequest::Refresh { repo });
    }

    /// Request forced sync (blocking until complete).
    pub fn sync(&self, repo: RepoKey) -> Result<GenerationRef, CacheError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Sync { repo, reply: tx })
            .map_err(|_| CacheError::LockFailed)?;
        rx.recv().map_err(|_| CacheError::LockFailed)?
    }

    /// Request unshallow (blocking until complete).
    pub fn unshallow(&self, repo: RepoKey) -> Result<GenerationRef, CacheError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Unshallow { repo, reply: tx })
            .map_err(|_| CacheError::LockFailed)?;
        rx.recv().map_err(|_| CacheError::LockFailed)?
    }

    /// Request reshallow (blocking until complete).
    pub fn reshallow(&self, repo: RepoKey) -> Result<GenerationRef, CacheError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(WorkerRequest::Reshallow { repo, reply: tx })
            .map_err(|_| CacheError::LockFailed)?;
        rx.recv().map_err(|_| CacheError::LockFailed)?
    }

    /// Shutdown the worker.
    pub fn shutdown(&mut self) {
        let _ = self.sender.send(WorkerRequest::Shutdown);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
