//! Scheduler thread for periodic background refresh of stale repos.
//!
//! This module provides a scheduler that periodically checks for stale repositories
//! and triggers background refreshes via the worker thread.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::Sender;

use crate::daemon::state::State;
use crate::daemon::worker::WorkerRequest;
use crate::types::RepoKey;

/// How often to check for stale repos.
const CHECK_INTERVAL: Duration = Duration::from_secs(5 * 60); // 5 minutes

/// Max age before a repo is considered stale.
const MAX_AGE_SECS: i64 = 24 * 60 * 60; // 24 hours

/// Background scheduler that periodically checks for stale repos.
pub struct Scheduler {
    state: Arc<State>,
    worker_tx: Sender<WorkerRequest>,
    shutdown: Arc<AtomicBool>,
}

impl Scheduler {
    /// Create a new scheduler.
    pub fn new(
        state: Arc<State>,
        worker_tx: Sender<WorkerRequest>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            state,
            worker_tx,
            shutdown,
        }
    }

    /// Run the scheduler loop.
    pub fn run(self) {
        log::info!("Scheduler thread started");

        loop {
            // Sleep in small increments to check shutdown more often
            for _ in 0..(CHECK_INTERVAL.as_secs() / 5) {
                if self.shutdown.load(Ordering::SeqCst) {
                    log::info!("Scheduler shutting down");
                    return;
                }
                thread::sleep(Duration::from_secs(5));
            }

            if self.shutdown.load(Ordering::SeqCst) {
                log::info!("Scheduler shutting down");
                return;
            }

            self.check_and_refresh();
        }
    }

    /// Check all repos and schedule refreshes for stale ones.
    fn check_and_refresh(&self) {
        log::debug!("Scheduler checking for stale repos");

        let repos = match self.state.list_repos() {
            Ok(r) => r,
            Err(e) => {
                log::warn!("Failed to list repos: {}", e);
                return;
            }
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        for repo in repos {
            // Check if stale
            let is_stale = match repo.last_sync_at {
                Some(ts) => now - ts > MAX_AGE_SECS,
                None => true, // Never synced
            };

            if is_stale {
                let key_str = format!("{}/{}", repo.owner, repo.repo);
                let key: RepoKey = match key_str.parse() {
                    Ok(k) => k,
                    Err(e) => {
                        log::warn!("Invalid repo key in db: {} - {}", key_str, e);
                        continue;
                    }
                };

                log::info!("Scheduling background refresh for {}", key);

                if self
                    .worker_tx
                    .send(WorkerRequest::Refresh { repo: key })
                    .is_err()
                {
                    log::warn!("Worker channel closed, stopping scheduler");
                    return;
                }
            }
        }
    }
}

/// Handle for managing the scheduler thread.
pub struct SchedulerHandle {
    thread: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl SchedulerHandle {
    /// Spawn the scheduler thread.
    pub fn spawn(
        state: Arc<State>,
        worker_tx: Sender<WorkerRequest>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        let scheduler = Scheduler::new(state, worker_tx, shutdown.clone());

        let thread = thread::Builder::new()
            .name("ghfs-scheduler".to_string())
            .spawn(move || scheduler.run())
            .expect("failed to spawn scheduler thread");

        Self {
            thread: Some(thread),
            shutdown,
        }
    }

    /// Signal shutdown and wait for thread to exit.
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for SchedulerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
