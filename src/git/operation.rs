//! Cancellation and reservations outlive the future awaiting a blocking worker.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use super::GitError;

#[derive(Clone, Debug)]
pub(crate) struct Limits {
    pub max_pack_bytes: u64,
    pub max_object_bytes: u64,
    pub max_metadata_object_bytes: u64,
    pub max_expanded_bytes: u64,
    pub max_objects: usize,
    pub max_delta_depth: u32,
    pub max_wants: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_pack_bytes: 512 << 20,
            max_object_bytes: 512 << 20,
            max_metadata_object_bytes: 8 << 20,
            max_expanded_bytes: 1 << 30,
            max_objects: 100_000,
            max_delta_depth: 64,
            max_wants: 1000,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Work {
    pub token: CancellationToken,
    pub deadline: Instant,
    timeout: Duration,
}

impl Work {
    pub fn new(timeout: Duration) -> Self {
        Self::with_token(timeout, CancellationToken::new())
    }

    pub fn with_token(timeout: Duration, token: CancellationToken) -> Self {
        Self {
            token,
            deadline: Instant::now() + timeout,
            timeout,
        }
    }

    pub fn check(&self) -> Result<(), GitError> {
        if self.token.is_cancelled() {
            Err(GitError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(GitError::Timeout(self.timeout))
        } else {
            Ok(())
        }
    }

    pub async fn wait<T>(
        &self,
        future: impl Future<Output = Result<T, GitError>>,
    ) -> Result<T, GitError> {
        self.check()?;
        tokio::select! {
            biased;
            _ = self.token.cancelled() => Err(GitError::Cancelled),
            _ = tokio::time::sleep_until(self.deadline.into()) => Err(GitError::Timeout(self.timeout)),
            result = future => result,
        }
    }

    pub fn cancel_on_drop(&self) -> CancelOnDrop {
        CancelOnDrop(self.token.clone())
    }
}

pub(crate) struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Fail-fast reservations cannot deadlock while holding a partial allocation.
#[derive(Debug)]
pub(crate) struct Budget {
    name: &'static str,
    limit: u64,
    used: AtomicU64,
    peak: AtomicU64,
}

impl Budget {
    pub fn new(name: &'static str, limit: u64) -> Arc<Self> {
        Arc::new(Self {
            name,
            limit,
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
        })
    }

    pub fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation, GitError> {
        let mut reservation = Reservation {
            budget: self.clone(),
            bytes: 0,
        };
        reservation.grow(bytes)?;
        Ok(reservation)
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Acquire)
    }
}

pub(crate) struct Reservation {
    budget: Arc<Budget>,
    bytes: u64,
}

impl Reservation {
    pub fn grow(&mut self, bytes: u64) -> Result<(), GitError> {
        let result = self
            .budget
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.budget.limit)
            });
        let old = result.map_err(|_| GitError::Quota(self.budget.name.to_owned()))?;
        self.bytes += bytes;
        self.budget.peak.fetch_max(old + bytes, Ordering::AcqRel);
        Ok(())
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

#[derive(Default)]
struct State {
    closed: bool,
    next: u64,
    active: HashMap<u64, Work>,
}

#[derive(Default)]
pub(crate) struct Workers {
    state: Mutex<State>,
    changed: Notify,
}

struct Registered {
    workers: Arc<Workers>,
    id: u64,
}

impl Drop for Registered {
    fn drop(&mut self) {
        self.workers
            .state
            .lock()
            .expect("workers poisoned")
            .active
            .remove(&self.id);
        self.workers.changed.notify_waiters();
    }
}

impl Workers {
    pub async fn run<T, F>(self: &Arc<Self>, work: Work, f: F) -> Result<T, GitError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, GitError> + Send + 'static,
    {
        work.check()?;
        let registered = {
            let mut state = self.state.lock().expect("workers poisoned");
            if state.closed {
                return Err(GitError::Cancelled);
            }
            let id = state.next;
            state.next += 1;
            state.active.insert(id, work.clone());
            Registered {
                workers: self.clone(),
                id,
            }
        };
        let task = tokio::task::spawn_blocking(move || {
            let _registered = registered;
            work.check()?;
            f()
        });
        task.await
            .map_err(|e| GitError::Command(format!("worker failed: {e}")))?
    }

    pub async fn shutdown(&self) {
        {
            let mut state = self.state.lock().expect("workers poisoned");
            state.closed = true;
            for work in state.active.values() {
                work.token.cancel();
            }
        }
        loop {
            let notified = self.changed.notified();
            if self
                .state
                .lock()
                .expect("workers poisoned")
                .active
                .is_empty()
            {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_are_atomic_and_released() {
        let budget = Budget::new("test", 10);
        let mut a = budget.reserve(6).unwrap();
        assert!(matches!(a.grow(5), Err(GitError::Quota(_))));
        assert_eq!(budget.used(), 6);
        {
            let _b = budget.reserve(4).unwrap();
            assert_eq!(budget.peak(), 10);
        }
        assert_eq!(budget.used(), 6);
        drop(a);
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn caller_drop_cancels_but_shutdown_joins_worker() {
        let workers = Arc::new(Workers::default());
        let work = Work::new(Duration::from_secs(10));
        let token = work.token.clone();
        let active = Arc::new(AtomicU64::new(0));
        let task_workers = workers.clone();
        let task_active = active.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _cancel = work.cancel_on_drop();
            let worker_work = work.clone();
            task_workers
                .run(work, move || {
                    task_active.store(1, Ordering::Release);
                    let _ = started_tx.send(());
                    while worker_work.check().is_ok() {
                        std::thread::yield_now();
                    }
                    task_active.store(0, Ordering::Release);
                    Ok(())
                })
                .await
        });
        started_rx.await.unwrap();
        task.abort();
        let _ = task.await;
        workers.shutdown().await;
        assert!(token.is_cancelled());
        assert_eq!(active.load(Ordering::Acquire), 0);
    }
}
