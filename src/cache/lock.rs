use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use fs2::FileExt;

/// Default lock timeout (5 minutes) - prevents indefinite hangs
const DEFAULT_LOCK_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// A guard that holds an exclusive lock on a repo.
/// Lock is released when dropped.
#[derive(Debug)]
pub struct RepoLock {
    file: File,
}

impl RepoLock {
    /// Acquire an exclusive lock for a repo, blocking until available or timeout.
    /// Creates the lock file and parent dirs if needed.
    ///
    /// Uses a default timeout of 5 minutes to prevent indefinite hangs.
    pub fn acquire(lock_path: &Path) -> io::Result<Self> {
        Self::acquire_with_timeout(lock_path, DEFAULT_LOCK_TIMEOUT)
    }

    /// Acquire an exclusive lock with a custom timeout.
    /// Returns an error with `ErrorKind::TimedOut` if the lock cannot be acquired
    /// within the specified duration.
    pub fn acquire_with_timeout(lock_path: &Path, timeout: Duration) -> io::Result<Self> {
        // Create parent dirs
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open/create lock file
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;

        // Try to acquire lock with polling and exponential backoff
        let start = Instant::now();
        let mut sleep_duration = Duration::from_millis(10);
        let max_sleep = Duration::from_millis(500);

        loop {
            match file.try_lock_exclusive() {
                Ok(()) => return Ok(Self { file }),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // Lock is held by another process, check timeout
                    if start.elapsed() >= timeout {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            format!("lock acquisition timed out after {:?}", timeout),
                        ));
                    }
                    // Sleep with exponential backoff
                    std::thread::sleep(sleep_duration);
                    sleep_duration = (sleep_duration * 2).min(max_sleep);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Try to acquire lock without blocking.
    /// Returns None if lock is held by another process.
    pub fn try_acquire(lock_path: &Path) -> io::Result<Option<Self>> {
        if let Some(parent) = lock_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Some(Self { file })),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Drop for RepoLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;

    #[test]
    fn test_acquire_creates_lock_file() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("subdir").join("repo.lock");

        assert!(!lock_path.exists());

        let lock = RepoLock::acquire(&lock_path).unwrap();

        assert!(lock_path.exists());

        drop(lock);
    }

    #[test]
    fn test_try_acquire_returns_none_when_locked() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("repo.lock");

        let _lock = RepoLock::acquire(&lock_path).unwrap();
        let result = RepoLock::try_acquire(&lock_path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_lock_released_on_drop() {
        let dir = tempdir().unwrap();
        let lock_path = dir.path().join("repo.lock");

        {
            let _lock = RepoLock::acquire(&lock_path).unwrap();
            assert!(RepoLock::try_acquire(&lock_path).unwrap().is_none());
        }
        let lock = RepoLock::try_acquire(&lock_path).unwrap();
        assert!(lock.is_some());
    }

    #[test]
    fn test_two_threads_cannot_hold_same_lock() {
        let dir = tempdir().unwrap();
        let lock_path = Arc::new(dir.path().join("repo.lock"));
        let barrier = Arc::new(Barrier::new(2));

        let lock_path_clone = Arc::clone(&lock_path);
        let barrier_clone = Arc::clone(&barrier);

        let handle1 = thread::spawn(move || {
            let lock = RepoLock::acquire(&lock_path_clone).unwrap();
            barrier_clone.wait();
            thread::sleep(std::time::Duration::from_millis(100));
            drop(lock);
        });

        let handle2 = thread::spawn(move || {
            barrier.wait();
            let result = RepoLock::try_acquire(&lock_path).unwrap();
            assert!(
                result.is_none(),
                "Should not be able to acquire lock held by another thread"
            );
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    #[test]
    fn test_acquire_with_timeout_times_out() {
        let dir = tempdir().unwrap();
        let lock_path = Arc::new(dir.path().join("repo.lock"));
        let barrier = Arc::new(Barrier::new(2));

        let lock_path_clone = Arc::clone(&lock_path);
        let barrier_clone = Arc::clone(&barrier);

        let handle1 = thread::spawn(move || {
            let lock = RepoLock::acquire(&lock_path_clone).unwrap();
            barrier_clone.wait();
            thread::sleep(Duration::from_millis(500));
            drop(lock);
        });

        let handle2 = thread::spawn(move || {
            barrier.wait();
            let result = RepoLock::acquire_with_timeout(&lock_path, Duration::from_millis(100));
            assert!(result.is_err(), "Should have timed out");
            let err = result.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }

    #[test]
    fn test_acquire_with_timeout_succeeds_when_lock_released() {
        let dir = tempdir().unwrap();
        let lock_path = Arc::new(dir.path().join("repo.lock"));
        let barrier = Arc::new(Barrier::new(2));

        let lock_path_clone = Arc::clone(&lock_path);
        let barrier_clone = Arc::clone(&barrier);

        let handle1 = thread::spawn(move || {
            let lock = RepoLock::acquire(&lock_path_clone).unwrap();
            barrier_clone.wait();
            thread::sleep(Duration::from_millis(50));
            drop(lock);
        });

        let handle2 = thread::spawn(move || {
            barrier.wait();
            let result = RepoLock::acquire_with_timeout(&lock_path, Duration::from_secs(2));
            assert!(
                result.is_ok(),
                "Should have acquired lock after thread 1 released"
            );
        });

        handle1.join().unwrap();
        handle2.join().unwrap();
    }
}
