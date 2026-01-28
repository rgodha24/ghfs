//! Atomic symlink swapping for generation switching.

use std::io;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Global counter for unique temp file names within a process
static SWAP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Atomically swap a symlink to point to a new target.
///
/// This is atomic on POSIX: creates a temp symlink, then renames it.
/// The rename is atomic, so readers never see a broken state.
pub fn atomic_symlink_swap(link_path: &Path, new_target: &Path) -> io::Result<()> {
    loop {
        // Generate a unique temp symlink path: {link_path}.tmp.{pid}.{counter}
        // Using PID + atomic counter guarantees uniqueness across threads.
        // Retry if the temp path already exists (e.g., from a prior crash).
        let counter = SWAP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let unique_id = format!("{}.{}", std::process::id(), counter);
        let temp_path = link_path.with_extension(format!("tmp.{}", unique_id));

        // Create symlink at temp path pointing to new_target
        match symlink(new_target, &temp_path) {
            Ok(()) => {
                // Rename temp symlink to link_path (atomic on POSIX)
                // If rename fails, clean up the temp symlink
                if let Err(e) = std::fs::rename(&temp_path, link_path) {
                    let _ = std::fs::remove_file(&temp_path);
                    return Err(e);
                }

                return Ok(());
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Read the target of a symlink, returning None if it doesn't exist.
pub fn read_symlink_target(link_path: &Path) -> io::Result<Option<PathBuf>> {
    match std::fs::read_link(link_path) {
        Ok(target) => Ok(Some(target)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_atomic_symlink_swap_creates_working_symlink() {
        let temp_dir = TempDir::new().unwrap();
        let target = temp_dir.path().join("target");
        let link = temp_dir.path().join("current");

        std::fs::create_dir(&target).unwrap();
        atomic_symlink_swap(&link, &target).unwrap();
        assert!(link.is_symlink());
        assert_eq!(std::fs::read_link(&link).unwrap(), target);
    }

    #[test]
    fn test_atomic_symlink_swap_updates_existing_symlink() {
        let temp_dir = TempDir::new().unwrap();
        let target1 = temp_dir.path().join("target1");
        let target2 = temp_dir.path().join("target2");
        let link = temp_dir.path().join("current");

        std::fs::create_dir(&target1).unwrap();
        std::fs::create_dir(&target2).unwrap();
        atomic_symlink_swap(&link, &target1).unwrap();
        assert_eq!(std::fs::read_link(&link).unwrap(), target1);
        atomic_symlink_swap(&link, &target2).unwrap();
        assert_eq!(std::fs::read_link(&link).unwrap(), target2);
    }

    #[test]
    fn test_concurrent_swaps_dont_corrupt() {
        let temp_dir = TempDir::new().unwrap();
        let link = temp_dir.path().join("current");

        let num_threads = 10;
        let num_iterations = 100;
        let targets: Vec<PathBuf> = (0..num_threads)
            .map(|i| {
                let target = temp_dir.path().join(format!("target_{}", i));
                std::fs::create_dir(&target).unwrap();
                target
            })
            .collect();
        atomic_symlink_swap(&link, &targets[0]).unwrap();
        let barrier = Arc::new(Barrier::new(num_threads));
        let link = Arc::new(link);
        let targets = Arc::new(targets);

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let barrier = Arc::clone(&barrier);
                let link = Arc::clone(&link);
                let targets = Arc::clone(&targets);

                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..num_iterations {
                        let target = &targets[i];
                        // Ignore errors from concurrent renames - the important thing
                        // is that the symlink is never in a broken state
                        let _ = atomic_symlink_swap(&link, target);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        assert!(link.is_symlink());
        let final_target = std::fs::read_link(link.as_ref()).unwrap();
        assert!(targets.contains(&final_target));
    }

    #[test]
    fn test_atomic_symlink_swap_retries_on_existing_temp_path() {
        let temp_dir = TempDir::new().unwrap();
        let target1 = temp_dir.path().join("target1");
        let target2 = temp_dir.path().join("target2");
        let link = temp_dir.path().join("current");

        std::fs::create_dir(&target1).unwrap();
        std::fs::create_dir(&target2).unwrap();

        SWAP_COUNTER.store(0, Ordering::Relaxed);
        let pid = std::process::id();
        let temp_path = link.with_extension(format!("tmp.{}.0", pid));
        std::os::unix::fs::symlink(&target1, &temp_path).unwrap();
        atomic_symlink_swap(&link, &target2).unwrap();
        assert_eq!(std::fs::read_link(&link).unwrap(), target2);
        let _ = std::fs::remove_file(&temp_path);
    }
}
