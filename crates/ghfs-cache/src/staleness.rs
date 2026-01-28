//! Staleness checking for cached repositories.

use std::fs;
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime};

/// Check if a cache entry is stale based on symlink modification time.
///
/// Returns true if:
/// - The symlink doesn't exist
/// - The symlink is older than `max_age`
/// - We can't read the symlink's metadata (treat as stale to be safe)
pub fn is_stale(current_symlink: &Path, max_age: Duration) -> bool {
    let metadata = match fs::symlink_metadata(current_symlink) {
        Ok(m) => m,
        Err(_) => return true, // Doesn't exist or can't read -> stale
    };

    let modified = match metadata.modified() {
        Ok(t) => t,
        Err(_) => return true, // Can't get mtime -> stale
    };

    let now = SystemTime::now();
    let threshold = now.checked_sub(max_age).unwrap_or(SystemTime::UNIX_EPOCH);

    modified < threshold
}

/// Touch a symlink to update its modification time to now.
/// Used after refresh to reset the staleness clock.
///
/// Uses atomic swap to avoid race conditions: creates a new symlink at a temp path,
/// then atomically renames it over the original.
pub fn touch_symlink(link_path: &Path) -> io::Result<()> {
    use crate::swap::atomic_symlink_swap;

    // Read the current target
    let target = fs::read_link(link_path)?;

    // Use atomic swap to recreate the symlink with a fresh mtime
    // This avoids TOCTOU race conditions between remove and create
    atomic_symlink_swap(link_path, &target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_is_stale_nonexistent_path() {
        let path = Path::new("/nonexistent/path/to/symlink");
        assert!(is_stale(path, Duration::from_secs(60)));
    }

    #[test]
    #[cfg(unix)]
    fn test_is_stale_fresh_symlink() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        fs::create_dir(&target).unwrap();

        let link = dir.path().join("current");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        // Just created, should not be stale with 1 hour max age
        assert!(!is_stale(&link, Duration::from_secs(3600)));
    }

    #[test]
    #[cfg(unix)]
    fn test_is_stale_old_symlink() {
        use filetime::{FileTime, set_symlink_file_times};

        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        fs::create_dir(&target).unwrap();

        let link = dir.path().join("current");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        // Set mtime to 2 hours ago
        let two_hours_ago = SystemTime::now() - Duration::from_secs(7200);
        let ft = FileTime::from_system_time(two_hours_ago);
        set_symlink_file_times(&link, ft, ft).unwrap();

        // Should be stale with 1 hour max age
        assert!(is_stale(&link, Duration::from_secs(3600)));

        // Should not be stale with 3 hour max age
        assert!(!is_stale(&link, Duration::from_secs(10800)));
    }

    #[test]
    #[cfg(unix)]
    fn test_touch_symlink_updates_mtime() {
        use filetime::{FileTime, set_symlink_file_times};

        let dir = tempdir().unwrap();
        let target = dir.path().join("target");
        fs::create_dir(&target).unwrap();

        let link = dir.path().join("current");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        // Set mtime to 2 hours ago
        let two_hours_ago = SystemTime::now() - Duration::from_secs(7200);
        let ft = FileTime::from_system_time(two_hours_ago);
        set_symlink_file_times(&link, ft, ft).unwrap();

        // Verify it's stale
        assert!(is_stale(&link, Duration::from_secs(3600)));

        // Touch it
        touch_symlink(&link).unwrap();

        // Should no longer be stale
        assert!(!is_stale(&link, Duration::from_secs(3600)));

        // Verify symlink still points to same target
        assert_eq!(fs::read_link(&link).unwrap(), target);
    }
}
