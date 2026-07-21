//! Cache directory path management.
//!
//! Layout after the by-ref redesign:
//!
//! ```text
//! ~/.cache/ghfs/
//! ├── mirrors/
//! │   └── <owner>/
//! │       └── <repo>.git/        # Blobless bare mirror (commits + trees)
//! ├── blobs/
//! │   └── <algo>/                # Content-addressed hydrated blobs (shared)
//! │       └── <oid>
//! └── locks/
//!     └── <owner>__<repo>.lock   # flock-based per-repo serialization
//! ```
//!
//! There are no per-generation worktree directories and no `current` symlink:
//! each commit's tree is served directly from git objects, and blobs live in a
//! global content-addressed cache shared across all repos and refs.

use std::path::{Path, PathBuf};

use crate::types::RepoKey;

/// Manages all filesystem paths for the cache directory layout.
#[derive(Debug, Clone)]
pub struct CachePaths {
    root: PathBuf,
}

impl CachePaths {
    /// Creates a new `CachePaths` with the specified root directory.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Returns the root cache directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the mirrors directory path: `{root}/mirrors`.
    pub fn mirrors_dir(&self) -> PathBuf {
        self.root.join("mirrors")
    }

    /// Returns the blobs directory path: `{root}/blobs` (content-addressed
    /// shared blob cache; subdirectories by hash algorithm live below this).
    pub fn blobs_dir(&self) -> PathBuf {
        self.root.join("blobs")
    }

    /// Returns the locks directory path: `{root}/locks`.
    pub fn locks_dir(&self) -> PathBuf {
        self.root.join("locks")
    }

    /// Returns the mirror directory for a specific repository:
    /// `{root}/mirrors/{owner}/{repo}.git`.
    pub fn mirror_dir(&self, key: &RepoKey) -> PathBuf {
        let repo = key.repo.as_str();
        let mut path = self.mirrors_dir().join(key.owner.as_str());
        // Avoid double ".git" suffix if the repo name already ends with it.
        if repo.ends_with(".git") {
            path.push(repo);
        } else {
            path.push(format!("{}.git", repo));
        }
        path
    }

    /// Returns the lock file path: `{root}/locks/{owner}__{repo}.lock`.
    pub fn lock_path(&self, key: &RepoKey) -> PathBuf {
        self.locks_dir().join(format!(
            "{}__{}.lock",
            key.owner.as_str(),
            key.repo.as_str()
        ))
    }
}

impl Default for CachePaths {
    /// Creates a `CachePaths` using the system cache directory + "ghfs".
    ///
    /// Resolves via `dirs::cache_dir()`:
    /// - Linux: `~/.cache`
    /// - macOS: `~/Library/Caches`
    fn default() -> Self {
        let cache_dir = dirs::cache_dir().unwrap_or_else(|| PathBuf::from(".cache"));
        Self::new(cache_dir.join("ghfs"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_root_path() -> PathBuf {
        PathBuf::from("cache-root")
    }

    fn test_paths() -> CachePaths {
        CachePaths::new(test_root_path())
    }

    fn test_repo_key() -> RepoKey {
        "octocat/hello-world".parse().unwrap()
    }

    #[test]
    fn test_mirrors_dir() {
        assert_eq!(test_paths().mirrors_dir(), test_root_path().join("mirrors"));
    }

    #[test]
    fn test_blobs_dir() {
        assert_eq!(test_paths().blobs_dir(), test_root_path().join("blobs"));
    }

    #[test]
    fn test_locks_dir() {
        assert_eq!(test_paths().locks_dir(), test_root_path().join("locks"));
    }

    #[test]
    fn test_mirror_dir() {
        let key = test_repo_key();
        assert_eq!(
            test_paths().mirror_dir(&key),
            test_root_path()
                .join("mirrors")
                .join("octocat")
                .join("hello-world.git")
        );
    }

    #[test]
    fn test_mirror_dir_with_git_suffix() {
        let key: RepoKey = "my-org/my-repo.git".parse().unwrap();
        assert_eq!(
            test_paths().mirror_dir(&key),
            test_root_path()
                .join("mirrors")
                .join("my-org")
                .join("my-repo.git")
        );
    }

    #[test]
    fn test_lock_path() {
        let key = test_repo_key();
        assert_eq!(
            test_paths().lock_path(&key),
            test_root_path()
                .join("locks")
                .join("octocat__hello-world.lock")
        );
    }
}
