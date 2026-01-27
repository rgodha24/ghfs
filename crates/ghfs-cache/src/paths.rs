//! Cache directory path management
//!
//! This module provides the `CachePaths` struct which manages all filesystem paths
//! for the cache directory layout:
//!
//! ```text
//! ~/.cache/ghfs/
//! ├── mirrors/
//! │   └── <owner>/
//! │       └── <repo>.git/           # Bare repo (shallow)
//! ├── worktrees/
//! │   └── <owner>/
//! │       └── <repo>/
//! │           ├── gen-000001/       # Immutable generation
//! │           └── current -> gen-...  # Atomic symlink
//! └── locks/
//!     └── <owner>__<repo>.lock      # flock-based locking
//! ```

use std::path::{Path, PathBuf};

use ghfs_types::{GenerationId, RepoKey};

/// Manages all filesystem paths for the cache directory layout
#[derive(Debug, Clone)]
pub struct CachePaths {
    root: PathBuf,
}

impl CachePaths {
    /// Creates a new CachePaths with the specified root directory
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Returns the root cache directory
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the mirrors directory path: `{root}/mirrors`
    pub fn mirrors_dir(&self) -> PathBuf {
        self.root.join("mirrors")
    }

    /// Returns the worktrees directory path: `{root}/worktrees`
    pub fn worktrees_dir(&self) -> PathBuf {
        self.root.join("worktrees")
    }

    /// Returns the locks directory path: `{root}/locks`
    pub fn locks_dir(&self) -> PathBuf {
        self.root.join("locks")
    }

    /// Returns the mirror directory for a specific repository: `{root}/mirrors/{owner}/{repo}.git`
    pub fn mirror_dir(&self, key: &RepoKey) -> PathBuf {
        let repo = key.repo.as_str();
        let mut path = self.mirrors_dir().join(key.owner.as_str());

        // Avoid double ".git" suffix if the repo name already ends with ".git".
        if repo.ends_with(".git") {
            path.push(repo);
        } else {
            path.push(format!("{}.git", repo));
        }

        path
    }

    /// Returns the worktree base directory for a repository: `{root}/worktrees/{owner}/{repo}`
    pub fn worktree_base(&self, key: &RepoKey) -> PathBuf {
        self.worktrees_dir()
            .join(key.owner.as_str())
            .join(key.repo.as_str())
    }

    /// Returns the generation directory: `{root}/worktrees/{owner}/{repo}/gen-NNNNNN`
    pub fn generation_dir(&self, key: &RepoKey, generation: GenerationId) -> PathBuf {
        self.worktree_base(key)
            .join(format!("gen-{:0>6}", generation.as_u64()))
    }

    /// Returns the current symlink path: `{root}/worktrees/{owner}/{repo}/current`
    pub fn current_symlink(&self, key: &RepoKey) -> PathBuf {
        self.worktree_base(key).join("current")
    }

    /// Returns the lock file path: `{root}/locks/{owner}__{repo}.lock`
    pub fn lock_path(&self, key: &RepoKey) -> PathBuf {
        self.locks_dir().join(format!(
            "{}__{}.lock",
            key.owner.as_str(),
            key.repo.as_str()
        ))
    }
}

impl Default for CachePaths {
    /// Creates a CachePaths using the system cache directory + "ghfs"
    ///
    /// Uses `dirs::cache_dir()` which resolves to:
    /// - Linux: `~/.cache`
    /// - macOS: `~/Library/Caches`
    /// - Windows: `C:\Users\<user>\AppData\Local`
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
    fn test_new() {
        let paths = CachePaths::new("tmp-cache");
        assert_eq!(paths.root(), Path::new("tmp-cache"));
    }

    #[test]
    fn test_new_from_pathbuf() {
        let paths = CachePaths::new(PathBuf::from("tmp-cache"));
        assert_eq!(paths.root(), Path::new("tmp-cache"));
    }

    #[test]
    fn test_default() {
        let paths = CachePaths::default();
        // Should end with "ghfs"
        assert!(paths.root().ends_with("ghfs"));
    }

    #[test]
    fn test_root() {
        let paths = test_paths();
        assert_eq!(paths.root(), Path::new("cache-root"));
    }

    #[test]
    fn test_mirrors_dir() {
        let paths = test_paths();
        assert_eq!(paths.mirrors_dir(), test_root_path().join("mirrors"));
    }

    #[test]
    fn test_worktrees_dir() {
        let paths = test_paths();
        assert_eq!(paths.worktrees_dir(), test_root_path().join("worktrees"));
    }

    #[test]
    fn test_locks_dir() {
        let paths = test_paths();
        assert_eq!(paths.locks_dir(), test_root_path().join("locks"));
    }

    #[test]
    fn test_mirror_dir() {
        let paths = test_paths();
        let key = test_repo_key();
        assert_eq!(
            paths.mirror_dir(&key),
            test_root_path()
                .join("mirrors")
                .join("octocat")
                .join("hello-world.git")
        );
    }

    #[test]
    fn test_mirror_dir_with_hyphen() {
        let paths = test_paths();
        let key: RepoKey = "my-org/my-repo".parse().unwrap();
        assert_eq!(
            paths.mirror_dir(&key),
            test_root_path()
                .join("mirrors")
                .join("my-org")
                .join("my-repo.git")
        );
    }

    #[test]
    fn test_mirror_dir_with_git_suffix() {
        let paths = test_paths();
        let key: RepoKey = "my-org/my-repo.git".parse().unwrap();
        assert_eq!(
            paths.mirror_dir(&key),
            test_root_path()
                .join("mirrors")
                .join("my-org")
                .join("my-repo.git")
        );
    }

    #[test]
    fn test_worktree_base() {
        let paths = test_paths();
        let key = test_repo_key();
        assert_eq!(
            paths.worktree_base(&key),
            test_root_path()
                .join("worktrees")
                .join("octocat")
                .join("hello-world")
        );
    }

    #[test]
    fn test_generation_dir() {
        let paths = test_paths();
        let key = test_repo_key();
        let generation = GenerationId::new(1);
        assert_eq!(
            paths.generation_dir(&key, generation),
            test_root_path()
                .join("worktrees")
                .join("octocat")
                .join("hello-world")
                .join("gen-000001")
        );
    }

    #[test]
    fn test_generation_dir_large_number() {
        let paths = test_paths();
        let key = test_repo_key();
        let generation = GenerationId::new(123456);
        assert_eq!(
            paths.generation_dir(&key, generation),
            test_root_path()
                .join("worktrees")
                .join("octocat")
                .join("hello-world")
                .join("gen-123456")
        );
    }

    #[test]
    fn test_generation_dir_overflow_padding() {
        let paths = test_paths();
        let key = test_repo_key();
        let generation = GenerationId::new(1234567);
        assert_eq!(
            paths.generation_dir(&key, generation),
            test_root_path()
                .join("worktrees")
                .join("octocat")
                .join("hello-world")
                .join("gen-1234567")
        );
    }

    #[test]
    fn test_current_symlink() {
        let paths = test_paths();
        let key = test_repo_key();
        assert_eq!(
            paths.current_symlink(&key),
            test_root_path()
                .join("worktrees")
                .join("octocat")
                .join("hello-world")
                .join("current")
        );
    }

    #[test]
    fn test_lock_path() {
        let paths = test_paths();
        let key = test_repo_key();
        assert_eq!(
            paths.lock_path(&key),
            test_root_path()
                .join("locks")
                .join("octocat__hello-world.lock")
        );
    }

    #[test]
    fn test_lock_path_with_special_chars() {
        let paths = test_paths();
        let key: RepoKey = "my-org/my_repo.v2".parse().unwrap();
        assert_eq!(
            paths.lock_path(&key),
            test_root_path()
                .join("locks")
                .join("my-org__my_repo.v2.lock")
        );
    }

    #[test]
    fn test_clone() {
        let paths = test_paths();
        let cloned = paths.clone();
        assert_eq!(paths.root(), cloned.root());
    }
}
