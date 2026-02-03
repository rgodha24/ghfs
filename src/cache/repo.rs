//! Repository cache manager
//!
//! This module provides the `RepoCache` struct which is the main cache manager
//! that ties together all cache operations: cloning, fetching, worktree management,
//! and symlink swapping.

use super::git::{is_shallow_repo, open_repository, resolve_default_branch};
use super::{CachePaths, GitCli, RepoLock, atomic_symlink_swap, is_stale};
use crate::types::{GenerationId, RepoKey};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

/// Errors returned by cache operations.
#[derive(Error, Debug)]
pub enum CacheError {
    /// A git operation failed.
    #[error("git error: {0}")]
    Git(#[from] super::git::GitError),
    /// An underlying IO operation failed.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    /// Failed to acquire the repository lock within the timeout.
    #[error("lock acquisition failed")]
    LockFailed,
    /// A generation directory name could not be parsed.
    #[error("invalid generation directory name: {0}")]
    InvalidGenerationName(String),
    /// The current symlink points at a missing target.
    #[error("symlink target does not exist: {0}")]
    SymlinkTargetMissing(PathBuf),
    /// The repository does not exist on GitHub.
    #[error("repository does not exist: {0}")]
    RepoNotFound(String),
}

/// A reference to a materialized repo generation.
#[derive(Debug, Clone)]
pub struct GenerationRef {
    /// Filesystem path to the generation worktree.
    pub path: PathBuf,
    /// Generation identifier for this worktree.
    pub generation: GenerationId,
    /// Resolved commit SHA for the generation's HEAD.
    pub commit: String,
}

/// Result of ensuring a repo is current, with refresh status.
#[derive(Debug, Clone)]
pub(crate) struct EnsureCurrentResult {
    /// The current generation reference.
    pub gen_ref: GenerationRef,
    /// Whether a refresh or clone was performed.
    pub refreshed: bool,
}

/// Cache manager for GitHub repositories.
pub struct RepoCache {
    paths: CachePaths,
    git: GitCli,
    max_age: Duration,
}

fn resolve_symlink_target(link_path: &Path, target: PathBuf) -> PathBuf {
    if target.is_absolute() {
        target
    } else if let Some(parent) = link_path.parent() {
        parent.join(&target)
    } else {
        target
    }
}

fn cleanup_worktree(path: &Path) {
    if std::fs::remove_dir_all(path).is_err() {
        let _ = std::fs::remove_file(path);
    }
}

impl RepoCache {
    /// Create a new cache manager rooted at the provided cache paths.
    pub fn new(paths: CachePaths) -> Self {
        Self {
            paths,
            git: GitCli::new(),
            max_age: Duration::from_secs(24 * 60 * 60), // 24 hours
        }
    }

    /// Set the maximum age for cached generations before refresh.
    pub fn with_max_age(mut self, max_age: Duration) -> Self {
        self.max_age = max_age;
        self
    }

    /// Return the cache paths used by this cache manager.
    pub fn paths(&self) -> &CachePaths {
        &self.paths
    }

    /// Force refresh a repo, ignoring staleness.
    /// Returns a reference to the new generation.
    ///
    /// This will:
    /// 1. Clone if not present
    /// 2. Fetch and create new generation if present
    pub fn force_refresh(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        let lock_path = self.paths.lock_path(key);
        let _lock = match RepoLock::acquire(&lock_path) {
            Ok(lock) => lock,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Err(CacheError::LockFailed);
            }
            Err(e) => return Err(CacheError::Io(e)),
        };

        let mirror_path = self.paths.mirror_dir(key);

        if !mirror_path.exists() {
            self.initial_clone(key)?;
        } else {
            self.refresh(key)?;
        }

        self.read_current_ref(key)
    }

    /// Ensure a repo is materialized and current.
    /// Returns a reference to the current generation.
    ///
    /// This will:
    /// 1. Clone if not present
    /// 2. Refresh if stale
    /// 3. Return existing if fresh
    pub fn ensure_current(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        self.ensure_current_with_status(key)
            .map(|result| result.gen_ref)
    }

    /// Ensure a repo is materialized and return its generation with refresh info.
    pub(crate) fn ensure_current_with_status(
        &self,
        key: &RepoKey,
    ) -> Result<EnsureCurrentResult, CacheError> {
        let current_link = self.paths.current_symlink(key);

        if current_link.exists() && !is_stale(&current_link, self.max_age) {
            if let Ok(current) = self.read_current_ref(key) {
                return Ok(EnsureCurrentResult {
                    gen_ref: current,
                    refreshed: false,
                });
            }
        }

        let lock_path = self.paths.lock_path(key);
        let _lock = match RepoLock::acquire(&lock_path) {
            Ok(lock) => lock,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Err(CacheError::LockFailed);
            }
            Err(e) => return Err(CacheError::Io(e)),
        };

        if current_link.exists() && !is_stale(&current_link, self.max_age) {
            if let Ok(current) = self.read_current_ref(key) {
                return Ok(EnsureCurrentResult {
                    gen_ref: current,
                    refreshed: false,
                });
            }
        }

        let mirror_path = self.paths.mirror_dir(key);

        let refresh_result = if !mirror_path.exists() {
            self.initial_clone(key)
        } else {
            self.refresh(key)
        };

        match refresh_result {
            Ok(()) => {
                let current = self.read_current_ref(key)?;
                Ok(EnsureCurrentResult {
                    gen_ref: current,
                    refreshed: true,
                })
            }
            Err(err) => {
                if let Ok(current) = self.read_current_ref(key) {
                    Ok(EnsureCurrentResult {
                        gen_ref: current,
                        refreshed: false,
                    })
                } else {
                    Err(err)
                }
            }
        }
    }

    fn initial_clone(&self, key: &RepoKey) -> Result<(), CacheError> {
        let mirror_path = self.paths.mirror_dir(key);

        if let Err(err) =
            self.git
                .clone_bare_shallow(key.owner.as_str(), key.repo.as_str(), &mirror_path)
        {
            let _ = std::fs::remove_dir_all(&mirror_path);
            return Err(err.into());
        }

        let repo = open_repository(&mirror_path)?;
        let (_branch, commit) = resolve_default_branch(&repo)?;

        let generation = self.next_generation(key);
        let gen_path = self.paths.generation_dir(key, generation);
        if let Err(err) = self.git.create_worktree(&mirror_path, &gen_path, &commit) {
            cleanup_worktree(&gen_path);
            return Err(err.into());
        }

        let current_link = self.paths.current_symlink(key);
        if let Err(err) = atomic_symlink_swap(&current_link, &gen_path) {
            cleanup_worktree(&gen_path);
            return Err(err.into());
        }

        Ok(())
    }

    fn refresh(&self, key: &RepoKey) -> Result<(), CacheError> {
        let mirror_path = self.paths.mirror_dir(key);
        let repo = open_repository(&mirror_path)?;
        let (branch, _old_commit) = resolve_default_branch(&repo)?;

        // Use full fetch if repo is not shallow, otherwise shallow fetch
        let is_shallow = is_shallow_repo(&mirror_path).unwrap_or(true);
        if is_shallow {
            self.git.fetch_shallow(&mirror_path, &branch)?;
        } else {
            self.git.fetch_full(&mirror_path, &branch)?;
        }

        let repo = open_repository(&mirror_path)?;
        let (_branch, commit) = resolve_default_branch(&repo)?;

        let next_gen = self.next_generation(key);
        let gen_path = self.paths.generation_dir(key, next_gen);

        if let Err(err) = self.git.create_worktree(&mirror_path, &gen_path, &commit) {
            cleanup_worktree(&gen_path);
            return Err(err.into());
        }

        let current_link = self.paths.current_symlink(key);
        if let Err(err) = atomic_symlink_swap(&current_link, &gen_path) {
            cleanup_worktree(&gen_path);
            return Err(err.into());
        }

        Ok(())
    }

    /// Unshallow a repository: fetch full history for the default branch.
    ///
    /// If the mirror doesn't exist, performs a full clone.
    /// If already not shallow, performs a normal full fetch.
    /// Creates a new generation only if no current generation exists.
    pub fn unshallow(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        let lock_path = self.paths.lock_path(key);
        let _lock = match RepoLock::acquire(&lock_path) {
            Ok(lock) => lock,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Err(CacheError::LockFailed);
            }
            Err(e) => return Err(CacheError::Io(e)),
        };

        let mirror_path = self.paths.mirror_dir(key);

        if !mirror_path.exists() {
            // Clone with full history
            if let Err(err) =
                self.git
                    .clone_bare_full(key.owner.as_str(), key.repo.as_str(), &mirror_path)
            {
                let _ = std::fs::remove_dir_all(&mirror_path);
                return Err(err.into());
            }
        } else {
            // Unshallow if needed, then fetch full
            let repo = open_repository(&mirror_path)?;
            let (branch, _) = resolve_default_branch(&repo)?;

            let is_shallow = is_shallow_repo(&mirror_path).unwrap_or(false);
            if is_shallow {
                self.git.fetch_unshallow(&mirror_path, &branch)?;
            } else {
                self.git.fetch_full(&mirror_path, &branch)?;
            }
        }

        // Ensure we have a current generation
        let current_link = self.paths.current_symlink(key);
        if !current_link.exists() {
            let repo = open_repository(&mirror_path)?;
            let (_, commit) = resolve_default_branch(&repo)?;

            let generation = self.next_generation(key);
            let gen_path = self.paths.generation_dir(key, generation);
            if let Err(err) = self.git.create_worktree(&mirror_path, &gen_path, &commit) {
                cleanup_worktree(&gen_path);
                return Err(err.into());
            }

            if let Err(err) = atomic_symlink_swap(&current_link, &gen_path) {
                cleanup_worktree(&gen_path);
                return Err(err.into());
            }
        }

        self.read_current_ref(key)
    }

    /// Reshallow a repository: convert back to depth=1 and run gc.
    ///
    /// If the mirror doesn't exist, performs a shallow clone.
    /// If already shallow, performs a normal shallow fetch.
    /// Creates a new generation only if no current generation exists.
    pub fn reshallow(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        let lock_path = self.paths.lock_path(key);
        let _lock = match RepoLock::acquire(&lock_path) {
            Ok(lock) => lock,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Err(CacheError::LockFailed);
            }
            Err(e) => return Err(CacheError::Io(e)),
        };

        let mirror_path = self.paths.mirror_dir(key);

        if !mirror_path.exists() {
            // Clone shallow
            if let Err(err) =
                self.git
                    .clone_bare_shallow(key.owner.as_str(), key.repo.as_str(), &mirror_path)
            {
                let _ = std::fs::remove_dir_all(&mirror_path);
                return Err(err.into());
            }
        } else {
            // Reshallow if needed
            let repo = open_repository(&mirror_path)?;
            let (branch, _) = resolve_default_branch(&repo)?;

            let is_shallow = is_shallow_repo(&mirror_path).unwrap_or(true);
            if is_shallow {
                self.git.fetch_shallow(&mirror_path, &branch)?;
            } else {
                self.git.fetch_reshallow(&mirror_path, &branch)?;
            }
        }

        // Ensure we have a current generation
        let current_link = self.paths.current_symlink(key);
        if !current_link.exists() {
            let repo = open_repository(&mirror_path)?;
            let (_, commit) = resolve_default_branch(&repo)?;

            let generation = self.next_generation(key);
            let gen_path = self.paths.generation_dir(key, generation);
            if let Err(err) = self.git.create_worktree(&mirror_path, &gen_path, &commit) {
                cleanup_worktree(&gen_path);
                return Err(err.into());
            }

            if let Err(err) = atomic_symlink_swap(&current_link, &gen_path) {
                cleanup_worktree(&gen_path);
                return Err(err.into());
            }
        }

        self.read_current_ref(key)
    }

    fn next_generation(&self, key: &RepoKey) -> GenerationId {
        // Find highest existing generation and increment
        let base = self.paths.worktree_base(key);
        let mut max = 0u64;

        if let Ok(entries) = std::fs::read_dir(&base) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(num_str) = name.strip_prefix("gen-") {
                        if let Ok(num) = num_str.parse::<u64>() {
                            max = max.max(num);
                        }
                    }
                }
            }
        }

        GenerationId::new(max + 1)
    }

    fn read_current_ref(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        let current_link = self.paths.current_symlink(key);
        let target = std::fs::read_link(&current_link)?;
        let target = resolve_symlink_target(&current_link, target);

        // Verify symlink target exists before trying to open it
        if !target.exists() {
            return Err(CacheError::SymlinkTargetMissing(target));
        }

        // Parse generation from path name - return error if invalid format
        let file_name = target
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| CacheError::InvalidGenerationName(target.display().to_string()))?;

        let gen_num = file_name
            .strip_prefix("gen-")
            .and_then(|s| s.parse::<u64>().ok())
            .ok_or_else(|| CacheError::InvalidGenerationName(file_name.to_string()))?;

        // Read commit from worktree's HEAD
        let repo = open_repository(&target)?;
        let head = repo.head().map_err(super::git::GitError::Git)?;
        let commit = head
            .peel_to_commit()
            .map_err(super::git::GitError::Git)?
            .id()
            .to_string();

        Ok(GenerationRef {
            path: target,
            generation: GenerationId::new(gen_num),
            commit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    fn network_tests_enabled() -> bool {
        match std::env::var("GHFS_RUN_NETWORK_TESTS") {
            Ok(value) => {
                let value = value.to_ascii_lowercase();
                value == "1" || value == "true" || value == "yes"
            }
            Err(_) => false,
        }
    }

    fn require_network() -> bool {
        if network_tests_enabled() {
            true
        } else {
            eprintln!("skipping network test (set GHFS_RUN_NETWORK_TESTS=1)");
            false
        }
    }

    #[test]
    fn next_generation_returns_1_for_empty_dir() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths);
        let key: RepoKey = "octocat/hello-world".parse().unwrap();
        let next_gen = cache.next_generation(&key);
        assert_eq!(next_gen.as_u64(), 1);
    }

    #[test]
    fn next_generation_returns_1_for_empty_worktree_base() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone());
        let key: RepoKey = "octocat/hello-world".parse().unwrap();
        fs::create_dir_all(paths.worktree_base(&key)).unwrap();

        let next_gen = cache.next_generation(&key);
        assert_eq!(next_gen.as_u64(), 1);
    }

    #[test]
    fn next_generation_increments_existing() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone());
        let key: RepoKey = "octocat/hello-world".parse().unwrap();
        let gen1_path = paths.generation_dir(&key, GenerationId::new(1));
        fs::create_dir_all(&gen1_path).unwrap();

        let next_gen = cache.next_generation(&key);
        assert_eq!(next_gen.as_u64(), 2);
    }

    #[test]
    fn next_generation_finds_max_with_gaps() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone());
        let key: RepoKey = "octocat/hello-world".parse().unwrap();
        let gen1_path = paths.generation_dir(&key, GenerationId::new(1));
        let gen3_path = paths.generation_dir(&key, GenerationId::new(3));
        fs::create_dir_all(&gen1_path).unwrap();
        fs::create_dir_all(&gen3_path).unwrap();

        let next_gen = cache.next_generation(&key);
        assert_eq!(next_gen.as_u64(), 4);
    }

    #[test]
    fn next_generation_ignores_non_gen_entries() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone());
        let key: RepoKey = "octocat/hello-world".parse().unwrap();
        let gen2_path = paths.generation_dir(&key, GenerationId::new(2));
        fs::create_dir_all(&gen2_path).unwrap();
        fs::create_dir_all(paths.worktree_base(&key).join("current")).unwrap();
        fs::create_dir_all(paths.worktree_base(&key).join("other")).unwrap();

        let next_gen = cache.next_generation(&key);
        assert_eq!(next_gen.as_u64(), 3);
    }

    #[test]
    fn next_generation_handles_large_numbers() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone());
        let key: RepoKey = "octocat/hello-world".parse().unwrap();
        let gen_path = paths.generation_dir(&key, GenerationId::new(999999));
        fs::create_dir_all(&gen_path).unwrap();

        let next_gen = cache.next_generation(&key);
        assert_eq!(next_gen.as_u64(), 1000000);
    }

    #[test]
    fn resolve_symlink_target_keeps_absolute_paths() {
        let link = Path::new("/cache/worktrees/owner/repo/current");
        let target = PathBuf::from("/cache/worktrees/owner/repo/gen-000001");
        let resolved = resolve_symlink_target(link, target.clone());
        assert_eq!(resolved, target);
    }

    #[test]
    fn resolve_symlink_target_resolves_relative_paths() {
        let link = Path::new("cache/worktrees/owner/repo/current");
        let target = PathBuf::from("gen-000001");
        let resolved = resolve_symlink_target(link, target);
        assert_eq!(
            resolved,
            PathBuf::from("cache/worktrees/owner/repo").join("gen-000001")
        );
    }

    #[test]
    fn ensure_current_clones_new_repo() {
        if !require_network() {
            return;
        }
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone());
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();

        let result = cache.ensure_current(&key);
        assert!(result.is_ok(), "ensure_current failed: {:?}", result.err());

        let gen_ref = result.unwrap();
        assert_eq!(gen_ref.generation.as_u64(), 1);
        assert_eq!(gen_ref.commit.len(), 40);
        assert!(gen_ref.commit.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(gen_ref.path.exists());
        assert!(gen_ref.path.join("README").exists());
        let current_link = paths.current_symlink(&key);
        assert!(current_link.exists());
        let target = fs::read_link(&current_link).unwrap();
        assert!(
            target.to_str().unwrap().contains("gen-000001"),
            "Expected target to contain 'gen-000001', got: {:?}",
            target
        );
    }

    #[test]
    fn ensure_current_returns_immediately_if_fresh() {
        if !require_network() {
            return;
        }
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone()).with_max_age(Duration::from_secs(3600));
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();
        let result1 = cache.ensure_current(&key);
        assert!(result1.is_ok());
        let gen_ref1 = result1.unwrap();
        let result2 = cache.ensure_current(&key);
        assert!(result2.is_ok());
        let gen_ref2 = result2.unwrap();
        assert_eq!(gen_ref1.generation.as_u64(), gen_ref2.generation.as_u64());
        assert_eq!(gen_ref1.commit, gen_ref2.commit);
        assert_eq!(gen_ref1.path, gen_ref2.path);
    }

    #[test]
    fn ensure_current_refreshes_if_stale() {
        if !require_network() {
            return;
        }
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone()).with_max_age(Duration::from_secs(0));
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();
        let result1 = cache.ensure_current(&key);
        assert!(result1.is_ok());
        let gen_ref1 = result1.unwrap();
        assert_eq!(gen_ref1.generation.as_u64(), 1);
        let result2 = cache.ensure_current(&key);
        assert!(result2.is_ok());
        let gen_ref2 = result2.unwrap();
        assert_eq!(gen_ref2.generation.as_u64(), 2);
        let gen1_path = paths.generation_dir(&key, GenerationId::new(1));
        let gen2_path = paths.generation_dir(&key, GenerationId::new(2));
        assert!(gen1_path.exists());
        assert!(gen2_path.exists());
        let current_link = paths.current_symlink(&key);
        let target = fs::read_link(&current_link).unwrap();
        assert!(
            target.to_str().unwrap().contains("gen-000002"),
            "Expected target to contain 'gen-000002', got: {:?}",
            target
        );
    }

    #[test]
    fn concurrent_ensure_current_doesnt_corrupt() {
        if !require_network() {
            return;
        }
        use std::sync::Arc;
        use std::thread;
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = Arc::new(RepoCache::new(paths.clone()));
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let key = key.clone();
                thread::spawn(move || cache.ensure_current(&key))
            })
            .collect();
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        for (i, result) in results.iter().enumerate() {
            assert!(
                result.is_ok(),
                "Thread {} failed: {:?}",
                i,
                result.as_ref().err()
            );
        }
        let worktree_base = paths.worktree_base(&key);
        let gen_dirs: Vec<_> = fs::read_dir(&worktree_base)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|s| s.starts_with("gen-"))
            })
            .collect();
        assert_eq!(
            gen_dirs.len(),
            1,
            "Expected 1 generation directory, found {}: {:?}",
            gen_dirs.len(),
            gen_dirs.iter().map(|e| e.file_name()).collect::<Vec<_>>()
        );
        let gen_name = gen_dirs[0].file_name();
        assert_eq!(
            gen_name.to_str().unwrap(),
            "gen-000001",
            "Expected gen-000001, got {:?}",
            gen_name
        );
        let current_link = paths.current_symlink(&key);
        assert!(current_link.exists(), "Current symlink does not exist");
        let target = fs::read_link(&current_link).unwrap();
        assert!(
            target.exists(),
            "Symlink target does not exist: {:?}",
            target
        );
        assert!(
            target.join("README").exists(),
            "README file not found in worktree"
        );
    }
}
