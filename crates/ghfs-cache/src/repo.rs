//! Repository cache manager
//!
//! This module provides the `RepoCache` struct which is the main cache manager
//! that ties together all cache operations: cloning, fetching, worktree management,
//! and symlink swapping.

use crate::git::{open_repository, resolve_default_branch};
use crate::{atomic_symlink_swap, is_stale, CachePaths, GitCli, RepoLock};
use ghfs_types::{GenerationId, RepoKey};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;

/// Errors returned by cache operations.
#[derive(Error, Debug)]
pub enum CacheError {
    /// A git operation failed.
    #[error("git error: {0}")]
    Git(#[from] crate::git::GitError),
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

    /// Ensure a repo is materialized and current.
    /// Returns a reference to the current generation.
    ///
    /// This will:
    /// 1. Clone if not present
    /// 2. Refresh if stale
    /// 3. Return existing if fresh
    pub fn ensure_current(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        let current_link = self.paths.current_symlink(key);

        // Fast path: if exists and not stale, return immediately (no lock needed)
        if current_link.exists() && !is_stale(&current_link, self.max_age) {
            if let Ok(current) = self.read_current_ref(key) {
                return Ok(current);
            }
        }

        // Slow path: need to materialize or refresh
        let lock_path = self.paths.lock_path(key);
        let _lock = match RepoLock::acquire(&lock_path) {
            Ok(lock) => lock,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => return Err(CacheError::LockFailed),
            Err(e) => return Err(CacheError::Io(e)),
        };

        // Re-check under lock (another process may have updated)
        if current_link.exists() && !is_stale(&current_link, self.max_age) {
            if let Ok(current) = self.read_current_ref(key) {
                return Ok(current);
            }
        }

        let mirror_path = self.paths.mirror_dir(key);

        let refresh_result = if !mirror_path.exists() {
            // First time: clone + create initial worktree
            self.initial_clone(key)
        } else {
            // Refresh: fetch + create new worktree
            self.refresh(key)
        };

        match refresh_result {
            Ok(()) => self.read_current_ref(key),
            Err(err) => {
                if let Ok(current) = self.read_current_ref(key) {
                    Ok(current)
                } else {
                    Err(err)
                }
            }
        }
    }

    fn initial_clone(&self, key: &RepoKey) -> Result<(), CacheError> {
        let mirror_path = self.paths.mirror_dir(key);

        // Clone bare shallow
        if let Err(err) = self
            .git
            .clone_bare_shallow(key.owner.as_str(), key.repo.as_str(), &mirror_path)
        {
            let _ = std::fs::remove_dir_all(&mirror_path);
            return Err(err.into());
        }

        // Get HEAD commit
        let repo = open_repository(&mirror_path)?;
        let (_branch, commit) = resolve_default_branch(&repo)?;

        // Create first generation worktree
        let generation = self.next_generation(key);
        let gen_path = self.paths.generation_dir(key, generation);
        if let Err(err) = self.git.create_worktree(&mirror_path, &gen_path, &commit) {
            cleanup_worktree(&gen_path);
            return Err(err.into());
        }

        // Set current symlink
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

        // Fetch latest
        self.git.fetch_shallow(&mirror_path, &branch)?;

        // Re-read commit after fetch
        let repo = open_repository(&mirror_path)?;
        let (_branch, commit) = resolve_default_branch(&repo)?;

        // Determine next generation number
        let next_gen = self.next_generation(key);
        let gen_path = self.paths.generation_dir(key, next_gen);

        // Create new worktree
        if let Err(err) = self.git.create_worktree(&mirror_path, &gen_path, &commit) {
            cleanup_worktree(&gen_path);
            return Err(err.into());
        }

        // Swap current
        let current_link = self.paths.current_symlink(key);
        if let Err(err) = atomic_symlink_swap(&current_link, &gen_path) {
            cleanup_worktree(&gen_path);
            return Err(err.into());
        }

        Ok(())
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
        let head = repo.head().map_err(crate::git::GitError::Git)?;
        let commit = head
            .peel_to_commit()
            .map_err(crate::git::GitError::Git)?
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

    // -------------------------------------------------------------------------
    // Unit Tests for next_generation logic
    // -------------------------------------------------------------------------

    #[test]
    fn next_generation_returns_1_for_empty_dir() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths);
        let key: RepoKey = "octocat/hello-world".parse().unwrap();

        // No worktree base exists yet
        let next_gen = cache.next_generation(&key);
        assert_eq!(next_gen.as_u64(), 1);
    }

    #[test]
    fn next_generation_returns_1_for_empty_worktree_base() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone());
        let key: RepoKey = "octocat/hello-world".parse().unwrap();

        // Create empty worktree base
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

        // Create worktree base with gen-000001
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

        // Create worktree base with gen-000001 and gen-000003 (gap at 2)
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

        // Create worktree base with gen-000002 and some other entries
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

        // Create worktree base with gen-999999
        let gen_path = paths.generation_dir(&key, GenerationId::new(999999));
        fs::create_dir_all(&gen_path).unwrap();

        let next_gen = cache.next_generation(&key);
        assert_eq!(next_gen.as_u64(), 1000000);
    }

    // -------------------------------------------------------------------------
    // Unit Tests for RepoCache construction
    // -------------------------------------------------------------------------

    #[test]
    fn repo_cache_new_sets_default_max_age() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths);

        // Default is 24 hours
        assert_eq!(cache.max_age, Duration::from_secs(24 * 60 * 60));
    }

    #[test]
    fn repo_cache_with_max_age_sets_custom() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths).with_max_age(Duration::from_secs(3600));

        assert_eq!(cache.max_age, Duration::from_secs(3600));
    }

    // -------------------------------------------------------------------------
    // Unit Tests for symlink target resolution
    // -------------------------------------------------------------------------

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

    // -------------------------------------------------------------------------
    // Integration Tests (require network access)
    // -------------------------------------------------------------------------

    #[test]
    #[ignore] // Requires network access
    fn ensure_current_clones_new_repo() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = RepoCache::new(paths.clone());
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();

        let result = cache.ensure_current(&key);
        assert!(result.is_ok(), "ensure_current failed: {:?}", result.err());

        let gen_ref = result.unwrap();

        // Verify generation is 1
        assert_eq!(gen_ref.generation.as_u64(), 1);

        // Verify commit SHA is 40 hex chars
        assert_eq!(gen_ref.commit.len(), 40);
        assert!(gen_ref.commit.chars().all(|c| c.is_ascii_hexdigit()));

        // Verify generation dir exists and has files
        assert!(gen_ref.path.exists());
        assert!(gen_ref.path.join("README").exists());

        // Verify current symlink exists and points to gen-000001
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
    #[ignore] // Requires network access
    fn ensure_current_returns_immediately_if_fresh() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        // Use a long max_age so it won't be stale
        let cache = RepoCache::new(paths.clone()).with_max_age(Duration::from_secs(3600));
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();

        // First call: clones
        let result1 = cache.ensure_current(&key);
        assert!(result1.is_ok());
        let gen_ref1 = result1.unwrap();

        // Second call: should return immediately (not stale)
        let result2 = cache.ensure_current(&key);
        assert!(result2.is_ok());
        let gen_ref2 = result2.unwrap();

        // Both should have the same generation and commit
        assert_eq!(gen_ref1.generation.as_u64(), gen_ref2.generation.as_u64());
        assert_eq!(gen_ref1.commit, gen_ref2.commit);
        assert_eq!(gen_ref1.path, gen_ref2.path);
    }

    #[test]
    #[ignore] // Requires network access
    fn ensure_current_refreshes_if_stale() {
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        // Use a 0 second max_age so it's always stale
        let cache = RepoCache::new(paths.clone()).with_max_age(Duration::from_secs(0));
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();

        // First call: clones
        let result1 = cache.ensure_current(&key);
        assert!(result1.is_ok());
        let gen_ref1 = result1.unwrap();
        assert_eq!(gen_ref1.generation.as_u64(), 1);

        // Second call: should refresh (always stale with 0s max_age)
        let result2 = cache.ensure_current(&key);
        assert!(result2.is_ok());
        let gen_ref2 = result2.unwrap();

        // Should be generation 2
        assert_eq!(gen_ref2.generation.as_u64(), 2);

        // Both generations should exist
        let gen1_path = paths.generation_dir(&key, GenerationId::new(1));
        let gen2_path = paths.generation_dir(&key, GenerationId::new(2));
        assert!(gen1_path.exists());
        assert!(gen2_path.exists());

        // Current should point to gen-000002
        let current_link = paths.current_symlink(&key);
        let target = fs::read_link(&current_link).unwrap();
        assert!(
            target.to_str().unwrap().contains("gen-000002"),
            "Expected target to contain 'gen-000002', got: {:?}",
            target
        );
    }

    #[test]
    #[ignore] // requires network
    fn concurrent_ensure_current_doesnt_corrupt() {
        use std::sync::Arc;
        use std::thread;

        // Setup: temp dir, cache
        let temp_dir = tempdir().unwrap();
        let paths = CachePaths::new(temp_dir.path());
        let cache = Arc::new(RepoCache::new(paths.clone()));
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();

        // Spawn 5 threads all calling ensure_current for same repo simultaneously
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let cache = Arc::clone(&cache);
                let key = key.clone();
                thread::spawn(move || cache.ensure_current(&key))
            })
            .collect();

        // Collect all results with join()
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Verify all succeeded
        for (i, result) in results.iter().enumerate() {
            assert!(
                result.is_ok(),
                "Thread {} failed: {:?}",
                i,
                result.as_ref().err()
            );
        }

        // Verify only one clone happened (check for single gen-000001 or highest gen)
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

        // Should have exactly one generation directory (all threads should have
        // used the same clone, not created duplicates)
        assert_eq!(
            gen_dirs.len(),
            1,
            "Expected 1 generation directory, found {}: {:?}",
            gen_dirs.len(),
            gen_dirs.iter().map(|e| e.file_name()).collect::<Vec<_>>()
        );

        // Verify the generation is gen-000001
        let gen_name = gen_dirs[0].file_name();
        assert_eq!(
            gen_name.to_str().unwrap(),
            "gen-000001",
            "Expected gen-000001, got {:?}",
            gen_name
        );

        // Verify symlink is valid
        let current_link = paths.current_symlink(&key);
        assert!(current_link.exists(), "Current symlink does not exist");
        let target = fs::read_link(&current_link).unwrap();
        assert!(
            target.exists(),
            "Symlink target does not exist: {:?}",
            target
        );

        // Verify files present in worktree
        assert!(
            target.join("README").exists(),
            "README file not found in worktree"
        );
    }
}
