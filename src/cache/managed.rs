//! Managed cache that integrates with SQLite state tracking.

use std::sync::Arc;

use crate::daemon::state::State;
use crate::types::RepoKey;

use super::repo::RepoCache;
use super::{CacheError, CachePaths, GenerationRef};

/// Cache manager that integrates with SQLite state tracking.
///
/// This wraps RepoCache and updates the State database after each operation.
pub struct ManagedCache {
    cache: RepoCache,
    state: Arc<State>,
}

impl ManagedCache {
    /// Create a new managed cache.
    pub fn new(paths: CachePaths, state: Arc<State>) -> Self {
        Self {
            cache: RepoCache::new(paths),
            state,
        }
    }

    /// Get the underlying cache paths.
    pub fn paths(&self) -> &CachePaths {
        self.cache.paths()
    }

    /// Ensure a repo is materialized and current.
    /// Updates access time in state. Updates sync state on refresh.
    pub fn ensure_current(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        let _ = self.state.get_or_create_repo(key);
        // Record access
        let _ = self.state.touch_access(key);

        let result = match self.cache.ensure_current_with_status(key) {
            Ok(r) => r,
            Err(e) => {
                // Clean up DB entry if this repo was never successfully synced
                // (i.e., it was a spurious access to a non-existent repo)
                let _ = self.state.delete_repo_if_never_synced(key);
                return Err(e);
            }
        };

        // Update state with new generation info
        if result.refreshed {
            let _ = self.state.update_sync(
                key,
                result.gen_ref.generation.as_u64(),
                &result.gen_ref.commit,
            );
        }
        let _ = self.state.upsert_generation(
            key,
            result.gen_ref.generation.as_u64(),
            &result.gen_ref.commit,
            dir_size(&result.gen_ref.path),
        );
        let mirror_size = dir_size(self.cache.paths().mirror_dir(key));
        let _ = self.state.update_mirror_size(key, mirror_size);
        self.reconcile_generation_rows(key);

        Ok(result.gen_ref)
    }

    /// Force refresh a repo, ignoring staleness.
    /// Updates access time and sync state in database.
    pub fn force_refresh(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        let _ = self.state.get_or_create_repo(key);
        // Record access
        let _ = self.state.touch_access(key);

        let result = match self.cache.force_refresh(key) {
            Ok(r) => r,
            Err(e) => {
                // Clean up DB entry if this repo was never successfully synced
                let _ = self.state.delete_repo_if_never_synced(key);
                return Err(e);
            }
        };

        // Update state with new generation info
        let _ = self
            .state
            .update_sync(key, result.generation.as_u64(), &result.commit);
        let _ = self.state.upsert_generation(
            key,
            result.generation.as_u64(),
            &result.commit,
            dir_size(&result.path),
        );
        let mirror_size = dir_size(self.cache.paths().mirror_dir(key));
        let _ = self.state.update_mirror_size(key, mirror_size);
        self.reconcile_generation_rows(key);

        Ok(result)
    }

    fn reconcile_generation_rows(&self, key: &RepoKey) {
        let keep_generations = existing_generation_numbers(self.cache.paths().worktree_base(key));
        let _ = self.state.delete_generations_except(key, &keep_generations);
    }
}

fn existing_generation_numbers(path: impl AsRef<std::path::Path>) -> Vec<u64> {
    let mut generations = Vec::new();

    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }

            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };

            if let Some(generation) = parse_generation_number(&name) {
                generations.push(generation);
            }
        }
    }

    generations.sort_unstable();
    generations
}

fn parse_generation_number(name: &str) -> Option<u64> {
    name.strip_prefix("gen-")
        .and_then(|num| num.parse::<u64>().ok())
}

fn dir_size(path: impl AsRef<std::path::Path>) -> u64 {
    let path = path.as_ref();
    let mut total = 0u64;
    if let Ok(meta) = std::fs::symlink_metadata(path) {
        if meta.is_file() {
            return meta.len();
        }
    }
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(meta) = std::fs::symlink_metadata(&path) {
                if meta.is_file() {
                    total = total.saturating_add(meta.len());
                } else if meta.is_dir() {
                    total = total.saturating_add(dir_size(path));
                }
            }
        }
    }
    total
}
