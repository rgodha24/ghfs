//! Managed cache that integrates with SQLite state tracking.

use std::sync::Arc;

use crate::daemon::state::State;
use crate::types::RepoKey;

use super::{CacheError, CachePaths, GenerationRef, RepoCache};

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

        let result = self.cache.ensure_current_with_status(key)?;

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

        Ok(result.gen_ref)
    }

    /// Force refresh a repo, ignoring staleness.
    /// Updates access time and sync state in database.
    pub fn force_refresh(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        let _ = self.state.get_or_create_repo(key);
        // Record access
        let _ = self.state.touch_access(key);

        let result = self.cache.force_refresh(key)?;

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

        Ok(result)
    }
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
