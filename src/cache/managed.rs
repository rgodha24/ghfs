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
    /// Updates access time in state. Updates sync state on success.
    pub fn ensure_current(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        // Record access
        let _ = self.state.touch_access(key);

        let result = self.cache.ensure_current(key)?;

        // Update state with new generation info
        let _ = self
            .state
            .update_sync(key, result.generation.as_u64(), &result.commit);

        Ok(result)
    }

    /// Force refresh a repo, ignoring staleness.
    /// Updates access time and sync state in database.
    pub fn force_refresh(&self, key: &RepoKey) -> Result<GenerationRef, CacheError> {
        // Record access
        let _ = self.state.touch_access(key);

        let result = self.cache.force_refresh(key)?;

        // Update state with new generation info
        let _ = self
            .state
            .update_sync(key, result.generation.as_u64(), &result.commit);

        Ok(result)
    }
}
