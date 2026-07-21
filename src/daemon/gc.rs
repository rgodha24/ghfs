//! Garbage collection for cache metadata and stale state.
//!
//! After the by-ref redesign, GC responsibilities are simpler:
//! - Reconcile the `repos` DB table with on-disk mirrors (remove rows whose
//!   mirror is gone, add rows for mirrors that appear).
//! - Clean up orphaned blob cache entries (future: LRU by size).
//! - Clear stale sync metadata for repos whose mirror no longer resolves
//!   a HEAD.

use std::path::Path;

use crate::cache::CachePaths;
use crate::daemon::state::State;
use crate::types::RepoKey;

use super::backfill;

#[derive(Debug, Clone, Copy, Default)]
pub struct GcStats {
    pub repos_scanned: u64,
    pub repos_removed: u64,
    pub sync_resets: u64,
}

pub fn run_gc(state: &State, cache_paths: &CachePaths) -> GcStats {
    backfill::backfill_cache_state(state, cache_paths);

    let repos = match state.list_repos() {
        Ok(repos) => repos,
        Err(err) => {
            log::warn!("gc: failed to list repos: {err}");
            return GcStats::default();
        }
    };

    let mut stats = GcStats {
        repos_scanned: repos.len() as u64,
        ..GcStats::default()
    };

    for repo in repos {
        let key_str = format!("{}/{}", repo.owner, repo.repo);
        let key: RepoKey = match key_str.parse() {
            Ok(key) => key,
            Err(err) => {
                log::warn!("gc: invalid repo key in database '{key_str}': {err}");
                continue;
            }
        };

        let mirror_path = cache_paths.mirror_dir(&key);

        if !mirror_path.exists() {
            if let Err(err) = state.delete_repo(&key) {
                log::warn!("gc: failed to remove orphaned repo row for {key}: {err}");
            } else {
                stats.repos_removed += 1;
            }
            continue;
        }

        // If the mirror exists but HEAD can't be resolved, clear stale sync
        // metadata so the next access re-syncs rather than trusting a bad
        // cached commit.
        if repo.head_commit.is_some() {
            if let Ok(repo_handle) = git2::Repository::open(&mirror_path) {
                if repo_handle.head().is_err() {
                    if let Err(err) = state.clear_sync(&key) {
                        log::warn!("gc: failed to clear stale sync for {key}: {err}");
                    } else {
                        stats.sync_resets += 1;
                    }
                }
            } else if let Err(err) = state.clear_sync(&key) {
                log::warn!("gc: failed to clear stale sync for {key}: {err}");
            } else {
                stats.sync_resets += 1;
            }
        }
    }

    // Future: prune blob cache by LRU/size cap.

    stats
}

// Keep `Path` import for potential future blob-cache pruning.
#[allow(dead_code)]
fn _path_unused(_p: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_env() -> (State, CachePaths, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let cache_paths = CachePaths::new(dir.path().join("cache"));
        std::fs::create_dir_all(cache_paths.root()).unwrap();
        std::fs::create_dir_all(cache_paths.mirrors_dir()).unwrap();
        std::fs::create_dir_all(cache_paths.locks_dir()).unwrap();

        let db_path = cache_paths.root().join("ghfs.db");
        let state = State::open(&db_path).unwrap();
        state.init().unwrap();

        (state, cache_paths, dir)
    }

    #[test]
    fn gc_removes_repo_rows_when_mirror_missing() {
        let (state, cache_paths, _dir) = create_test_env();
        let key: RepoKey = "octocat/hello-world".parse().unwrap();

        state.get_or_create_repo(&key).unwrap();
        state.update_sync(&key, 0, "abc123").unwrap();

        let stats = run_gc(&state, &cache_paths);

        assert_eq!(stats.repos_scanned, 1);
        assert_eq!(stats.repos_removed, 1);
        assert_eq!(state.list_repos().unwrap().len(), 0);
    }
}
