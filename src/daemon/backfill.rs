//! Backfill SQLite state from existing cache contents.
//!
//! Scans the mirrors directory on disk and creates repo rows in the state
//! database for any repos that have a mirror but no DB row yet. After the
//! by-ref redesign there are no generation directories or `current` symlinks
//! to reconcile — repos are simply present-or-not based on their mirror.

use crate::cache::CachePaths;
use crate::daemon::state::State;
use crate::types::{Owner, Repo, RepoKey};

/// Scan the cache on disk and ensure every mirrored repo has a DB row.
pub fn backfill_cache_state(state: &State, cache_paths: &CachePaths) {
    let owners = match std::fs::read_dir(cache_paths.mirrors_dir()) {
        Ok(entries) => entries,
        Err(err) => {
            log::debug!("Backfill skipped: cannot read mirrors dir: {err}");
            return;
        }
    };

    for owner_entry in owners.flatten() {
        if !owner_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let owner_name = match owner_entry.file_name().into_string() {
            Ok(name) => name,
            Err(_) => continue,
        };
        let owner: Owner = match owner_name.parse() {
            Ok(owner) => owner,
            Err(_) => continue,
        };

        let repos = match std::fs::read_dir(owner_entry.path()) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for repo_entry in repos.flatten() {
            if !repo_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let name = match repo_entry.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            let Some(stripped) = name.strip_suffix(".git") else {
                continue;
            };
            let Ok(repo) = stripped.parse::<Repo>() else {
                continue;
            };
            let key = RepoKey::new(owner.clone(), repo);
            let _ = state.get_or_create_repo(&key);
        }
    }
}
