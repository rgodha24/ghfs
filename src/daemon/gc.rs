//! Garbage collection for cache metadata and stale state.

use std::path::{Path, PathBuf};

use crate::cache::{CachePaths, open_repository};
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
            log::warn!("gc: failed to list repos: {}", err);
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
                log::warn!("gc: invalid repo key in database '{}': {}", key_str, err);
                continue;
            }
        };

        let worktree_base = cache_paths.worktree_base(&key);
        let mirror_path = cache_paths.mirror_dir(&key);
        let current_link = cache_paths.current_symlink(&key);

        let keep_generations = existing_generation_numbers(&worktree_base);
        if let Err(err) = state.delete_generations_except(&key, &keep_generations) {
            log::warn!(
                "gc: failed to reconcile generation rows for {}: {}",
                key,
                err
            );
        }

        let mirror_size = dir_size(&mirror_path);
        if let Err(err) = state.update_mirror_size(&key, mirror_size) {
            log::warn!("gc: failed to update mirror size for {}: {}", key, err);
        }

        if let Some((generation, commit, sync_at)) = read_current_sync(&current_link) {
            if let Err(err) = state.update_sync_at(&key, generation, &commit, sync_at) {
                log::warn!("gc: failed to update sync metadata for {}: {}", key, err);
            }
        } else if repo.current_generation.is_some()
            || repo.head_commit.is_some()
            || repo.last_sync_at.is_some()
        {
            if let Err(err) = state.clear_sync(&key) {
                log::warn!(
                    "gc: failed to clear stale sync metadata for {}: {}",
                    key,
                    err
                );
            } else {
                stats.sync_resets += 1;
            }
        }

        if !worktree_base.exists() && !mirror_path.exists() {
            if let Err(err) = state.delete_repo(&key) {
                log::warn!(
                    "gc: failed to remove orphaned repo row for {}: {}",
                    key,
                    err
                );
            } else {
                stats.repos_removed += 1;
            }
        }
    }

    stats
}

fn existing_generation_numbers(path: impl AsRef<Path>) -> Vec<u64> {
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

fn read_current_sync(current_link: &Path) -> Option<(u64, String, i64)> {
    let target = std::fs::read_link(current_link).ok()?;
    let target = resolve_symlink_target(current_link, target);
    let generation = target
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(parse_generation_number)?;

    if !target.exists() {
        return None;
    }

    let commit = read_commit(&target)?;
    let sync_at = symlink_mtime_secs(current_link).unwrap_or_else(now_unix);
    Some((generation, commit, sync_at))
}

fn read_commit(path: &Path) -> Option<String> {
    let repo = open_repository(path).ok()?;
    let head = repo.head().ok()?;
    let commit = head.peel_to_commit().ok()?;
    Some(commit.id().to_string())
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

fn symlink_mtime_secs(path: &Path) -> Option<i64> {
    let metadata = std::fs::symlink_metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    let secs = modified
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    Some(secs as i64)
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn dir_size(path: impl AsRef<Path>) -> u64 {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_env() -> (State, CachePaths, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let cache_paths = CachePaths::new(dir.path().join("cache"));
        std::fs::create_dir_all(cache_paths.root()).unwrap();
        std::fs::create_dir_all(cache_paths.mirrors_dir()).unwrap();
        std::fs::create_dir_all(cache_paths.worktrees_dir()).unwrap();
        std::fs::create_dir_all(cache_paths.locks_dir()).unwrap();

        let db_path = cache_paths.root().join("ghfs.db");
        let state = State::open(&db_path).unwrap();
        state.init().unwrap();

        (state, cache_paths, dir)
    }

    #[test]
    fn gc_removes_repo_rows_when_cache_missing() {
        let (state, cache_paths, _dir) = create_test_env();
        let key: RepoKey = "octocat/hello-world".parse().unwrap();

        state.get_or_create_repo(&key).unwrap();
        state.update_sync(&key, 1, "abc123").unwrap();

        let stats = run_gc(&state, &cache_paths);

        assert_eq!(stats.repos_scanned, 1);
        assert_eq!(stats.repos_removed, 1);
        assert_eq!(state.list_repos().unwrap().len(), 0);
    }

    #[test]
    fn gc_clears_stale_sync_metadata() {
        let (state, cache_paths, _dir) = create_test_env();
        let key: RepoKey = "rust-lang/rust".parse().unwrap();

        state.get_or_create_repo(&key).unwrap();
        state.update_sync(&key, 42, "abc123").unwrap();
        let stats = run_gc(&state, &cache_paths);

        assert_eq!(stats.repos_scanned, 1);
        assert_eq!(stats.repos_removed, 1);
        assert_eq!(stats.sync_resets, 1);

        let repos = state.list_repos().unwrap();
        assert!(repos.is_empty());
    }
}
