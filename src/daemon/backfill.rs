//! Backfill SQLite state from existing cache contents.

use std::path::{Path, PathBuf};

use crate::cache::{CachePaths, open_repository};
use crate::daemon::state::State;
use crate::types::{Owner, Repo, RepoKey};

/// Scan the cache on disk and populate repo + generation metadata in SQLite.
pub fn backfill_cache_state(state: &State, cache_paths: &CachePaths) {
    let worktrees_dir = cache_paths.worktrees_dir();
    let owners = match std::fs::read_dir(&worktrees_dir) {
        Ok(entries) => entries,
        Err(err) => {
            log::warn!("Backfill skipped: cannot read worktrees dir: {}", err);
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

        let repos_dir = match std::fs::read_dir(owner_entry.path()) {
            Ok(entries) => entries,
            Err(_) => continue,
        };

        for repo_entry in repos_dir.flatten() {
            if !repo_entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                continue;
            }
            let repo_name = match repo_entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };
            let repo: Repo = match repo_name.parse() {
                Ok(repo) => repo,
                Err(_) => continue,
            };

            let key = RepoKey::new(owner.clone(), repo);
            let repo_id = match state.get_or_create_repo_id(&key) {
                Ok(id) => id,
                Err(err) => {
                    log::warn!("Backfill: failed to create repo {}: {}", key, err);
                    continue;
                }
            };

            let repo_dir = repo_entry.path();
            let mut gen_commits = std::collections::HashMap::<u64, String>::new();

            if let Ok(entries) = std::fs::read_dir(&repo_dir) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    let generation = match parse_generation(&name) {
                        Some(generation_id) => generation_id,
                        None => continue,
                    };

                    let path = entry.path();
                    let commit = match read_commit(&path) {
                        Some(commit) => commit,
                        None => continue,
                    };
                    let size_bytes = dir_size(&path);
                    let _ = state
                        .upsert_generation_for_repo_id(repo_id, generation, &commit, size_bytes);
                    gen_commits.insert(generation, commit);
                }
            }

            let mirror_size = dir_size(cache_paths.mirror_dir(&key));
            let _ = state.update_mirror_size(&key, mirror_size);

            let current_link = cache_paths.current_symlink(&key);
            if let Ok(target) = std::fs::read_link(&current_link) {
                let target = resolve_symlink_target(&current_link, target);
                if let Some(generation) = target
                    .file_name()
                    .and_then(|s| s.to_str())
                    .and_then(parse_generation)
                {
                    let commit = gen_commits
                        .get(&generation)
                        .cloned()
                        .or_else(|| read_commit(&target));
                    if let Some(commit) = commit {
                        let ts = symlink_mtime_secs(&current_link).unwrap_or_else(now_unix);
                        let _ = state.update_sync_at(&key, generation, &commit, ts);
                    }
                }
            }
        }
    }
}

fn parse_generation(name: &str) -> Option<u64> {
    name.strip_prefix("gen-")
        .and_then(|s| s.parse::<u64>().ok())
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
