//! On-disk cache layout.
//!
//! ```text
//! <cache>/ghfs/
//! └── v2/
//!     ├── repos/<owner>/<repo>.git   # shallow, blobless bare repo per GitHub repo
//!     └── blobs/<xx>/<oid>           # blob contents materialized for reads
//! ```
//!
//! Directories older versions created next to `v2/` (`mirrors/`, `worktrees/`,
//! ...) are deleted on daemon startup (see [`CachePaths::remove_legacy`]).

use std::path::{Path, PathBuf};

use crate::types::RepoKey;

const LAYOUT_DIR: &str = "v2";

/// Everything pre-v2 ghfs versions created directly under the base dir.
const LEGACY_ENTRIES: &[&str] = &[
    "mirrors",
    "worktrees",
    "blobs",
    "locks",
    "ghfs.db",
    "ghfs.db-wal",
    "ghfs.db-shm",
    "ghfs.db-journal",
];

#[derive(Debug, Clone)]
pub struct CachePaths {
    base: PathBuf,
}

impl CachePaths {
    /// `base` is the `ghfs` cache directory (the parent of `v2/`).
    pub fn new(base: impl Into<PathBuf>) -> Self {
        Self { base: base.into() }
    }

    /// `$GHFS_CACHE_DIR`, else `dirs::cache_dir()/ghfs`.
    pub fn from_env() -> Self {
        if let Some(dir) = std::env::var_os("GHFS_CACHE_DIR") {
            return Self::new(dir);
        }
        let cache = dirs::cache_dir().unwrap_or_else(|| PathBuf::from(".cache"));
        Self::new(cache.join("ghfs"))
    }

    pub fn base(&self) -> &Path {
        &self.base
    }

    pub fn root(&self) -> PathBuf {
        self.base.join(LAYOUT_DIR)
    }

    pub fn repos_dir(&self) -> PathBuf {
        self.root().join("repos")
    }

    pub fn blobs_dir(&self) -> PathBuf {
        self.root().join("blobs")
    }

    pub fn repo_dir(&self, key: &RepoKey) -> PathBuf {
        self.repos_dir()
            .join(key.owner.as_str())
            .join(format!("{}.git", key.repo.as_str()))
    }

    /// Materialized blob file, fanned out by the first two hex digits.
    pub fn blob_path(&self, oid: &str) -> PathBuf {
        self.blobs_dir().join(&oid[..2]).join(oid)
    }

    pub fn create_dirs(&self) -> std::io::Result<()> {
        std::fs::create_dir_all(self.repos_dir())?;
        std::fs::create_dir_all(self.blobs_dir())
    }

    /// Delete what older ghfs versions left in the base dir. Only these exact
    /// names are touched, so pointing `GHFS_CACHE_DIR` at a shared directory
    /// can never delete anything else. Returns the names that were removed.
    pub fn remove_legacy(&self) -> Vec<String> {
        let mut removed = Vec::new();
        for name in LEGACY_ENTRIES {
            let path = self.base.join(name);
            let Ok(meta) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            let result = if meta.is_dir() {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
            match result {
                Ok(()) => removed.push(name.to_string()),
                Err(e) => log::warn!("failed to remove legacy {}: {e}", path.display()),
            }
        }
        removed
    }

    /// Repos that have a local mirror, sorted.
    pub fn cached_repos(&self) -> Vec<RepoKey> {
        let mut out = Vec::new();
        let Ok(owners) = std::fs::read_dir(self.repos_dir()) else {
            return out;
        };
        for owner in owners.flatten() {
            let Some(owner_name) = owner.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Ok(repos) = std::fs::read_dir(owner.path()) else {
                continue;
            };
            for repo in repos.flatten() {
                let Some(name) = repo.file_name().to_str().map(str::to_owned) else {
                    continue;
                };
                let Some(repo_name) = name.strip_suffix(".git") else {
                    continue;
                };
                if let Ok(key) = format!("{owner_name}/{repo_name}").parse() {
                    out.push(key);
                }
            }
        }
        out.sort_by_key(|k: &RepoKey| k.to_string());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remove_legacy_keeps_current_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = CachePaths::new(tmp.path());
        paths.create_dirs().unwrap();
        std::fs::create_dir_all(tmp.path().join("mirrors/a/b.git")).unwrap();
        std::fs::write(tmp.path().join("ghfs.db"), b"x").unwrap();
        // Things that aren't ours survive, even with GHFS_CACHE_DIR pointed
        // at a shared directory.
        std::fs::create_dir_all(tmp.path().join("someone-elses-cache")).unwrap();
        std::fs::write(tmp.path().join("notes.txt"), b"keep").unwrap();

        let mut removed = paths.remove_legacy();
        removed.sort();
        assert_eq!(removed, vec!["ghfs.db", "mirrors"]);
        assert!(paths.repos_dir().exists());
        assert!(tmp.path().join("someone-elses-cache").exists());
        assert!(tmp.path().join("notes.txt").exists());
    }

    #[test]
    fn cached_repos_lists_mirrors() {
        let tmp = tempfile::tempdir().unwrap();
        let paths = CachePaths::new(tmp.path());
        let key: RepoKey = "octo/hello".parse().unwrap();
        std::fs::create_dir_all(paths.repo_dir(&key)).unwrap();
        std::fs::create_dir_all(paths.repos_dir().join("octo/not-a-mirror")).unwrap();
        assert_eq!(paths.cached_repos(), vec![key]);
    }
}
