//! Negative cache for tracking repositories that don't exist.
//!
//! This module provides a simple in-memory cache to remember repositories
//! that have failed to clone (404, private, etc.) to avoid repeatedly
//! hitting GitHub with requests for non-existent repos.

use crate::types::RepoKey;
use dashmap::DashMap;
use std::time::{Duration, Instant};

/// Default TTL for negative cache entries (1 hour).
const DEFAULT_TTL: Duration = Duration::from_secs(60 * 60);

/// Entry in the negative cache.
struct NegativeCacheEntry {
    /// When this entry was created.
    cached_at: Instant,
}

/// Cache of repositories that are known not to exist.
///
/// Entries expire after `ttl` to allow retrying in case:
/// - A repo was created after we first tried
/// - A private repo was made public
/// - Transient network issues caused a false negative
pub struct NegativeCache {
    entries: DashMap<RepoKey, NegativeCacheEntry>,
    ttl: Duration,
}

impl Default for NegativeCache {
    fn default() -> Self {
        Self::new()
    }
}

impl NegativeCache {
    /// Create a new negative cache with the default TTL.
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            ttl: DEFAULT_TTL,
        }
    }

    /// Create a new negative cache with a custom TTL.
    #[cfg(test)]
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
        }
    }

    /// Check if a repo is in the negative cache (and not expired).
    pub fn contains(&self, key: &RepoKey) -> bool {
        if let Some(entry) = self.entries.get(key) {
            if entry.cached_at.elapsed() < self.ttl {
                return true;
            }
            // Entry expired, remove it
            drop(entry);
            self.entries.remove(key);
        }
        false
    }

    /// Add a repo to the negative cache after verifying it doesn't exist via GitHub API.
    ///
    /// Returns true if the repo was added to the cache (confirmed not to exist).
    /// Returns false if the repo exists (API returned 200) or we couldn't verify.
    pub fn insert_if_not_exists(&self, key: &RepoKey) -> bool {
        match check_repo_exists(key.owner.as_str(), key.repo.as_str()) {
            RepoStatus::NotFound => {
                log::info!("Confirmed {} does not exist, adding to negative cache", key);
                self.entries.insert(
                    key.clone(),
                    NegativeCacheEntry {
                        cached_at: Instant::now(),
                    },
                );
                true
            }
            RepoStatus::Exists => {
                log::debug!(
                    "Repo {} exists (may be private or transient error), not caching",
                    key
                );
                false
            }
            RepoStatus::Unknown(reason) => {
                log::debug!(
                    "Could not verify if {} exists ({}), not caching",
                    key,
                    reason
                );
                false
            }
        }
    }
}

/// Result of checking if a repo exists via GitHub API.
#[derive(Debug)]
enum RepoStatus {
    /// Repo definitely exists (got 200 OK).
    Exists,
    /// Repo definitely doesn't exist (got 404).
    NotFound,
    /// Couldn't determine - rate limited, network error, etc.
    Unknown(String),
}

/// Check if a GitHub repo exists using the public API.
///
/// Makes a HEAD request to `https://api.github.com/repos/{owner}/{repo}`
/// - 200 = repo exists (public)
/// - 404 = repo does not exist
/// - 403 = rate limited or private (treat as unknown)
fn check_repo_exists(owner: &str, repo: &str) -> RepoStatus {
    let url = format!("https://api.github.com/repos/{}/{}", owner, repo);

    // Use a short timeout since this is just a quick existence check
    let result = ureq::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .head(&url)
        .set("User-Agent", "ghfs")
        .call();

    match result {
        Ok(response) => {
            if response.status() == 200 {
                RepoStatus::Exists
            } else {
                // Unexpected success status
                RepoStatus::Unknown(format!("unexpected status: {}", response.status()))
            }
        }
        Err(ureq::Error::Status(404, _)) => RepoStatus::NotFound,
        Err(ureq::Error::Status(403, _)) => {
            // Could be rate limited or private repo
            RepoStatus::Unknown("forbidden (rate limited or private)".to_string())
        }
        Err(ureq::Error::Status(401, _)) => {
            // Auth required - could be private
            RepoStatus::Unknown("unauthorized".to_string())
        }
        Err(ureq::Error::Status(code, _)) => RepoStatus::Unknown(format!("HTTP {}", code)),
        Err(ureq::Error::Transport(e)) => RepoStatus::Unknown(format!("transport error: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn negative_cache_contains_returns_false_for_missing() {
        let cache = NegativeCache::new();
        let key: RepoKey = "octocat/nonexistent".parse().unwrap();
        assert!(!cache.contains(&key));
    }

    #[test]
    fn negative_cache_expires() {
        let cache = NegativeCache::with_ttl(Duration::from_millis(10));
        let key: RepoKey = "octocat/nonexistent".parse().unwrap();

        // Manually insert an entry for testing expiration
        cache.entries.insert(
            key.clone(),
            NegativeCacheEntry {
                cached_at: Instant::now(),
            },
        );
        assert!(cache.contains(&key));

        std::thread::sleep(Duration::from_millis(20));
        assert!(!cache.contains(&key));
    }

    // Network tests - only run with GHFS_RUN_NETWORK_TESTS=1
    fn network_tests_enabled() -> bool {
        match std::env::var("GHFS_RUN_NETWORK_TESTS") {
            Ok(value) => {
                let value = value.to_ascii_lowercase();
                value == "1" || value == "true" || value == "yes"
            }
            Err(_) => false,
        }
    }

    #[test]
    fn check_repo_exists_finds_real_repo() {
        if !network_tests_enabled() {
            eprintln!("skipping network test (set GHFS_RUN_NETWORK_TESTS=1)");
            return;
        }

        match check_repo_exists("octocat", "Hello-World") {
            RepoStatus::Exists => {} // expected
            other => panic!("Expected Exists, got {:?}", other),
        }
    }

    #[test]
    fn check_repo_exists_detects_nonexistent() {
        if !network_tests_enabled() {
            eprintln!("skipping network test (set GHFS_RUN_NETWORK_TESTS=1)");
            return;
        }

        // This repo definitely doesn't exist
        match check_repo_exists("octocat", "this-repo-definitely-does-not-exist-12345") {
            RepoStatus::NotFound => {} // expected
            other => panic!("Expected NotFound, got {:?}", other),
        }
    }

    #[test]
    fn insert_if_not_exists_caches_nonexistent_repo() {
        if !network_tests_enabled() {
            eprintln!("skipping network test (set GHFS_RUN_NETWORK_TESTS=1)");
            return;
        }

        let cache = NegativeCache::new();
        let key: RepoKey = "octocat/this-repo-definitely-does-not-exist-12345"
            .parse()
            .unwrap();

        assert!(!cache.contains(&key));
        assert!(cache.insert_if_not_exists(&key)); // Should return true (added)
        assert!(cache.contains(&key));
    }

    #[test]
    fn insert_if_not_exists_does_not_cache_existing_repo() {
        if !network_tests_enabled() {
            eprintln!("skipping network test (set GHFS_RUN_NETWORK_TESTS=1)");
            return;
        }

        let cache = NegativeCache::new();
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();

        assert!(!cache.insert_if_not_exists(&key)); // Should return false (exists)
        assert!(!cache.contains(&key));
    }
}
