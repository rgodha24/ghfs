//! SQLite-based state persistence for tracking repos, sync state, and priorities.

use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;

use crate::types::RepoKey;

/// Returns the current Unix timestamp in seconds.
fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Convert Option<i64> to Option<u64> for generation IDs.
/// SQLite stores integers as i64, but generation IDs are u64.
fn i64_to_u64_opt(val: Option<i64>) -> Option<u64> {
    val.map(|v| v as u64)
}

/// Manages persistent state for the GHFS daemon.
pub struct State {
    conn: Mutex<Connection>,
}

/// Represents the state of a repository in the database.
#[derive(Debug, Clone)]
pub struct RepoState {
    pub id: i64,
    pub owner: String,
    pub repo: String,
    pub priority: i32,
    pub current_generation: Option<u64>,
    pub head_commit: Option<String>,
    pub last_access_at: Option<i64>,
    pub last_sync_at: Option<i64>,
}

/// Repo state with aggregated generation stats.
#[derive(Debug, Clone)]
pub struct RepoStats {
    pub owner: String,
    pub repo: String,
    pub priority: i32,
    pub current_generation: Option<u64>,
    pub head_commit: Option<String>,
    pub last_access_at: Option<i64>,
    pub last_sync_at: Option<i64>,
    pub generation_count: u64,
    pub commit_count: u64,
    pub total_size_bytes: u64,
}

impl State {
    /// Open or create the state database at the given path.
    pub fn open(path: &Path) -> Result<Self, rusqlite::Error> {
        let conn = Connection::open(path)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Initialize the database schema. This is idempotent.
    pub fn init(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;
            CREATE TABLE IF NOT EXISTS repos (
                id INTEGER PRIMARY KEY,
                owner TEXT NOT NULL,
                repo TEXT NOT NULL,
                priority INTEGER DEFAULT 0,
                current_generation INTEGER,
                head_commit TEXT,
                last_access_at INTEGER,
                last_sync_at INTEGER,
                mirror_size_bytes INTEGER DEFAULT 0,
                UNIQUE(owner, repo)
            );

            CREATE TABLE IF NOT EXISTS generations (
                id INTEGER PRIMARY KEY,
                repo_id INTEGER NOT NULL,
                generation INTEGER NOT NULL,
                commit_sha TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                UNIQUE(repo_id, generation),
                FOREIGN KEY(repo_id) REFERENCES repos(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_repos_sync ON repos(last_sync_at);
            CREATE INDEX IF NOT EXISTS idx_repos_priority ON repos(priority DESC, last_sync_at);
            CREATE INDEX IF NOT EXISTS idx_generations_repo ON generations(repo_id);
            ",
        )?;
        Ok(())
    }

    /// Get or create a repo record, returning just the repo id.
    pub fn get_or_create_repo_id(&self, key: &RepoKey) -> Result<i64, rusqlite::Error> {
        let owner = key.owner.as_str();
        let repo = key.repo.as_str();
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT OR IGNORE INTO repos (owner, repo) VALUES (?1, ?2)",
            params![owner, repo],
        )?;

        conn.query_row(
            "SELECT id FROM repos WHERE owner = ?1 AND repo = ?2",
            params![owner, repo],
            |row| row.get(0),
        )
    }

    /// Get or create a repo record, returning the repo state.
    ///
    /// Uses INSERT OR IGNORE followed by SELECT to ensure idempotency.
    pub fn get_or_create_repo(&self, key: &RepoKey) -> Result<RepoState, rusqlite::Error> {
        let owner = key.owner.as_str();
        let repo = key.repo.as_str();
        let conn = self.conn.lock().unwrap();

        // Insert if not exists
        conn.execute(
            "INSERT OR IGNORE INTO repos (owner, repo) VALUES (?1, ?2)",
            params![owner, repo],
        )?;

        // Select the record
        conn.query_row(
            "SELECT id, owner, repo, priority, current_generation, head_commit, last_access_at, last_sync_at
             FROM repos WHERE owner = ?1 AND repo = ?2",
            params![owner, repo],
            |row| {
                Ok(RepoState {
                    id: row.get(0)?,
                    owner: row.get(1)?,
                    repo: row.get(2)?,
                    priority: row.get(3)?,
                    current_generation: i64_to_u64_opt(row.get(4)?),
                    head_commit: row.get(5)?,
                    last_access_at: row.get(6)?,
                    last_sync_at: row.get(7)?,
                })
            },
        )
    }

    /// Update repo after a successful sync.
    ///
    /// Sets the generation, head commit, and last_sync_at timestamp.
    pub fn update_sync(
        &self,
        key: &RepoKey,
        generation: u64,
        commit: &str,
    ) -> Result<(), rusqlite::Error> {
        let now = now_unix();
        self.update_sync_at(key, generation, commit, now)
    }

    /// Update repo after a successful sync with a provided timestamp.
    pub fn update_sync_at(
        &self,
        key: &RepoKey,
        generation: u64,
        commit: &str,
        ts: i64,
    ) -> Result<(), rusqlite::Error> {
        let owner = key.owner.as_str();
        let repo = key.repo.as_str();
        let _ = self.get_or_create_repo_id(key)?;
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "UPDATE repos SET current_generation = ?1, head_commit = ?2, last_sync_at = ?3
             WHERE owner = ?4 AND repo = ?5",
            params![generation as i64, commit, ts, owner, repo],
        )?;
        Ok(())
    }

    /// Record an access time for GC decisions.
    pub fn touch_access(&self, key: &RepoKey) -> Result<(), rusqlite::Error> {
        let owner = key.owner.as_str();
        let repo = key.repo.as_str();
        let now = now_unix();
        let _ = self.get_or_create_repo_id(key)?;
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "UPDATE repos SET last_access_at = ?1 WHERE owner = ?2 AND repo = ?3",
            params![now, owner, repo],
        )?;
        Ok(())
    }

    /// Set priority for a repo. 0 = normal, higher = more important.
    pub fn set_priority(&self, key: &RepoKey, priority: i32) -> Result<(), rusqlite::Error> {
        let owner = key.owner.as_str();
        let repo = key.repo.as_str();
        let _ = self.get_or_create_repo_id(key)?;
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "UPDATE repos SET priority = ?1 WHERE owner = ?2 AND repo = ?3",
            params![priority, owner, repo],
        )?;
        Ok(())
    }

    /// Get repos that are stale (last_sync_at older than threshold).
    ///
    /// A repo is considered stale if its last_sync_at is NULL or older than
    /// `now - max_age_secs`.
    pub fn get_stale_repos(&self, max_age_secs: i64) -> Result<Vec<RepoState>, rusqlite::Error> {
        let threshold = now_unix() - max_age_secs;
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT id, owner, repo, priority, current_generation, head_commit, last_access_at, last_sync_at
             FROM repos
             WHERE last_sync_at IS NULL OR last_sync_at < ?1
             ORDER BY priority DESC, COALESCE(last_sync_at, 0)",
        )?;

        let rows = stmt.query_map(params![threshold], |row| {
            Ok(RepoState {
                id: row.get(0)?,
                owner: row.get(1)?,
                repo: row.get(2)?,
                priority: row.get(3)?,
                current_generation: i64_to_u64_opt(row.get(4)?),
                head_commit: row.get(5)?,
                last_access_at: row.get(6)?,
                last_sync_at: row.get(7)?,
            })
        })?;

        rows.collect()
    }

    /// Get all repos ordered by priority (descending) then staleness.
    pub fn list_repos(&self) -> Result<Vec<RepoState>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT id, owner, repo, priority, current_generation, head_commit, last_access_at, last_sync_at
             FROM repos
             ORDER BY priority DESC, COALESCE(last_sync_at, 0)",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(RepoState {
                id: row.get(0)?,
                owner: row.get(1)?,
                repo: row.get(2)?,
                priority: row.get(3)?,
                current_generation: i64_to_u64_opt(row.get(4)?),
                head_commit: row.get(5)?,
                last_access_at: row.get(6)?,
                last_sync_at: row.get(7)?,
            })
        })?;

        rows.collect()
    }

    /// List all repos with aggregated generation stats.
    pub fn list_repos_with_stats(&self) -> Result<Vec<RepoStats>, rusqlite::Error> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT r.owner, r.repo, r.priority, r.current_generation, r.head_commit, r.last_access_at, r.last_sync_at,
                    COALESCE(g.gen_count, 0) AS gen_count,
                    COALESCE(g.commit_count, 0) AS commit_count,
                    COALESCE(g.total_size, 0) + COALESCE(r.mirror_size_bytes, 0) AS total_size
             FROM repos r
             LEFT JOIN (
                 SELECT repo_id,
                        COUNT(*) AS gen_count,
                        COUNT(DISTINCT commit_sha) AS commit_count,
                        SUM(size_bytes) AS total_size
                 FROM generations
                 GROUP BY repo_id
             ) g ON g.repo_id = r.id
             ORDER BY r.priority DESC, COALESCE(r.last_sync_at, 0)",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(RepoStats {
                owner: row.get(0)?,
                repo: row.get(1)?,
                priority: row.get(2)?,
                current_generation: i64_to_u64_opt(row.get(3)?),
                head_commit: row.get(4)?,
                last_access_at: row.get(5)?,
                last_sync_at: row.get(6)?,
                generation_count: row.get::<_, i64>(7)? as u64,
                commit_count: row.get::<_, i64>(8)? as u64,
                total_size_bytes: row.get::<_, i64>(9)? as u64,
            })
        })?;

        rows.collect()
    }

    /// Insert or update a generation record for a repo.
    pub fn upsert_generation(
        &self,
        key: &RepoKey,
        generation: u64,
        commit: &str,
        size_bytes: u64,
    ) -> Result<(), rusqlite::Error> {
        let repo_id = self.get_or_create_repo_id(key)?;
        self.upsert_generation_for_repo_id(repo_id, generation, commit, size_bytes)
    }

    /// Insert or update a generation record for a known repo id.
    pub fn upsert_generation_for_repo_id(
        &self,
        repo_id: i64,
        generation: u64,
        commit: &str,
        size_bytes: u64,
    ) -> Result<(), rusqlite::Error> {
        let now = now_unix();
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "INSERT INTO generations (repo_id, generation, commit_sha, size_bytes, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(repo_id, generation) DO UPDATE SET
                commit_sha = excluded.commit_sha,
                size_bytes = excluded.size_bytes,
                created_at = excluded.created_at",
            params![
                repo_id,
                generation as i64,
                commit,
                size_bytes as i64,
                now
            ],
        )?;

        Ok(())
    }

    /// Update the stored mirror size for a repo.
    pub fn update_mirror_size(
        &self,
        key: &RepoKey,
        size_bytes: u64,
    ) -> Result<(), rusqlite::Error> {
        let owner = key.owner.as_str();
        let repo = key.repo.as_str();
        let _ = self.get_or_create_repo_id(key)?;
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "UPDATE repos SET mirror_size_bytes = ?1 WHERE owner = ?2 AND repo = ?3",
            params![size_bytes as i64, owner, repo],
        )?;
        Ok(())
    }

    /// Delete a repo record.
    pub fn delete_repo(&self, key: &RepoKey) -> Result<(), rusqlite::Error> {
        let owner = key.owner.as_str();
        let repo = key.repo.as_str();
        let conn = self.conn.lock().unwrap();

        conn.execute(
            "DELETE FROM repos WHERE owner = ?1 AND repo = ?2",
            params![owner, repo],
        )?;
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_state() -> (State, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let state = State::open(&db_path).unwrap();
        state.init().unwrap();
        (state, dir)
    }

    fn make_repo_key(owner: &str, repo: &str) -> RepoKey {
        RepoKey {
            owner: owner.parse().unwrap(),
            repo: repo.parse().unwrap(),
        }
    }

    #[test]
    fn test_create_and_get_repo() {
        let (state, _dir) = create_test_state();
        let key = make_repo_key("octocat", "hello-world");

        // First call creates the record
        let repo1 = state.get_or_create_repo(&key).unwrap();
        assert_eq!(repo1.owner, "octocat");
        assert_eq!(repo1.repo, "hello-world");
        assert_eq!(repo1.priority, 0);
        assert!(repo1.current_generation.is_none());
        assert!(repo1.head_commit.is_none());
        assert!(repo1.last_access_at.is_none());
        assert!(repo1.last_sync_at.is_none());

        // Second call returns the same record (idempotent)
        let repo2 = state.get_or_create_repo(&key).unwrap();
        assert_eq!(repo1.id, repo2.id);
        assert_eq!(repo1.owner, repo2.owner);
        assert_eq!(repo1.repo, repo2.repo);
    }

    #[test]
    fn test_update_sync() {
        let (state, _dir) = create_test_state();
        let key = make_repo_key("rust-lang", "rust");

        // Create the repo first
        state.get_or_create_repo(&key).unwrap();

        // Update sync info
        let generation = 42u64;
        let commit = "abc123def456";
        state.update_sync(&key, generation, commit).unwrap();

        // Verify the update
        let repo = state.get_or_create_repo(&key).unwrap();
        assert_eq!(repo.current_generation, Some(42));
        assert_eq!(repo.head_commit, Some("abc123def456".to_string()));
        assert!(repo.last_sync_at.is_some());

        // Verify timestamp is recent (within last 5 seconds)
        let now = now_unix();
        let sync_time = repo.last_sync_at.unwrap();
        assert!(now - sync_time < 5);
    }

    #[test]
    fn test_get_stale_repos() {
        let (state, _dir) = create_test_state();

        // Create two repos
        let key1 = make_repo_key("org1", "repo1");
        let key2 = make_repo_key("org2", "repo2");

        state.get_or_create_repo(&key1).unwrap();
        state.get_or_create_repo(&key2).unwrap();

        // Sync repo1 now
        state.update_sync(&key1, 1, "commit1").unwrap();

        // Manually set repo2's last_sync_at to 2 hours ago
        let two_hours_ago = now_unix() - 7200;
        state
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE repos SET last_sync_at = ?1 WHERE owner = 'org2' AND repo = 'repo2'",
                params![two_hours_ago],
            )
            .unwrap();

        // Get repos stale for more than 1 hour (3600 seconds)
        let stale = state.get_stale_repos(3600).unwrap();
        assert_eq!(stale.len(), 1);
        assert_eq!(stale[0].owner, "org2");
        assert_eq!(stale[0].repo, "repo2");

        // Create a third repo with no sync time (NULL) - should also be stale
        let key3 = make_repo_key("org3", "repo3");
        state.get_or_create_repo(&key3).unwrap();

        let stale = state.get_stale_repos(3600).unwrap();
        assert_eq!(stale.len(), 2);
    }

    #[test]
    fn test_priority() {
        let (state, _dir) = create_test_state();
        let key = make_repo_key("priority", "test");

        // Create repo with default priority
        let repo = state.get_or_create_repo(&key).unwrap();
        assert_eq!(repo.priority, 0);

        // Set high priority
        state.set_priority(&key, 10).unwrap();

        let repo = state.get_or_create_repo(&key).unwrap();
        assert_eq!(repo.priority, 10);

        // Set negative priority
        state.set_priority(&key, -5).unwrap();

        let repo = state.get_or_create_repo(&key).unwrap();
        assert_eq!(repo.priority, -5);
    }
}
