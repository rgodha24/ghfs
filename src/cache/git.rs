//! Git operations using a hybrid CLI + libgit2 approach.
//!
//! This module provides git operations for cloning, fetching, and managing
//! worktrees using:
//!
//! **CLI (with hardening) for write operations requiring shallow clone support:**
//! - `clone_bare_shallow` - needs `--depth=1` for bandwidth/disk savings
//! - `fetch_shallow` - needs `--depth=1` for efficient updates
//! - `create_worktree` - needs `--detach` flag not exposed in libgit2
//!
//! **libgit2 for read operations:**
//! - `open_repository` - clean API for opening existing repos
//! - `resolve_default_branch` - efficient ref/commit reading
//! - `repository_exists` - simple path validation

use git2::Repository;
use std::path::Path;
use std::process::{Command, Stdio};
use thiserror::Error;

/// Errors returned by git operations.
#[derive(Error, Debug)]
pub enum GitError {
    /// libgit2 reported an error.
    #[error("git operation failed: {0}")]
    Git(#[from] git2::Error),
    /// Repository path does not contain a git repo.
    #[error("repository not found at {0}")]
    NotFound(String),
    /// Output parsing or unexpected git data.
    #[error("failed to parse git data: {0}")]
    ParseError(String),
    /// Worktree creation failed.
    #[error("worktree creation failed: {0}")]
    WorktreeError(String),
    /// Clone failed.
    #[error("clone failed: {0}")]
    CloneError(String),
    /// Fetch failed.
    #[error("fetch failed: {0}")]
    FetchError(String),
    /// Underlying IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    /// Invalid inputs were provided.
    #[error("invalid input: {0}")]
    InvalidInput(String),
}

/// Validate that a git ref (branch name or commit SHA) does not contain dangerous patterns.
///
/// Rejects:
/// - Empty strings
/// - Strings containing `..` (path traversal)
/// - Strings starting with `-` (could be interpreted as flags)
/// - Strings containing null bytes or control characters
fn validate_git_ref(value: &str, name: &str) -> Result<(), GitError> {
    if value.is_empty() {
        return Err(GitError::InvalidInput(format!("{} cannot be empty", name)));
    }
    if value.contains("..") {
        return Err(GitError::InvalidInput(format!(
            "{} cannot contain '..'",
            name
        )));
    }
    if value.starts_with('-') {
        return Err(GitError::InvalidInput(format!(
            "{} cannot start with '-'",
            name
        )));
    }
    if value.bytes().any(|b| b == 0 || b < 0x20) {
        return Err(GitError::InvalidInput(format!(
            "{} cannot contain null or control characters",
            name
        )));
    }
    Ok(())
}

/// Validate that an owner or repo name is safe.
///
/// Rejects:
/// - Empty strings
/// - Strings containing `..` (path traversal)
/// - Strings containing `/` or `\` (path separators)
/// - Strings starting with `-` (could be interpreted as flags)
/// - Strings containing null bytes or control characters
fn validate_name(value: &str, name: &str) -> Result<(), GitError> {
    if value.is_empty() {
        return Err(GitError::InvalidInput(format!("{} cannot be empty", name)));
    }
    if value.contains("..") {
        return Err(GitError::InvalidInput(format!(
            "{} cannot contain '..'",
            name
        )));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(GitError::InvalidInput(format!(
            "{} cannot contain path separators",
            name
        )));
    }
    if value.starts_with('-') {
        return Err(GitError::InvalidInput(format!(
            "{} cannot start with '-'",
            name
        )));
    }
    if value.bytes().any(|b| b == 0 || b < 0x20) {
        return Err(GitError::InvalidInput(format!(
            "{} cannot contain null or control characters",
            name
        )));
    }
    Ok(())
}

/// Git CLI wrapper with security hardening.
///
/// Used for operations that require shallow clone support (`--depth=1`)
/// which libgit2 does not natively support.
pub struct GitCli {
    git_path: String,
}

impl Default for GitCli {
    fn default() -> Self {
        Self::new()
    }
}

impl GitCli {
    /// Create a new GitCli instance using the system git.
    pub fn new() -> Self {
        Self {
            git_path: "git".into(),
        }
    }

    /// Create a hardened Command with security settings.
    ///
    /// Applies:
    /// - `GIT_LFS_SKIP_SMUDGE=1` - skip LFS file downloads
    /// - `GIT_TERMINAL_PROMPT=0` - disable interactive prompts
    /// - `core.hooksPath=` - disable hooks execution
    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.git_path);
        cmd.env("GIT_LFS_SKIP_SMUDGE", "1");
        cmd.env("GIT_TERMINAL_PROMPT", "0");
        cmd.args(["-c", "core.hooksPath="]);
        cmd.stdin(Stdio::null());
        cmd
    }

    /// Clone a GitHub repository as a bare, shallow repository.
    ///
    /// Creates `<dest>` as a bare git repo with only the latest commit.
    /// This saves significant bandwidth and disk space compared to full clones.
    pub fn clone_bare_shallow(&self, owner: &str, repo: &str, dest: &Path) -> Result<(), GitError> {
        // Validate inputs to prevent injection attacks
        validate_name(owner, "owner")?;
        validate_name(repo, "repo")?;

        let dest_existed = dest.exists();

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let url = format!("https://github.com/{owner}/{repo}.git");
        let dest_str = dest.to_str().ok_or_else(|| {
            GitError::ParseError("destination path is not valid UTF-8".to_string())
        })?;

        let output = self
            .command()
            .args(["clone", "--bare", "--depth=1"])
            .arg(&url)
            .arg(dest_str)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !dest_existed {
                let _ = std::fs::remove_dir_all(dest);
            }
            return Err(GitError::CloneError(stderr.into_owned()));
        }

        Ok(())
    }

    /// Fetch updates from origin for a specific branch (shallow).
    ///
    /// Uses `--depth=1` to only fetch the latest commit, saving bandwidth.
    pub fn fetch_shallow(&self, mirror_path: &Path, branch: &str) -> Result<(), GitError> {
        // Validate branch name to prevent injection
        validate_git_ref(branch, "branch")?;

        let mirror_str = mirror_path
            .to_str()
            .ok_or_else(|| GitError::ParseError("mirror path is not valid UTF-8".to_string()))?;

        let refspec = format!("+refs/heads/{0}:refs/heads/{0}", branch);

        let output = self
            .command()
            .arg("-C")
            .arg(mirror_str)
            .args(["fetch", "--depth=1", "origin"])
            .arg(&refspec)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GitError::FetchError(stderr.into_owned()));
        }

        Ok(())
    }

    /// Create a detached worktree from a bare repository at a specific commit.
    ///
    /// Uses `--detach` flag which isn't exposed in the libgit2 API.
    pub fn create_worktree(
        &self,
        mirror_path: &Path,
        worktree_path: &Path,
        commit: &str,
    ) -> Result<(), GitError> {
        // Validate commit SHA to prevent injection
        validate_git_ref(commit, "commit")?;

        // Create parent directories of worktree_path if needed
        if let Some(parent) = worktree_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mirror_str = mirror_path
            .to_str()
            .ok_or_else(|| GitError::ParseError("mirror path is not valid UTF-8".to_string()))?;

        let worktree_str = worktree_path
            .to_str()
            .ok_or_else(|| GitError::ParseError("worktree path is not valid UTF-8".to_string()))?;

        let output = self
            .command()
            .arg("-C")
            .arg(mirror_str)
            .args(["worktree", "add", "--detach"])
            .arg(worktree_str)
            .arg(commit)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GitError::WorktreeError(stderr.into_owned()));
        }

        Ok(())
    }

    /// Clone a GitHub repository as a bare repository with full history.
    ///
    /// Creates `<dest>` as a bare git repo with complete history.
    pub fn clone_bare_full(&self, owner: &str, repo: &str, dest: &Path) -> Result<(), GitError> {
        // Validate inputs to prevent injection attacks
        validate_name(owner, "owner")?;
        validate_name(repo, "repo")?;

        let dest_existed = dest.exists();

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let url = format!("https://github.com/{owner}/{repo}.git");
        let dest_str = dest.to_str().ok_or_else(|| {
            GitError::ParseError("destination path is not valid UTF-8".to_string())
        })?;

        let output = self
            .command()
            .args(["clone", "--bare"])
            .arg(&url)
            .arg(dest_str)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !dest_existed {
                let _ = std::fs::remove_dir_all(dest);
            }
            return Err(GitError::CloneError(stderr.into_owned()));
        }

        Ok(())
    }

    /// Fetch updates from origin for a specific branch (full history, no depth limit).
    ///
    /// Used for repos that have been unshallowed.
    pub fn fetch_full(&self, mirror_path: &Path, branch: &str) -> Result<(), GitError> {
        // Validate branch name to prevent injection
        validate_git_ref(branch, "branch")?;

        let mirror_str = mirror_path
            .to_str()
            .ok_or_else(|| GitError::ParseError("mirror path is not valid UTF-8".to_string()))?;

        let refspec = format!("+refs/heads/{0}:refs/heads/{0}", branch);

        let output = self
            .command()
            .arg("-C")
            .arg(mirror_str)
            .args(["fetch", "origin"])
            .arg(&refspec)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GitError::FetchError(stderr.into_owned()));
        }

        Ok(())
    }

    /// Check if a repository is a shallow clone.
    pub fn is_shallow_repo(&self, mirror_path: &Path) -> Result<bool, GitError> {
        let mirror_str = mirror_path
            .to_str()
            .ok_or_else(|| GitError::ParseError("mirror path is not valid UTF-8".to_string()))?;

        let output = self
            .command()
            .arg("-C")
            .arg(mirror_str)
            .args(["rev-parse", "--is-shallow-repository"])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GitError::ParseError(format!(
                "failed to check shallow status: {}",
                stderr
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.trim() == "true")
    }

    /// Fetch full history for a shallow repository (unshallow).
    ///
    /// This converts a shallow clone to a full clone by fetching all history.
    pub fn fetch_unshallow(&self, mirror_path: &Path, branch: &str) -> Result<(), GitError> {
        // Validate branch name to prevent injection
        validate_git_ref(branch, "branch")?;

        let mirror_str = mirror_path
            .to_str()
            .ok_or_else(|| GitError::ParseError("mirror path is not valid UTF-8".to_string()))?;

        let refspec = format!("+refs/heads/{0}:refs/heads/{0}", branch);

        let output = self
            .command()
            .arg("-C")
            .arg(mirror_str)
            .args(["fetch", "--unshallow", "origin"])
            .arg(&refspec)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GitError::FetchError(stderr.into_owned()));
        }

        Ok(())
    }

    /// Convert a full-history repo back to shallow (depth=1) and run gc.
    ///
    /// This fetches with --depth=1 to update shallow grafts, then runs
    /// `git gc --prune=now` to actually remove the old objects from disk.
    pub fn fetch_reshallow(&self, mirror_path: &Path, branch: &str) -> Result<(), GitError> {
        // Validate branch name to prevent injection
        validate_git_ref(branch, "branch")?;

        let mirror_str = mirror_path
            .to_str()
            .ok_or_else(|| GitError::ParseError("mirror path is not valid UTF-8".to_string()))?;

        let refspec = format!("+refs/heads/{0}:refs/heads/{0}", branch);

        // Fetch with depth=1 to re-shallow
        let output = self
            .command()
            .arg("-C")
            .arg(mirror_str)
            .args(["fetch", "--depth=1", "origin"])
            .arg(&refspec)
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(GitError::FetchError(stderr.into_owned()));
        }

        // Run gc --prune=now to actually remove old objects
        let gc_output = self
            .command()
            .arg("-C")
            .arg(mirror_str)
            .args(["gc", "--prune=now"])
            .output()?;

        if !gc_output.status.success() {
            let stderr = String::from_utf8_lossy(&gc_output.stderr);
            // Log warning but don't fail - the fetch succeeded
            log::warn!("git gc failed after reshallow: {}", stderr);
        }

        Ok(())
    }
}

/// Open an existing repository at the given path.
pub fn open_repository(path: &Path) -> Result<Repository, GitError> {
    let repo = Repository::open(path).map_err(|e| {
        if e.code() == git2::ErrorCode::NotFound {
            GitError::NotFound(path.display().to_string())
        } else {
            GitError::Git(e)
        }
    })?;
    Ok(repo)
}

/// Resolve the default branch name and HEAD commit from a repository.
///
/// Returns (branch_name, commit_sha) e.g. ("main", "abc123...")
pub fn resolve_default_branch(repo: &Repository) -> Result<(String, String), GitError> {
    let head = repo.head()?;
    let ref_name = head
        .name()
        .ok_or_else(|| GitError::ParseError("HEAD reference has no name".to_string()))?;

    let branch_name = ref_name
        .strip_prefix("refs/heads/")
        .ok_or_else(|| {
            GitError::ParseError(format!(
                "unexpected HEAD format: expected 'refs/heads/<branch>', got '{}'",
                ref_name
            ))
        })?
        .to_string();

    let commit = head.peel_to_commit()?;
    let commit_sha = commit.id().to_string();

    Ok((branch_name, commit_sha))
}

/// Check if a path contains a valid git repository.
pub fn repository_exists(path: &Path) -> bool {
    Repository::open(path).is_ok()
}

/// Clone a GitHub repository as a bare, shallow repository.
///
/// Convenience wrapper around `GitCli::clone_bare_shallow`.
pub fn clone_bare_shallow(owner: &str, repo: &str, dest: &Path) -> Result<(), GitError> {
    GitCli::new().clone_bare_shallow(owner, repo, dest)
}

/// Clone a GitHub repository as a bare repository with full history.
///
/// Convenience wrapper around `GitCli::clone_bare_full`.
pub fn clone_bare_full(owner: &str, repo: &str, dest: &Path) -> Result<(), GitError> {
    GitCli::new().clone_bare_full(owner, repo, dest)
}

/// Fetch updates from origin for a specific branch (shallow).
///
/// Convenience wrapper around `GitCli::fetch_shallow`.
pub fn fetch_shallow(mirror_path: &Path, branch: &str) -> Result<(), GitError> {
    GitCli::new().fetch_shallow(mirror_path, branch)
}

/// Fetch updates from origin for a specific branch (full history).
///
/// Convenience wrapper around `GitCli::fetch_full`.
pub fn fetch_full(mirror_path: &Path, branch: &str) -> Result<(), GitError> {
    GitCli::new().fetch_full(mirror_path, branch)
}

/// Check if a repository is a shallow clone.
///
/// Convenience wrapper around `GitCli::is_shallow_repo`.
pub fn is_shallow_repo(mirror_path: &Path) -> Result<bool, GitError> {
    GitCli::new().is_shallow_repo(mirror_path)
}

/// Fetch full history for a shallow repository (unshallow).
///
/// Convenience wrapper around `GitCli::fetch_unshallow`.
pub fn fetch_unshallow(mirror_path: &Path, branch: &str) -> Result<(), GitError> {
    GitCli::new().fetch_unshallow(mirror_path, branch)
}

/// Convert a full-history repo back to shallow (depth=1) and run gc.
///
/// Convenience wrapper around `GitCli::fetch_reshallow`.
pub fn fetch_reshallow(mirror_path: &Path, branch: &str) -> Result<(), GitError> {
    GitCli::new().fetch_reshallow(mirror_path, branch)
}

/// Create a detached worktree from a bare repository at a specific commit.
///
/// Convenience wrapper around `GitCli::create_worktree`.
pub fn create_worktree(
    mirror_path: &Path,
    worktree_path: &Path,
    commit: &str,
) -> Result<(), GitError> {
    GitCli::new().create_worktree(mirror_path, worktree_path, commit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn network_tests_enabled() -> bool {
        match std::env::var("GHFS_RUN_NETWORK_TESTS") {
            Ok(value) => {
                let value = value.to_ascii_lowercase();
                value == "1" || value == "true" || value == "yes"
            }
            Err(_) => false,
        }
    }

    fn require_network() -> bool {
        if network_tests_enabled() {
            true
        } else {
            eprintln!("skipping network test (set GHFS_RUN_NETWORK_TESTS=1)");
            false
        }
    }

    #[test]
    fn repository_exists_returns_false_for_nonexistent() {
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let nonexistent = temp_dir.path().join("nonexistent");

        assert!(!repository_exists(&nonexistent));
    }

    #[test]
    fn open_repository_not_found() {
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let nonexistent = temp_dir.path().join("nonexistent");

        let result = open_repository(&nonexistent);
        assert!(result.is_err(), "Should fail for nonexistent path");

        let err = result.err().unwrap();
        match err {
            GitError::NotFound(path) => {
                assert!(path.contains("nonexistent"));
            }
            other => panic!("Expected NotFound error, got: {:?}", other),
        }
    }

    #[test]
    fn repository_exists_returns_false_for_regular_directory() {
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");

        assert!(!repository_exists(temp_dir.path()));
    }

    #[test]
    fn clone_bare_shallow_clones_real_repo() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let dest = temp_dir.path().join("hello-world.git");

        let result = clone_bare_shallow("octocat", "Hello-World", &dest);

        assert!(result.is_ok(), "Clone failed: {:?}", result.err());
        assert!(
            dest.join("HEAD").exists(),
            "HEAD file should exist in bare repo"
        );

        let repo = open_repository(&dest).expect("Should be able to open cloned repo");
        assert!(repo.is_bare(), "Repository should be bare");
    }

    #[test]
    fn resolve_default_branch_works_on_hello_world() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let dest = temp_dir.path().join("hello-world.git");

        clone_bare_shallow("octocat", "Hello-World", &dest).expect("Clone failed");
        let repo = open_repository(&dest).expect("Failed to open repo");
        let result = resolve_default_branch(&repo);
        assert!(
            result.is_ok(),
            "resolve_default_branch failed: {:?}",
            result.err()
        );

        let (branch_name, commit_sha) = result.unwrap();

        assert_eq!(
            branch_name, "master",
            "Expected default branch to be 'master'"
        );
        assert_eq!(
            commit_sha.len(),
            40,
            "Commit SHA should be 40 characters, got {}",
            commit_sha.len()
        );
        assert!(
            commit_sha.chars().all(|c| c.is_ascii_hexdigit()),
            "Commit SHA should be all hex digits, got '{}'",
            commit_sha
        );
    }

    #[test]
    fn create_worktree_creates_detached_worktree() {
        if !require_network() {
            return;
        }
        use std::fs;
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let mirror_path = temp_dir.path().join("hello-world.git");
        let worktree_path = temp_dir.path().join("hello-world-worktree");

        clone_bare_shallow("octocat", "Hello-World", &mirror_path).expect("Clone failed");
        let repo = open_repository(&mirror_path).expect("Failed to open repo");
        let (_branch_name, commit_sha) =
            resolve_default_branch(&repo).expect("Failed to resolve default branch");
        let result = create_worktree(&mirror_path, &worktree_path, &commit_sha);
        assert!(result.is_ok(), "create_worktree failed: {:?}", result.err());
        let git_path = worktree_path.join(".git");
        let metadata = fs::metadata(&git_path).expect(".git should exist");
        assert!(metadata.is_file(), ".git should be a file, not a directory");
        assert!(
            worktree_path.join("README").exists(),
            "README file should exist in worktree"
        );
        let git_contents = fs::read_to_string(&git_path).expect("Failed to read .git file");
        assert!(
            git_contents.contains("gitdir:"),
            ".git file should contain 'gitdir:', got: {}",
            git_contents
        );
    }

    #[test]
    fn fetch_shallow_updates_mirror() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let mirror_path = temp_dir.path().join("hello-world.git");

        clone_bare_shallow("octocat", "Hello-World", &mirror_path).expect("Clone failed");
        let repo = open_repository(&mirror_path).expect("Failed to open repo");
        let (branch_name, initial_commit) =
            resolve_default_branch(&repo).expect("Failed to resolve default branch");
        let result = fetch_shallow(&mirror_path, &branch_name);
        assert!(result.is_ok(), "fetch_shallow failed: {:?}", result.err());
        // Re-open because git2 caches refs.
        let repo = open_repository(&mirror_path).expect("Failed to open repo after fetch");
        let (_branch_name, post_fetch_commit) =
            resolve_default_branch(&repo).expect("Failed to resolve default branch after fetch");
        assert_eq!(
            post_fetch_commit.len(),
            40,
            "Post-fetch commit SHA should be 40 characters"
        );
        assert!(
            post_fetch_commit.chars().all(|c| c.is_ascii_hexdigit()),
            "Post-fetch commit SHA should be all hex digits"
        );

        println!("Initial commit: {}", initial_commit);
        println!("Post-fetch commit: {}", post_fetch_commit);
    }

    #[test]
    fn open_repository_works_after_clone() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let dest = temp_dir.path().join("hello-world.git");

        clone_bare_shallow("octocat", "Hello-World", &dest).expect("Clone failed");
        let result = open_repository(&dest);
        assert!(result.is_ok(), "open_repository failed: {:?}", result.err());

        let repo = result.unwrap();
        assert!(repo.is_bare(), "Repository should be bare");
    }

    #[test]
    fn repository_exists_returns_true_for_cloned_repo() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let dest = temp_dir.path().join("hello-world.git");

        clone_bare_shallow("octocat", "Hello-World", &dest).expect("Clone failed");

        assert!(repository_exists(&dest));
    }

    #[test]
    fn git_cli_methods_work_directly() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let mirror_path = temp_dir.path().join("hello-world.git");
        let worktree_path = temp_dir.path().join("worktree");

        let cli = GitCli::new();

        cli.clone_bare_shallow("octocat", "Hello-World", &mirror_path)
            .expect("Clone failed");
        let repo = open_repository(&mirror_path).expect("Failed to open");
        let (branch, commit) = resolve_default_branch(&repo).expect("Failed to resolve");
        cli.fetch_shallow(&mirror_path, &branch)
            .expect("Fetch failed");
        cli.create_worktree(&mirror_path, &worktree_path, &commit)
            .expect("Worktree failed");

        assert!(worktree_path.join("README").exists());
    }

    #[test]
    fn validate_name_rejects_empty() {
        let result = validate_name("", "owner");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn validate_name_rejects_path_traversal() {
        let result = validate_name("foo/../bar", "owner");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn validate_name_rejects_path_separators() {
        assert!(matches!(
            validate_name("foo/bar", "owner"),
            Err(GitError::InvalidInput(_))
        ));
        assert!(matches!(
            validate_name("foo\\bar", "owner"),
            Err(GitError::InvalidInput(_))
        ));
    }

    #[test]
    fn validate_name_rejects_leading_dash() {
        let result = validate_name("-malicious", "owner");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn validate_name_rejects_control_chars() {
        let result = validate_name("foo\0bar", "owner");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
        let result = validate_name("foo\nbar", "owner");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn validate_name_accepts_valid_names() {
        assert!(validate_name("octocat", "owner").is_ok());
        assert!(validate_name("Hello-World", "repo").is_ok());
        assert!(validate_name("my_repo.v2", "repo").is_ok());
        assert!(validate_name("rust-lang", "owner").is_ok());
    }

    #[test]
    fn validate_git_ref_rejects_empty() {
        let result = validate_git_ref("", "branch");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn validate_git_ref_rejects_path_traversal() {
        let result = validate_git_ref("foo/../bar", "branch");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn validate_git_ref_rejects_leading_dash() {
        let result = validate_git_ref("-malicious", "branch");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn validate_git_ref_accepts_valid_refs() {
        assert!(validate_git_ref("main", "branch").is_ok());
        assert!(validate_git_ref("feature/my-branch", "branch").is_ok());
        assert!(validate_git_ref("abc123def456", "commit").is_ok());
        assert!(validate_git_ref("v1.0.0", "tag").is_ok());
    }

    #[test]
    fn clone_rejects_invalid_owner() {
        use tempfile::tempdir;
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let dest = temp_dir.path().join("test.git");

        let result = clone_bare_shallow("../malicious", "repo", &dest);
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn clone_rejects_invalid_repo() {
        use tempfile::tempdir;
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let dest = temp_dir.path().join("test.git");

        let result = clone_bare_shallow("owner", "-malicious", &dest);
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn fetch_rejects_invalid_branch() {
        use tempfile::tempdir;
        let temp_dir = tempdir().expect("Failed to create temp directory");

        let result = fetch_shallow(temp_dir.path(), "-malicious");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn create_worktree_rejects_invalid_commit() {
        use tempfile::tempdir;
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let worktree_path = temp_dir.path().join("worktree");

        let result = create_worktree(temp_dir.path(), &worktree_path, "-malicious");
        assert!(matches!(result, Err(GitError::InvalidInput(_))));
    }

    #[test]
    fn is_shallow_repo_returns_true_for_shallow_clone() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let dest = temp_dir.path().join("hello-world.git");

        clone_bare_shallow("octocat", "Hello-World", &dest).expect("Clone failed");

        let result = is_shallow_repo(&dest);
        assert!(result.is_ok(), "is_shallow_repo failed: {:?}", result.err());
        assert!(
            result.unwrap(),
            "Shallow clone should be detected as shallow"
        );
    }

    #[test]
    fn clone_bare_full_creates_non_shallow_repo() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let dest = temp_dir.path().join("hello-world.git");

        let result = clone_bare_full("octocat", "Hello-World", &dest);
        assert!(result.is_ok(), "Full clone failed: {:?}", result.err());
        assert!(
            dest.join("HEAD").exists(),
            "HEAD file should exist in bare repo"
        );

        let is_shallow = is_shallow_repo(&dest).expect("is_shallow_repo failed");
        assert!(!is_shallow, "Full clone should not be shallow");

        let repo = open_repository(&dest).expect("Should be able to open cloned repo");
        assert!(repo.is_bare(), "Repository should be bare");
    }

    #[test]
    fn unshallow_converts_shallow_to_full() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let mirror_path = temp_dir.path().join("hello-world.git");

        // Start with shallow clone
        clone_bare_shallow("octocat", "Hello-World", &mirror_path).expect("Clone failed");
        assert!(
            is_shallow_repo(&mirror_path).unwrap(),
            "Should start as shallow"
        );

        // Unshallow
        let repo = open_repository(&mirror_path).expect("Failed to open repo");
        let (branch, _) = resolve_default_branch(&repo).expect("Failed to resolve branch");
        let result = fetch_unshallow(&mirror_path, &branch);
        assert!(result.is_ok(), "fetch_unshallow failed: {:?}", result.err());

        // Verify no longer shallow
        let is_shallow = is_shallow_repo(&mirror_path).expect("is_shallow_repo failed");
        assert!(
            !is_shallow,
            "Repo should no longer be shallow after unshallow"
        );
    }

    #[test]
    fn reshallow_converts_full_to_shallow() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let mirror_path = temp_dir.path().join("hello-world.git");

        // Start with full clone
        clone_bare_full("octocat", "Hello-World", &mirror_path).expect("Clone failed");
        assert!(
            !is_shallow_repo(&mirror_path).unwrap(),
            "Should start as non-shallow"
        );

        // Reshallow
        let repo = open_repository(&mirror_path).expect("Failed to open repo");
        let (branch, _) = resolve_default_branch(&repo).expect("Failed to resolve branch");
        let result = fetch_reshallow(&mirror_path, &branch);
        assert!(result.is_ok(), "fetch_reshallow failed: {:?}", result.err());

        // Verify now shallow
        let is_shallow = is_shallow_repo(&mirror_path).expect("is_shallow_repo failed");
        assert!(is_shallow, "Repo should be shallow after reshallow");
    }

    #[test]
    fn fetch_full_works_on_non_shallow_repo() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let mirror_path = temp_dir.path().join("hello-world.git");

        // Start with full clone
        clone_bare_full("octocat", "Hello-World", &mirror_path).expect("Clone failed");

        let repo = open_repository(&mirror_path).expect("Failed to open repo");
        let (branch, initial_commit) =
            resolve_default_branch(&repo).expect("Failed to resolve branch");

        // Fetch full
        let result = fetch_full(&mirror_path, &branch);
        assert!(result.is_ok(), "fetch_full failed: {:?}", result.err());

        // Re-open and verify still works
        let repo = open_repository(&mirror_path).expect("Failed to open repo after fetch");
        let (_, post_fetch_commit) =
            resolve_default_branch(&repo).expect("Failed to resolve branch after fetch");

        assert_eq!(
            post_fetch_commit.len(),
            40,
            "Post-fetch commit SHA should be 40 characters"
        );

        println!("Initial commit: {}", initial_commit);
        println!("Post-fetch commit: {}", post_fetch_commit);
    }

    #[test]
    fn unshallow_then_reshallow_roundtrip() {
        if !require_network() {
            return;
        }
        use tempfile::tempdir;

        let temp_dir = tempdir().expect("Failed to create temp directory");
        let mirror_path = temp_dir.path().join("hello-world.git");

        // Start with shallow clone
        clone_bare_shallow("octocat", "Hello-World", &mirror_path).expect("Clone failed");
        assert!(
            is_shallow_repo(&mirror_path).unwrap(),
            "Should start shallow"
        );

        let repo = open_repository(&mirror_path).expect("Failed to open repo");
        let (branch, _) = resolve_default_branch(&repo).expect("Failed to resolve branch");

        // Unshallow
        fetch_unshallow(&mirror_path, &branch).expect("Unshallow failed");
        assert!(
            !is_shallow_repo(&mirror_path).unwrap(),
            "Should be non-shallow after unshallow"
        );

        // Reshallow
        fetch_reshallow(&mirror_path, &branch).expect("Reshallow failed");
        assert!(
            is_shallow_repo(&mirror_path).unwrap(),
            "Should be shallow after reshallow"
        );
    }
}
