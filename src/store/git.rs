//! Git operations for the object-backed revision store.
//!
//! This module backs the new design: a **blobless** partial clone
//! (`--filter=blob:none`) keeps all commits and trees locally for every
//! branch and tag, while file blobs are fetched lazily by the hydration
//! layer. Ref resolution and tree traversal are done via libgit2 against the
//! on-disk mirror; the promisor is only contacted for missing blobs.
//!
//! Counterpart to the legacy [`crate::cache::git`] shallow-clone + worktree
//! path, which will be removed once the filesystem layer flips onto this
//! store.

use git2::{BranchType, Oid, Repository};
use std::path::Path;
use std::process::{Command, Stdio};
use thiserror::Error;

use crate::types::RepoKey;

/// Errors returned by store git operations.
#[derive(Error, Debug)]
pub enum GitError {
    #[error("git operation failed: {0}")]
    Git(#[from] git2::Error),
    #[error("repository not found at {0}")]
    NotFound(String),
    #[error("failed to parse git data: {0}")]
    ParseError(String),
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("clone failed: {0}")]
    CloneError(String),
    #[error("fetch failed: {0}")]
    FetchError(String),
    #[error("unresolved ref: {0}")]
    RefNotFound(String),
    #[error("ambiguous ref: {0}")]
    AmbiguousRef(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("git CLI is not installed or not on PATH")]
    GitCliMissing,
}

/// Minimum abbreviated commit OID length we accept.
pub const MIN_OID_LEN: usize = 4;

/// Validate that a string is non-empty, contains no `..`, does not start with
/// `-`, and has no control or null bytes. Used to reject obviously malicious
/// selectors before touching git. Git itself would reject most of these too,
/// but failing fast at the boundary keeps error messages predictable.
pub fn validate_selector(value: &str) -> Result<(), GitError> {
    if value.is_empty() {
        return Err(GitError::InvalidInput(
            "selector cannot be empty".to_string(),
        ));
    }
    if value.contains("..") {
        return Err(GitError::InvalidInput(
            "selector cannot contain '..'".to_string(),
        ));
    }
    if value.starts_with('-') {
        return Err(GitError::InvalidInput(
            "selector cannot start with '-'".to_string(),
        ));
    }
    if value
        .as_bytes()
        .iter()
        .any(|b| *b == 0 || (b < &0x20 && b != &b'\t'))
    {
        // Allow tab? Git refs disallow control chars entirely; reject.
        return Err(GitError::InvalidInput(
            "selector cannot contain control or null characters".to_string(),
        ));
    }
    Ok(())
}

/// Hardened git CLI wrapper used for network operations (clone/fetch).
///
/// Libgit2 does not reliably perform promisor lazy fetches, so partial-clone
/// network operations go through the CLI. All read operations (ref
/// resolution, tree walking) use libgit2 instead.
#[derive(Clone)]
pub struct GitCli {
    git_path: String,
}

impl Default for GitCli {
    fn default() -> Self {
        Self::new()
    }
}

impl GitCli {
    /// Create a new CLI wrapper using the system `git`.
    pub fn new() -> Self {
        Self {
            git_path: "git".to_string(),
        }
    }

    /// Build a hardened `git` [`Command`] with security settings:
    /// - `GIT_LFS_SKIP_SMUDGE=1` (skip LFS downloads)
    /// - `GIT_TERMINAL_PROMPT=0` (no interactive prompts)
    /// - `core.hooksPath=` (disable hooks)
    /// - null stdin
    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.git_path);
        cmd.env("GIT_LFS_SKIP_SMUDGE", "1");
        cmd.env("GIT_TERMINAL_PROMPT", "0");
        cmd.args(["-c", "core.hooksPath="]);
        cmd.stdin(Stdio::null());
        cmd
    }

    /// Clone `owner/repo` as a bare, **blobless** mirror: every branch and
    /// tag's full commit + tree history is downloaded; blobs are filtered out
    /// and fetched on demand by the hydration layer.
    ///
    /// The clone writes to a temporary sibling directory first and renames
    /// into place atomically so a crashed clone never leaves a half-mirror.
    pub fn clone_blobless(&self, key: &RepoKey, dest: &Path) -> Result<(), GitError> {
        let url = format!("https://github.com/{}/{}.git", key.owner, key.repo);

        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let temporary = dest.with_extension("clone.tmp");
        let _ = std::fs::remove_dir_all(&temporary);

        let output = self
            .command()
            .args(["clone", "--bare", "--filter=blob:none"])
            .arg(&url)
            .arg(temporary.to_str().unwrap())
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            let _ = std::fs::remove_dir_all(&temporary);
            return Err(GitError::CloneError(super::redact_creds(&stderr)));
        }

        std::fs::rename(&temporary, dest)?;
        Ok(())
    }

    /// Incrementally update a blobless mirror with the latest refs from
    /// `origin`. All branches and tags are force-updated; deleted remote refs
    /// are pruned locally so `/by-ref/<gone>` lookups fail promptly.
    pub fn fetch_blobless(&self, mirror_path: &Path) -> Result<(), GitError> {
        let mirror_str = mirror_path
            .to_str()
            .ok_or_else(|| GitError::ParseError("mirror path is not valid UTF-8".into()))?;

        let output = self
            .command()
            .arg("-C")
            .arg(mirror_str)
            .args([
                "fetch",
                "--filter=blob:none",
                "--prune",
                "origin",
                "+refs/heads/*:refs/heads/*",
                "+refs/tags/*:refs/tags/*",
            ])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
            return Err(GitError::FetchError(super::redact_creds(&stderr)));
        }
        Ok(())
    }
}

/// Open an existing repository at `path`.
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

/// Resolve a reference to the commit OID it ultimately points to, peeling
/// through annotated tags as needed.
fn reference_to_commit_oid(reference: &git2::Reference<'_>) -> Result<Oid, GitError> {
    let commit = reference.peel_to_commit()?;
    Ok(commit.id())
}

/// Resolve the repository's HEAD to its commit OID. Used by the default
/// `/<owner>/<repo>` alias.
pub fn resolve_head(repo: &Repository) -> Result<Oid, GitError> {
    let head = repo.head()?;
    let commit = head.peel_to_commit()?;
    Ok(commit.id())
}

/// Resolve a user-supplied ref selector to a concrete commit OID.
///
/// Resolution order (per the by-ref design):
/// 1. `HEAD` or any full ref path (`refs/heads/...`, `refs/tags/...`)
///    matches verbatim and is peeled to a commit.
/// 2. A pure-hex string (length >= [`MIN_OID_LEN`], can be abbreviated)
///    resolves to a commit object directly. Pure-hex selectors can never be
///    revspec expressions, so this is safe even though we ultimately call
///    revparse.
/// 3. Otherwise the selector is treated as a **short** branch or tag name.
///    If both a branch and a tag share the name, an [`AmbiguousRef`] error is
///    returned so the filesystem can surface it instead of silently choosing.
///
/// Arbitrary revspec expressions (`HEAD~3`, `main^`, `abc...def`) are not
/// supported.
pub fn resolve_revision(repo: &Repository, selector: &str) -> Result<Oid, GitError> {
    // 1. Full ref path or HEAD.
    if selector == "HEAD" || selector.starts_with("refs/") {
        let reference = repo
            .find_reference(selector)
            .map_err(|_| GitError::RefNotFound(selector.to_string()))?;
        return reference_to_commit_oid(&reference);
    }

    // 2. Pure-hex commit OID (abbreviated allowed).
    if selector.len() >= MIN_OID_LEN && selector.chars().all(|c| c.is_ascii_hexdigit()) {
        if let Ok(obj) = repo.revparse_single(selector) {
            if let Ok(commit) = obj.peel_to_commit() {
                return Ok(commit.id());
            }
        }
        return Err(GitError::RefNotFound(selector.to_string()));
    }

    // 3. Short branch or tag name, with ambiguity detection.
    let branch_ref = repo.find_reference(&format!("refs/heads/{selector}")).ok();
    let tag_ref = repo.find_reference(&format!("refs/tags/{selector}")).ok();

    match (branch_ref, tag_ref) {
        (Some(b), None) => reference_to_commit_oid(&b),
        (None, Some(t)) => reference_to_commit_oid(&t),
        (Some(_), Some(_)) => Err(GitError::AmbiguousRef(selector.to_string())),
        (None, None) => Err(GitError::RefNotFound(selector.to_string())),
    }
}

/// List short branch names (`refs/heads/*`, without the prefix) present in the
/// mirror, sorted lexicographically.
pub fn list_branches(repo: &Repository) -> Result<Vec<String>, GitError> {
    let mut names = Vec::new();
    for branch in repo.branches(Some(BranchType::Local))? {
        let (b, _type) = branch?;
        if let Some(name) = b.name()? {
            names.push(name.to_string());
        }
    }
    names.sort();
    Ok(names)
}

/// List short tag names (`refs/tags/*`, without the prefix) present in the
/// mirror, sorted lexicographically. Annotated peel targets (`refs/tags/x^{}`)
/// are not surfaced as separate refs by libgit2, so no filtering is needed.
pub fn list_tags(repo: &Repository) -> Result<Vec<String>, GitError> {
    let mut names = Vec::new();
    for reference in repo.references()? {
        let reference = reference?;
        let Some(name) = reference.name() else {
            continue;
        };
        if let Some(short) = name.strip_prefix("refs/tags/") {
            names.push(short.to_string());
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn check_git_available() -> bool {
        Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Build a temporary repository with a couple of commits, branches, and
    /// tags, returning the opened libgit2 [`Repository`] and the temp dir
    /// keeping it alive.
    fn make_local_repo() -> (Repository, tempfile::TempDir) {
        assert!(check_git_available(), "git CLI required for store tests");
        let dir = tempdir().unwrap();
        let dir_path = dir.path().to_path_buf();

        let cfg = |args: &[&str]| {
            let status = Command::new("git")
                .arg("-C")
                .arg(&dir_path)
                .args(args)
                .status()
                .unwrap();
            assert!(status.success(), "git {:?} failed", args);
        };
        cfg(&["init", "-q"]);
        cfg(&["config", "user.email", "test@example.com"]);
        cfg(&["config", "user.name", "Test"]);
        // Opt out of the host's global signing config so tags/commits don't
        // require a key or a tag message in the test sandbox.
        cfg(&["config", "commit.gpgsign", "false"]);
        cfg(&["config", "tag.gpgsign", "false"]);

        let write = |name: &str, body: &str| {
            std::fs::write(dir_path.join(name), body).unwrap();
        };
        write("a.txt", "hello\n");
        cfg(&["add", "a.txt"]);
        cfg(&["commit", "-q", "-m", "first"]);
        cfg(&["branch", "feature/x"]);
        cfg(&["tag", "v1.0"]);
        write("b.txt", "world\n");
        cfg(&["add", "b.txt"]);
        cfg(&["commit", "-q", "-m", "second"]);
        cfg(&["branch", "release"]);
        cfg(&["tag", "-a", "v1.1", "-m", "annotated"]);

        let repo = Repository::open(&dir_path).unwrap();
        (repo, dir)
    }

    #[test]
    fn resolve_head_returns_commit() {
        let (repo, _dir) = make_local_repo();
        let oid = resolve_head(&repo).unwrap();
        assert!(repo.find_commit(oid).is_ok());
    }

    #[test]
    fn resolve_revision_full_ref() {
        let (repo, _dir) = make_local_repo();
        let oid = resolve_revision(&repo, "refs/heads/feature/x").unwrap();
        assert!(repo.find_commit(oid).is_ok());
    }

    #[test]
    fn resolve_revision_head_keyword() {
        let (repo, _dir) = make_local_repo();
        let oid = resolve_revision(&repo, "HEAD").unwrap();
        assert_eq!(oid, resolve_head(&repo).unwrap());
    }

    #[test]
    fn resolve_revision_short_branch() {
        let (repo, _dir) = make_local_repo();
        assert!(
            repo.find_commit(resolve_revision(&repo, "feature/x").unwrap())
                .is_ok()
        );
        assert!(
            repo.find_commit(resolve_revision(&repo, "release").unwrap())
                .is_ok()
        );
    }

    #[test]
    fn resolve_revision_tag_peels_annotated() {
        let (repo, _dir) = make_local_repo();
        // v1.1 is annotated: must peel to a commit, not the tag object.
        let oid = resolve_revision(&repo, "v1.1").unwrap();
        assert!(repo.find_commit(oid).is_ok());
        // v1.0 is lightweight: points directly at a commit.
        let oid2 = resolve_revision(&repo, "v1.0").unwrap();
        assert!(repo.find_commit(oid2).is_ok());
    }

    #[test]
    fn resolve_revision_full_commit_oid() {
        let (repo, _dir) = make_local_repo();
        let head = resolve_head(&repo).unwrap().to_string();
        let short = &head[..7];
        assert_eq!(
            resolve_revision(&repo, &head).unwrap(),
            resolve_head(&repo).unwrap()
        );
        assert_eq!(
            resolve_revision(&repo, short).unwrap(),
            resolve_head(&repo).unwrap()
        );
    }

    #[test]
    fn resolve_revision_missing_returns_not_found() {
        let (repo, _dir) = make_local_repo();
        assert!(matches!(
            resolve_revision(&repo, "nope"),
            Err(GitError::RefNotFound(_))
        ));
        assert!(matches!(
            resolve_revision(&repo, "refs/heads/nope"),
            Err(GitError::RefNotFound(_))
        ));
    }

    #[test]
    fn validate_selector_rejects_bad_inputs() {
        assert!(matches!(
            validate_selector(""),
            Err(GitError::InvalidInput(_))
        ));
        assert!(matches!(
            validate_selector("../x"),
            Err(GitError::InvalidInput(_))
        ));
        assert!(matches!(
            validate_selector("-foo"),
            Err(GitError::InvalidInput(_))
        ));
        assert!(matches!(
            validate_selector("foo\nbar"),
            Err(GitError::InvalidInput(_))
        ));
        assert!(matches!(
            validate_selector("foo\0bar"),
            Err(GitError::InvalidInput(_))
        ));
        assert!(validate_selector("main").is_ok());
        assert!(validate_selector("feature/x").is_ok());
        assert!(validate_selector("refs/heads/main").is_ok());
    }

    #[test]
    fn list_branches_and_tags() {
        let (repo, _dir) = make_local_repo();
        let branches = list_branches(&repo).unwrap();
        assert!(branches.contains(&"feature/x".to_string()));
        assert!(branches.contains(&"release".to_string()));
        let tags = list_tags(&repo).unwrap();
        assert!(tags.contains(&"v1.0".to_string()));
        assert!(tags.contains(&"v1.1".to_string()));
    }

    // ---- network-gated tests (real GitHub) --------------------------------

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
    fn clone_blobless_real_repo() {
        if !require_network() {
            return;
        }
        let dir = tempdir().unwrap();
        let dest = dir.path().join("hello-world.git");
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();
        GitCli::new().clone_blobless(&key, &dest).unwrap();
        assert!(dest.join("HEAD").exists());
        let repo = open_repository(&dest).unwrap();
        assert!(repo.is_bare());
        // HEAD must be resolvable even without any blobs locally.
        let head = resolve_head(&repo).unwrap();
        assert!(repo.find_commit(head).is_ok());
    }

    #[test]
    fn fetch_blobless_updates_refs() {
        if !require_network() {
            return;
        }
        let dir = tempdir().unwrap();
        let dest = dir.path().join("hello-world.git");
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();
        let cli = GitCli::new();
        cli.clone_blobless(&key, &dest).unwrap();
        cli.fetch_blobless(&dest).unwrap();
        let repo = open_repository(&dest).unwrap();
        assert!(!list_branches(&repo).unwrap().is_empty());
    }
}
