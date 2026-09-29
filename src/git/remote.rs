//! Async wrappers around the `git` CLI for everything that touches the network.
//!
//! libgit2 can't do partial or shallow fetches reliably, so network work goes
//! through `git` subprocesses on tokio. Every command has a wall-clock timeout
//! and a low-speed abort, so one stalled connection can't hang a request for
//! minutes.

use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use git2::Oid;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio::sync::Semaphore;

use super::GitError;

/// Abort transfers slower than 1 KiB/s for this many seconds.
const LOW_SPEED_TIME_SECS: u32 = 20;
/// Most `git` processes running at once, so a burst of directory prefetches
/// can't fork hundreds of clients at GitHub.
const MAX_CONCURRENT_GIT: usize = 24;

/// Remote refs as reported by `git ls-remote`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteRefs {
    /// Commit HEAD points at.
    pub head: Option<Oid>,
    /// Full ref name HEAD points at (e.g. `refs/heads/main`).
    pub head_ref: Option<String>,
    /// `(full ref name, commit oid)`, tags already peeled to their commit.
    pub refs: Vec<(String, Oid)>,
}

impl RemoteRefs {
    /// Resolve a user-facing selector (`main`, `v1.0`, `refs/heads/x`) to a
    /// commit. Branches win over tags, like `git rev-parse`.
    pub fn resolve(&self, selector: &str) -> Option<Oid> {
        if selector == "HEAD" {
            return self.head;
        }
        let candidates = [
            selector.to_string(),
            format!("refs/heads/{selector}"),
            format!("refs/tags/{selector}"),
        ];
        candidates
            .iter()
            .find_map(|want| self.refs.iter().find(|(name, _)| name == want))
            .map(|(_, oid)| *oid)
    }

    /// Short branch and tag names, sorted and deduplicated.
    pub fn short_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .refs
            .iter()
            .filter_map(|(name, _)| {
                name.strip_prefix("refs/heads/")
                    .or_else(|| name.strip_prefix("refs/tags/"))
            })
            .map(str::to_owned)
            .collect();
        names.sort();
        names.dedup();
        names
    }
}

/// Parse `git ls-remote --symref` output.
pub fn parse_ls_remote(output: &str) -> RemoteRefs {
    let mut out = RemoteRefs::default();
    let mut peeled: Vec<(String, Oid)> = Vec::new();
    for line in output.lines() {
        if let Some(rest) = line.strip_prefix("ref: ") {
            // `ref: refs/heads/main\tHEAD`
            if let Some((target, name)) = rest.split_once('\t')
                && name == "HEAD"
            {
                out.head_ref = Some(target.to_string());
            }
            continue;
        }
        let Some((oid, name)) = line.split_once('\t') else {
            continue;
        };
        let Ok(oid) = Oid::from_str(oid) else {
            continue;
        };
        if name == "HEAD" {
            out.head = Some(oid);
        } else if let Some(base) = name.strip_suffix("^{}") {
            peeled.push((base.to_string(), oid));
        } else {
            out.refs.push((name.to_string(), oid));
        }
    }
    // Annotated tags: replace the tag object id with the peeled commit.
    for (name, commit) in peeled {
        if let Some(entry) = out.refs.iter_mut().find(|(n, _)| *n == name) {
            entry.1 = commit;
        }
    }
    out
}

/// Classify git stderr: GitHub answers unknown and private-without-creds repos
/// the same way, and both mean "not found" to us.
fn classify_failure(stderr: &str) -> GitError {
    let lower = stderr.to_ascii_lowercase();
    if lower.contains("repository not found")
        || lower.contains("could not read username")
        || lower.contains("terminal prompts disabled")
        || lower.contains("not our ref")
        || lower.contains("couldn't find remote ref")
        || lower.contains("does not appear to be a git repository")
    {
        return GitError::RepoNotFound(redact_creds(stderr.trim()));
    }
    GitError::Command(redact_creds(stderr.trim()))
}

#[derive(Debug, Clone)]
pub struct GitCli {
    /// Base URL; repos are `<base>/<owner>/<repo>.git`. Overridable for tests.
    base_url: String,
    slots: Arc<Semaphore>,
}

impl Default for GitCli {
    fn default() -> Self {
        Self::new()
    }
}

impl GitCli {
    pub fn new() -> Self {
        let base_url =
            std::env::var("GHFS_GIT_BASE_URL").unwrap_or_else(|_| "https://github.com".to_string());
        Self::with_base_url(base_url)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            slots: Arc::new(Semaphore::new(MAX_CONCURRENT_GIT)),
        }
    }

    pub fn url(&self, owner: &str, repo: &str) -> String {
        format!("{}/{owner}/{repo}.git", self.base_url.trim_end_matches('/'))
    }

    fn command(&self, dir: Option<&Path>) -> Command {
        let mut cmd = Command::new("git");
        if let Some(dir) = dir {
            cmd.arg("-C").arg(dir);
        }
        cmd.args([
            "-c",
            "protocol.version=2",
            "-c",
            "http.lowSpeedLimit=1024",
            "-c",
            &format!("http.lowSpeedTime={LOW_SPEED_TIME_SECS}"),
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
        ])
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GCM_INTERACTIVE", "never")
        .env("GIT_ASKPASS", "")
        .env("SSH_ASKPASS", "")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
        cmd
    }

    async fn run(
        &self,
        mut cmd: Command,
        stdin: Option<Vec<u8>>,
        timeout: Duration,
    ) -> Result<String, GitError> {
        let _slot = self.slots.acquire().await.expect("semaphore never closed");
        if stdin.is_some() {
            cmd.stdin(Stdio::piped());
        }
        let mut child = cmd.spawn().map_err(GitError::Spawn)?;
        if let Some(input) = stdin {
            let mut pipe = child.stdin.take().expect("stdin piped");
            // Write concurrently with waiting so a large input can't deadlock
            // against a full stdout pipe.
            tokio::spawn(async move {
                let _ = pipe.write_all(&input).await;
                let _ = pipe.shutdown().await;
            });
        }
        let output = match tokio::time::timeout(timeout, child.wait_with_output()).await {
            Ok(result) => result.map_err(GitError::Spawn)?,
            Err(_) => return Err(GitError::Timeout(timeout)),
        };
        if !output.status.success() {
            return Err(classify_failure(&String::from_utf8_lossy(&output.stderr)));
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    /// Resolve HEAD only. One small round trip.
    pub async fn ls_remote_head(&self, url: &str) -> Result<RemoteRefs, GitError> {
        let mut cmd = self.command(None);
        cmd.args(["ls-remote", "--symref", url, "HEAD"]);
        let out = self.run(cmd, None, Duration::from_secs(30)).await?;
        let refs = parse_ls_remote(&out);
        if refs.head.is_none() {
            return Err(GitError::RepoNotFound(format!("{url} has no HEAD")));
        }
        Ok(refs)
    }

    /// HEAD plus all branches and tags (no `refs/pull/*`, which can be huge).
    pub async fn ls_remote_all(&self, url: &str) -> Result<RemoteRefs, GitError> {
        let mut cmd = self.command(None);
        cmd.args([
            "ls-remote",
            "--symref",
            url,
            "HEAD",
            "refs/heads/*",
            "refs/tags/*",
        ]);
        let out = self.run(cmd, None, Duration::from_secs(60)).await?;
        Ok(parse_ls_remote(&out))
    }

    /// Create an empty bare repo configured as a blobless partial clone of `url`.
    pub async fn init_repo(&self, dir: &Path, url: &str) -> Result<(), GitError> {
        let mut cmd = self.command(None);
        cmd.args(["init", "-q", "--bare"]).arg(dir);
        self.run(cmd, None, Duration::from_secs(30)).await?;
        for (key, value) in [
            ("core.repositoryformatversion", "1"),
            ("extensions.partialClone", "origin"),
            ("remote.origin.url", url),
            ("remote.origin.promisor", "true"),
            ("remote.origin.partialclonefilter", "blob:none"),
            ("gc.auto", "0"),
            ("maintenance.auto", "false"),
            ("fetch.writeCommitGraph", "false"),
        ] {
            let mut cmd = self.command(Some(dir));
            cmd.args(["config", key, value]);
            self.run(cmd, None, Duration::from_secs(30)).await?;
        }
        Ok(())
    }

    /// Fetch one commit and all of its trees, but no blobs and no history.
    pub async fn fetch_commit(&self, dir: &Path, commit: Oid) -> Result<(), GitError> {
        let mut cmd = self.command(Some(dir));
        cmd.args([
            "fetch",
            "-q",
            "--depth=1",
            "--filter=blob:none",
            "--no-tags",
            "--no-write-fetch-head",
            "--recurse-submodules=no",
            "origin",
        ])
        .arg(commit.to_string());
        self.run(cmd, None, Duration::from_secs(300)).await?;
        Ok(())
    }

    /// Fetch specific blobs in one round trip (what git's own promisor
    /// backfill does, but batched).
    pub async fn fetch_blobs(&self, dir: &Path, blobs: &[Oid]) -> Result<(), GitError> {
        if blobs.is_empty() {
            return Ok(());
        }
        let mut input = String::with_capacity(blobs.len() * 41);
        for oid in blobs {
            input.push_str(&oid.to_string());
            input.push('\n');
        }
        let mut cmd = self.command(Some(dir));
        cmd.args([
            "-c",
            "fetch.negotiationAlgorithm=noop",
            "fetch",
            "-q",
            "--no-tags",
            "--no-write-fetch-head",
            "--recurse-submodules=no",
            "--filter=blob:none",
            "origin",
            "--stdin",
        ]);
        self.run(cmd, Some(input.into_bytes()), Duration::from_secs(600))
            .await?;
        Ok(())
    }

    /// Write a local blob's contents to `out`, streamed (no full copy in
    /// memory). Never lazily fetches: the caller makes sure the blob is local.
    pub async fn cat_blob_to_file(
        &self,
        dir: &Path,
        blob: Oid,
        out: std::fs::File,
    ) -> Result<(), GitError> {
        let mut cmd = self.command(Some(dir));
        cmd.env("GIT_NO_LAZY_FETCH", "1")
            .args(["cat-file", "blob"])
            .arg(blob.to_string())
            .stdout(Stdio::from(out));
        self.run(cmd, None, Duration::from_secs(600)).await?;
        Ok(())
    }

    /// Consolidate packs. Every fetch leaves a pack behind, and object lookup
    /// cost grows with the pack count.
    pub async fn repack(&self, dir: &Path) -> Result<(), GitError> {
        let mut cmd = self.command(Some(dir));
        cmd.args(["repack", "-a", "-d", "-q", "--no-write-bitmap-index"]);
        self.run(cmd, None, Duration::from_secs(1800)).await?;
        Ok(())
    }
}

/// Redact `user:token@` from URLs in git error output.
pub fn redact_creds(s: &str) -> String {
    let needle = "https://";
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(idx) = rest.find(needle) {
        out.push_str(&rest[..idx + needle.len()]);
        rest = &rest[idx + needle.len()..];
        let end = rest
            .find(|c: char| c.is_whitespace() || c == '\'' || c == '"')
            .unwrap_or(rest.len());
        if let Some(at) = rest[..end].find('@') {
            out.push_str("***@");
            rest = &rest[at + 1..];
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const LS_REMOTE: &str = "ref: refs/heads/main\tHEAD\n\
        1111111111111111111111111111111111111111\tHEAD\n\
        1111111111111111111111111111111111111111\trefs/heads/main\n\
        2222222222222222222222222222222222222222\trefs/heads/feature/x\n\
        3333333333333333333333333333333333333333\trefs/tags/v1\n\
        4444444444444444444444444444444444444444\trefs/tags/v1^{}\n\
        5555555555555555555555555555555555555555\trefs/tags/main\n";

    fn oid(c: char) -> Oid {
        Oid::from_str(&c.to_string().repeat(40)).unwrap()
    }

    #[test]
    fn parses_symref_and_peels_tags() {
        let refs = parse_ls_remote(LS_REMOTE);
        assert_eq!(refs.head, Some(oid('1')));
        assert_eq!(refs.head_ref.as_deref(), Some("refs/heads/main"));
        assert_eq!(refs.resolve("v1"), Some(oid('4')));
        assert_eq!(refs.resolve("feature/x"), Some(oid('2')));
        assert_eq!(refs.resolve("refs/tags/main"), Some(oid('5')));
        // Branch wins over a same-named tag.
        assert_eq!(refs.resolve("main"), Some(oid('1')));
        assert_eq!(refs.resolve("nope"), None);
        assert_eq!(refs.short_names(), vec!["feature/x", "main", "v1"]);
    }

    #[test]
    fn classifies_not_found() {
        assert!(matches!(
            classify_failure("remote: Repository not found.\nfatal: repository 'x' not found"),
            GitError::RepoNotFound(_)
        ));
        assert!(matches!(
            classify_failure("fatal: could not read Username for 'https://github.com'"),
            GitError::RepoNotFound(_)
        ));
        assert!(matches!(
            classify_failure("fatal: unable to access: Could not resolve host: github.com"),
            GitError::Command(_)
        ));
        // A broken credential helper plus a network error is not "no such repo".
        assert!(matches!(
            classify_failure("sh: gh: not found\nfatal: unable to access: Connection reset"),
            GitError::Command(_)
        ));
    }

    #[test]
    fn redacts_userinfo() {
        assert_eq!(
            redact_creds("fatal: https://octo:tok@github.com/x.git failed"),
            "fatal: https://***@github.com/x.git failed"
        );
        assert_eq!(
            redact_creds("https://github.com/a/b.git and https://u:p@h/c"),
            "https://github.com/a/b.git and https://***@h/c"
        );
    }
}
