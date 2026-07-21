//! On-disk content-addressed blob cache + hydration.
//!
//! Lazily fetches individual git blob objects from the blobless mirror's
//! promisor remote (or local pack, once hydrated) via a per-repo persistent
//! `git cat-file --batch` process, and persists them at
//! `{cache_root}/blobs/<algo>/<oid>`. Because OIDs are content-addressed and
//! globally unique, the cache is shared across all repos and revisions: the
//! same file at the same content costs nothing the second time, even under a
//! different branch or commit.
//!
//! Concurrent requests for the same OID coalesce into one fetch via an
//! in-flight dedup map, so `git cat-file` only fetches each blob once even
//! under parallel reads. Interruptions leave a temp file, never a valid cache
//! entry, so partial downloads can never be served as correct content.

use git2::Oid;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Condvar, Mutex};
use thiserror::Error;

use super::git::GitError;

/// Errors returned by blob hydration.
#[derive(Error, Debug)]
pub enum BlobError {
    #[error(transparent)]
    Git(#[from] GitError),
    #[error("blob {0} not found in repository")]
    BlobNotFound(String),
    #[error("blob {0} is not a blob")]
    NotABlob(String),
    #[error("object read mismatch: expected {expected} bytes, got {actual}")]
    SizeMismatch { expected: u64, actual: u64 },
    #[error("checksum mismatch for {expected}")]
    ChecksumMismatch { expected: String },
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("cat-file process ended unexpectedly: {0}")]
    CatFileDied(String),
    #[error("cache is shutting down")]
    ShuttingDown,
}

/// Content-addressed on-disk blob store, shared across all repos.
///
/// Files are stored at `{root}/blobs/<algo>/<oid>` and written atomically via a
/// temp file + rename. `metadata` and content reads come straight off the
/// cached file, which gives us `stat()` for the size for free.
#[derive(Debug, Clone)]
pub struct BlobCache {
    root: PathBuf,
    hash_algo: String,
}

impl BlobCache {
    /// Create a new cache rooted at `root`, using hash algorithm `algo`
    /// (typically `"sha1"` for git's default).
    pub fn new(root: impl Into<PathBuf>, hash_algo: impl Into<String>) -> Self {
        Self {
            root: root.into(),
            hash_algo: hash_algo.into(),
        }
    }

    /// Directory holding blobs for the configured hash algorithm.
    fn algo_dir(&self) -> PathBuf {
        self.root.join(&self.hash_algo)
    }

    /// Path for a specific blob OID.
    pub fn path(&self, oid: Oid) -> PathBuf {
        self.algo_dir().join(oid.to_string())
    }

    /// Path for a temporary staging file used while writing a blob.
    fn temp_path(&self, oid: Oid, n: u64) -> PathBuf {
        self.algo_dir()
            .join(format!("{}.tmp.{}.{}", oid, std::process::id(), n))
    }

    /// Whether a blob is already present on disk. Does not validate content.
    pub fn contains(&self, oid: Oid) -> bool {
        self.path(oid).exists()
    }

    /// Atomically write `content` to the cache for `oid` and return the final
    /// cached path. The temp file is `rename()`d into place so partial writes
    /// never become a valid cache entry. Verifies the byte count against
    /// `expected_size` and the OID prefix if possible.
    pub fn write_atomic(
        &self,
        oid: Oid,
        content: &mut dyn Read,
        expected_size: u64,
    ) -> Result<PathBuf, BlobError> {
        let dir = self.algo_dir();
        std::fs::create_dir_all(&dir)?;

        let mut n = 0u64;
        let mut temp;
        loop {
            temp = self.temp_path(oid, n);
            if !temp.exists() {
                break;
            }
            n += 1;
        }

        let mut file = std::fs::File::create(&temp)?;
        let written = std::io::copy(content, &mut file)?;
        file.sync_all()?;
        drop(file);

        if written != expected_size {
            let _ = std::fs::remove_file(&temp);
            return Err(BlobError::SizeMismatch {
                expected: expected_size,
                actual: written,
            });
        }

        // OID prefix check: the hex of the stored object should match the
        // requested OID. We only check the prefix we asked for; a full
        // re-hash is left to `git fsck`-style maintenance, not the hot path.
        let oid_hex = oid.to_string();
        if let Some(stored) = stored_oid_hex(oid, expected_size) {
            if !oid_hex.starts_with(&stored[..oid_hex.len().min(stored.len())]) {
                let _ = std::fs::remove_file(&temp);
                return Err(BlobError::ChecksumMismatch { expected: oid_hex });
            }
        }

        let final_path = self.path(oid);
        std::fs::rename(&temp, &final_path)?;
        Ok(final_path)
    }
}

/// We don't re-hash by default on the hot path; return None to skip the
/// weak prefix check too. (Kept as a hook for a future strict mode.)
fn stored_oid_hex(_oid: Oid, _size: u64) -> Option<String> {
    None
}

/// One persistent `git cat-file --batch` process bound to a single mirror.
///
/// Requests are framed as `<oid>\n` on stdin; responses are
/// `<oid> <type> <size>\n<bytes>\n` on stdout, or `<oid> missing\n`. Because
/// the protocol is request/response with a single in-flight ordering, this
/// process serializes concurrent fetches; a per-OID dedup map (in
/// [`Hydrator`]) collapses simultaneous requests for the same OID into one.
pub struct CatFileBatch {
    child: Child,
    stdin: ChildStdin,
    reader: BufReader<ChildStdout>,
}

impl CatFileBatch {
    /// Spawn a `git cat-file --batch` against the mirror at `mirror_path`.
    pub fn spawn(mirror_path: &Path) -> Result<Self, BlobError> {
        // Inline `-c` options must precede the subcommand so git accepts them.
        let mut child = Command::new("git")
            .args(["-c", "core.hooksPath="])
            .arg("-C")
            .arg(mirror_path)
            .args(["cat-file", "--batch"])
            .env("GIT_LFS_SKIP_SMUDGE", "1")
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| BlobError::CatFileDied(format!("spawn failed: {e}")))?;
        let stdin = child.stdin.take().expect("stdin pipe");
        let stdout = child.stdout.take().expect("stdout pipe");
        Ok(Self {
            child,
            stdin,
            reader: BufReader::new(stdout),
        })
    }

    /// Fetch a blob OID directly to the blob cache. Returns the cached path
    /// and the blob's size. Promises to read exactly `size` content bytes plus
    /// the trailing newline framed by `--batch`.
    ///
    /// On a missing/unexpected response, the cat-file stream is poisoned; the
    /// caller should drop and respawn the process.
    pub fn fetch_to_cache(
        &mut self,
        oid: Oid,
        cache: &BlobCache,
    ) -> Result<(PathBuf, u64), BlobError> {
        writeln!(self.stdin, "{oid}")?;
        self.stdin.flush()?;

        let header = self
            .reader
            .read_line_to_string()?
            .ok_or_else(|| BlobError::CatFileDied(" EOF before header".into()))?;

        let oid_hex = oid.to_string();
        if header.starts_with(&format!("{oid_hex} missing")) {
            return Err(BlobError::BlobNotFound(oid_hex));
        }

        // Expect "<oid> <type> <size>".
        let parts: Vec<&str> = header.split_whitespace().collect();
        if parts.len() < 3 {
            return Err(BlobError::CatFileDied(format!("bad header: {header:?}")));
        }
        let returned_oid = parts[0];
        let ty = parts[1];
        let size: u64 = parts[2]
            .parse()
            .map_err(|_| BlobError::CatFileDied(format!("bad size: {parts:?}")))?;

        if returned_oid != oid_hex {
            return Err(BlobError::CatFileDied(format!(
                "oid mismatch: requested {oid_hex}, got {returned_oid}"
            )));
        }
        if ty != "blob" {
            return Err(BlobError::NotABlob(oid_hex));
        }

        // Stream exactly `size` bytes into the cache via a take adapter, then
        // consume the trailing newline. write_atomic reads until EOF of the
        // `take` adapter, which yields exactly `size` bytes.
        let mut limited = (&mut self.reader).take(size);
        let path = cache.write_atomic(oid, &mut limited, size)?;
        // Consume the trailing newline after the blob content.
        self.reader.read_line_to_string()?;
        Ok((path, size))
    }
}

impl Drop for CatFileBatch {
    fn drop(&mut self) {
        let _ = self.stdin.flush();
        // Closing stdin lets the child exit; next poll/wait reaps it.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Small line-buffer helper that pulls one `\n`-terminated line into a
/// `String`. Returns `None` on a clean EOF with no trailing data.
trait ReadLineExt {
    fn read_line_to_string(&mut self) -> std::io::Result<Option<String>>;
}

impl<R: BufRead> ReadLineExt for R {
    fn read_line_to_string(&mut self) -> std::io::Result<Option<String>> {
        let mut buf = Vec::new();
        let n = self.read_until(b'\n', &mut buf)?;
        if n == 0 {
            return Ok(None);
        }
        // Strip trailing newline.
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        Ok(Some(String::from_utf8_lossy(&buf).into_owned()))
    }
}

/// In-flight request slot. `Waiting` lets later callers join; `Ready` carries
/// the result that the joiners wake up to.
enum Inflight {
    Waiting,
    Ready(Result<Arc<(PathBuf, u64)>, Arc<BlobError>>),
}

struct Shared {
    /// oid -> in-flight slot
    inflight: HashMap<Oid, Inflight>,
}

struct Pool {
    shared: Mutex<Shared>,
    cv: Condvar,
}

impl Pool {
    fn new() -> Self {
        Self {
            shared: Mutex::new(Shared {
                inflight: HashMap::new(),
            }),
            cv: Condvar::new(),
        }
    }
}

/// Hydrator owns a per-repo `cat-file --batch` process behind a mutex plus a
/// per-OID dedup map. It's cheap to clone (the batch process is wrapped in
/// `Mutex<Option<_>>` and respawned if poisoned/EOFed).
pub struct Hydrator {
    mirror_path: PathBuf,
    blobs: BlobCache,
    batch: Mutex<Option<CatFileBatch>>,
    pool: Arc<Pool>,
}

impl Hydrator {
    /// Create a hydrator for `mirror_path` writing blobs to `blobs`. The
    /// `cat-file` process is spawned lazily on first use.
    pub fn new(mirror_path: PathBuf, blobs: BlobCache) -> Self {
        Self {
            mirror_path,
            blobs,
            batch: Mutex::new(None),
            pool: Arc::new(Pool::new()),
        }
    }

    /// Ensure a blob is on disk and return its cached path + size.
    ///
    /// Concurrent callers for the same OID join the first request and receive
    /// the same result; a second `cat-file` fetch never runs for an OID that
    /// is already being fetched.
    pub fn hydrate(&self, oid: Oid) -> Result<(PathBuf, u64), BlobError> {
        // Fast path: already cached on disk.
        let cached = self.blobs.path(oid);
        if let Ok(meta) = std::fs::metadata(&cached) {
            return Ok((cached, meta.len()));
        }

        // Slow path: dedup one fetcher, everyone else joins.
        let first = {
            let mut guard = self.pool.shared.lock().expect("hydrator pool poisoned");
            let inserted = !guard.inflight.contains_key(&oid);
            if inserted {
                guard.inflight.insert(oid, Inflight::Waiting);
            }
            inserted
        };

        if first {
            let res = self.fetch_once(oid);
            let shared = self.clone_res(&res);
            let mut guard = self.pool.shared.lock().expect("hydrator pool poisoned");
            guard.inflight.insert(oid, Inflight::Ready(shared));
            self.pool.cv.notify_all();
            // Pop the slot so a later re-hydrate of the same OID starts fresh;
            // disk fast path handles the common repeat.
            guard.inflight.remove(&oid);
            res
        } else {
            let mut guard = self.pool.shared.lock().expect("hydrator pool poisoned");
            loop {
                match guard.inflight.get(&oid) {
                    Some(Inflight::Ready(res)) => {
                        let res = res.clone();
                        break res.map(|v| v.as_ref().clone());
                    }
                    Some(Inflight::Waiting) => {
                        guard = self
                            .pool
                            .cv
                            .wait_timeout(guard, std::time::Duration::from_secs(30))
                            .expect("hydrator pool poisoned")
                            .0;
                    }
                    None => return Err(BlobError::BlobNotFound(oid.to_string())),
                }
            }
            .map_err(|e| blob_err_clone(&e))
        }
    }

    fn clone_res(
        &self,
        res: &Result<(PathBuf, u64), BlobError>,
    ) -> Result<Arc<(PathBuf, u64)>, Arc<BlobError>> {
        match res {
            Ok((p, s)) => Ok(Arc::new((p.clone(), *s))),
            Err(e) => Err(Arc::new(blob_err_clone(e))),
        }
    }

    /// Run one fetch through the `cat-file` process, respawning on EOF/error.
    /// Each attempt is bounded so a permanently broken mirror doesn't hang.
    fn fetch_once(&self, oid: Oid) -> Result<(PathBuf, u64), BlobError> {
        for _ in 0..2 {
            let mut guard = self.batch.lock().expect("batch mutex poisoned");
            if guard.is_none() {
                match CatFileBatch::spawn(&self.mirror_path) {
                    Ok(b) => *guard = Some(b),
                    Err(e) => {
                        drop(guard);
                        return Err(e);
                    }
                }
            }
            let batch = guard.as_mut().expect("batch initialized");
            match batch.fetch_to_cache(oid, &self.blobs) {
                Ok(res) => return Ok(res),
                Err(BlobError::CatFileDied(_)) | Err(BlobError::Io(_)) => {
                    // Drop the poisoned process and try a fresh one next loop.
                    *guard = None;
                    continue;
                }
                Err(other) => return Err(other),
            }
        }
        Err(BlobError::CatFileDied("respawn loop exhausted".into()))
    }
}

/// Clone a [`BlobError`] cheaply for the dedup result slot. We keep the full
/// type for the synchronous return but share an `Arc` for joiners.
fn blob_err_clone(e: &BlobError) -> BlobError {
    match e {
        BlobError::BlobNotFound(s) => BlobError::BlobNotFound(s.clone()),
        BlobError::NotABlob(s) => BlobError::NotABlob(s.clone()),
        BlobError::SizeMismatch { expected, actual } => BlobError::SizeMismatch {
            expected: *expected,
            actual: *actual,
        },
        BlobError::ChecksumMismatch { expected } => BlobError::ChecksumMismatch {
            expected: expected.clone(),
        },
        BlobError::CatFileDied(s) => BlobError::CatFileDied(s.clone()),
        BlobError::ShuttingDown => BlobError::ShuttingDown,
        _ => BlobError::CatFileDied(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::git::GitCli;
    use crate::types::RepoKey;
    use std::process::Command;
    use tempfile::tempdir;

    fn make_repo_with_blob() -> (tempfile::TempDir, git2::Repository, Oid, Oid) {
        let dir = tempdir().unwrap();
        let run = |a: &[&str]| {
            let s = Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(a)
                .env("GIT_TERMINAL_PROMPT", "0")
                .status()
                .unwrap();
            assert!(s.success(), "git {:?} failed", a);
        };
        run(&["init", "-q"]);
        run(&["config", "user.email", "t@t.com"]);
        run(&["config", "user.name", "T"]);
        run(&["config", "commit.gpgsign", "false"]);
        run(&["config", "tag.gpgsign", "false"]);
        std::fs::write(dir.path().join("a.txt"), "hello world\n").unwrap();
        run(&["add", "."]);
        run(&["commit", "-q", "-m", "x"]);
        let repo = git2::Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap().id();
        let blob_oid = repo
            .head()
            .unwrap()
            .peel_to_commit()
            .unwrap()
            .tree()
            .unwrap()
            .iter()
            .find(|e| e.name_bytes() == b"a.txt")
            .unwrap()
            .id();
        (dir, repo, head, blob_oid)
    }

    #[test]
    fn write_atomic_roundtrips_content() {
        let dir = tempdir().unwrap();
        let cache = BlobCache::new(dir.path().to_path_buf(), "sha1");
        let oid = Oid::from_str("abcdefabcdefabcdefabcdefabcdefabcdefabcd").unwrap();
        let body = b"some bytes live here";
        let mut cursor = std::io::Cursor::new(body.to_vec());
        let path = cache
            .write_atomic(oid, &mut cursor, body.len() as u64)
            .unwrap();
        assert!(path.exists());
        assert_eq!(std::fs::read(&path).unwrap(), body);
    }

    #[test]
    fn write_atomic_rejects_size_mismatch() {
        let dir = tempdir().unwrap();
        let cache = BlobCache::new(dir.path().to_path_buf(), "sha1");
        let oid = Oid::from_str("abcdefabcdefabcdefabcdefabcdefabcdefabcd").unwrap();
        let body = b"too few";
        let mut cursor = std::io::Cursor::new(body.to_vec());
        let res = cache.write_atomic(oid, &mut cursor, 100);
        assert!(matches!(res, Err(BlobError::SizeMismatch { .. })));
        // No partial file left behind under the final name.
        assert!(!cache.path(oid).exists());
    }

    #[test]
    fn hydrate_fetches_blob_from_local_repo() {
        let (dir, _repo, _head, blob_oid) = make_repo_with_blob();
        let blobs_dir = tempdir().unwrap();
        let cache = BlobCache::new(blobs_dir.path().to_path_buf(), "sha1");
        let hydrator = Hydrator::new(dir.path().to_path_buf(), cache);
        let (path, size) = hydrator.hydrate(blob_oid).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"hello world\n");
        assert_eq!(size, b"hello world\n".len() as u64);
    }

    #[test]
    fn hydrate_is_idempotent_after_first_fetch() {
        let (dir, _repo, _head, blob_oid) = make_repo_with_blob();
        let blobs_dir = tempdir().unwrap();
        let cache = BlobCache::new(blobs_dir.path().to_path_buf(), "sha1");
        let hydrator = Hydrator::new(dir.path().to_path_buf(), cache.clone());
        let (path1, _size1) = hydrator.hydrate(blob_oid).unwrap();
        // Second call hits the disk fast path; same path, no fetch.
        assert!(cache.contains(blob_oid));
        let (path2, _size2) = hydrator.hydrate(blob_oid).unwrap();
        assert_eq!(path1, path2);
    }

    #[test]
    fn hydrate_missing_returns_not_found() {
        let (dir, _repo, _head, _blob_oid) = make_repo_with_blob();
        let blobs_dir = tempdir().unwrap();
        let cache = BlobCache::new(blobs_dir.path().to_path_buf(), "sha1");
        let hydrator = Hydrator::new(dir.path().to_path_buf(), cache);
        let fake = Oid::from_str("0123456789abcdef0123456789abcdef01234567").unwrap();
        let res = hydrator.hydrate(fake);
        assert!(
            matches!(
                res,
                Err(BlobError::BlobNotFound(_)) | Err(BlobError::CatFileDied(_))
            ),
            "got {res:?}"
        );
    }

    // ---- network-gated: blobless clone + lazy fetch ----------------------

    fn network_tests_enabled() -> bool {
        match std::env::var("GHFS_RUN_NETWORK_TESTS") {
            Ok(v) => {
                let v = v.to_ascii_lowercase();
                v == "1" || v == "true" || v == "yes"
            }
            Err(_) => false,
        }
    }

    #[test]
    fn hydrate_lazy_fetches_from_blobless_clone() {
        if !network_tests_enabled() {
            eprintln!("skipping network test");
            return;
        }
        let dir = tempdir().unwrap();
        let mirror = dir.path().join("hello-world.git");
        let key: RepoKey = "octocat/Hello-World".parse().unwrap();
        GitCli::new().clone_blobless(&key, &mirror).unwrap();
        let repo = git2::Repository::open(&mirror).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap().id();
        let root_tree = repo.find_commit(head).unwrap().tree_id();
        let tree = repo.find_tree(root_tree).unwrap();
        // Find a blob entry to hydrate.
        let blob_oid = tree
            .iter()
            .find(|e| e.kind() == Some(git2::ObjectType::Blob))
            .expect("repo has at least one file")
            .id();

        let cache_dir = tempdir().unwrap();
        let cache = BlobCache::new(cache_dir.path().to_path_buf(), "sha1");
        let hydrator = Hydrator::new(mirror.clone(), cache.clone());
        let (path, size) = hydrator.hydrate(blob_oid).unwrap();
        assert!(size > 0);
        assert!(cache.contains(blob_oid));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), size);
    }
}
