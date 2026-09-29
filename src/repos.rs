//! Per-repo state: ref resolution, commit/blob fetching, and local reads.
//!
//! Filesystem requests should only wait on work they actually need:
//!
//! - HEAD and ref lists are cached. A stale cached value is returned right away
//!   and refreshed in the background, so after the first visit a repo never
//!   waits on the network just to resolve HEAD (and keeps working offline).
//! - Commits are fetched shallowly (one commit, its trees, no blobs).
//! - Blobs are fetched per directory in one batch the first time any file in
//!   that directory needs its size or contents.
//! - Concurrent requests for the same commit/directory share one fetch.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use dashmap::DashMap;
use git2::{Oid, Repository};
use thiserror::Error;

use crate::git::{GitCli, GitError, RemoteRefs, TreeEntry, odb};
use crate::paths::CachePaths;
use crate::singleflight::SingleFlight;
use crate::types::RepoKey;

/// How long a resolved HEAD is trusted before a background re-check.
const DEFAULT_HEAD_TTL: Duration = Duration::from_secs(60);
/// How long a branch/tag listing is trusted before a background re-check.
const REFS_TTL: Duration = Duration::from_secs(300);
/// How long "this repo doesn't exist" is remembered.
const NOT_FOUND_TTL: Duration = Duration::from_secs(600);
/// Directories with more missing blobs than this are fetched per file on
/// demand (and in chunks of this size when prefetching).
const DIR_BATCH_MAX: usize = 1000;
/// Repack once a repo has accumulated this many packs.
const REPACK_THRESHOLD: usize = 50;
/// Most directories a single subtree prefetch covers.
const PREFETCH_MAX_TREES: usize = 256;
/// Blobs up to this size are written to the file cache straight from libgit2;
/// bigger ones are streamed through `git cat-file` so they're never fully in
/// memory.
const STREAM_BLOB_OVER: u64 = 8 << 20;
/// Most libgit2 handles (each holding pack files open) per repo. Blocking
/// reads beyond this wait for a free handle instead of opening more, which
/// would run out of file descriptors (macOS defaults to 256).
const MAX_REPO_HANDLES: usize = 8;
/// Background directory prefetches running at once. Requests that actually
/// wait on a directory don't take a slot, so they never queue behind these.
const MAX_PREFETCHES: usize = 8;

const HEAD_META_FILE: &str = "ghfs-head";

#[derive(Debug, Clone, Error)]
pub enum RepoError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Failed(String),
}

impl From<GitError> for RepoError {
    fn from(e: GitError) -> Self {
        match e {
            GitError::RepoNotFound(msg) => Self::NotFound(msg),
            GitError::MissingObject(oid) => Self::NotFound(format!("object {oid}")),
            other => Self::Failed(other.to_string()),
        }
    }
}

impl RepoError {
    pub fn errno(&self) -> i32 {
        match self {
            Self::NotFound(_) => libc::ENOENT,
            Self::Failed(_) => libc::EIO,
        }
    }
}

pub type Result<T> = std::result::Result<T, RepoError>;

#[derive(Clone)]
struct Cached<T> {
    value: T,
    at: Instant,
}

/// All repos known to this daemon.
pub struct Repos {
    paths: CachePaths,
    git: GitCli,
    repos: DashMap<RepoKey, Arc<RepoHandle>>,
    not_found: DashMap<RepoKey, Instant>,
    started: SystemTime,
    prefetch_slots: Arc<tokio::sync::Semaphore>,
    /// Directory prefetches spawned and not yet finished.
    prefetches: Arc<std::sync::atomic::AtomicUsize>,
    head_ttl_ms: AtomicU64,
}

impl Repos {
    pub fn new(paths: CachePaths, git: GitCli) -> Arc<Self> {
        Arc::new(Self {
            paths,
            git,
            repos: DashMap::new(),
            not_found: DashMap::new(),
            started: SystemTime::now(),
            prefetch_slots: Arc::new(tokio::sync::Semaphore::new(MAX_PREFETCHES)),
            prefetches: Arc::default(),
            head_ttl_ms: AtomicU64::new(DEFAULT_HEAD_TTL.as_millis() as u64),
        })
    }

    /// How long a resolved HEAD is trusted before a background re-check.
    pub fn set_head_ttl(&self, ttl: Duration) {
        self.head_ttl_ms
            .store(ttl.as_millis() as u64, Ordering::Relaxed);
    }

    fn head_ttl(&self) -> Duration {
        Duration::from_millis(self.head_ttl_ms.load(Ordering::Relaxed))
    }

    pub fn paths(&self) -> &CachePaths {
        &self.paths
    }

    pub fn get(self: &Arc<Self>, key: &RepoKey) -> Arc<RepoHandle> {
        let key = &key.normalized();
        if let Some(handle) = self.repos.get(key) {
            return handle.clone();
        }
        self.repos
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(RepoHandle {
                    key: key.clone(),
                    dir: self.paths.repo_dir(key),
                    url: self.git.url(key.owner.as_str(), key.repo.as_str()),
                    git: self.git.clone(),
                    repos: Arc::downgrade(self),
                    init: tokio::sync::OnceCell::new(),
                    pool: Mutex::new(Vec::new()),
                    pool_generation: AtomicU64::new(0),
                    pool_slots: Arc::new(tokio::sync::Semaphore::new(MAX_REPO_HANDLES)),
                    head: Mutex::new(None),
                    refs: Mutex::new(None),
                    head_flight: SingleFlight::default(),
                    refs_flight: SingleFlight::default(),
                    commit_flight: SingleFlight::default(),
                    blob_flight: SingleFlight::default(),
                    bulk_flight: SingleFlight::default(),
                    materialize_flight: SingleFlight::default(),
                    verified: Mutex::new(std::collections::HashSet::new()),
                    commit_fetch: tokio::sync::Mutex::new(()),
                    prefetched: Mutex::new(std::collections::HashSet::new()),
                    repacking: AtomicBool::new(false),
                    changed_at: Mutex::new(self.started),
                })
            })
            .clone()
    }

    /// Whether `key` is known not to exist (cached for [`NOT_FOUND_TTL`]).
    pub fn known_missing(&self, key: &RepoKey) -> bool {
        // Copy the answer out before removing: holding the read guard across
        // `remove` deadlocks on the shard.
        let Some(expired) = self
            .not_found
            .get(key)
            .map(|at| at.elapsed() >= NOT_FOUND_TTL)
        else {
            return false;
        };
        if expired {
            self.not_found
                .remove_if(key, |_, at| at.elapsed() >= NOT_FOUND_TTL);
        }
        !expired
    }

    fn mark_missing(&self, key: &RepoKey) {
        self.not_found.insert(key.clone(), Instant::now());
    }

    pub fn started(&self) -> SystemTime {
        self.started
    }

    /// Human-readable descriptions of in-flight network work.
    pub fn activity(&self) -> Vec<String> {
        let mut out = Vec::new();
        let prefetches = self.prefetches.load(Ordering::Acquire);
        if prefetches > 0 {
            out.push(format!("{prefetches} directory prefetch(es) pending"));
        }
        for entry in self.repos.iter() {
            let handle = entry.value();
            if !handle.head_flight.keys().is_empty() {
                out.push(format!("{}: resolving HEAD", handle.key));
            }
            if !handle.refs_flight.keys().is_empty() {
                out.push(format!("{}: listing refs", handle.key));
            }
            for commit in handle.commit_flight.keys() {
                out.push(format!("{}: fetching commit {commit}", handle.key));
            }
            let blobs = handle.blob_flight.keys().len() + handle.bulk_flight.keys().len();
            if blobs > 0 {
                out.push(format!("{}: fetching {blobs} blob batch(es)", handle.key));
            }
            if handle.repacking.load(Ordering::Relaxed) {
                out.push(format!("{}: repacking", handle.key));
            }
        }
        out.sort();
        out
    }
}

pub struct RepoHandle {
    pub key: RepoKey,
    dir: PathBuf,
    url: String,
    git: GitCli,
    repos: std::sync::Weak<Repos>,
    init: tokio::sync::OnceCell<()>,
    /// Checked-out libgit2 handles for blocking reads. `Repository` isn't
    /// `Sync`, so each blocking task borrows its own.
    pool: Mutex<Vec<Repository>>,
    /// Bumped after a repack so handles with stale pack lists get dropped.
    pool_generation: AtomicU64,
    /// Caps concurrent blocking reads (and so open handles) per repo.
    pool_slots: Arc<tokio::sync::Semaphore>,
    head: Mutex<Option<Cached<RemoteRefs>>>,
    refs: Mutex<Option<Cached<Arc<RemoteRefs>>>>,
    head_flight: SingleFlight<(), Result<RemoteRefs>>,
    refs_flight: SingleFlight<(), Result<Arc<RemoteRefs>>>,
    commit_flight: SingleFlight<Oid, Result<()>>,
    /// Keyed by directory tree oid (batch) or blob oid (single). Every fetch
    /// under these keys is at most one batch, so a request that joins one
    /// waits for at most one round trip.
    /// The value says whether the directory fit in one batch.
    blob_flight: SingleFlight<Oid, Result<bool>>,
    /// Background chunked fetches of directories too big for one batch. Kept
    /// apart so foreground requests never join them.
    bulk_flight: SingleFlight<Oid, Result<()>>,
    /// Writing a blob's contents to the on-disk cache, keyed by blob oid.
    materialize_flight: SingleFlight<Oid, Result<PathBuf>>,
    /// Cached blob files whose length was checked against the object this run.
    verified: Mutex<std::collections::HashSet<Oid>>,
    /// Shallow fetches take `shallow.lock`, and git fails instead of waiting
    /// when it's held, so commit fetches in one repo run one at a time.
    commit_fetch: tokio::sync::Mutex<()>,
    /// Trees whose subtree prefetch is scheduled or done (FUSE and NFS list
    /// a directory in pages; only the first page should start one).
    prefetched: Mutex<std::collections::HashSet<Oid>>,
    repacking: AtomicBool,
    /// When HEAD or the ref listing last changed. Used as the directory mtime
    /// of `<owner>/<repo>` and `by-ref/<owner>/<repo>` so kernels revalidate
    /// cached names after a branch moves.
    changed_at: Mutex<SystemTime>,
}

impl RepoHandle {
    pub fn dir(&self) -> &std::path::Path {
        &self.dir
    }

    pub fn changed_at(&self) -> SystemTime {
        *self.changed_at.lock().expect("changed_at poisoned")
    }

    fn touch(&self) {
        *self.changed_at.lock().expect("changed_at poisoned") = SystemTime::now();
    }

    /// Age of the ref listing, if we have one.
    pub fn refs_age(&self) -> Option<Duration> {
        self.refs
            .lock()
            .expect("refs poisoned")
            .as_ref()
            .map(|c| c.at.elapsed())
    }

    /// Run a blocking libgit2 read on the blocking pool.
    pub async fn with_repo<T, F>(self: &Arc<Self>, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: Fn(&Repository) -> std::result::Result<T, GitError> + Send + 'static,
    {
        let slot = self
            .pool_slots
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore never closed");
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = slot;
            this.with_repo_blocking(f)
        })
        .await
        .map_err(|e| RepoError::Failed(format!("blocking task failed: {e}")))?
    }

    fn with_repo_blocking<T>(
        &self,
        f: impl Fn(&Repository) -> std::result::Result<T, GitError>,
    ) -> Result<T> {
        for attempt in 0..2 {
            let generation = self.pool_generation.load(Ordering::Acquire);
            let repo = match self.pool.lock().expect("repo pool poisoned").pop() {
                Some(repo) => repo,
                None => odb::open(&self.dir)?,
            };
            let result = f(&repo);
            let transient = matches!(result, Err(GitError::Libgit2(_)));
            if !transient && self.pool_generation.load(Ordering::Acquire) == generation {
                self.pool.lock().expect("repo pool poisoned").push(repo);
            }
            match result {
                // A pack may have been swapped out by a repack under us; retry
                // once with a fresh handle.
                Err(GitError::Libgit2(e)) if attempt == 0 => {
                    log::debug!("{}: retrying libgit2 read after: {e}", self.key);
                }
                other => return other.map_err(Into::into),
            }
        }
        unreachable!("loop returns on second attempt")
    }

    fn cached_head(&self) -> Option<Cached<RemoteRefs>> {
        if let Some(cached) = self.head.lock().expect("head poisoned").clone() {
            return Some(cached);
        }
        // Fall back to the HEAD persisted by a previous run; treat it as stale.
        let text = std::fs::read_to_string(self.dir.join(HEAD_META_FILE)).ok()?;
        let mut lines = text.lines();
        let head = Oid::from_str(lines.next()?).ok()?;
        let head_ref = lines.next().filter(|s| !s.is_empty()).map(str::to_owned);
        let cached = Cached {
            value: RemoteRefs {
                head: Some(head),
                head_ref,
                refs: Vec::new(),
            },
            // Stale: re-check on first use.
            at: Instant::now() - Duration::from_secs(24 * 3600),
        };
        *self.head.lock().expect("head poisoned") = Some(cached.clone());
        Some(cached)
    }

    /// HEAD as last resolved (memory or disk), without any network access.
    pub fn cached_head_commit(&self) -> Option<Oid> {
        self.cached_head().and_then(|c| c.value.head)
    }

    /// Start a background HEAD refresh if the cached one is stale. Never waits.
    pub fn poke_head(self: &Arc<Self>) {
        let Some(cached) = self.cached_head() else {
            return;
        };
        let ttl = self
            .repos
            .upgrade()
            .map_or(DEFAULT_HEAD_TTL, |r| r.head_ttl());
        if cached.at.elapsed() < ttl || self.head_flight.contains(&()) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.refresh_head().await {
                log::warn!("{}: background HEAD refresh failed: {e}", this.key);
            }
        });
    }

    /// Start a background refs refresh if the cached listing is stale.
    pub fn poke_refs(self: &Arc<Self>) {
        if self.refs_age().is_none_or(|age| age < REFS_TTL) || self.refs_flight.contains(&()) {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            if let Err(e) = this.refresh_refs().await {
                log::warn!("{}: background refs refresh failed: {e}", this.key);
            }
        });
    }

    /// Default-branch commit. Only waits on the network the very first time.
    pub async fn head(self: &Arc<Self>) -> Result<Oid> {
        if let Some(cached) = self.cached_head()
            && let Some(head) = cached.value.head
        {
            self.poke_head();
            return Ok(head);
        }
        if let Some(repos) = self.repos.upgrade()
            && repos.known_missing(&self.key)
        {
            return Err(RepoError::NotFound(self.key.to_string()));
        }
        self.refresh_head()
            .await?
            .head
            .ok_or_else(|| RepoError::NotFound(self.key.to_string()))
    }

    /// Re-resolve HEAD from GitHub now.
    pub async fn refresh_head(self: &Arc<Self>) -> Result<RemoteRefs> {
        let this = self.clone();
        self.head_flight
            .run((), || async move {
                let refs = match this.git.ls_remote_head(&this.url).await {
                    Ok(refs) => refs,
                    Err(e) => {
                        if matches!(e, GitError::RepoNotFound(_))
                            && let Some(repos) = this.repos.upgrade()
                        {
                            repos.mark_missing(&this.key);
                        }
                        // Keep serving the cached HEAD and wait a full TTL
                        // before retrying, so being offline doesn't turn every
                        // lookup into a failing `git ls-remote`.
                        if let Some(cached) = this.head.lock().expect("head poisoned").as_mut() {
                            cached.at = Instant::now();
                        }
                        return Err(e.into());
                    }
                };
                let previous = this.head.lock().expect("head poisoned").replace(Cached {
                    value: refs.clone(),
                    at: Instant::now(),
                });
                let changed = previous.and_then(|p| p.value.head) != refs.head;
                if changed {
                    this.touch();
                    this.persist_head(&refs);
                }
                Ok(refs)
            })
            .await
    }

    fn persist_head(&self, refs: &RemoteRefs) {
        let Some(head) = refs.head else { return };
        if !self.dir.exists() {
            return;
        }
        let text = format!("{head}\n{}\n", refs.head_ref.as_deref().unwrap_or(""));
        let tmp = self.dir.join(format!("{HEAD_META_FILE}.tmp"));
        if std::fs::write(&tmp, text).is_ok() {
            let _ = std::fs::rename(&tmp, self.dir.join(HEAD_META_FILE));
        }
    }

    /// Branches and tags. Waits on the network only the first time.
    pub async fn refs(self: &Arc<Self>, force: bool) -> Result<Arc<RemoteRefs>> {
        let cached = self.refs.lock().expect("refs poisoned").clone();
        if !force && let Some(cached) = cached {
            self.poke_refs();
            return Ok(cached.value);
        }
        self.refresh_refs().await
    }

    async fn refresh_refs(self: &Arc<Self>) -> Result<Arc<RemoteRefs>> {
        let this = self.clone();
        self.refs_flight
            .run((), || async move {
                let refs = match this.git.ls_remote_all(&this.url).await {
                    Ok(refs) => Arc::new(refs),
                    Err(e) => {
                        if let Some(cached) = this.refs.lock().expect("refs poisoned").as_mut() {
                            cached.at = Instant::now();
                        }
                        return Err(e.into());
                    }
                };
                let previous = this.refs.lock().expect("refs poisoned").replace(Cached {
                    value: refs.clone(),
                    at: Instant::now(),
                });
                if previous.is_some_and(|p| p.value != refs) {
                    this.touch();
                }
                Ok(refs)
            })
            .await
    }

    /// Resolve a by-ref selector: a full commit id, `HEAD`, or a branch/tag.
    pub async fn resolve(self: &Arc<Self>, selector: &str) -> Result<Oid> {
        if selector.len() == 40
            && selector.bytes().all(|b| b.is_ascii_hexdigit())
            && let Ok(oid) = Oid::from_str(selector)
        {
            return Ok(oid);
        }
        if selector == "HEAD" {
            return self.head().await;
        }
        let refs = self.refs(false).await?;
        if let Some(oid) = refs.resolve(selector) {
            return Ok(oid);
        }
        // Maybe the ref is newer than our listing; re-list at most every 30s
        // so a stream of bogus names can't turn into a stream of requests.
        if self
            .refs_age()
            .is_some_and(|age| age < Duration::from_secs(30))
        {
            return Err(RepoError::NotFound(format!("{}: ref {selector}", self.key)));
        }
        let refs = self.refs(true).await?;
        refs.resolve(selector)
            .ok_or_else(|| RepoError::NotFound(format!("{}: ref {selector}", self.key)))
    }

    async fn ensure_init(self: &Arc<Self>) -> Result<()> {
        self.init
            .get_or_try_init(|| async {
                if self.dir.join("objects").exists() {
                    return Ok(());
                }
                // Init into a temp dir and rename, so a crash never leaves a
                // half-configured repo in place.
                let tmp = self.dir.with_extension("init.tmp");
                let _ = tokio::fs::remove_dir_all(&tmp).await;
                if let Some(parent) = self.dir.parent() {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .map_err(|e| RepoError::Failed(e.to_string()))?;
                }
                self.git.init_repo(&tmp, &self.url).await?;
                tokio::fs::rename(&tmp, &self.dir)
                    .await
                    .map_err(|e| RepoError::Failed(e.to_string()))?;
                if let Some(cached) = self.head.lock().expect("head poisoned").clone() {
                    self.persist_head(&cached.value);
                }
                Ok(())
            })
            .await
            .map(|_| ())
    }

    /// Make sure `commit` and its trees are local. Returns `(root tree, commit time)`.
    pub async fn ensure_commit(self: &Arc<Self>, commit: Oid) -> Result<(Oid, i64)> {
        self.ensure_init().await?;
        if let Ok(info) = self.with_repo(move |r| odb::commit_info(r, commit)).await {
            return Ok(info);
        }
        let this = self.clone();
        self.commit_flight
            .run(commit, || async move {
                let _one_at_a_time = this.commit_fetch.lock().await;
                // Nothing else fetches commits into this repo right now, so a
                // lock left by a killed fetch is stale; git would refuse to
                // shallow-fetch until it's gone.
                let lock = this.dir.join("shallow.lock");
                if tokio::fs::try_exists(&lock).await.unwrap_or(false) {
                    log::warn!("{}: removing stale shallow.lock", this.key);
                    let _ = tokio::fs::remove_file(&lock).await;
                }
                let started = Instant::now();
                this.git.fetch_commit(&this.dir, commit).await?;
                log::info!(
                    "{}: fetched commit {commit} in {:?}",
                    this.key,
                    started.elapsed()
                );
                this.pin(commit);
                this.maybe_repack();
                Ok(())
            })
            .await?;
        self.with_repo(move |r| odb::commit_info(r, commit)).await
    }

    /// Keep fetched commits reachable so a repack never drops them.
    fn pin(&self, commit: Oid) {
        let dir = self.dir.join("refs/ghfs");
        if std::fs::create_dir_all(&dir).is_ok() {
            let _ = std::fs::write(dir.join(commit.to_string()), format!("{commit}\n"));
        }
    }

    pub async fn tree(self: &Arc<Self>, tree: Oid) -> Result<Vec<TreeEntry>> {
        self.with_repo(move |r| odb::read_tree(r, tree)).await
    }

    /// Size of a blob, fetching it (with the rest of `dir_tree`) if needed.
    pub async fn blob_size(self: &Arc<Self>, dir_tree: Oid, blob: Oid) -> Result<u64> {
        if let Some(size) = self.with_repo(move |r| odb::blob_size(r, blob)).await? {
            return Ok(size);
        }
        self.fetch_blob(dir_tree, blob).await?;
        self.with_repo(move |r| odb::blob_size(r, blob))
            .await?
            .ok_or_else(|| RepoError::NotFound(format!("{}: blob {blob}", self.key)))
    }

    /// Blob contents, fetching them (with the rest of `dir_tree`) if needed.
    /// Loads the whole blob; meant for small things like symlink targets.
    pub async fn read_blob(self: &Arc<Self>, dir_tree: Oid, blob: Oid) -> Result<Vec<u8>> {
        match self.with_repo(move |r| odb::read_blob(r, blob)).await {
            Err(RepoError::NotFound(_)) => {}
            other => return other,
        }
        self.fetch_blob(dir_tree, blob).await?;
        self.with_repo(move |r| odb::read_blob(r, blob)).await
    }

    /// Path of a file holding the blob's contents, writing it once if needed.
    /// Content-addressed, so concurrent readers share one write, and the file
    /// only appears (by rename) once it has the full expected length.
    pub async fn materialize(self: &Arc<Self>, dir_tree: Oid, blob: Oid) -> Result<PathBuf> {
        let Some(repos) = self.repos.upgrade() else {
            return Err(RepoError::Failed("shutting down".into()));
        };
        let path = repos.paths.blob_path(&blob.to_string());
        if self
            .verified
            .lock()
            .expect("verified poisoned")
            .contains(&blob)
            && tokio::fs::try_exists(&path).await.unwrap_or(false)
        {
            return Ok(path);
        }
        let this = self.clone();
        self.materialize_flight
            .run(blob, || async move {
                let size = this.blob_size(dir_tree, blob).await?;
                // Files aren't fsynced (too slow for thousands of small
                // files), so the first use each run checks that a file left
                // by an earlier run wasn't truncated by a crash.
                if let Ok(meta) = tokio::fs::metadata(&path).await {
                    if meta.len() == size {
                        this.verified
                            .lock()
                            .expect("verified poisoned")
                            .insert(blob);
                        return Ok(path);
                    }
                    log::warn!(
                        "{}: cached blob {blob} has the wrong size; rewriting",
                        this.key
                    );
                    let _ = tokio::fs::remove_file(&path).await;
                }
                let dir = path.parent().expect("blob path has a parent").to_path_buf();
                tokio::fs::create_dir_all(&dir)
                    .await
                    .map_err(|e| RepoError::Failed(format!("create {}: {e}", dir.display())))?;
                let tmp = dir.join(format!(".{blob}.{}.tmp", std::process::id()));
                let result = async {
                    let file = std::fs::File::create(&tmp)
                        .map_err(|e| RepoError::Failed(format!("create {}: {e}", tmp.display())))?;
                    if size > STREAM_BLOB_OVER {
                        let out = file
                            .try_clone()
                            .map_err(|e| RepoError::Failed(e.to_string()))?;
                        this.git.cat_blob_to_file(&this.dir, blob, out).await?;
                    } else {
                        let data = this.with_repo(move |r| odb::read_blob(r, blob)).await?;
                        let mut out = file
                            .try_clone()
                            .map_err(|e| RepoError::Failed(e.to_string()))?;
                        tokio::task::spawn_blocking(move || {
                            std::io::Write::write_all(&mut out, &data)
                        })
                        .await
                        .map_err(|e| RepoError::Failed(e.to_string()))?
                        .map_err(|e| RepoError::Failed(format!("write blob: {e}")))?;
                    }
                    tokio::task::spawn_blocking(move || -> Result<u64> {
                        Ok(file
                            .metadata()
                            .map_err(|e| RepoError::Failed(e.to_string()))?
                            .len())
                    })
                    .await
                    .map_err(|e| RepoError::Failed(e.to_string()))?
                }
                .await;
                match result {
                    Ok(written) if written == size => {
                        tokio::fs::rename(&tmp, &path)
                            .await
                            .map_err(|e| RepoError::Failed(format!("rename blob: {e}")))?;
                        this.verified
                            .lock()
                            .expect("verified poisoned")
                            .insert(blob);
                        Ok(path)
                    }
                    Ok(written) => {
                        let _ = tokio::fs::remove_file(&tmp).await;
                        Err(RepoError::Failed(format!(
                            "blob {blob}: wrote {written} bytes, expected {size}"
                        )))
                    }
                    Err(e) => {
                        let _ = tokio::fs::remove_file(&tmp).await;
                        Err(e)
                    }
                }
            })
            .await
    }

    async fn fetch_blob(self: &Arc<Self>, dir_tree: Oid, blob: Oid) -> Result<()> {
        self.fetch_dir_blobs(dir_tree).await?;
        let present = self
            .with_repo(move |r| odb::missing_objects(r, &[blob]))
            .await?
            .is_empty();
        if present {
            return Ok(());
        }
        // Directory too big to batch (or the batch raced a repack): fetch just this one.
        let this = self.clone();
        self.blob_flight
            .run(blob, || async move {
                this.git.fetch_blobs(&this.dir, &[blob]).await?;
                this.maybe_repack();
                Ok(true)
            })
            .await
            .map(|_| ())
    }

    async fn missing_in_dir(self: &Arc<Self>, tree: Oid) -> Result<Vec<Oid>> {
        self.with_repo(move |r| {
            let blobs: Vec<Oid> = odb::read_tree(r, tree)?
                .into_iter()
                .filter(|e| e.kind.has_blob())
                .map(|e| e.oid)
                .collect();
            odb::missing_objects(r, &blobs)
        })
        .await
    }

    /// Fetch every missing blob directly inside `tree` in one round trip.
    /// Returns `false` without fetching if the directory is too big for one
    /// batch; callers then fetch single blobs (or [`Self::prefetch_dir`] fetches
    /// it in chunks in the background).
    pub async fn fetch_dir_blobs(self: &Arc<Self>, tree: Oid) -> Result<bool> {
        let this = self.clone();
        self.blob_flight
            .run(tree, || async move {
                let missing = this.missing_in_dir(tree).await?;
                if missing.len() > DIR_BATCH_MAX {
                    return Ok(false);
                }
                if missing.is_empty() {
                    return Ok(true);
                }
                let started = Instant::now();
                this.git.fetch_blobs(&this.dir, &missing).await?;
                log::info!(
                    "{}: fetched {} blobs of tree {tree} in {:?}",
                    this.key,
                    missing.len(),
                    started.elapsed()
                );
                this.maybe_repack();
                Ok(true)
            })
            .await
    }

    /// Chunked background fetch of a directory too big for one batch.
    async fn fetch_huge_dir(self: &Arc<Self>, tree: Oid) -> Result<()> {
        let this = self.clone();
        self.bulk_flight
            .run(tree, || async move {
                let missing = this.missing_in_dir(tree).await?;
                let started = Instant::now();
                for chunk in missing.chunks(DIR_BATCH_MAX) {
                    this.git.fetch_blobs(&this.dir, chunk).await?;
                    this.maybe_repack();
                }
                log::info!(
                    "{}: fetched {} blobs of big tree {tree} in {:?}",
                    this.key,
                    missing.len(),
                    started.elapsed()
                );
                Ok(())
            })
            .await
    }

    /// Start fetching the blobs under `tree` in the background: the
    /// directory itself plus as much of its subtree (breadth-first) as fits in
    /// one batch. A tree walk (`find`, `grep -r`, the macOS NFS client's
    /// GETATTR-per-entry) then finds the next levels already local instead of
    /// paying one round trip per directory.
    pub fn prefetch_dir(self: &Arc<Self>, tree: Oid) {
        if !self
            .prefetched
            .lock()
            .expect("prefetched poisoned")
            .insert(tree)
        {
            return;
        }
        let Some(repos) = self.repos.upgrade() else {
            return;
        };
        let (slots, pending) = (repos.prefetch_slots.clone(), repos.prefetches.clone());
        pending.fetch_add(1, Ordering::AcqRel);
        let this = self.clone();
        tokio::spawn(async move {
            let _done = DecrementOnDrop(pending);
            let _slot = slots.acquire_owned().await.expect("semaphore never closed");
            let result = async {
                // The listed directory first, on its own, so reading a file
                // right after `ls` doesn't wait on the bigger subtree batch.
                if !this.fetch_dir_blobs(tree).await? {
                    this.fetch_huge_dir(tree).await?;
                }
                this.fetch_subtree_blobs(tree).await
            }
            .await;
            if let Err(e) = result {
                log::warn!("{}: prefetch of tree {tree} failed: {e}", this.key);
                // Let a later listing try again.
                this.prefetched
                    .lock()
                    .expect("prefetched poisoned")
                    .remove(&tree);
            }
        });
    }

    async fn fetch_subtree_blobs(self: &Arc<Self>, root: Oid) -> Result<()> {
        let this = self.clone();
        let (trees, missing) = self
            .with_repo(move |r| {
                let mut queue = std::collections::VecDeque::from([root]);
                let mut visited = 0;
                let mut trees = Vec::new();
                let mut missing = Vec::new();
                while let Some(tree) = queue.pop_front() {
                    visited += 1;
                    if visited > PREFETCH_MAX_TREES {
                        break;
                    }
                    let entries = odb::read_tree(r, tree)?;
                    queue.extend(
                        entries
                            .iter()
                            .filter(|e| e.kind == crate::git::EntryKind::Tree)
                            .map(|e| e.oid),
                    );
                    // Someone is already fetching this directory.
                    if this.blob_flight.contains(&tree) || this.bulk_flight.contains(&tree) {
                        continue;
                    }
                    let blobs: Vec<Oid> = entries
                        .iter()
                        .filter(|e| e.kind.has_blob())
                        .map(|e| e.oid)
                        .collect();
                    let dir_missing = odb::missing_objects(r, &blobs)?;
                    if dir_missing.is_empty() {
                        continue;
                    }
                    // Keep directories whole and the batch bounded, so anyone
                    // joining it waits for one round trip at most.
                    if missing.len() + dir_missing.len() > DIR_BATCH_MAX {
                        continue;
                    }
                    trees.push(tree);
                    missing.extend(dir_missing);
                }
                Ok((trees, missing))
            })
            .await?;
        if missing.is_empty() {
            return Ok(());
        }
        let this = self.clone();
        let dirs = trees.len();
        self.blob_flight
            .run_group(&trees, || async move {
                let started = Instant::now();
                this.git.fetch_blobs(&this.dir, &missing).await?;
                log::info!(
                    "{}: prefetched {} blobs in {} dirs under {root} in {:?}",
                    this.key,
                    missing.len(),
                    dirs,
                    started.elapsed()
                );
                this.maybe_repack();
                Ok(true)
            })
            .await
            .map(|_| ())
    }

    pub fn maybe_repack(self: &Arc<Self>) {
        if odb::pack_count(&self.dir) < REPACK_THRESHOLD {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            this.repack().await;
        });
    }

    /// Consolidate packs. Safe alongside concurrent fetches: git only deletes
    /// the packs it rolled up.
    pub async fn repack(self: &Arc<Self>) {
        if self.repacking.swap(true, Ordering::AcqRel) {
            return;
        }
        let started = Instant::now();
        let before = odb::pack_count(&self.dir);
        match self.git.repack(&self.dir).await {
            Ok(()) => log::info!(
                "{}: repacked {before} packs in {:?}",
                self.key,
                started.elapsed()
            ),
            Err(e) => log::warn!("{}: repack failed: {e}", self.key),
        }
        self.pool_generation.fetch_add(1, Ordering::AcqRel);
        self.pool.lock().expect("repo pool poisoned").clear();
        self.repacking.store(false, Ordering::Release);
    }
}

struct DecrementOnDrop(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for DecrementOnDrop {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: an expired not-found entry used to deadlock (read guard
    /// held across `remove` on the same DashMap shard).
    #[test]
    fn expired_not_found_entry_does_not_deadlock() {
        let tmp = tempfile::tempdir().unwrap();
        let repos = Repos::new(
            CachePaths::new(tmp.path()),
            GitCli::with_base_url("file:///x"),
        );
        let key: RepoKey = "octo/gone".parse().unwrap();
        repos
            .not_found
            .insert(key.clone(), Instant::now() - NOT_FOUND_TTL * 2);
        let (tx, rx) = std::sync::mpsc::channel();
        let probe = repos.clone();
        std::thread::spawn(move || {
            tx.send(probe.known_missing(&key)).unwrap();
        });
        let missing = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("known_missing deadlocked");
        assert!(!missing);
        assert!(repos.not_found.is_empty());

        let fresh: RepoKey = "octo/fresh".parse().unwrap();
        repos.mark_missing(&fresh);
        assert!(repos.known_missing(&fresh));
    }

    #[test]
    fn keys_are_case_insensitive() {
        let tmp = tempfile::tempdir().unwrap();
        let repos = Repos::new(
            CachePaths::new(tmp.path()),
            GitCli::with_base_url("file:///x"),
        );
        let a = repos.get(&"Octo/Hello".parse().unwrap());
        let b = repos.get(&"octo/hello".parse().unwrap());
        assert!(Arc::ptr_eq(&a, &b));
        assert!(a.dir().ends_with("octo/hello.git"));
    }
}
