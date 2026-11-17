//! Protocol-v2 remote operations with bounded staging and supervised workers.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use git2::Oid;
use tokio::sync::{Mutex, OnceCell, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::operation::{Budget, Limits, Work, Workers};
use super::transport::{HttpsTransport, Response, Transport};
use super::{GitError, loose, pack, wire};

const OPERATIONS: usize = 24;
const DECODERS: usize = 2;
const CAPABILITY_TTL: Duration = Duration::from_secs(300);
const MAX_CAPABILITY_ENTRIES: usize = 256;
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

struct CapabilityEntry {
    created: Instant,
    value: Arc<OnceCell<wire::Capabilities>>,
}

struct Inner {
    transport: Arc<dyn Transport>,
    capabilities: Mutex<HashMap<(String, u64), CapabilityEntry>>,
    slots: Arc<Semaphore>,
    decoders: Arc<Semaphore>,
    shutdown: CancellationToken,
    workers: Arc<Workers>,
    spool_budget: Arc<Budget>,
    decode_budget: Arc<Budget>,
    limits: Limits,
    reference_requests: AtomicU64,
    discovery_requests: AtomicU64,
    fetch_requests: AtomicU64,
}

#[derive(Clone)]
pub struct GitRemote {
    base_url: String,
    inner: Arc<Inner>,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for GitRemote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GitRemote")
            .field("base_url", &redact_creds(&self.base_url))
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct RemoteStats {
    pub reference_requests: u64,
    pub discovery_requests: u64,
    pub fetch_requests: u64,
    pub spool_bytes: u64,
    pub spool_peak_bytes: u64,
    pub decode_bytes: u64,
    pub decode_peak_bytes: u64,
    pub available_operation_slots: usize,
    pub available_decoder_slots: usize,
}

impl Default for GitRemote {
    fn default() -> Self {
        Self::new()
    }
}

impl GitRemote {
    pub fn new() -> Self {
        Self::with_base_url(
            std::env::var("GHFS_GIT_BASE_URL").unwrap_or_else(|_| "https://github.com".to_owned()),
        )
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        let base_url = base_url.into();
        let transport = Arc::new(HttpsTransport::new(&base_url));
        Self::with_transport(base_url, transport)
    }

    pub fn with_transport(base_url: impl Into<String>, transport: Arc<dyn Transport>) -> Self {
        Self {
            base_url: base_url.into(),
            inner: Arc::new(Inner {
                transport,
                capabilities: Mutex::new(HashMap::new()),
                slots: Arc::new(Semaphore::new(OPERATIONS)),
                decoders: Arc::new(Semaphore::new(DECODERS)),
                shutdown: CancellationToken::new(),
                workers: Arc::new(Workers::default()),
                spool_budget: Budget::new("downloaded packs", 2 << 30),
                decode_budget: Budget::new("expanded objects", 4 << 30),
                limits: Limits::default(),
                reference_requests: AtomicU64::new(0),
                discovery_requests: AtomicU64::new(0),
                fetch_requests: AtomicU64::new(0),
            }),
            timeout: None,
        }
    }

    /// Override the complete operation deadline, including queueing and retries.
    pub fn with_operation_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    pub fn url(&self, owner: &str, repo: &str) -> String {
        format!("{}/{owner}/{repo}.git", self.base_url.trim_end_matches('/'))
    }

    fn work(&self, timeout: Duration) -> Work {
        Work::with_token(
            self.timeout.unwrap_or(timeout),
            self.inner.shutdown.child_token(),
        )
    }

    async fn acquire(
        semaphore: &Arc<Semaphore>,
        work: &Work,
    ) -> Result<OwnedSemaphorePermit, GitError> {
        work.wait(async {
            semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| GitError::Cancelled)
        })
        .await
    }

    fn response(response: Response, expected: &str) -> Result<Response, GitError> {
        match response.status {
            200 => {}
            404 => {
                return Err(GitError::RepoNotFound(
                    "remote repository is unavailable".into(),
                ));
            }
            status => return Err(GitError::Transport(format!("remote HTTP status {status}"))),
        }
        let actual = response
            .content_type
            .as_deref()
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .trim();
        if !actual.eq_ignore_ascii_case(expected) {
            return Err(GitError::Protocol("unexpected HTTP content type".into()));
        }
        Ok(response)
    }

    async fn capabilities(&self, url: &str, work: &Work) -> Result<wire::Capabilities, GitError> {
        let identity = work
            .wait(async { Ok(self.inner.transport.identity(url).await) })
            .await?;
        let key = (url.to_owned(), identity);
        let cell = work
            .wait(async {
                let mut cache = self.inner.capabilities.lock().await;
                cache.retain(|_, entry| entry.created.elapsed() < CAPABILITY_TTL);
                if !cache.contains_key(&key)
                    && cache.len() >= MAX_CAPABILITY_ENTRIES
                    && let Some(key) = cache.keys().next().cloned()
                {
                    cache.remove(&key);
                }
                Ok(cache
                    .entry(key)
                    .or_insert_with(|| CapabilityEntry {
                        created: Instant::now(),
                        value: Arc::new(OnceCell::new()),
                    })
                    .value
                    .clone())
            })
            .await?;
        work.wait(async {
            cell.get_or_try_init(|| async {
                self.inner
                    .discovery_requests
                    .fetch_add(1, Ordering::Relaxed);
                let response = self.inner.transport.discover(url).await?;
                let response =
                    Self::response(response, "application/x-git-upload-pack-advertisement")?;
                wire::advertisement(response.body, work).await
            })
            .await
            .cloned()
        })
        .await
    }

    async fn ls_refs(&self, url: &str, all: bool) -> Result<RemoteRefs, GitError> {
        let work = self.work(if all {
            Duration::from_secs(60)
        } else {
            Duration::from_secs(30)
        });
        let _cancel = work.cancel_on_drop();
        let _slot = Self::acquire(&self.inner.slots, &work).await?;
        self.capabilities(url, &work).await?.require_refs()?;
        let mut args = vec!["symrefs".into(), "peel".into(), "ref-prefix HEAD".into()];
        if all {
            args.extend([
                "ref-prefix refs/heads/".into(),
                "ref-prefix refs/tags/".into(),
            ]);
        }
        self.inner
            .reference_requests
            .fetch_add(1, Ordering::Relaxed);
        let response = work
            .wait(
                self.inner
                    .transport
                    .command(url, wire::command("ls-refs", args)),
            )
            .await?;
        let response = Self::response(response, "application/x-git-upload-pack-result")?;
        wire::refs(response.body, &work, all).await
    }

    pub async fn ls_remote_head(&self, url: &str) -> Result<RemoteRefs, GitError> {
        let refs = self.ls_refs(url, false).await?;
        if refs.head.is_none() {
            return Err(GitError::RepoNotFound("remote has no HEAD".into()));
        }
        Ok(refs)
    }

    pub async fn ls_remote_all(&self, url: &str) -> Result<RemoteRefs, GitError> {
        self.ls_refs(url, true).await
    }

    /// Initialize privately, then publish the complete bare repository directory.
    pub async fn init_repo(&self, dir: &Path, url: &str) -> Result<(), GitError> {
        let work = self.work(Duration::from_secs(30));
        let _cancel = work.cancel_on_drop();
        let slot = Self::acquire(&self.inner.slots, &work).await?;
        let dir = dir.to_owned();
        let url = url.to_owned();
        let worker_work = work.clone();
        work.wait(self.inner.workers.run(work.clone(), move || {
            let _slot = slot;
            worker_work.check()?;
            if dir.join("objects").is_dir() {
                return Ok(());
            }
            let parent = dir
                .parent()
                .ok_or_else(|| GitError::Command("repository has no parent".into()))?;
            std::fs::create_dir_all(parent)?;
            let temp = tempfile::Builder::new()
                .prefix(".ghfs-init-")
                .tempdir_in(parent)?;
            let repository = git2::Repository::init_bare(temp.path())?;
            repository.config()?.set_str("remote.origin.url", &url)?;
            drop(repository);
            worker_work.check()?;
            match std::fs::rename(temp.path(), &dir) {
                Ok(()) => Ok(()),
                Err(_) if dir.join("objects").is_dir() => Ok(()),
                Err(error) => Err(error.into()),
            }
        }))
        .await
    }

    async fn remote_url(&self, dir: &Path, work: &Work) -> Result<String, GitError> {
        let slot = Self::acquire(&self.inner.slots, work).await?;
        let dir = dir.to_owned();
        work.wait(self.inner.workers.run(work.clone(), move || {
            let _slot = slot;
            let repository = git2::Repository::open_bare(dir)?;
            let url = repository.config()?.get_string("remote.origin.url")?;
            if url.len() > 8192 {
                return Err(GitError::ResourceLimit("remote URL too long".into()));
            }
            Ok(url)
        }))
        .await
    }

    async fn fetch_once(
        &self,
        dir: &Path,
        url: &str,
        wants: &[Oid],
        commit: Option<Oid>,
        work: &Work,
    ) -> Result<(), GitError> {
        let slot = Self::acquire(&self.inner.slots, work).await?;
        self.capabilities(url, work)
            .await?
            .require_fetch(commit.is_some())?;
        let dir = dir.to_owned();
        let setup_dir = dir.clone();
        let (slot, staging) = work
            .wait(self.inner.workers.run(work.clone(), move || {
                let parent = setup_dir.join("ghfs-staging");
                std::fs::create_dir_all(&parent)?;
                let staging = tempfile::Builder::new()
                    .prefix("transfer-")
                    .tempdir_in(parent)?;
                Ok((slot, staging))
            }))
            .await?;
        let mut args: Vec<String> = wants.iter().map(|oid| format!("want {oid}")).collect();
        args.extend([
            "filter blob:none".into(),
            "ofs-delta".into(),
            "no-progress".into(),
        ]);
        if commit.is_some() {
            args.push("deepen 1".into());
        }
        args.push("done".into());
        self.inner.fetch_requests.fetch_add(1, Ordering::Relaxed);
        let response = work
            .wait(
                self.inner
                    .transport
                    .command(url, wire::command("fetch", args)),
            )
            .await?;
        let response = Self::response(response, "application/x-git-upload-pack-result")?;
        let path = staging.path().join("transfer.pack");
        let mut output = work
            .wait(async { tokio::fs::File::create(&path).await.map_err(GitError::Io) })
            .await?;
        let mut reservation = self.inner.spool_budget.reserve(0)?;
        wire::fetch(
            response.body,
            work,
            &mut output,
            &mut reservation,
            &self.inner.limits,
            wants[0],
        )
        .await?;
        // flush in wire::fetch completes pending asynchronous writes.
        drop(output);
        let decoder = Self::acquire(&self.inner.decoders, work).await?;
        let wants = wants.to_vec();
        let limits = self.inner.limits.clone();
        let budget = self.inner.decode_budget.clone();
        let worker_work = work.clone();
        work.wait(self.inner.workers.run(work.clone(), move || {
            let (_slot, _decoder, _spool) = (slot, decoder, reservation);
            pack::ingest(
                &dir,
                staging.path(),
                &path,
                &wants,
                commit,
                &worker_work,
                &limits,
                &budget,
            )
        }))
        .await
    }

    pub async fn fetch_commit(&self, dir: &Path, commit: Oid) -> Result<(), GitError> {
        let work = self.work(Duration::from_secs(300));
        let _cancel = work.cancel_on_drop();
        let url = self.remote_url(dir, &work).await?;
        self.fetch_once(dir, &url, &[commit], Some(commit), &work)
            .await
    }

    pub async fn fetch_blobs(&self, dir: &Path, blobs: &[Oid]) -> Result<(), GitError> {
        if blobs.is_empty() {
            return Ok(());
        }
        let work = self.work(Duration::from_secs(600));
        let _cancel = work.cancel_on_drop();
        let url = self.remote_url(dir, &work).await?;
        // A single unfetchable want (over the size limit, or rejected by the
        // server) must not strand the rest of the batch: remember its error,
        // keep fetching the others, and report it at the end.
        let mut deferred = None;
        for chunk in blobs.chunks(self.inner.limits.max_wants) {
            let mut pending = vec![chunk];
            while let Some(wants) = pending.pop() {
                match self.fetch_once(dir, &url, wants, None, &work).await {
                    Err(GitError::ResourceLimit(_) | GitError::MissingObject(_))
                        if wants.len() > 1 =>
                    {
                        // The previous attempt has returned and dropped all its
                        // staging/quota guards. Global Quota failures never split.
                        let (left, right) = wants.split_at(wants.len() / 2);
                        pending.push(right);
                        pending.push(left);
                    }
                    Err(error @ (GitError::ResourceLimit(_) | GitError::MissingObject(_))) => {
                        deferred.get_or_insert(error);
                    }
                    result => result?,
                }
            }
        }
        deferred.map_or(Ok(()), Err)
    }

    pub async fn cat_blob_to_file(
        &self,
        dir: &Path,
        blob: Oid,
        out: std::fs::File,
    ) -> Result<(), GitError> {
        let work = self.work(Duration::from_secs(600));
        let _cancel = work.cancel_on_drop();
        let slot = Self::acquire(&self.inner.slots, &work).await?;
        let decoder = Self::acquire(&self.inner.decoders, &work).await?;
        let dir = dir.to_owned();
        let limits = self.inner.limits.clone();
        let worker_work = work.clone();
        work.wait(self.inner.workers.run(work.clone(), move || {
            let (_slot, _decoder) = (slot, decoder);
            loose::copy_blob(&dir, blob, out, &worker_work, &limits)
        }))
        .await
    }

    pub async fn validate_commit(&self, dir: &Path, oid: Oid) -> Result<(Oid, i64), GitError> {
        let work = self.work(Duration::from_secs(300));
        let _cancel = work.cancel_on_drop();
        let slot = Self::acquire(&self.inner.slots, &work).await?;
        let decoder = Self::acquire(&self.inner.decoders, &work).await?;
        let dir = dir.to_owned();
        let limits = self.inner.limits.clone();
        let worker_work = work.clone();
        work.wait(self.inner.workers.run(work.clone(), move || {
            let (_slot, _decoder) = (slot, decoder);
            loose::validate_commit(&dir, oid, &worker_work, &limits)
        }))
        .await
    }

    pub fn stats(&self) -> RemoteStats {
        RemoteStats {
            reference_requests: self.inner.reference_requests.load(Ordering::Relaxed),
            discovery_requests: self.inner.discovery_requests.load(Ordering::Relaxed),
            fetch_requests: self.inner.fetch_requests.load(Ordering::Relaxed),
            spool_bytes: self.inner.spool_budget.used(),
            spool_peak_bytes: self.inner.spool_budget.peak(),
            decode_bytes: self.inner.decode_budget.used(),
            decode_peak_bytes: self.inner.decode_budget.peak(),
            available_operation_slots: self.inner.slots.available_permits(),
            available_decoder_slots: self.inner.decoders.available_permits(),
        }
    }

    /// Stop admission and HTTP work, cancel and join every registered worker.
    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        self.inner.slots.close();
        self.inner.decoders.close();
        self.inner.workers.shutdown().await;
    }
}

/// Redact embedded URL credentials when describing configuration.
pub fn redact_creds(s: &str) -> String {
    let mut result = s.to_owned();
    for scheme in ["https://", "http://"] {
        let mut start = 0;
        while let Some(relative) = result[start..].find(scheme) {
            let begin = start + relative + scheme.len();
            let end = result[begin..]
                .find(|c: char| c.is_whitespace() || matches!(c, '\'' | '"' | '/'))
                .map(|v| begin + v)
                .unwrap_or(result.len());
            if let Some(at) = result[begin..end].find('@') {
                result.replace_range(begin..begin + at, "***");
                start = begin + 4;
            } else {
                start = end;
            }
            if start >= result.len() {
                break;
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_ref_contract_and_redaction() {
        let refs = parse_ls_remote(
            "ref: refs/heads/main\tHEAD\n1111111111111111111111111111111111111111\tHEAD\n2222222222222222222222222222222222222222\trefs/tags/v1\n3333333333333333333333333333333333333333\trefs/tags/v1^{}\n",
        );
        assert_eq!(refs.head_ref.as_deref(), Some("refs/heads/main"));
        assert_eq!(
            refs.resolve("v1"),
            Some(Oid::from_str("3333333333333333333333333333333333333333").unwrap())
        );
        assert_eq!(
            redact_creds("https://user:secret@github.com/a/b.git"),
            "https://***@github.com/a/b.git"
        );
        assert_eq!(
            redact_creds("http://a:b@host/path https://c:d@other/repo"),
            "http://***@host/path https://***@other/repo"
        );
    }
}

#[cfg(test)]
mod fetch_tests {
    use super::super::transport::{Body, TransportFuture};
    use super::*;
    use bytes::Bytes;
    use sha1::{Digest, Sha1};
    use std::collections::VecDeque;
    use std::io::{Cursor, Write};
    use std::sync::Mutex as StdMutex;

    fn pkt(data: &[u8]) -> Vec<u8> {
        [format!("{:04x}", data.len() + 4).as_bytes(), data].concat()
    }
    fn framed(pack: Vec<u8>) -> Vec<u8> {
        [
            pkt(b"packfile\n"),
            pkt(&[&[1u8], pack.as_slice()].concat()),
            b"0000".to_vec(),
        ]
        .concat()
    }
    fn blob(data: &[u8]) -> (Oid, Vec<u8>) {
        let mut oid = Sha1::new();
        oid.update(format!("blob {}\0", data.len()));
        oid.update(data);
        let oid = Oid::from_bytes(&oid.finalize()).unwrap();
        let mut raw = b"PACK\0\0\0\x02\0\0\0\x01".to_vec();
        assert!(data.len() < 16);
        raw.push(0x30 | data.len() as u8);
        let mut z = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        z.write_all(data).unwrap();
        raw.extend(z.finish().unwrap());
        let checksum = Sha1::digest(&raw);
        raw.extend_from_slice(&checksum);
        (oid, framed(raw))
    }
    #[derive(Debug)]
    struct Scripted(StdMutex<VecDeque<Vec<u8>>>);
    impl Transport for Scripted {
        fn discover<'a>(&'a self, _: &'a str) -> TransportFuture<'a> {
            Box::pin(async move {
                Ok(Response {
                    status: 200,
                    content_type: Some("application/x-git-upload-pack-advertisement".into()),
                    body: Box::pin(Cursor::new(
                        [
                            pkt(b"version 2\n"),
                            pkt(b"ls-refs\n"),
                            pkt(b"fetch=shallow filter\n"),
                            b"0000".to_vec(),
                        ]
                        .concat(),
                    )) as Body,
                })
            })
        }
        fn command<'a>(&'a self, _: &'a str, _: Bytes) -> TransportFuture<'a> {
            Box::pin(async move {
                Ok(Response {
                    status: 200,
                    content_type: Some("application/x-git-upload-pack-result".into()),
                    body: Box::pin(Cursor::new(
                        self.0
                            .lock()
                            .unwrap()
                            .pop_front()
                            .expect("unexpected retry"),
                    )),
                })
            })
        }
    }
    #[tokio::test]
    async fn oversized_batches_split_and_release_all_reservations() {
        let (a, pack_a) = blob(b"first");
        let (b, pack_b) = blob(b"second");
        let transport = Arc::new(Scripted(StdMutex::new(VecDeque::from([
            framed(vec![0; 100]),
            pack_a,
            pack_b,
        ]))));
        let mut git = GitRemote::with_transport("https://fixture", transport);
        Arc::get_mut(&mut git.inner).unwrap().limits.max_pack_bytes = 80;
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo.git");
        git.init_repo(&repo, "https://fixture/a/b.git")
            .await
            .unwrap();
        git.fetch_blobs(&repo, &[a, b]).await.unwrap();
        assert_eq!(git.stats().fetch_requests, 3);
        assert_eq!(git.stats().spool_bytes, 0);
        assert_eq!(git.stats().decode_bytes, 0);
        assert_eq!(git.stats().available_operation_slots, 24);
        assert_eq!(git.stats().available_decoder_slots, 2);
        assert_eq!(
            git2::Repository::open_bare(&repo)
                .unwrap()
                .find_blob(a)
                .unwrap()
                .content(),
            b"first"
        );
        assert!(
            std::fs::read_dir(repo.join("ghfs-staging"))
                .unwrap()
                .next()
                .is_none()
        );
    }
    #[tokio::test]
    async fn global_quota_pressure_does_not_split_or_redownload() {
        let (a, pack_a) = blob(b"first");
        let (b, _) = blob(b"second");
        let transport = Arc::new(Scripted(StdMutex::new(VecDeque::from([pack_a]))));
        let mut git = GitRemote::with_transport("https://fixture", transport);
        Arc::get_mut(&mut git.inner).unwrap().spool_budget = Budget::new("tiny-test", 1);
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("repo.git");
        git.init_repo(&repo, "https://fixture/a/b.git")
            .await
            .unwrap();
        assert!(matches!(
            git.fetch_blobs(&repo, &[a, b]).await,
            Err(GitError::Quota(_))
        ));
        assert_eq!(git.stats().fetch_requests, 1);
        assert_eq!(git.stats().spool_bytes, 0);
    }
    #[derive(Debug)]
    struct ChangingIdentity {
        generation: AtomicU64,
        inner: Scripted,
    }
    impl Transport for ChangingIdentity {
        fn identity<'a>(&'a self, _: &'a str) -> super::super::transport::IdentityFuture<'a> {
            Box::pin(async move { self.generation.load(Ordering::Acquire) })
        }
        fn discover<'a>(&'a self, url: &'a str) -> TransportFuture<'a> {
            self.inner.discover(url)
        }
        fn command<'a>(&'a self, url: &'a str, body: Bytes) -> TransportFuture<'a> {
            self.inner.command(url, body)
        }
    }
    #[tokio::test]
    async fn capability_cache_is_separated_by_credential_generation() {
        let response = [
            pkt(b"1111111111111111111111111111111111111111 HEAD\n"),
            b"0000".to_vec(),
        ]
        .concat();
        let transport = Arc::new(ChangingIdentity {
            generation: AtomicU64::new(0),
            inner: Scripted(StdMutex::new(VecDeque::from([
                response.clone(),
                response.clone(),
                response,
            ]))),
        });
        let git = GitRemote::with_transport("https://fixture", transport.clone());
        let url = git.url("a", "b");
        git.ls_remote_head(&url).await.unwrap();
        git.ls_remote_head(&url).await.unwrap();
        assert_eq!(git.stats().discovery_requests, 1);
        transport.generation.store(1, Ordering::Release);
        git.ls_remote_head(&url).await.unwrap();
        assert_eq!(git.stats().discovery_requests, 2);
    }
}
