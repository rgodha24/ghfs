//! Store façade tying together the blobless mirror, tree reader, and hydrator.
//!
//! This is the single object the filesystem layer (`crate::fs`) and the daemon
//! worker both talk to. It's backend-agnostic and cheap to clone. All read
//! operations (ref resolution, tree traversal, blob hydration) go through
//! here; the FUSE and NFS adapters share it.

use git2::{Oid, Repository};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use thiserror::Error;

use super::blob::{BlobCache, Hydrator};
use super::git::{self, GitCli, GitError};
use super::tree::{TreeCache, TreeEntry, TreeReader};

use crate::cache::CachePaths;
use crate::cache::lock::RepoLock;
use crate::types::RepoKey;

/// Errors returned by [`Store`] operations.
#[derive(Error, Debug)]
pub enum StoreError {
    #[error(transparent)]
    Git(#[from] GitError),
    #[error(transparent)]
    Tree(#[from] super::tree::TreeError),
    #[error(transparent)]
    Blob(#[from] super::blob::BlobError),
    #[error("repository not found: {0}")]
    RepoNotFound(String),
    #[error("lock acquisition failed")]
    LockFailed,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

/// An opened mirror with its hydrator.
struct OpenRepo {
    repo: Repository,
    hydrator: Hydrator,
}

/// The object-backed revision store.
#[derive(Clone)]
pub struct Store {
    cli: GitCli,
    paths: CachePaths,
    tree_cache: TreeCache,
    blob_cache: BlobCache,
    open: dashmap::DashMap<RepoKey, Arc<Mutex<OpenRepo>>>,
}

impl Store {
    /// Create a new store rooted at `paths`. The blob cache lives under
    /// `paths.root()/blobs`; mirrors under `paths.root()/mirrors`.
    pub fn new(paths: CachePaths) -> Self {
        let blob_cache = BlobCache::new(paths.root().join("blobs"), "sha1");
        Self {
            cli: GitCli::new(),
            paths,
            tree_cache: TreeCache::new(),
            blob_cache,
            open: dashmap::DashMap::new(),
        }
    }

    /// Return the cache paths.
    pub fn paths(&self) -> &CachePaths {
        &self.paths
    }

    /// Ensure the mirror for `key` is present (cloning bloblessly if missing)
    /// and opened, returning a cloned handle to the locked open repo. Safe to
    /// call concurrently; a per-repo flock serializes the clone.
    fn ensure_open(&self, key: &RepoKey) -> Result<Arc<Mutex<OpenRepo>>, StoreError> {
        if let Some(h) = self.open.get(key) {
            return Ok(Arc::clone(&h));
        }

        // Serialize first-access clone per repo via flock.
        let lock_path = self.paths.lock_path(key);
        let _lock = match RepoLock::acquire(&lock_path) {
            Ok(lock) => lock,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => {
                return Err(StoreError::LockFailed);
            }
            Err(e) => return Err(StoreError::Io(e)),
        };

        // Re-check after acquiring the lock.
        if let Some(h) = self.open.get(key) {
            return Ok(Arc::clone(&h));
        }

        let mirror = self.paths.mirror_dir(key);
        if !mirror.exists() {
            self.cli.clone_blobless(key, &mirror)?;
        }
        let repo = git::open_repository(&mirror)?;
        let hydrator = Hydrator::new(mirror.clone(), self.blob_cache.clone());
        let handle = Arc::new(Mutex::new(OpenRepo { repo, hydrator }));
        //Insert, keeping the first entry if raced.
        self.open
            .entry(key.clone())
            .or_insert_with(|| handle.clone());
        Ok(handle)
    }

    /// Refresh a repo's refs by fetching incrementally (blobless). No-op safe
    /// to call periodically; the mirror must already exist or it will be
    /// created.
    pub fn refresh(&self, key: &RepoKey) -> Result<(), StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        self.cli.fetch_blobless(&self.paths.mirror_dir(key))?;
        // Drop the guard; opened repo caches libgit2's ref cache though, so
        // path resolution after a fetch should re-open if stale. For now the
        // tree cache is keyed by OID (immutable) so stale ref pointers are the
        // only concern, handled at the FS layer by re-resolving refs per
        // lookup with a short TTL.
        drop(guard);
        // Removing the cached handle forces a reopen so libgit2 sees new refs.
        self.open.remove(key);
        Ok(())
    }

    /// Resolve the default-branch (HEAD) commit for `key`.
    pub fn resolve_head(&self, key: &RepoKey) -> Result<Oid, StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        Ok(git::resolve_head(&guard.repo)?)
    }

    /// Resolve a ref selector to a commit OID.
    pub fn resolve_revision(&self, key: &RepoKey, selector: &str) -> Result<Oid, StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        Ok(git::resolve_revision(&guard.repo, selector)?)
    }

    /// List short branch names.
    pub fn list_branches(&self, key: &RepoKey) -> Result<Vec<String>, StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        Ok(git::list_branches(&guard.repo)?)
    }

    /// List short tag names.
    pub fn list_tags(&self, key: &RepoKey) -> Result<Vec<String>, StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        Ok(git::list_tags(&guard.repo)?)
    }

    /// Resolve a commit's root tree OID.
    pub fn root_tree(&self, key: &RepoKey, commit: Oid) -> Result<Oid, StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        let reader = TreeReader::new(&guard.repo, &self.tree_cache);
        Ok(reader.root_tree(commit)?)
    }

    /// All direct children of a tree (for `readdir`).
    pub fn tree_entries(&self, key: &RepoKey, tree: Oid) -> Result<Arc<[TreeEntry]>, StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        let reader = TreeReader::new(&guard.repo, &self.tree_cache);
        Ok(reader.entries(tree)?)
    }

    /// Look up a single named child of a tree (the hot `lookup` path).
    pub fn tree_entry(
        &self,
        key: &RepoKey,
        tree: Oid,
        name: &[u8],
    ) -> Result<Option<TreeEntry>, StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        let reader = TreeReader::new(&guard.repo, &self.tree_cache);
        Ok(reader.entry(tree, name)?)
    }

    /// Ensure a blob is on disk and return its cached path + size.
    pub fn hydrate_blob(&self, key: &RepoKey, oid: Oid) -> Result<(PathBuf, u64), StoreError> {
        let handle = self.ensure_open(key)?;
        let guard = handle.lock().expect("open repo poisoned");
        Ok(guard.hydrator.hydrate(oid)?)
    }
}
