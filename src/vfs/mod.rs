//! Backend-agnostic async filesystem shared by the FUSE (Linux) and NFS
//! (macOS) frontends.
//!
//! ```text
//! /<owner>/<repo>/...                      default branch, follows HEAD
//! /by-ref/<owner>/<repo>/<ref>/...         branch, tag, or full commit id
//! ```
//!
//! `<owner>/<repo>` is one stable inode that always shows the current HEAD
//! commit; entries below it belong to a specific commit, so when HEAD moves
//! new lookups get new inodes while old handles keep reading the old commit.

pub mod handle;
pub mod refname;

use std::collections::VecDeque;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use git2::Oid;

use crate::git::{EntryKind, TreeEntry};
use crate::repos::{RepoError, RepoHandle, Repos};
use crate::types::{Owner, Repo, RepoKey};
use handle::Desc;
use refname::{BY_REF_ROOT, decode_ref, encode_ref, plausible_ref};

pub const ROOT_INO: u64 = 1;
pub const BY_REF_INO: u64 = 2;
const FIRST_DYNAMIC_INO: u64 = 3;

/// Errno-style result used by both frontends.
pub type FsResult<T> = std::result::Result<T, i32>;

fn repo_errno(key: &RepoKey, e: RepoError) -> i32 {
    match &e {
        RepoError::NotFound(_) => log::debug!("{key}: {e}"),
        RepoError::Failed(_) => log::warn!("{key}: {e}"),
    }
    e.errno()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Directory,
    File,
    Symlink,
}

#[derive(Debug, Clone)]
pub struct Attr {
    pub ino: u64,
    pub kind: FileKind,
    pub size: u64,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub mtime: SystemTime,
}

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub name: Vec<u8>,
    pub kind: FileKind,
}

/// How long a kernel may cache a node's attributes / name lookups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Volatility {
    /// Pinned to an immutable commit.
    Immutable,
    /// Can change when a branch moves.
    Ref,
    /// Discovery directories (owners, repo lists).
    Virtual,
}

impl Volatility {
    pub fn ttl(self) -> Duration {
        match self {
            Self::Immutable => Duration::from_secs(3600),
            Self::Ref => Duration::from_secs(5),
            Self::Virtual => Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
enum Node {
    Root,
    ByRef,
    Owner(Owner),
    RefOwner(Owner),
    /// `by-ref/<owner>/<repo>`: lists branches and tags.
    RefRepo(RepoKey),
    /// `<owner>/<repo>`: the current HEAD commit.
    Head(RepoKey),
    /// `by-ref/<owner>/<repo>/<ref>`: a fixed commit.
    Commit {
        key: RepoKey,
        commit: Oid,
        /// Selector was a commit id (content can never change).
        pinned: bool,
    },
    /// Anything inside a commit.
    Path {
        key: RepoKey,
        commit_time: i64,
        oid: Oid,
        kind: EntryKind,
        /// [`handle::path_hash`] of where this sits in the commit.
        path: u64,
        /// Tree of the containing directory, for batched blob fetches.
        parent_tree: Oid,
        /// The commit this belongs to; zero for files rebuilt from a handle
        /// (only directories need it, to find their parent again).
        commit: Oid,
        /// Directly below `<owner>/<repo>`, so it changes when HEAD moves.
        under_head: bool,
        /// Anywhere below `<owner>/<repo>` (rather than `by-ref`).
        in_head: bool,
        /// Below a `by-ref` commit id.
        pinned: bool,
    },
}

/// [`Inode::up`] when it isn't known.
const NO_INO: u64 = 0;

/// A directory's resolved git contents.
struct TreeCtx {
    key: RepoKey,
    handle: Arc<RepoHandle>,
    /// Commit, for the HEAD and commit roots (their children are per-commit).
    commit: Option<Oid>,
    /// Commit this directory belongs to, always.
    root: Oid,
    tree: Oid,
    /// This directory's [`handle::path_hash`] (0 for a commit's root).
    path: u64,
    commit_time: i64,
    under_head: bool,
    in_head: bool,
    pinned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChildKey {
    parent: u64,
    name: Vec<u8>,
    /// Children of a HEAD node are per-commit.
    commit: Option<Oid>,
}

/// How an inode can be found: by name under its parent, or (rebuilt from
/// an NFS handle) by that handle.
#[derive(Clone)]
enum Name {
    Child(ChildKey),
    Handle(Box<[u8]>),
}

/// One inode: what it shows, how it's found (so it can be unlinked again),
/// and whether anyone may still use it.
struct Inode {
    node: Node,
    key: Option<Name>,
    /// FUSE lookup count: how many references the kernel holds.
    lookups: AtomicU64,
    /// Seconds since [`Vfs::epoch`] of the last use.
    touched: AtomicU64,
    /// Inode of `..` for paths inside a commit, once known; it may have been
    /// swept since (see [`Vfs::parent`]).
    up: AtomicU64,
}

impl Inode {
    fn new(node: Node, key: Option<Name>, now: u64) -> Self {
        let up = match &key {
            Some(Name::Child(k)) => k.parent,
            _ => NO_INO,
        };
        Self {
            node,
            key,
            lookups: AtomicU64::new(0),
            touched: AtomicU64::new(now),
            up: AtomicU64::new(up),
        }
    }
}

/// Inode tables are hit on every request; the default shard count follows
/// the (possibly cgroup-limited) CPU count and can be as low as 4.
const SHARDS: usize = 64;

pub struct Vfs {
    repos: Arc<Repos>,
    nodes: DashMap<u64, Inode>,
    children: DashMap<ChildKey, u64>,
    /// Live inode per NFS handle handed out or rebuilt.
    by_handle: DashMap<Box<[u8]>, u64>,
    next_ino: AtomicU64,
    epoch: Instant,
    uid: u32,
    gid: u32,
}

impl Vfs {
    pub fn new(repos: Arc<Repos>) -> Arc<Self> {
        let nodes = DashMap::with_shard_amount(SHARDS);
        nodes.insert(ROOT_INO, Inode::new(Node::Root, None, 0));
        nodes.insert(BY_REF_INO, Inode::new(Node::ByRef, None, 0));
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        Arc::new(Self {
            repos,
            nodes,
            children: DashMap::with_shard_amount(SHARDS),
            by_handle: DashMap::with_shard_amount(SHARDS),
            next_ino: AtomicU64::new(FIRST_DYNAMIC_INO),
            epoch: Instant::now(),
            uid,
            gid,
        })
    }

    pub fn repos(&self) -> &Arc<Repos> {
        &self.repos
    }

    fn now(&self) -> u64 {
        self.epoch.elapsed().as_secs()
    }

    fn node(&self, ino: u64) -> FsResult<Node> {
        let inode = self.nodes.get(&ino).ok_or(libc::ENOENT)?;
        inode.touched.store(self.now(), Ordering::Relaxed);
        Ok(inode.node.clone())
    }

    pub fn exists(&self, ino: u64) -> bool {
        self.nodes.contains_key(&ino)
    }

    /// Mark `ino` as just used; false if it's gone.
    fn touch(&self, ino: u64) -> bool {
        match self.nodes.get(&ino) {
            Some(inode) => {
                inode.touched.store(self.now(), Ordering::Relaxed);
                true
            }
            None => false,
        }
    }

    /// Number of live inodes.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The kernel now holds one more reference to `ino` (a FUSE lookup reply).
    pub fn retain(&self, ino: u64) {
        if let Some(inode) = self.nodes.get(&ino) {
            inode.lookups.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The kernel dropped `n` references to `ino` (FUSE forget). Once none are
    /// left, [`Vfs::sweep`] may remove it.
    pub fn forget(&self, ino: u64, n: u64) {
        if let Some(inode) = self.nodes.get(&ino) {
            let _ = inode
                .lookups
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                    Some(c.saturating_sub(n))
                });
        }
    }

    /// Remove inodes nobody holds (no FUSE kernel references) and nobody used
    /// in the last `idle`. NFS never holds references; its handles describe
    /// the node, so [`Vfs::resolve`] rebuilds one that was swept. Returns how
    /// many inodes were removed.
    pub fn sweep(&self, idle: Duration) -> usize {
        let Some(cutoff) = self.now().checked_sub(idle.as_secs()) else {
            return 0;
        };
        let unused = |inode: &Inode| {
            inode.lookups.load(Ordering::Relaxed) == 0
                && inode.touched.load(Ordering::Relaxed) <= cutoff
        };
        let stale: Vec<(u64, Option<Name>, Node)> = self
            .nodes
            .iter()
            .filter(|e| *e.key() >= FIRST_DYNAMIC_INO && unused(e.value()))
            .map(|e| (*e.key(), e.value().key.clone(), e.value().node.clone()))
            .collect();
        // Whether the name/handle entry `v` still points at `ino` and `ino` is
        // still unused. Checked under that entry's guard: callers that find an
        // inode through it touch the inode while holding the same guard, so
        // either their touch is seen here (and nothing is unlinked) or they
        // can't find it any more.
        let unlink =
            |v: &u64, ino: u64| *v == ino && self.nodes.get(&ino).is_none_or(|i| unused(&i));
        let mut removed = 0;
        for (ino, key, node) in stale {
            if !self.by_handle.is_empty()
                && let Some(raw) = Self::describe(&node).and_then(|d| d.encode())
            {
                self.by_handle
                    .remove_if(raw.as_slice(), |_, v| unlink(v, ino));
            }
            match key {
                Some(Name::Child(key)) => {
                    self.children.remove_if(&key, |_, v| unlink(v, ino));
                }
                Some(Name::Handle(raw)) => {
                    self.by_handle.remove_if(&raw, |_, v| unlink(v, ino));
                }
                None => {}
            }
            if self.nodes.remove_if(&ino, |_, i| unused(i)).is_some() {
                removed += 1;
            }
        }
        removed
    }

    fn child(&self, key: ChildKey, make: impl FnOnce() -> Node) -> u64 {
        if let Some(ino) = self.children.get(&key) {
            // Touch under the name's guard; see `sweep`. A name always points
            // at a live inode: `sweep` unlinks the name before the inode.
            if let Some(inode) = self.nodes.get(&ino) {
                inode.touched.store(self.now(), Ordering::Relaxed);
            }
            return *ino;
        }
        let now = self.now();
        *self.children.entry(key.clone()).or_insert_with(|| {
            let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
            self.nodes
                .insert(ino, Inode::new(make(), Some(Name::Child(key)), now));
            ino
        })
    }

    fn virtual_child(&self, parent: u64, name: &str, make: impl FnOnce() -> Node) -> u64 {
        self.child(
            ChildKey {
                parent,
                name: name.as_bytes().to_vec(),
                commit: None,
            },
            make,
        )
    }

    fn path_child(&self, parent: u64, ctx: &TreeCtx, entry: &TreeEntry) -> u64 {
        self.child(
            ChildKey {
                parent,
                name: entry.name.clone(),
                commit: ctx.commit.filter(|_| ctx.under_head),
            },
            || Node::Path {
                key: ctx.key.clone(),
                commit_time: ctx.commit_time,
                oid: entry.oid,
                kind: entry.kind,
                path: handle::path_hash(ctx.path, &entry.name),
                parent_tree: ctx.tree,
                commit: ctx.root,
                under_head: ctx.under_head,
                in_head: ctx.in_head,
                pinned: ctx.pinned,
            },
        )
    }

    /// Inode of `..`. Recreates ancestors that were swept; a directory
    /// rebuilt from a handle finds its parent once by searching its commit.
    pub async fn parent(&self, ino: u64) -> u64 {
        let Some((node, up)) = self
            .nodes
            .get(&ino)
            .map(|n| (n.node.clone(), n.up.load(Ordering::Relaxed)))
        else {
            return ROOT_INO;
        };
        match node {
            Node::Owner(_) | Node::ByRef | Node::Root => ROOT_INO,
            Node::RefOwner(_) => BY_REF_INO,
            Node::Head(key) => self.owner_ino(&key, false),
            Node::RefRepo(key) => self.owner_ino(&key, true),
            Node::Commit { key, .. } => self.ref_repo_ino(&key),
            // Touching it keeps the parent of a directory in use alive.
            Node::Path { .. } if up != NO_INO && self.touch(up) => up,
            Node::Path {
                key,
                oid,
                path,
                commit,
                commit_time,
                in_head,
                pinned,
                ..
            } => {
                let Some(found) = self
                    .find_parent(&key, commit, commit_time, in_head, pinned, path, oid)
                    .await
                else {
                    return ROOT_INO;
                };
                if let Some(inode) = self.nodes.get(&ino) {
                    inode.up.store(found, Ordering::Relaxed);
                }
                found
            }
        }
    }

    fn owner_ino(&self, key: &RepoKey, by_ref: bool) -> u64 {
        let owner = key.owner.clone();
        if by_ref {
            self.virtual_child(BY_REF_INO, owner.as_str(), || Node::RefOwner(owner.clone()))
        } else {
            self.virtual_child(ROOT_INO, owner.as_str(), || Node::Owner(owner.clone()))
        }
    }

    fn ref_repo_ino(&self, key: &RepoKey) -> u64 {
        let owner = self.owner_ino(key, true);
        self.virtual_child(owner, key.repo.as_str(), || Node::RefRepo(key.clone()))
    }

    fn head_ino(&self, key: &RepoKey) -> u64 {
        let owner = self.owner_ino(key, false);
        self.virtual_child(owner, key.repo.as_str(), || Node::Head(key.clone()))
    }

    /// Search `commit` for the directory holding the entry at `path`. Only
    /// needed for `..` of an inode rebuilt from an NFS handle (or whose
    /// parent was swept); the answer is then remembered in [`Inode::up`].
    #[allow(clippy::too_many_arguments)]
    async fn find_parent(
        &self,
        key: &RepoKey,
        commit: Oid,
        commit_time: i64,
        in_head: bool,
        pinned: bool,
        path: u64,
        oid: Oid,
    ) -> Option<u64> {
        if commit.is_zero() {
            return None;
        }
        let handle = self.repos.get(key);
        let (root, _) = handle.ensure_commit(commit).await.ok()?;
        // (tree, its path hash, depth) of directories still to search.
        let mut queue = VecDeque::from([(root, 0u64, 0usize)]);
        let (dir, dir_path, depth) = 'search: loop {
            let (tree, dir_path, depth) = queue.pop_front()?;
            let entries = handle.tree(tree).await.ok()?;
            for e in entries.iter() {
                let p = handle::path_hash(dir_path, &e.name);
                if p == path && e.oid == oid {
                    break 'search (tree, dir_path, depth);
                }
                if e.kind == EntryKind::Tree {
                    queue.push_back((e.oid, p, depth + 1));
                }
            }
        };
        let repo = handle::repo_id(key);
        if depth == 0 {
            return if in_head {
                Some(self.head_ino(key))
            } else {
                self.restore(Desc::Commit {
                    repo,
                    commit,
                    pinned,
                })
                .await
                .ok()
            };
        }
        self.restore(Desc::Path {
            repo,
            kind: EntryKind::Tree,
            under_head: in_head && depth == 1,
            in_head,
            pinned,
            commit_time: handle::handle_time(commit_time),
            path: dir_path,
            oid: dir,
            aux: commit,
        })
        .await
        .ok()
    }

    /// Self-describing handle for `ino` (see [`handle`]). Indexed, so
    /// resolving it again is one map lookup.
    pub fn handle(&self, ino: u64) -> Option<Vec<u8>> {
        let node = self.nodes.get(&ino)?.node.clone();
        let raw = Self::describe(&node)?.encode()?;
        // Touch under the index entry's guard, like `resolve`, so a
        // concurrent sweep either sees the touch or the entry is new.
        if let Some(v) = self.by_handle.get(raw.as_slice())
            && *v == ino
            && self.touch(ino)
        {
            return Some(raw);
        }
        let mut v = self.by_handle.entry(raw.clone().into()).or_insert(ino);
        if *v != ino && !self.touch(*v) {
            // The indexed inode was swept: this one takes over.
            *v = ino;
        }
        if !self.touch(*v) {
            drop(v);
            self.by_handle
                .remove_if(raw.as_slice(), |_, v| !self.nodes.contains_key(v));
            return None;
        }
        Some(raw)
    }

    fn describe(node: &Node) -> Option<Desc> {
        Some(match node {
            Node::Root => Desc::Root,
            Node::ByRef => Desc::ByRef,
            Node::Owner(owner) => Desc::Owner(owner.as_str().to_owned()),
            Node::RefOwner(owner) => Desc::RefOwner(owner.as_str().to_owned()),
            Node::RefRepo(key) => Desc::RefRepo(handle::repo_id(key)),
            Node::Head(key) => Desc::Head(handle::repo_id(key)),
            Node::Commit {
                key,
                commit,
                pinned,
            } => Desc::Commit {
                repo: handle::repo_id(key),
                commit: *commit,
                pinned: *pinned,
            },
            Node::Path {
                key,
                commit_time,
                oid,
                kind,
                path,
                parent_tree,
                commit,
                under_head,
                in_head,
                pinned,
            } => {
                let aux = if *kind == EntryKind::Tree {
                    *commit
                } else {
                    *parent_tree
                };
                // A directory rebuilt without its commit can't be described.
                if aux.is_zero() {
                    return None;
                }
                Desc::Path {
                    repo: handle::repo_id(key),
                    kind: *kind,
                    under_head: *under_head,
                    in_head: *in_head,
                    pinned: *pinned,
                    commit_time: handle::handle_time(*commit_time),
                    path: *path,
                    oid: *oid,
                    aux,
                }
            }
        })
    }

    /// Inode for a handle from [`Vfs::handle`], rebuilding it if it was swept
    /// (or the daemon restarted). `EBADF` if malformed, `ESTALE` if it names
    /// something that isn't there.
    pub async fn resolve(&self, raw: &[u8]) -> FsResult<u64> {
        if let Some(ino) = self.by_handle.get(raw) {
            // Touch under the handle's guard; see `sweep`.
            if let Some(inode) = self.nodes.get(&ino) {
                inode.touched.store(self.now(), Ordering::Relaxed);
                return Ok(*ino);
            }
        }
        let desc = Desc::decode(raw).ok_or(libc::EBADF)?;
        self.restore(desc).await
    }

    async fn restore(&self, desc: Desc) -> FsResult<u64> {
        let repo = |id: u64| self.repos.by_id(id).ok_or(libc::ESTALE);
        let by_ref = matches!(desc, Desc::RefOwner(_));
        Ok(match desc {
            Desc::Root => ROOT_INO,
            Desc::ByRef => BY_REF_INO,
            Desc::Owner(name) | Desc::RefOwner(name) => {
                let owner: Owner = name.parse().map_err(|_| libc::EBADF)?;
                let parent = if by_ref { BY_REF_INO } else { ROOT_INO };
                self.virtual_child(parent, &name, || {
                    if by_ref {
                        Node::RefOwner(owner)
                    } else {
                        Node::Owner(owner)
                    }
                })
            }
            Desc::Head(id) => self.head_ino(&repo(id)?),
            Desc::RefRepo(id) => self.ref_repo_ino(&repo(id)?),
            Desc::Commit {
                repo: id,
                commit,
                pinned,
            } => {
                let key = repo(id)?;
                self.restored_node(&desc, || Node::Commit {
                    key,
                    commit,
                    pinned,
                })?
            }
            Desc::Path {
                repo: id,
                kind,
                under_head,
                in_head,
                pinned,
                commit_time,
                path,
                oid,
                aux,
            } => {
                let key = repo(id)?;
                // Check it's still there (e.g. the cache was cleared): a
                // directory's tree is local with its commit, a file is
                // listed in its directory's tree. Only a missing object is
                // stale (permanent for the client); other failures are I/O.
                let handle = self.repos.get(&key);
                let tree = handle.tree(if kind == EntryKind::Tree { oid } else { aux });
                let present = match tree.await {
                    Ok(entries) => {
                        kind == EntryKind::Tree
                            || entries.iter().any(|e| e.oid == oid && e.kind == kind)
                    }
                    Err(RepoError::NotFound(_)) => false,
                    Err(e) => return Err(repo_errno(&key, e)),
                };
                if !present {
                    return Err(libc::ESTALE);
                }
                let (parent_tree, commit) = if kind == EntryKind::Tree {
                    (Oid::zero(), aux)
                } else {
                    (aux, Oid::zero())
                };
                self.restored_node(&desc, || Node::Path {
                    key,
                    commit_time: commit_time.into(),
                    oid,
                    kind,
                    path,
                    parent_tree,
                    commit,
                    under_head,
                    in_head,
                    pinned,
                })?
            }
        })
    }

    /// Get or create the inode for a rebuilt node, keyed by its handle.
    fn restored_node(&self, desc: &Desc, make: impl FnOnce() -> Node) -> FsResult<u64> {
        let raw: Box<[u8]> = desc.encode().ok_or(libc::EBADF)?.into();
        let now = self.now();
        let mut ino = self.by_handle.entry(raw.clone()).or_insert(NO_INO);
        match self.nodes.get(&*ino) {
            Some(inode) => inode.touched.store(now, Ordering::Relaxed),
            None => {
                // New, or the indexed inode was swept in the meantime.
                *ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
                self.nodes
                    .insert(*ino, Inode::new(make(), Some(Name::Handle(raw)), now));
            }
        }
        Ok(*ino)
    }

    pub fn volatility(&self, ino: u64) -> Volatility {
        match self.nodes.get(&ino).map(|n| n.node.clone()) {
            Some(Node::Commit { pinned: true, .. }) => Volatility::Immutable,
            Some(Node::Commit { .. }) | Some(Node::Head(_)) => Volatility::Ref,
            Some(Node::Path { under_head, .. }) => {
                if under_head {
                    Volatility::Ref
                } else {
                    Volatility::Immutable
                }
            }
            _ => Volatility::Virtual,
        }
    }

    /// Kind and permission bits without touching git (for NFS ACCESS).
    pub fn mode(&self, ino: u64) -> Option<(FileKind, u16)> {
        Some(match &self.nodes.get(&ino)?.node {
            Node::Path { kind, .. } => match kind {
                EntryKind::Tree => (FileKind::Directory, 0o500),
                EntryKind::Executable => (FileKind::File, 0o500),
                EntryKind::Symlink => (FileKind::Symlink, 0o777),
                _ => (FileKind::File, 0o400),
            },
            _ => (FileKind::Directory, 0o500),
        })
    }

    fn dir_attr(&self, ino: u64, mtime: SystemTime) -> Attr {
        Attr {
            ino,
            kind: FileKind::Directory,
            size: 4096,
            perm: 0o500,
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            mtime,
        }
    }

    /// Resolve what git tree a directory node shows. May fetch the commit.
    async fn tree_ctx(&self, node: &Node) -> FsResult<TreeCtx> {
        match node {
            Node::Head(key) => {
                let handle = self.repos.get(key);
                let head = handle.head().await.map_err(|e| repo_errno(key, e))?;
                let (tree, time) = handle
                    .ensure_commit(head)
                    .await
                    .map_err(|e| repo_errno(key, e))?;
                Ok(TreeCtx {
                    key: key.clone(),
                    handle,
                    commit: Some(head),
                    root: head,
                    tree,
                    path: 0,
                    commit_time: time,
                    under_head: true,
                    in_head: true,
                    pinned: false,
                })
            }
            Node::Commit {
                key,
                commit,
                pinned,
            } => {
                let handle = self.repos.get(key);
                let (tree, time) = handle
                    .ensure_commit(*commit)
                    .await
                    .map_err(|e| repo_errno(key, e))?;
                Ok(TreeCtx {
                    key: key.clone(),
                    handle,
                    commit: Some(*commit),
                    root: *commit,
                    tree,
                    path: 0,
                    commit_time: time,
                    under_head: false,
                    in_head: false,
                    pinned: *pinned,
                })
            }
            Node::Path {
                key,
                oid,
                kind: EntryKind::Tree,
                path,
                commit_time,
                commit,
                in_head,
                pinned,
                ..
            } => Ok(TreeCtx {
                key: key.clone(),
                handle: self.repos.get(key),
                commit: None,
                root: *commit,
                tree: *oid,
                path: *path,
                commit_time: *commit_time,
                under_head: false,
                in_head: *in_head,
                pinned: *pinned,
            }),
            Node::Path { .. } => Err(libc::ENOTDIR),
            _ => Err(libc::EINVAL),
        }
    }

    /// Look up `name` in directory `parent`. Never fetches blobs; for files
    /// call [`Vfs::getattr`] afterwards if attributes are needed.
    pub async fn lookup(&self, parent: u64, name: &[u8]) -> FsResult<u64> {
        if name == b"." {
            return Ok(parent);
        }
        if name == b".." {
            return Ok(self.parent(parent).await);
        }
        let node = self.node(parent)?;
        let name_str = std::str::from_utf8(name).map_err(|_| libc::ENOENT);
        match node {
            Node::Root => {
                let name = name_str?;
                if name == BY_REF_ROOT {
                    return Ok(BY_REF_INO);
                }
                let owner: Owner = name.parse().map_err(|_| libc::ENOENT)?;
                Ok(self.virtual_child(ROOT_INO, name, || Node::Owner(owner)))
            }
            Node::ByRef => {
                let name = name_str?;
                let owner: Owner = name.parse().map_err(|_| libc::ENOENT)?;
                Ok(self.virtual_child(BY_REF_INO, name, || Node::RefOwner(owner)))
            }
            Node::Owner(owner) => {
                let key = repo_key(&owner, name_str?)?;
                // Only confirms the repo exists; cached after the first time.
                self.repos
                    .get(&key)
                    .head()
                    .await
                    .map_err(|e| repo_errno(&key, e))?;
                Ok(self.virtual_child(parent, key.repo.as_str(), || Node::Head(key.clone())))
            }
            Node::RefOwner(owner) => {
                let key = repo_key(&owner, name_str?)?;
                self.repos
                    .get(&key)
                    .head()
                    .await
                    .map_err(|e| repo_errno(&key, e))?;
                Ok(self.virtual_child(parent, key.repo.as_str(), || Node::RefRepo(key.clone())))
            }
            Node::RefRepo(key) => {
                let selector = decode_ref(name).ok_or(libc::ENOENT)?;
                if !plausible_ref(&selector) {
                    return Err(libc::ENOENT);
                }
                let handle = self.repos.get(&key);
                let commit = handle
                    .resolve(&selector)
                    .await
                    .map_err(|e| repo_errno(&key, e))?;
                let pinned = selector.len() == 40 && commit.to_string() == selector;
                if pinned {
                    // A raw commit id isn't validated by the ref listing;
                    // fetch it now so a bogus id fails the lookup.
                    handle
                        .ensure_commit(commit)
                        .await
                        .map_err(|e| repo_errno(&key, e))?;
                }
                Ok(self.child(
                    ChildKey {
                        parent,
                        name: name.to_vec(),
                        commit: Some(commit),
                    },
                    || Node::Commit {
                        key: key.clone(),
                        commit,
                        pinned,
                    },
                ))
            }
            Node::Head(_) | Node::Commit { .. } | Node::Path { .. } => {
                let ctx = self.tree_ctx(&node).await?;
                let entries = ctx
                    .handle
                    .tree(ctx.tree)
                    .await
                    .map_err(|e| repo_errno(&ctx.key, e))?;
                let entry = entries
                    .iter()
                    .find(|e| e.name == name)
                    .ok_or(libc::ENOENT)?;
                Ok(self.path_child(parent, &ctx, entry))
            }
        }
    }

    /// Attributes, fetching a file's blob (with its directory) if needed.
    pub async fn getattr(&self, ino: u64) -> FsResult<Attr> {
        self.attr(ino, true)
            .await
            .map(|a| a.expect("fetch allowed"))
    }

    /// Attributes if they're available without any network access.
    pub async fn getattr_local(&self, ino: u64) -> FsResult<Option<Attr>> {
        self.attr(ino, false).await
    }

    async fn attr(&self, ino: u64, fetch: bool) -> FsResult<Option<Attr>> {
        let node = self.node(ino)?;
        let started = self.repos.started();
        let attr = match node {
            Node::Root | Node::ByRef | Node::Owner(_) | Node::RefOwner(_) => {
                self.dir_attr(ino, started)
            }
            Node::Head(key) => {
                // The macOS client revalidates cached names only when this
                // mtime changes, and may send no LOOKUP until then; so GETATTR
                // has to be what notices a stale HEAD.
                let handle = self.repos.get(&key);
                handle.poke_head();
                self.dir_attr(ino, handle.changed_at())
            }
            Node::RefRepo(key) => {
                let handle = self.repos.get(&key);
                handle.poke_refs();
                self.dir_attr(ino, handle.changed_at())
            }
            Node::Commit { key, commit, .. } => {
                // Commit time if the commit is already local; don't fetch just
                // for a timestamp.
                let handle = self.repos.get(&key);
                let time = handle
                    .with_repo(move |r| crate::git::odb::commit_info(r, commit))
                    .await
                    .map(|(_, t)| unix_time(t))
                    .unwrap_or(started);
                self.dir_attr(ino, time)
            }
            Node::Path {
                key,
                commit_time,
                oid,
                kind,
                parent_tree,
                ..
            } => {
                let mtime = unix_time(commit_time);
                match kind {
                    EntryKind::Tree => self.dir_attr(ino, mtime),
                    EntryKind::Gitlink => self.file_attr(ino, kind, 0, mtime),
                    _ => {
                        let handle = self.repos.get(&key);
                        let size = if fetch {
                            handle
                                .blob_size(parent_tree, oid)
                                .await
                                .map_err(|e| repo_errno(&key, e))?
                        } else {
                            let local = handle
                                .with_repo(move |r| crate::git::odb::blob_size(r, oid))
                                .await
                                .map_err(|e| repo_errno(&key, e))?;
                            match local {
                                Some(size) => size,
                                None => return Ok(None),
                            }
                        };
                        self.file_attr(ino, kind, size, mtime)
                    }
                }
            }
        };
        Ok(Some(attr))
    }

    fn file_attr(&self, ino: u64, kind: EntryKind, size: u64, mtime: SystemTime) -> Attr {
        let (kind, perm) = match kind {
            EntryKind::Executable => (FileKind::File, 0o500),
            EntryKind::Symlink => (FileKind::Symlink, 0o777),
            _ => (FileKind::File, 0o400),
        };
        Attr {
            ino,
            kind,
            size,
            perm,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            mtime,
        }
    }

    /// Directory listing (names, inodes, kinds). Starts fetching the
    /// directory's blobs in the background, since reading or `ls -l` usually
    /// follows.
    pub async fn readdir(&self, ino: u64) -> FsResult<Vec<DirEntry>> {
        let node = self.node(ino)?;
        let dir = |ino, name: &str| DirEntry {
            ino,
            name: name.as_bytes().to_vec(),
            kind: FileKind::Directory,
        };
        match node {
            Node::Root => {
                let mut out = vec![dir(BY_REF_INO, BY_REF_ROOT)];
                for owner in self.cached_owners() {
                    if owner.as_str() == BY_REF_ROOT {
                        continue;
                    }
                    let ino =
                        self.virtual_child(ROOT_INO, owner.as_str(), || Node::Owner(owner.clone()));
                    out.push(dir(ino, owner.as_str()));
                }
                Ok(out)
            }
            Node::ByRef => Ok(self
                .cached_owners()
                .into_iter()
                .map(|owner| {
                    let ino = self.virtual_child(BY_REF_INO, owner.as_str(), || {
                        Node::RefOwner(owner.clone())
                    });
                    dir(ino, owner.as_str())
                })
                .collect()),
            Node::Owner(ref owner) | Node::RefOwner(ref owner) => {
                let by_ref = matches!(node, Node::RefOwner(_));
                Ok(self
                    .repos
                    .paths()
                    .cached_repos()
                    .into_iter()
                    .filter(|key| key.owner.as_str().eq_ignore_ascii_case(owner.as_str()))
                    .map(|key| {
                        let name = key.repo.as_str().to_owned();
                        let ino = self.virtual_child(ino, &name, || {
                            if by_ref {
                                Node::RefRepo(key.clone())
                            } else {
                                Node::Head(key.clone())
                            }
                        });
                        dir(ino, &name)
                    })
                    .collect())
            }
            Node::RefRepo(key) => {
                let handle = self.repos.get(&key);
                let refs = handle.refs(false).await.map_err(|e| repo_errno(&key, e))?;
                let mut out = Vec::new();
                for short in refs.short_names() {
                    let Some(commit) = refs.resolve(&short) else {
                        continue;
                    };
                    let name = encode_ref(&short);
                    let ino = self.child(
                        ChildKey {
                            parent: ino,
                            name: name.as_bytes().to_vec(),
                            commit: Some(commit),
                        },
                        || Node::Commit {
                            key: key.clone(),
                            commit,
                            pinned: false,
                        },
                    );
                    out.push(dir(ino, &name));
                }
                Ok(out)
            }
            Node::Head(_) | Node::Commit { .. } | Node::Path { .. } => {
                let ctx = self.tree_ctx(&node).await?;
                let entries = ctx
                    .handle
                    .tree(ctx.tree)
                    .await
                    .map_err(|e| repo_errno(&ctx.key, e))?;
                if entries.iter().any(|e| e.kind.has_blob()) {
                    ctx.handle.prefetch_dir(ctx.tree);
                }
                let mut out: Vec<DirEntry> = entries
                    .iter()
                    .map(|entry| DirEntry {
                        ino: self.path_child(ino, &ctx, entry),
                        name: entry.name.clone(),
                        kind: match entry.kind {
                            EntryKind::Tree => FileKind::Directory,
                            EntryKind::Symlink => FileKind::Symlink,
                            _ => FileKind::File,
                        },
                    })
                    .collect();
                out.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(out)
            }
        }
    }

    fn cached_owners(&self) -> Vec<Owner> {
        let mut owners: Vec<Owner> = self
            .repos
            .paths()
            .cached_repos()
            .into_iter()
            .map(|k| k.owner)
            .collect();
        owners.dedup();
        owners
    }

    async fn blob_node(
        &self,
        ino: u64,
    ) -> FsResult<(Arc<RepoHandle>, RepoKey, Oid, Oid, EntryKind)> {
        match self.node(ino)? {
            Node::Path {
                key,
                oid,
                kind,
                parent_tree,
                ..
            } => {
                if kind == EntryKind::Tree {
                    return Err(libc::EISDIR);
                }
                Ok((self.repos.get(&key), key, oid, parent_tree, kind))
            }
            _ => Err(libc::EISDIR),
        }
    }

    /// Read up to `count` bytes at `offset`. Returns the data and whether it
    /// reached end of file.
    pub async fn read(&self, ino: u64, offset: u64, count: u32) -> FsResult<(Vec<u8>, bool)> {
        let (handle, key, oid, parent_tree, kind) = self.blob_node(ino).await?;
        if kind == EntryKind::Gitlink {
            return Ok((Vec::new(), true));
        }
        // Written to disk once (streamed, deduplicated across concurrent
        // readers), then every read is a pread from the file.
        let mut attempts = 0;
        let file = loop {
            let path = handle
                .materialize(parent_tree, oid)
                .await
                .map_err(|e| repo_errno(&key, e))?;
            match std::fs::File::open(&path) {
                Ok(file) => break file,
                // `ghfs gc` removed it between materialize and open.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound && attempts == 0 => {
                    attempts += 1;
                }
                Err(e) => return Err(e.raw_os_error().unwrap_or(libc::EIO)),
            }
        };
        tokio::task::spawn_blocking(move || -> FsResult<(Vec<u8>, bool)> {
            let len = file
                .metadata()
                .map_err(|e| e.raw_os_error().unwrap_or(libc::EIO))?
                .len();
            if offset >= len {
                return Ok((Vec::new(), true));
            }
            let want = (count as u64).min(len - offset) as usize;
            let mut buf = vec![0u8; want];
            let mut read = 0;
            while read < want {
                let n = file
                    .read_at(&mut buf[read..], offset + read as u64)
                    .map_err(|e| e.raw_os_error().unwrap_or(libc::EIO))?;
                if n == 0 {
                    break;
                }
                read += n;
            }
            buf.truncate(read);
            Ok((buf, offset + read as u64 >= len))
        })
        .await
        .map_err(|_| libc::EIO)?
    }

    pub async fn readlink(&self, ino: u64) -> FsResult<Vec<u8>> {
        let (handle, key, oid, parent_tree, kind) = self.blob_node(ino).await?;
        if kind != EntryKind::Symlink {
            return Err(libc::EINVAL);
        }
        handle
            .read_blob(parent_tree, oid)
            .await
            .map_err(|e| repo_errno(&key, e))
    }
}

fn repo_key(owner: &Owner, name: &str) -> FsResult<RepoKey> {
    let repo: Repo = name.parse().map_err(|_| libc::ENOENT)?;
    Ok(RepoKey::new(owner.clone(), repo))
}

fn unix_time(secs: i64) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH
    }
}
