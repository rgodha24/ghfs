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

pub mod refname;

use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use git2::Oid;

use crate::git::{EntryKind, TreeEntry};
use crate::repos::{RepoError, RepoHandle, Repos};
use crate::types::{Owner, Repo, RepoKey};
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
        parent: u64,
        /// Tree of the containing directory, for batched blob fetches.
        parent_tree: Oid,
        /// Directly below `<owner>/<repo>`, so it changes when HEAD moves.
        under_head: bool,
    },
}

/// A directory's resolved git contents.
struct TreeCtx {
    key: RepoKey,
    handle: Arc<RepoHandle>,
    commit: Option<Oid>,
    tree: Oid,
    commit_time: i64,
    under_head: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ChildKey {
    parent: u64,
    name: Vec<u8>,
    /// Children of a HEAD node are per-commit.
    commit: Option<Oid>,
}

pub struct Vfs {
    repos: Arc<Repos>,
    nodes: DashMap<u64, Node>,
    children: DashMap<ChildKey, u64>,
    next_ino: AtomicU64,
    uid: u32,
    gid: u32,
}

impl Vfs {
    pub fn new(repos: Arc<Repos>) -> Arc<Self> {
        let nodes = DashMap::new();
        nodes.insert(ROOT_INO, Node::Root);
        nodes.insert(BY_REF_INO, Node::ByRef);
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        Arc::new(Self {
            repos,
            nodes,
            children: DashMap::new(),
            next_ino: AtomicU64::new(FIRST_DYNAMIC_INO),
            uid,
            gid,
        })
    }

    pub fn repos(&self) -> &Arc<Repos> {
        &self.repos
    }

    fn node(&self, ino: u64) -> FsResult<Node> {
        self.nodes.get(&ino).map(|n| n.clone()).ok_or(libc::ENOENT)
    }

    pub fn exists(&self, ino: u64) -> bool {
        self.nodes.contains_key(&ino)
    }

    fn child(&self, key: ChildKey, make: impl FnOnce() -> Node) -> u64 {
        if let Some(ino) = self.children.get(&key) {
            return *ino;
        }
        *self.children.entry(key).or_insert_with(|| {
            let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
            self.nodes.insert(ino, make());
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
                parent,
                parent_tree: ctx.tree,
                under_head: ctx.under_head,
            },
        )
    }

    pub fn parent(&self, ino: u64) -> u64 {
        match self.nodes.get(&ino).map(|n| n.clone()) {
            Some(Node::Owner(_)) | Some(Node::ByRef) | Some(Node::Root) | None => ROOT_INO,
            Some(Node::RefOwner(_)) => BY_REF_INO,
            Some(Node::Head(key)) => self
                .children
                .get(&ChildKey {
                    parent: ROOT_INO,
                    name: key.owner.as_str().as_bytes().to_vec(),
                    commit: None,
                })
                .map(|i| *i)
                .unwrap_or(ROOT_INO),
            Some(Node::RefRepo(key)) => self
                .children
                .get(&ChildKey {
                    parent: BY_REF_INO,
                    name: key.owner.as_str().as_bytes().to_vec(),
                    commit: None,
                })
                .map(|i| *i)
                .unwrap_or(BY_REF_INO),
            Some(Node::Commit { key, .. }) => {
                // Copy each inode out before the next lookup; nested DashMap
                // guards can deadlock against a waiting writer.
                let owner = self
                    .children
                    .get(&ChildKey {
                        parent: BY_REF_INO,
                        name: key.owner.as_str().as_bytes().to_vec(),
                        commit: None,
                    })
                    .map(|i| *i);
                owner
                    .and_then(|o| {
                        self.children
                            .get(&ChildKey {
                                parent: o,
                                name: key.repo.as_str().as_bytes().to_vec(),
                                commit: None,
                            })
                            .map(|i| *i)
                    })
                    .unwrap_or(BY_REF_INO)
            }
            Some(Node::Path { parent, .. }) => parent,
        }
    }

    pub fn volatility(&self, ino: u64) -> Volatility {
        match self.nodes.get(&ino).map(|n| n.clone()) {
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
        Some(match self.nodes.get(&ino)?.value() {
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
                    tree,
                    commit_time: time,
                    under_head: true,
                })
            }
            Node::Commit { key, commit, .. } => {
                let handle = self.repos.get(key);
                let (tree, time) = handle
                    .ensure_commit(*commit)
                    .await
                    .map_err(|e| repo_errno(key, e))?;
                Ok(TreeCtx {
                    key: key.clone(),
                    handle,
                    commit: Some(*commit),
                    tree,
                    commit_time: time,
                    under_head: false,
                })
            }
            Node::Path {
                key,
                oid,
                kind: EntryKind::Tree,
                commit_time,
                ..
            } => Ok(TreeCtx {
                key: key.clone(),
                handle: self.repos.get(key),
                commit: None,
                tree: *oid,
                commit_time: *commit_time,
                under_head: false,
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
            return Ok(self.parent(parent));
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
