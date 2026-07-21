//! GHFS filesystem backends.
//!
//! Linux uses FUSE (`fuser`) and macOS uses an in-process NFSv3 server
//! (`nfsserve`). Both backends delegate to the same store-backed [`GhFs`],
//! which synthesizes inodes from git object identity (no worktree passthrough):
//! directory inodes carry a git tree OID, file inodes carry a blob OID that is
//! hydrated lazily by the store.

use crate::cache::CachePaths;
use crate::daemon::WorkerHandle;
use crate::store::git::MIN_OID_LEN;
use crate::store::ref_selector::{BY_REF_ROOT, decode_ref, encode_ref};
use crate::store::{EntryKind, Store, StoreError};
use crate::types::{Owner, Repo, RepoKey};
#[cfg(target_os = "linux")]
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::{OsStrExt, OsStringExt};

#[cfg(target_os = "linux")]
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyXattr, Request,
};

mod inode;
#[cfg(target_os = "macos")]
mod nfs;

pub use inode::{
    BY_REF_INO, InodeData, InodeTable, PASSTHROUGH_INO_START, PathKey, ROOT_INO, VIRTUAL_INO_END,
    VIRTUAL_INO_START,
};

/// TTL for virtual discovery nodes (root, owners, by-ref roots, ref-repo
/// directory listings).
#[cfg(target_os = "linux")]
const VIRTUAL_TTL: Duration = Duration::from_secs(60);
/// TTL for mutable ref resolution (default-branch alias and named ref
/// selectors). Short so a moved branch re-resolves promptly.
#[cfg(target_os = "linux")]
const REF_TTL: Duration = Duration::from_secs(5);
/// TTL for content pinned to an immutable commit (commit-OID selectors and
/// all repository path nodes). Long, since git objects never change.
#[cfg(target_os = "linux")]
const COMMIT_TTL: Duration = Duration::from_secs(3600);

#[cfg(target_os = "linux")]
const FINDER_INFO_XATTR: &str = "com.apple.FinderInfo";
#[cfg(target_os = "linux")]
const FINDER_INFO_XATTR_LIST: &[u8] = b"com.apple.FinderInfo\0";
#[cfg(target_os = "linux")]
const FINDER_INFO_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FsKind {
    Directory,
    RegularFile,
    Symlink,
}

#[derive(Debug, Clone)]
#[cfg_attr(target_os = "macos", allow(dead_code))]
struct NodeAttr {
    ino: u64,
    size: u64,
    blocks: u64,
    atime: SystemTime,
    mtime: SystemTime,
    ctime: SystemTime,
    kind: FsKind,
    perm: u16,
    nlink: u32,
    uid: u32,
    gid: u32,
    rdev: u32,
    blksize: u32,
}

#[derive(Debug, Clone)]
#[cfg_attr(target_os = "macos", allow(dead_code))]
struct DirEntryInfo {
    ino: u64,
    kind: FsKind,
    name: OsString,
}

fn io_errno(err: std::io::Error, fallback: i32) -> i32 {
    err.raw_os_error().unwrap_or(fallback)
}

fn store_err_errno(err: &StoreError) -> i32 {
    match err {
        StoreError::Git(g) => match g {
            crate::store::GitError::NotFound(_) => libc::ENOENT,
            crate::store::GitError::RefNotFound(_) => libc::ENOENT,
            crate::store::GitError::AmbiguousRef(_) => libc::EINVAL,
            crate::store::GitError::InvalidInput(_) => libc::EINVAL,
            _ => libc::EIO,
        },
        StoreError::Tree(t) => match t {
            crate::store::TreeError::NotATree(_) => libc::ENOTDIR,
            crate::store::TreeError::InvalidPath => libc::ENOENT,
            _ => libc::EIO,
        },
        StoreError::Blob(b) => match b {
            crate::store::BlobError::BlobNotFound(_) => libc::ENOENT,
            _ => libc::EIO,
        },
        StoreError::RepoNotFound(_) => libc::ENOENT,
        StoreError::LockFailed => libc::EIO,
        StoreError::Io(e) => io_errno(std::io::Error::from(e.kind()), libc::EIO),
    }
}

fn entry_kind_to_fs(kind: EntryKind) -> FsKind {
    match kind {
        EntryKind::Tree => FsKind::Directory,
        EntryKind::Blob | EntryKind::Executable | EntryKind::Gitlink => FsKind::RegularFile,
        EntryKind::Symlink => FsKind::Symlink,
    }
}

fn entry_mode(kind: EntryKind) -> u16 {
    match kind {
        EntryKind::Tree => 0o040755,
        EntryKind::Blob | EntryKind::Gitlink => 0o100644,
        EntryKind::Executable => 0o100755,
        EntryKind::Symlink => 0o120777,
    }
}

#[cfg(target_os = "linux")]
fn kind_to_fuse(kind: FsKind) -> FileType {
    match kind {
        FsKind::Directory => FileType::Directory,
        FsKind::RegularFile => FileType::RegularFile,
        FsKind::Symlink => FileType::Symlink,
    }
}

#[cfg(target_os = "linux")]
impl NodeAttr {
    fn to_fuse_attr(&self) -> FileAttr {
        FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: self.blocks,
            atime: self.atime,
            mtime: self.mtime,
            ctime: self.ctime,
            crtime: UNIX_EPOCH,
            kind: kind_to_fuse(self.kind),
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: self.rdev,
            blksize: self.blksize,
            flags: 0,
        }
    }
}

/// The GHFS filesystem. Backend-agnostic; both FUSE and NFS adapters delegate
/// to the same store-backed methods.
#[cfg_attr(target_os = "macos", allow(dead_code))]
pub struct GhFs {
    store: Store,
    worker: Arc<WorkerHandle>,
    cache_paths: CachePaths,
    inodes: InodeTable,
    uid: u32,
    gid: u32,
    #[cfg(target_os = "linux")]
    open_files: Mutex<HashMap<u64, File>>,
    #[cfg(target_os = "linux")]
    next_fh: AtomicU64,
}

impl GhFs {
    /// Create a new filesystem instance.
    pub fn new(store: Store, worker: Arc<WorkerHandle>) -> Self {
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        Self {
            store,
            worker,
            cache_paths: CachePaths::default(),
            inodes: InodeTable::new(),
            uid,
            gid,
            #[cfg(target_os = "linux")]
            open_files: Mutex::new(HashMap::new()),
            #[cfg(target_os = "linux")]
            next_fh: AtomicU64::new(1),
        }
    }

    fn virtual_dir_attr(&self, ino: u64) -> NodeAttr {
        NodeAttr {
            ino,
            size: 4096,
            blocks: 8,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            kind: FsKind::Directory,
            perm: 0o755,
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
        }
    }

    fn file_attr(&self, ino: u64, kind: EntryKind, size: u64) -> NodeAttr {
        let fskind = entry_kind_to_fs(kind);
        NodeAttr {
            ino,
            size,
            blocks: (size + 511) / 512,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            kind: fskind,
            perm: entry_mode(kind) & 0o7777,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
        }
    }

    fn is_valid_owner(name: &str) -> bool {
        name.parse::<Owner>().is_ok()
    }

    fn is_valid_repo(name: &str) -> bool {
        name.parse::<Repo>().is_ok()
    }

    /// Whether a path component looks like a commit-OID selector (pure hex,
    /// long enough). Used to pick the immutable-commit TTL.
    fn is_commit_oid_selector(name: &str) -> bool {
        name.len() >= MIN_OID_LEN && name.chars().all(|c| c.is_ascii_hexdigit())
    }

    fn list_cached_owners(&self) -> Vec<String> {
        let mut owners = Vec::new();
        if let Ok(entries) = std::fs::read_dir(self.cache_paths.mirrors_dir()) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false)
                    && let Some(name) = entry.file_name().to_str()
                    && Self::is_valid_owner(name)
                {
                    owners.push(name.to_string());
                }
            }
        }
        owners.sort();
        owners
    }

    fn list_cached_repos(&self, owner: &str) -> Vec<String> {
        let dir = self.cache_paths.mirrors_dir().join(owner);
        let mut repos = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    continue;
                }
                if let Some(name) = entry.file_name().to_str()
                    && let Some(stripped) = name.strip_suffix(".git")
                    && Self::is_valid_repo(stripped)
                {
                    repos.push(stripped.to_string());
                }
            }
        }
        repos.sort();
        repos
    }

    /// Ensure a repo's mirror exists and return its default-branch (HEAD)
    /// commit. Off-loaded to the worker so the mount thread isn't the one
    /// performing a network clone.
    fn materialize_head(&self, key: &RepoKey) -> Result<String, i32> {
        self.worker.materialize(key.clone()).map_err(|e| {
            log::error!("materialize {key} failed: {e}");
            store_err_errno(&e)
        })
    }

    /// Ensure a repo mirror exists and resolve a ref selector to a commit OID.
    fn resolve_selector(&self, key: &RepoKey, selector: &str) -> Result<String, i32> {
        self.worker
            .resolve(key.clone(), selector.to_string())
            .map_err(|e| {
                log::error!("resolve {key} {selector} failed: {e}");
                store_err_errno(&e)
            })
    }

    fn lookup_inode(&self, parent: u64, name: &OsStr) -> Result<u64, i32> {
        let name_str = name.to_str().ok_or(libc::ENOENT)?;

        // ---- virtual discovery hierarchy ----
        if parent == ROOT_INO {
            if name_str == BY_REF_ROOT {
                return Ok(BY_REF_INO);
            }
            if !Self::is_valid_owner(name_str) {
                return Err(libc::ENOENT);
            }
            return Ok(self.inodes.get_or_alloc_virtual(
                parent,
                name_str,
                InodeData::Owner(name_str.parse::<Owner>().unwrap()),
            )?);
        }

        if parent == BY_REF_INO {
            if !Self::is_valid_owner(name_str) {
                return Err(libc::ENOENT);
            }
            return Ok(self.inodes.get_or_alloc_virtual(
                parent,
                name_str,
                InodeData::RefOwner(name_str.parse::<Owner>().unwrap()),
            )?);
        }

        let parent_data = self.inodes.get(parent).ok_or(libc::ENOENT)?;

        match parent_data {
            InodeData::Owner(owner) => {
                if !Self::is_valid_repo(name_str) {
                    return Err(libc::ENOENT);
                }
                let repo: Repo = name_str.parse().unwrap();
                let key = RepoKey::new(owner, repo);
                let commit = self.materialize_head(&key)?;
                let root_tree = self
                    .store
                    .root_tree(&key, parse_oid(&commit)?)
                    .map_err(|e| store_err_errno(&e))?;
                Ok(self.inodes.get_or_alloc_virtual(
                    parent,
                    name_str,
                    InodeData::Repo {
                        key,
                        selector: None,
                        commit,
                        root_tree: root_tree.to_string(),
                    },
                )?)
            }
            InodeData::RefOwner(owner) => {
                if !Self::is_valid_repo(name_str) {
                    return Err(libc::ENOENT);
                }
                let repo: Repo = name_str.parse().unwrap();
                let key = RepoKey::new(owner, repo);
                Ok(self
                    .inodes
                    .get_or_alloc_virtual(parent, name_str, InodeData::RefRepo(key))?)
            }
            InodeData::RefRepo(key) => {
                // Child is an encoded ref selector.
                let raw = decode_ref(name.as_bytes()).ok_or(libc::ENOENT)?;
                let commit = self.resolve_selector(&key, &raw)?;
                let root_tree = self
                    .store
                    .root_tree(&key, parse_oid(&commit)?)
                    .map_err(|e| store_err_errno(&e))?;
                Ok(self.inodes.get_or_alloc_virtual(
                    parent,
                    name_str,
                    InodeData::Repo {
                        key,
                        selector: Some(raw),
                        commit,
                        root_tree: root_tree.to_string(),
                    },
                )?)
            }
            // ---- commit-pinned path descent ----
            InodeData::Repo {
                key,
                commit,
                root_tree,
                ..
            } => {
                let tree_oid = parse_oid(&root_tree)?;
                self.lookup_path_child(parent, &key, &commit, tree_oid, &[], name)
            }
            InodeData::Path {
                repo,
                commit,
                path,
                oid,
                kind,
                ..
            } => {
                if kind != EntryKind::Tree {
                    return Err(libc::ENOTDIR);
                }
                let tree_oid = parse_oid(&oid)?;
                self.lookup_path_child(parent, &repo, &commit, tree_oid, &path, name)
            }
            // Root, ByRefRoot, RefOwner are handled by explicit arms above;
            // nothing else should reach here.
            _ => Err(libc::ENOENT),
        }
    }

    /// Look up a named child of a directory identified by its tree OID.
    /// `prefix` is the parent path (repo-relative) of the directory.
    fn lookup_path_child(
        &self,
        parent: u64,
        repo: &RepoKey,
        commit: &str,
        tree_oid: git2::Oid,
        prefix: &[u8],
        name: &OsStr,
    ) -> Result<u64, i32> {
        let entry = self
            .store
            .tree_entry(repo, tree_oid, name.as_bytes())
            .map_err(|e| store_err_errno(&e))?
            .ok_or(libc::ENOENT)?;
        let child_path = join_path(prefix, name.as_bytes());
        let key = PathKey {
            repo: repo.clone(),
            commit: commit.to_string(),
            path: child_path,
        };
        Ok(self
            .inodes
            .get_or_alloc_path(key, entry.oid.to_string(), entry.kind, parent))
    }

    fn stat_inode(&self, ino: u64) -> Result<NodeAttr, i32> {
        let data = self.inodes.get(ino).ok_or(libc::ENOENT)?;
        match data {
            InodeData::Root
            | InodeData::ByRefRoot
            | InodeData::Owner(_)
            | InodeData::RefOwner(_)
            | InodeData::RefRepo(_) => Ok(self.virtual_dir_attr(ino)),
            InodeData::Repo { .. } => Ok(self.virtual_dir_attr(ino)),
            InodeData::Path {
                kind, oid, repo, ..
            } => {
                if kind == EntryKind::Tree {
                    return Ok(self.virtual_dir_attr(ino));
                }
                if kind == EntryKind::Gitlink {
                    return Ok(self.file_attr(ino, kind, 0));
                }
                // File or symlink: hydrate to learn the size (one-time,
                // content-addressed and cached thereafter).
                let blob_oid = parse_oid(&oid)?;
                let (_path, size) = self
                    .store
                    .hydrate_blob(&repo, blob_oid)
                    .map_err(|e| store_err_errno(&e))?;
                Ok(self.file_attr(ino, kind, size))
            }
        }
    }

    fn list_children(&self, ino: u64) -> Result<Vec<DirEntryInfo>, i32> {
        let data = self.inodes.get(ino).ok_or(libc::ENOENT)?;
        match data {
            InodeData::Root => {
                let mut out = vec![DirEntryInfo {
                    ino: BY_REF_INO,
                    kind: FsKind::Directory,
                    name: OsString::from(BY_REF_ROOT),
                }];
                for owner in self.list_cached_owners() {
                    let owner_ino = self.inodes.get_or_alloc_virtual(
                        ino,
                        &owner,
                        InodeData::Owner(owner.parse::<Owner>().unwrap()),
                    )?;
                    out.push(DirEntryInfo {
                        ino: owner_ino,
                        kind: FsKind::Directory,
                        name: OsString::from(owner),
                    });
                }
                Ok(out)
            }
            InodeData::ByRefRoot => {
                let mut out = Vec::new();
                for owner in self.list_cached_owners() {
                    let owner_ino = self.inodes.get_or_alloc_virtual(
                        ino,
                        &owner,
                        InodeData::RefOwner(owner.parse::<Owner>().unwrap()),
                    )?;
                    out.push(DirEntryInfo {
                        ino: owner_ino,
                        kind: FsKind::Directory,
                        name: OsString::from(owner),
                    });
                }
                Ok(out)
            }
            InodeData::Owner(owner) => {
                let mut out = Vec::new();
                for repo in self.list_cached_repos(owner.as_str()) {
                    let name = repo.clone();
                    let key = RepoKey::new(owner.clone(), name.parse::<Repo>().unwrap());
                    // Don't clone on a mere listing of cached repos; only show
                    // repos whose mirror already exists (list_cached_repos
                    // scans the mirror dir, so this holds).
                    let ino = self
                        .inodes
                        .get_or_alloc_virtual(ino, &repo, InodeData::RefRepo(key))
                        .map_err(|_| libc::EIO)?; // shouldn't run out of vnodes for small lists
                    out.push(DirEntryInfo {
                        ino,
                        kind: FsKind::Directory,
                        name: OsString::from(repo),
                    });
                }
                Ok(out)
            }
            InodeData::RefOwner(owner) => {
                let mut out = Vec::new();
                for repo in self.list_cached_repos(owner.as_str()) {
                    let key = RepoKey::new(owner.clone(), repo.parse::<Repo>().unwrap());
                    let ino =
                        self.inodes
                            .get_or_alloc_virtual(ino, &repo, InodeData::RefRepo(key))?;
                    out.push(DirEntryInfo {
                        ino,
                        kind: FsKind::Directory,
                        name: OsString::from(repo),
                    });
                }
                Ok(out)
            }
            InodeData::RefRepo(key) => {
                // Listing refs requires the mirror to exist.
                self.materialize_head(&key)?;
                let mut refs = self
                    .store
                    .list_branches(&key)
                    .map_err(|e| store_err_errno(&e))?;
                refs.extend(
                    self.store
                        .list_tags(&key)
                        .map_err(|e| store_err_errno(&e))?,
                );
                refs.sort();
                refs.dedup();
                let mut out = Vec::new();
                for raw in refs {
                    let enc = encode_ref(&raw);
                    let commit = match self.store.resolve_revision(&key, &raw) {
                        Ok(c) => c.to_string(),
                        Err(e) => {
                            log::warn!("resolve {key} {raw} for listing: {e}");
                            continue;
                        }
                    };
                    let root_tree = match self.store.root_tree(&key, parse_oid(&commit)?) {
                        Ok(t) => t.to_string(),
                        Err(e) => {
                            log::warn!("root_tree {key} {commit}: {e}");
                            continue;
                        }
                    };
                    let ino = self.inodes.get_or_alloc_virtual(
                        ino,
                        &enc,
                        InodeData::Repo {
                            key: key.clone(),
                            selector: Some(raw),
                            commit,
                            root_tree,
                        },
                    )?;
                    out.push(DirEntryInfo {
                        ino,
                        kind: FsKind::Directory,
                        name: OsString::from(enc),
                    });
                }
                Ok(out)
            }
            InodeData::Repo {
                key,
                commit,
                root_tree,
                ..
            } => {
                let tree_oid = parse_oid(&root_tree)?;
                self.list_tree_children(ino, &key, &commit, tree_oid, &[])
            }
            InodeData::Path {
                repo,
                commit,
                path,
                oid,
                kind,
                ..
            } => {
                if kind != EntryKind::Tree {
                    return Err(libc::ENOTDIR);
                }
                let tree_oid = parse_oid(&oid)?;
                self.list_tree_children(ino, &repo, &commit, tree_oid, &path)
            }
        }
    }

    fn list_tree_children(
        &self,
        parent_ino: u64,
        repo: &RepoKey,
        commit: &str,
        tree_oid: git2::Oid,
        prefix: &[u8],
    ) -> Result<Vec<DirEntryInfo>, i32> {
        let entries = self
            .store
            .tree_entries(repo, tree_oid)
            .map_err(|e| store_err_errno(&e))?;
        let mut out = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            let child_path = join_path(prefix, &entry.name);
            let key = PathKey {
                repo: repo.clone(),
                commit: commit.to_string(),
                path: child_path,
            };
            let ino =
                self.inodes
                    .get_or_alloc_path(key, entry.oid.to_string(), entry.kind, parent_ino);
            out.push(DirEntryInfo {
                ino,
                kind: entry_kind_to_fs(entry.kind),
                name: OsString::from_vec(entry.name.clone()),
            });
        }
        out.sort_by(|a, b| {
            a.name
                .as_os_str()
                .as_bytes()
                .cmp(b.name.as_os_str().as_bytes())
        });
        Ok(out)
    }

    fn parent_inode(&self, ino: u64) -> u64 {
        if ino == ROOT_INO {
            return ROOT_INO;
        }
        if ino == BY_REF_INO {
            return ROOT_INO;
        }
        match self.inodes.get(ino) {
            Some(InodeData::Owner(_)) => ROOT_INO,
            Some(InodeData::RefOwner(_)) => BY_REF_INO,
            Some(InodeData::RefRepo(_)) => {
                // parent is the RefOwner; we don't store it on the node. Walk
                // back via the virtual_children map is overkill; return
                // BY_REF_INO as a safe ancestor (nfs readdir ".." only).
                BY_REF_INO
            }
            Some(InodeData::Repo { .. }) => {
                // Top-level repo -> Owner; by-ref selector -> RefRepo. Without
                // storing parent, approximate: return ROOT_INO. FUSE readdir
                // uses parent_inode for ".."; a wrong-but-valid ancestor is
                // tolerable. We store parent on Path nodes precisely.
                ROOT_INO
            }
            Some(InodeData::Path { parent, .. }) => parent,
            _ => ROOT_INO,
        }
    }

    fn readlink_bytes(&self, ino: u64) -> Result<Vec<u8>, i32> {
        let data = self.inodes.get(ino).ok_or(libc::ENOENT)?;
        match data {
            InodeData::Path {
                kind, oid, repo, ..
            } if kind == EntryKind::Symlink => {
                let blob_oid = parse_oid(&oid)?;
                let (path, _size) = self
                    .store
                    .hydrate_blob(&repo, blob_oid)
                    .map_err(|e| store_err_errno(&e))?;
                Ok(std::fs::read(&path).map_err(|e| io_errno(e, libc::EIO))?)
            }
            _ => Err(libc::EINVAL),
        }
    }

    /// Hydrate (if needed) and open a file's cached blob for offset reads.
    fn open_blob(&self, ino: u64) -> Result<File, i32> {
        let data = self.inodes.get(ino).ok_or(libc::ENOENT)?;
        match data {
            InodeData::Path {
                kind, oid, repo, ..
            } if kind == EntryKind::Blob
                || kind == EntryKind::Executable
                || kind == EntryKind::Symlink =>
            {
                let blob_oid = parse_oid(&oid)?;
                let (path, _size) = self
                    .store
                    .hydrate_blob(&repo, blob_oid)
                    .map_err(|e| store_err_errno(&e))?;
                File::open(&path).map_err(|e| io_errno(e, libc::EIO))
            }
            InodeData::Path {
                kind: EntryKind::Gitlink,
                ..
            } => {
                // Submodule gitlink: serve as an empty file.
                File::open("/dev/null").map_err(|e| io_errno(e, libc::EIO))
            }
            _ => Err(libc::EISDIR),
        }
    }

    #[cfg(target_os = "macos")]
    fn read_file_range(&self, ino: u64, offset: u64, size: u32) -> Result<(Vec<u8>, bool), i32> {
        if InodeTable::is_virtual_ino(ino) {
            return Err(libc::EISDIR);
        }
        let mut file = self.open_blob(ino)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| io_errno(e, libc::EIO))?;
        let mut buf = vec![0u8; size as usize];
        let n = file.read(&mut buf).map_err(|e| io_errno(e, libc::EIO))?;
        buf.truncate(n);
        Ok((buf, n < size as usize))
    }

    #[cfg(target_os = "linux")]
    fn ttl_for_inode(&self, ino: u64) -> Duration {
        match self.inodes.get(ino) {
            Some(InodeData::Repo { selector, .. }) => {
                // Commit-pinned content is immutable.
                if selector
                    .as_ref()
                    .is_some_and(|s| Self::is_commit_oid_selector(s))
                    || selector.is_none()
                {
                    // default HEAD alias is mutable → short; named ref short;
                    // commit-OID selector long.
                    if selector
                        .as_ref()
                        .is_some_and(|s| Self::is_commit_oid_selector(s))
                    {
                        COMMIT_TTL
                    } else {
                        REF_TTL
                    }
                } else {
                    REF_TTL
                }
            }
            Some(InodeData::Path { .. }) => COMMIT_TTL,
            _ => VIRTUAL_TTL,
        }
    }

    /// TTL for a lookup reply, based on what was resolved.
    #[cfg(target_os = "linux")]
    fn lookup_ttl(&self, parent: u64, name: &str) -> Duration {
        // Default-branch repo node (under Owner): short.
        // Ref selector under RefRepo: short unless it's a commit OID.
        // Everything else discovery: virtual.
        let parent_data = self.inodes.get(parent);
        match parent_data.as_ref().map(|d| d) {
            Some(InodeData::Owner(_)) => REF_TTL,
            Some(InodeData::RefRepo(_)) => {
                if Self::is_commit_oid_selector(name) {
                    COMMIT_TTL
                } else {
                    REF_TTL
                }
            }
            _ => VIRTUAL_TTL,
        }
    }
}

fn join_path(prefix: &[u8], name: &[u8]) -> Vec<u8> {
    if prefix.is_empty() {
        return name.to_vec();
    }
    let mut out = Vec::with_capacity(prefix.len() + 1 + name.len());
    out.extend_from_slice(prefix);
    out.push(b'/');
    out.extend_from_slice(name);
    out
}

fn parse_oid(s: &str) -> Result<git2::Oid, i32> {
    git2::Oid::from_str(s).map_err(|_| libc::EIO)
}

#[cfg(target_os = "linux")]
impl GhFs {
    /// Mount the filesystem at the given path.
    pub fn mount(self, mountpoint: &Path, _shutdown: Arc<AtomicBool>) -> std::io::Result<()> {
        let options = vec![MountOption::FSName("ghfs".to_string()), MountOption::RO];
        fuser::mount2(self, mountpoint, &options)?;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
impl GhFs {
    /// Mount with smfs-core's NFS listener and RAII mount lifecycle.
    pub fn mount(self, mountpoint: &Path, shutdown: Arc<AtomicBool>) -> std::io::Result<()> {
        std::fs::create_dir_all(mountpoint)?;
        let opts = nfs::MountOpts::new(mountpoint.to_path_buf());
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| std::io::Error::other(format!("failed to start tokio runtime: {e}")))?;

        runtime.block_on(async move {
            let handle = nfs::mount_nfs(self, opts).await?;
            while !shutdown.load(Ordering::SeqCst) && nfs::is_mount_active(handle.mountpoint()) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            drop(handle);
            Ok(())
        })
    }
}

#[cfg(target_os = "linux")]
impl Filesystem for GhFs {
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.stat_inode(ino) {
            Ok(attr) => reply.attr(&self.ttl_for_inode(ino), &attr.to_fuse_attr()),
            Err(err) => reply.error(err),
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.lookup_inode(parent, name) {
            Ok(ino) => match self.stat_inode(ino) {
                Ok(attr) => {
                    let ttl = self.lookup_ttl(parent, name.to_str().unwrap_or(""));
                    reply.entry(&ttl, &attr.to_fuse_attr(), 0)
                }
                Err(err) => reply.error(err),
            },
            Err(err) => reply.error(err),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let parent = self.parent_inode(ino);
        let mut entries = vec![
            DirEntryInfo {
                ino,
                kind: FsKind::Directory,
                name: OsString::from("."),
            },
            DirEntryInfo {
                ino: parent,
                kind: FsKind::Directory,
                name: OsString::from(".."),
            },
        ];
        match self.list_children(ino) {
            Ok(mut children) => entries.append(&mut children),
            Err(err) => {
                reply.error(err);
                return;
            }
        }
        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(
                entry.ino,
                (i + 1) as i64,
                kind_to_fuse(entry.kind),
                entry.name,
            ) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            reply.error(libc::EROFS);
            return;
        }
        if InodeTable::is_virtual_ino(ino) {
            reply.error(libc::EISDIR);
            return;
        }
        match self.open_blob(ino) {
            Ok(file) => {
                let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
                match self.open_files.lock() {
                    Ok(mut files) => {
                        files.insert(fh, file);
                        reply.opened(fh, 0);
                    }
                    Err(_) => reply.error(libc::EIO),
                }
            }
            Err(err) => reply.error(err),
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let mut files = match self.open_files.lock() {
            Ok(files) => files,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };
        let file = match files.get_mut(&fh) {
            Some(file) => file,
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };
        if let Err(err) = file.seek(SeekFrom::Start(offset as u64)) {
            reply.error(io_errno(err, libc::EIO));
            return;
        }
        let mut buf = vec![0u8; size as usize];
        match file.read(&mut buf) {
            Ok(n) => reply.data(&buf[..n]),
            Err(err) => reply.error(io_errno(err, libc::EIO)),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        if let Ok(mut files) = self.open_files.lock() {
            files.remove(&fh);
        }
        reply.ok();
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        match self.readlink_bytes(ino) {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(err),
        }
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 4096, 255, 4096);
    }

    fn getxattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: ReplyXattr,
    ) {
        if self.inodes.get(ino).is_none() {
            reply.error(libc::ENOENT);
            return;
        }
        let Some(name_str) = name.to_str() else {
            reply.error(libc::ENODATA);
            return;
        };
        if name_str != FINDER_INFO_XATTR {
            reply.error(libc::ENODATA);
            return;
        }
        let data = [0u8; FINDER_INFO_SIZE];
        if size == 0 {
            reply.size(data.len() as u32);
            return;
        }
        if size < data.len() as u32 {
            reply.error(libc::ERANGE);
            return;
        }
        reply.data(&data);
    }

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: fuser::ReplyXattr) {
        if self.inodes.get(ino).is_none() {
            reply.error(libc::ENOENT);
            return;
        }
        if size == 0 {
            reply.size(FINDER_INFO_XATTR_LIST.len() as u32);
            return;
        }
        if size < FINDER_INFO_XATTR_LIST.len() as u32 {
            reply.error(libc::ERANGE);
            return;
        }
        reply.data(FINDER_INFO_XATTR_LIST);
    }

    fn setxattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _name: &OsStr,
        _value: &[u8],
        _flags: i32,
        _position: u32,
        reply: ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }

    fn removexattr(&mut self, _req: &Request<'_>, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        reply.error(libc::EROFS);
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        reply.error(libc::EROFS);
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }

    fn unlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }

    fn rmdir(&mut self, _req: &Request<'_>, _parent: u64, _name: &OsStr, reply: fuser::ReplyEmpty) {
        reply.error(libc::EROFS);
    }

    fn symlink(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _link_name: &OsStr,
        _target: &Path,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _newparent: u64,
        _newname: &OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }

    fn link(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _newparent: u64,
        _newname: &OsStr,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _offset: i64,
        _data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        reply.error(libc::EROFS);
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        reply.error(libc::EROFS);
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        _parent: u64,
        _name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        reply.error(libc::EROFS);
    }
}
