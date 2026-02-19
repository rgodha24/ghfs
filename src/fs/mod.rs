//! GHFS filesystem backends.
//!
//! Linux uses FUSE (`fuser`) and macOS uses an in-process NFSv3 server (`nfsserve`).

use crate::cache::CachePaths;
use crate::daemon::WorkerHandle;
use crate::types::{GenerationId, Owner, Repo, RepoKey};
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
#[cfg(target_os = "linux")]
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::ffi::{OsStrExt, OsStringExt};

#[cfg(target_os = "linux")]
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, ReplyXattr, Request,
};

#[cfg(target_os = "macos")]
use async_trait::async_trait;
#[cfg(target_os = "macos")]
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
};
#[cfg(target_os = "macos")]
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};

mod inode;

pub use inode::{
    InodeInfo, InodeTable, PASSTHROUGH_INO_START, ROOT_INO, UnderlyingKey, VIRTUAL_INO_END,
    VIRTUAL_INO_START,
};

/// TTL for virtual root and owner directories.
#[cfg(target_os = "linux")]
const VIRTUAL_TTL: Duration = Duration::from_secs(60);
/// TTL for repo boundary so generation changes surface quickly.
#[cfg(target_os = "linux")]
const REPO_TTL: Duration = Duration::from_secs(5);
/// TTL for immutable generation contents.
#[cfg(target_os = "linux")]
const FILE_TTL: Duration = Duration::from_secs(3600);

#[cfg(target_os = "linux")]
const FINDER_INFO_XATTR: &str = "com.apple.FinderInfo";
#[cfg(target_os = "linux")]
const FINDER_INFO_XATTR_LIST: &[u8] = b"com.apple.FinderInfo\0";
#[cfg(target_os = "linux")]
const FINDER_INFO_SIZE: usize = 32;

/// Virtual node types for dynamic owner/repo hierarchy.
#[derive(Debug, Clone)]
#[cfg_attr(target_os = "macos", allow(dead_code))]
enum VirtualNode {
    Root,
    Owner(String),
    Repo {
        owner: String,
        repo: String,
        parent: u64,
    },
}

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

fn metadata_kind(metadata: &std::fs::Metadata) -> FsKind {
    if metadata.is_dir() {
        FsKind::Directory
    } else if metadata.is_symlink() {
        FsKind::Symlink
    } else {
        FsKind::RegularFile
    }
}

fn metadata_to_attr(ino: u64, metadata: &std::fs::Metadata) -> NodeAttr {
    use std::os::unix::fs::MetadataExt;

    NodeAttr {
        ino,
        size: metadata.len(),
        blocks: metadata.blocks(),
        atime: UNIX_EPOCH + Duration::from_secs(metadata.atime().max(0) as u64),
        mtime: UNIX_EPOCH + Duration::from_secs(metadata.mtime().max(0) as u64),
        ctime: UNIX_EPOCH + Duration::from_secs(metadata.ctime().max(0) as u64),
        kind: metadata_kind(metadata),
        perm: (metadata.mode() & 0o7777) as u16,
        nlink: metadata.nlink() as u32,
        uid: metadata.uid(),
        gid: metadata.gid(),
        rdev: metadata.rdev() as u32,
        blksize: metadata.blksize() as u32,
    }
}

fn underlying_key_from_metadata(
    metadata: &std::fs::Metadata,
    generation: GenerationId,
) -> UnderlyingKey {
    use std::os::unix::fs::MetadataExt;
    UnderlyingKey {
        dev: metadata.dev(),
        ino: metadata.ino(),
        generation,
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

#[cfg(target_os = "macos")]
fn system_time_to_nfstime(time: SystemTime) -> nfstime3 {
    match time.duration_since(UNIX_EPOCH) {
        Ok(dur) => nfstime3 {
            seconds: dur.as_secs().min(u32::MAX as u64) as u32,
            nseconds: dur.subsec_nanos(),
        },
        Err(_) => nfstime3 {
            seconds: 0,
            nseconds: 0,
        },
    }
}

#[cfg(target_os = "macos")]
fn kind_to_nfs(kind: FsKind) -> ftype3 {
    match kind {
        FsKind::Directory => ftype3::NF3DIR,
        FsKind::RegularFile => ftype3::NF3REG,
        FsKind::Symlink => ftype3::NF3LNK,
    }
}

#[cfg(target_os = "macos")]
impl NodeAttr {
    fn to_nfs_attr(&self) -> fattr3 {
        fattr3 {
            ftype: kind_to_nfs(self.kind),
            mode: self.perm as u32,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            size: self.size,
            used: self.size,
            rdev: specdata3 {
                specdata1: self.rdev,
                specdata2: 0,
            },
            fsid: 1,
            fileid: self.ino,
            atime: system_time_to_nfstime(self.atime),
            mtime: system_time_to_nfstime(self.mtime),
            ctime: system_time_to_nfstime(self.ctime),
        }
    }
}

/// The GHFS filesystem.
#[cfg_attr(target_os = "macos", allow(dead_code))]
pub struct GhFs {
    worker: Arc<WorkerHandle>,
    cache_paths: CachePaths,
    inodes: InodeTable,
    uid: u32,
    gid: u32,
    #[cfg(target_os = "linux")]
    open_files: Mutex<HashMap<u64, File>>,
    #[cfg(target_os = "linux")]
    next_fh: AtomicU64,
    virtual_nodes: DashMap<u64, VirtualNode>,
    virtual_names: DashMap<(u64, String), u64>,
    next_virtual_ino: AtomicU64,
}

impl GhFs {
    /// Create a new filesystem instance with worker and cache paths.
    pub fn new(worker: Arc<WorkerHandle>, cache_paths: CachePaths) -> Self {
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };

        let virtual_nodes = DashMap::new();
        virtual_nodes.insert(ROOT_INO, VirtualNode::Root);

        Self {
            worker,
            cache_paths,
            inodes: InodeTable::new(),
            uid,
            gid,
            #[cfg(target_os = "linux")]
            open_files: Mutex::new(HashMap::new()),
            #[cfg(target_os = "linux")]
            next_fh: AtomicU64::new(1),
            virtual_nodes,
            virtual_names: DashMap::new(),
            next_virtual_ino: AtomicU64::new(VIRTUAL_INO_START),
        }
    }

    fn alloc_virtual_ino(&self) -> Result<u64, i32> {
        self.next_virtual_ino
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                if current >= PASSTHROUGH_INO_START {
                    None
                } else {
                    Some(current + 1)
                }
            })
            .map_err(|_| libc::ENOSPC)
    }

    fn get_or_create_owner(&self, owner: &str) -> Result<u64, i32> {
        let key = (ROOT_INO, owner.to_string());
        match self.virtual_names.entry(key) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let ino = self.alloc_virtual_ino()?;
                self.virtual_nodes
                    .insert(ino, VirtualNode::Owner(owner.to_string()));
                entry.insert(ino);
                Ok(ino)
            }
        }
    }

    fn get_or_create_repo(&self, parent_ino: u64, owner: &str, repo: &str) -> Result<u64, i32> {
        let key = (parent_ino, repo.to_string());
        match self.virtual_names.entry(key) {
            Entry::Occupied(entry) => Ok(*entry.get()),
            Entry::Vacant(entry) => {
                let ino = self.alloc_virtual_ino()?;
                self.virtual_nodes.insert(
                    ino,
                    VirtualNode::Repo {
                        owner: owner.to_string(),
                        repo: repo.to_string(),
                        parent: parent_ino,
                    },
                );
                entry.insert(ino);
                Ok(ino)
            }
        }
    }

    fn ensure_repo_materialized(&self, owner: &str, repo: &str) -> Option<(PathBuf, GenerationId)> {
        let key_str = format!("{owner}/{repo}");
        let key: RepoKey = key_str.parse().ok()?;

        let current_link = self.cache_paths.current_symlink(&key);
        if current_link.exists() {
            if let Ok(target) = std::fs::read_link(&current_link) {
                let target = if target.is_absolute() {
                    target
                } else {
                    current_link.parent().unwrap().join(&target)
                };

                if target.exists() {
                    if let Some(name) = target.file_name().and_then(|s| s.to_str()) {
                        if let Some(num_str) = name.strip_prefix("gen-") {
                            if let Ok(num) = num_str.parse::<u64>() {
                                self.worker.refresh(key);
                                return Some((target, GenerationId::new(num)));
                            }
                        }
                    }
                }
            }
        }

        match self.worker.materialize(key) {
            Ok(gen_ref) => Some((gen_ref.path, gen_ref.generation)),
            Err(err) => {
                log::error!("Failed to materialize repo {owner}/{repo}: {err}");
                None
            }
        }
    }

    fn list_cached_owners(&self) -> Vec<String> {
        let worktrees_dir = self.cache_paths.worktrees_dir();
        let mut owners = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&worktrees_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = entry.file_name().to_str() {
                        if Self::is_valid_owner(name) {
                            owners.push(name.to_string());
                        }
                    }
                }
            }
        }

        owners.sort();
        owners
    }

    fn list_cached_repos(&self, owner: &str) -> Vec<String> {
        let owner_dir = self.cache_paths.worktrees_dir().join(owner);
        let mut repos = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&owner_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = entry.file_name().to_str() {
                        if Self::is_valid_repo(name) {
                            repos.push(name.to_string());
                        }
                    }
                }
            }
        }

        repos.sort();
        repos
    }

    fn is_valid_owner(name: &str) -> bool {
        name.parse::<Owner>().is_ok()
    }

    fn is_valid_repo(name: &str) -> bool {
        name.parse::<Repo>().is_ok()
    }

    #[cfg(target_os = "linux")]
    fn inode_exists(&self, ino: u64) -> bool {
        if InodeTable::is_virtual(ino) {
            self.virtual_nodes.contains_key(&ino)
        } else {
            self.inodes.get(ino).is_some()
        }
    }

    #[cfg(target_os = "linux")]
    fn ttl_for_inode(&self, ino: u64) -> Duration {
        if InodeTable::is_virtual(ino) {
            if let Some(node) = self.virtual_nodes.get(&ino) {
                match node.value() {
                    VirtualNode::Root | VirtualNode::Owner(_) => VIRTUAL_TTL,
                    VirtualNode::Repo { .. } => REPO_TTL,
                }
            } else {
                VIRTUAL_TTL
            }
        } else {
            FILE_TTL
        }
    }

    fn virtual_dir_attr(&self, ino: u64) -> NodeAttr {
        NodeAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            kind: FsKind::Directory,
            perm: 0o755,
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 512,
        }
    }

    fn stat_inode(&self, ino: u64) -> Result<NodeAttr, i32> {
        if InodeTable::is_virtual(ino) {
            if self.virtual_nodes.contains_key(&ino) {
                Ok(self.virtual_dir_attr(ino))
            } else {
                Err(libc::ENOENT)
            }
        } else {
            let info = self.inodes.get(ino).ok_or(libc::ENOENT)?;
            let metadata =
                std::fs::symlink_metadata(&info.path).map_err(|e| io_errno(e, libc::EIO))?;
            Ok(metadata_to_attr(ino, &metadata))
        }
    }

    fn lookup_path_child(
        &self,
        parent: u64,
        base_path: &Path,
        generation: GenerationId,
        name: &OsStr,
    ) -> Result<u64, i32> {
        let child_path = base_path.join(name);
        let metadata =
            std::fs::symlink_metadata(&child_path).map_err(|e| io_errno(e, libc::ENOENT))?;
        let key = underlying_key_from_metadata(&metadata, generation);
        let (ino, _) = self.inodes.get_or_insert(child_path, key, parent);
        Ok(ino)
    }

    fn lookup_inode(&self, parent: u64, name: &OsStr) -> Result<u64, i32> {
        let name_str = name.to_str().ok_or(libc::ENOENT)?;

        if parent == ROOT_INO {
            if !Self::is_valid_owner(name_str) {
                return Err(libc::ENOENT);
            }
            return self.get_or_create_owner(name_str);
        }

        if let Some(parent_node) = self.virtual_nodes.get(&parent).map(|n| n.clone()) {
            return match parent_node {
                VirtualNode::Owner(owner) => {
                    if !Self::is_valid_repo(name_str) {
                        Err(libc::ENOENT)
                    } else {
                        self.get_or_create_repo(parent, &owner, name_str)
                    }
                }
                VirtualNode::Repo { owner, repo, .. } => {
                    let (gen_path, gen_id) = self
                        .ensure_repo_materialized(&owner, &repo)
                        .ok_or(libc::EIO)?;
                    self.lookup_path_child(parent, &gen_path, gen_id, name)
                }
                VirtualNode::Root => Err(libc::ENOENT),
            };
        }

        if InodeTable::is_virtual(parent) {
            return Err(libc::ENOENT);
        }

        let parent_info = self.inodes.get(parent).ok_or(libc::ENOENT)?;
        self.lookup_path_child(parent, &parent_info.path, parent_info.key.generation, name)
    }

    fn list_real_children(
        &self,
        parent_ino: u64,
        dir_path: &Path,
        generation: GenerationId,
    ) -> Result<Vec<DirEntryInfo>, i32> {
        let read_dir = std::fs::read_dir(dir_path).map_err(|e| io_errno(e, libc::EIO))?;
        let mut entries = Vec::new();

        for entry in read_dir.flatten() {
            let name = entry.file_name();
            let child_path = dir_path.join(&name);
            let Ok(metadata) = std::fs::symlink_metadata(&child_path) else {
                continue;
            };

            let key = underlying_key_from_metadata(&metadata, generation);
            let (child_ino, _) = self.inodes.get_or_insert(child_path, key, parent_ino);
            entries.push(DirEntryInfo {
                ino: child_ino,
                kind: metadata_kind(&metadata),
                name,
            });
        }

        entries.sort_by(|a, b| {
            a.name
                .as_os_str()
                .as_bytes()
                .cmp(b.name.as_os_str().as_bytes())
        });
        Ok(entries)
    }

    fn list_children(&self, ino: u64) -> Result<Vec<DirEntryInfo>, i32> {
        if ino == ROOT_INO {
            let mut out = Vec::new();
            for owner in self.list_cached_owners() {
                if let Ok(owner_ino) = self.get_or_create_owner(&owner) {
                    out.push(DirEntryInfo {
                        ino: owner_ino,
                        kind: FsKind::Directory,
                        name: OsString::from(owner),
                    });
                }
            }
            return Ok(out);
        }

        if let Some(node) = self.virtual_nodes.get(&ino).map(|n| n.clone()) {
            return match node {
                VirtualNode::Root => Ok(Vec::new()),
                VirtualNode::Owner(owner) => {
                    let mut out = Vec::new();
                    for repo in self.list_cached_repos(&owner) {
                        if let Ok(repo_ino) = self.get_or_create_repo(ino, &owner, &repo) {
                            out.push(DirEntryInfo {
                                ino: repo_ino,
                                kind: FsKind::Directory,
                                name: OsString::from(repo),
                            });
                        }
                    }
                    Ok(out)
                }
                VirtualNode::Repo { owner, repo, .. } => {
                    let (gen_path, gen_id) = self
                        .ensure_repo_materialized(&owner, &repo)
                        .ok_or(libc::EIO)?;
                    self.list_real_children(ino, &gen_path, gen_id)
                }
            };
        }

        if InodeTable::is_virtual(ino) {
            return Err(libc::ENOENT);
        }

        let info = self.inodes.get(ino).ok_or(libc::ENOENT)?;
        let metadata = std::fs::symlink_metadata(&info.path).map_err(|e| io_errno(e, libc::EIO))?;
        if !metadata.is_dir() {
            return Err(libc::ENOTDIR);
        }

        self.list_real_children(ino, &info.path, info.key.generation)
    }

    #[cfg(target_os = "linux")]
    fn parent_inode(&self, ino: u64) -> u64 {
        if ino == ROOT_INO {
            return ROOT_INO;
        }

        if let Some(node) = self.virtual_nodes.get(&ino) {
            return match node.value() {
                VirtualNode::Root => ROOT_INO,
                VirtualNode::Owner(_) => ROOT_INO,
                VirtualNode::Repo { parent, .. } => *parent,
            };
        }

        self.inodes.get(ino).map(|i| i.parent).unwrap_or(ROOT_INO)
    }

    fn readlink_bytes(&self, ino: u64) -> Result<Vec<u8>, i32> {
        if InodeTable::is_virtual(ino) {
            return Err(libc::EINVAL);
        }

        let info = self.inodes.get(ino).ok_or(libc::ENOENT)?;
        let target = std::fs::read_link(&info.path).map_err(|e| io_errno(e, libc::EIO))?;
        Ok(target.into_os_string().into_vec())
    }

    #[cfg(target_os = "macos")]
    fn read_file_range(&self, ino: u64, offset: u64, size: u32) -> Result<(Vec<u8>, bool), i32> {
        if InodeTable::is_virtual(ino) {
            return Err(libc::EISDIR);
        }

        let info = self.inodes.get(ino).ok_or(libc::ENOENT)?;
        let mut file = File::open(&info.path).map_err(|e| io_errno(e, libc::EIO))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| io_errno(e, libc::EIO))?;

        let mut buf = vec![0u8; size as usize];
        let n = file.read(&mut buf).map_err(|e| io_errno(e, libc::EIO))?;
        buf.truncate(n);
        Ok((buf, n < size as usize))
    }
}

#[cfg(target_os = "linux")]
impl GhFs {
    /// Mount the filesystem at the given path.
    pub fn mount(self, mountpoint: &Path) -> std::io::Result<()> {
        let options = vec![MountOption::FSName("ghfs".to_string()), MountOption::RO];

        fuser::mount2(self, mountpoint, &options)?;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
impl GhFs {
    /// Mount the filesystem at the given path using an in-process NFSv3 server.
    pub fn mount(self, mountpoint: &Path) -> std::io::Result<()> {
        if !mountpoint.exists() {
            std::fs::create_dir_all(mountpoint)?;
        }

        let mountpoint = mountpoint.to_path_buf();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| std::io::Error::other(format!("failed to start tokio runtime: {e}")))?;

        runtime.block_on(self.mount_nfs_foreground(mountpoint))
    }

    async fn mount_nfs_foreground(self, mountpoint: PathBuf) -> std::io::Result<()> {
        use nfsserve::tcp::{NFSTcp, NFSTcpListener};

        let mut listener = NFSTcpListener::bind("127.0.0.1:0", self).await?;
        let port = listener.get_listen_port();
        let ip = listener.get_listen_ip();
        let (mount_tx, mut mount_rx) = tokio::sync::mpsc::channel::<bool>(8);
        listener.set_mount_listener(mount_tx);

        let server_task = tokio::spawn(async move {
            let _ = listener.handle_forever().await;
        });

        let source = format!("{ip}:/");
        let options = format!(
            "rdonly,nolocks,vers=3,tcp,rsize=131072,actimeo=120,port={port},mountport={port}"
        );
        let target = mountpoint.to_string_lossy().to_string();

        let status = std::process::Command::new("mount_nfs")
            .args(["-o", &options, &source, &target])
            .status()?;

        if !status.success() {
            server_task.abort();
            let _ = server_task.await;
            return Err(std::io::Error::other(format!(
                "mount_nfs failed with status: {status}"
            )));
        }

        loop {
            tokio::select! {
                signal = mount_rx.recv() => {
                    match signal {
                        Some(false) | None => break,
                        Some(true) => {}
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(2)) => {
                    if !is_mount_active(&mountpoint) {
                        break;
                    }
                }
            }
        }

        server_task.abort();
        let _ = server_task.await;
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn is_mount_active(mountpoint: &Path) -> bool {
    let Ok(output) = std::process::Command::new("mount").output() else {
        return true;
    };

    if !output.status.success() {
        return true;
    }

    let needle = mountpoint.to_string_lossy();
    let mounts = String::from_utf8_lossy(&output.stdout);
    mounts.lines().any(|line| line.contains(needle.as_ref()))
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
                Ok(attr) => reply.entry(&self.ttl_for_inode(ino), &attr.to_fuse_attr(), 0),
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
        let access_mode = flags & libc::O_ACCMODE;
        if access_mode != libc::O_RDONLY {
            reply.error(libc::EROFS);
            return;
        }

        if InodeTable::is_virtual(ino) {
            reply.error(libc::EISDIR);
            return;
        }

        let info = match self.inodes.get(ino) {
            Some(info) => info,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        match File::open(&info.path) {
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
            Err(err) => reply.error(io_errno(err, libc::EIO)),
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
        if !self.inode_exists(ino) {
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

    fn listxattr(&mut self, _req: &Request<'_>, ino: u64, size: u32, reply: ReplyXattr) {
        if !self.inode_exists(ino) {
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

#[cfg(target_os = "macos")]
fn errno_to_nfs(err: i32) -> nfsstat3 {
    match err {
        libc::ENOENT => nfsstat3::NFS3ERR_NOENT,
        libc::EACCES => nfsstat3::NFS3ERR_ACCES,
        libc::EPERM => nfsstat3::NFS3ERR_PERM,
        libc::ENOTDIR => nfsstat3::NFS3ERR_NOTDIR,
        libc::EISDIR => nfsstat3::NFS3ERR_ISDIR,
        libc::EINVAL => nfsstat3::NFS3ERR_INVAL,
        libc::ENOSPC => nfsstat3::NFS3ERR_NOSPC,
        libc::EROFS => nfsstat3::NFS3ERR_ROFS,
        libc::EEXIST => nfsstat3::NFS3ERR_EXIST,
        libc::ENAMETOOLONG => nfsstat3::NFS3ERR_NAMETOOLONG,
        libc::ENOTEMPTY => nfsstat3::NFS3ERR_NOTEMPTY,
        _ => nfsstat3::NFS3ERR_IO,
    }
}

#[cfg(target_os = "macos")]
#[async_trait]
impl NFSFileSystem for GhFs {
    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadOnly
    }

    fn root_dir(&self) -> fileid3 {
        ROOT_INO
    }

    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let name = OsStr::from_bytes(&filename.0);
        self.lookup_inode(dirid, name).map_err(errno_to_nfs)
    }

    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        self.stat_inode(id)
            .map(|attr| attr.to_nfs_attr())
            .map_err(errno_to_nfs)
    }

    async fn setattr(&self, _id: fileid3, _setattr: sattr3) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        self.read_file_range(id, offset, count)
            .map_err(errno_to_nfs)
    }

    async fn write(&self, _id: fileid3, _offset: u64, _data: &[u8]) -> Result<fattr3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
        _attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn create_exclusive(
        &self,
        _dirid: fileid3,
        _filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn mkdir(
        &self,
        _dirid: fileid3,
        _dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn remove(&self, _dirid: fileid3, _filename: &filename3) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn rename(
        &self,
        _from_dirid: fileid3,
        _from_filename: &filename3,
        _to_dirid: fileid3,
        _to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let entries = self.list_children(dirid).map_err(errno_to_nfs)?;
        let start_index = if start_after == 0 {
            0
        } else {
            entries
                .iter()
                .position(|entry| entry.ino == start_after)
                .map(|idx| idx + 1)
                .unwrap_or(0)
        };

        let mut out = Vec::new();
        for entry in entries.iter().skip(start_index).take(max_entries) {
            let attr = self
                .stat_inode(entry.ino)
                .map(|a| a.to_nfs_attr())
                .map_err(errno_to_nfs)?;
            out.push(DirEntry {
                fileid: entry.ino,
                name: entry.name.as_os_str().as_bytes().to_vec().into(),
                attr,
            });
        }

        let end = start_index + out.len() >= entries.len();
        Ok(ReadDirResult { entries: out, end })
    }

    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        Err(nfsstat3::NFS3ERR_ROFS)
    }

    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        self.readlink_bytes(id)
            .map(Into::into)
            .map_err(errno_to_nfs)
    }
}
