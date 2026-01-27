//! GHFS FUSE filesystem implementation.

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use ghfs_cache::{CachePaths, RepoCache};
use ghfs_types::{GenerationId, RepoKey};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

mod inode;

pub use inode::{
    InodeInfo, InodeTable, UnderlyingKey, PASSTHROUGH_INO_START, ROOT_INO, VIRTUAL_INO_END,
    VIRTUAL_INO_START,
};

const TTL: Duration = Duration::from_secs(1);

/// Virtual node types for dynamic owner/repo hierarchy
#[derive(Debug, Clone)]
enum VirtualNode {
    /// Root directory (inode 1)
    Root,
    /// Owner directory (e.g., "octocat")
    Owner(String),
    /// Repository directory
    Repo {
        owner: String,
        repo: String,
        parent: u64,
    },
}

/// Create an UnderlyingKey from filesystem metadata and generation ID.
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

/// The GHFS filesystem.
pub struct GhFs {
    cache: Arc<RepoCache>,
    /// Inode table for managing passthrough inodes
    inodes: InodeTable,
    /// Cached UID for file attributes
    uid: u32,
    /// Cached GID for file attributes
    gid: u32,
    /// Open file handles
    open_files: Mutex<HashMap<u64, File>>,
    /// Next file handle to assign
    next_fh: AtomicU64,
    /// Virtual nodes indexed by inode
    virtual_nodes: DashMap<u64, VirtualNode>,
    /// Map from (parent_ino, name) to child inode for virtual nodes
    virtual_names: DashMap<(u64, String), u64>,
    /// Next virtual inode to allocate (starts at 2, since 1 is root)
    next_virtual_ino: AtomicU64,
    /// Cache of materialized repo paths: (owner, repo) -> (path, generation_id)
    repo_generations: DashMap<(String, String), (PathBuf, GenerationId)>,
}

impl GhFs {
    pub fn new(cache: Arc<RepoCache>) -> Self {
        // Cache uid/gid at startup to avoid repeated unsafe calls
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };

        let virtual_nodes = DashMap::new();
        // Insert root node
        virtual_nodes.insert(ROOT_INO, VirtualNode::Root);

        Self {
            cache,
            inodes: InodeTable::new(),
            uid,
            gid,
            open_files: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            virtual_nodes,
            virtual_names: DashMap::new(),
            next_virtual_ino: AtomicU64::new(VIRTUAL_INO_START), // Start at 2
            repo_generations: DashMap::new(),
        }
    }

    pub fn with_default_cache() -> Self {
        let paths = CachePaths::default();
        let cache = Arc::new(RepoCache::new(paths));
        Self::new(cache)
    }

    /// Allocate a new virtual inode
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

    /// Get or create an owner virtual node
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

    /// Get or create a repo virtual node under an owner
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

    /// Ensure a repo is materialized and return its generation path and ID.
    /// Called when traversing INTO a repo (lookup child or readdir).
    fn ensure_repo_materialized(&self, owner: &str, repo: &str) -> Option<(PathBuf, GenerationId)> {
        let cache_key = (owner.to_string(), repo.to_string());

        // Fast path: check if already cached
        if let Some(entry) = self.repo_generations.get(&cache_key) {
            return Some(entry.clone());
        }

        // Slow path: materialize the repo
        let key_str = format!("{}/{}", owner, repo);
        let key: RepoKey = match key_str.parse() {
            Ok(k) => k,
            Err(_) => return None,
        };
        match self.cache.ensure_current(&key) {
            Ok(g) => {
                let result = (g.path.clone(), g.generation);
                self.repo_generations.insert(cache_key, result.clone());
                Some(result)
            }
            Err(e) => {
                log::error!("Failed to materialize repo {}/{}: {}", owner, repo, e);
                None
            }
        }
    }

    /// List cached owners by scanning the worktrees directory
    fn list_cached_owners(&self) -> Vec<String> {
        let worktrees_dir = self.cache_paths().worktrees_dir();
        let mut owners = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&worktrees_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = entry.file_name().to_str() {
                        owners.push(name.to_string());
                    }
                }
            }
        }

        owners
    }

    /// List cached repos for an owner by scanning their worktrees directory
    fn list_cached_repos(&self, owner: &str) -> Vec<String> {
        let owner_dir = self.cache_paths().worktrees_dir().join(owner);
        let mut repos = Vec::new();

        if let Ok(entries) = std::fs::read_dir(&owner_dir) {
            for entry in entries.flatten() {
                if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                    if let Some(name) = entry.file_name().to_str() {
                        repos.push(name.to_string());
                    }
                }
            }
        }

        repos
    }

    /// Get cache paths for directory scanning
    fn cache_paths(&self) -> CachePaths {
        // We need to get the paths from the cache
        // Since RepoCache doesn't expose paths, we'll use default for now
        // In a real implementation, we'd want RepoCache to expose its paths
        CachePaths::default()
    }

    /// Check if a name is a valid GitHub owner/repo name
    fn is_valid_github_name(name: &str) -> bool {
        !name.is_empty()
            && !name.starts_with('.')
            && !name.starts_with('-')
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    }

    /// Mount the filesystem at the given path.
    pub fn mount(self, mountpoint: &std::path::Path) -> std::io::Result<()> {
        let mut options = vec![
            MountOption::RO, // Read-only
            MountOption::FSName("ghfs".to_string()),
            MountOption::AutoUnmount, // Unmount when process exits
        ];

        // AllowOther requires user_allow_other in /etc/fuse.conf, try without if it fails
        if std::path::Path::new("/etc/fuse.conf").exists() {
            if let Ok(content) = std::fs::read_to_string("/etc/fuse.conf") {
                if content.lines().any(|l| l.trim() == "user_allow_other") {
                    options.push(MountOption::AllowOther);
                }
            }
        }

        fuser::mount2(self, mountpoint, &options)?;
        Ok(())
    }
}

impl Default for GhFs {
    fn default() -> Self {
        Self::with_default_cache()
    }
}

impl GhFs {
    /// Create directory attributes with cached uid/gid
    fn dir_attr(&self, ino: u64) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }
}

/// Convert std::fs::Metadata to fuser::FileAttr for passthrough inodes.
fn metadata_to_attr(ino: u64, metadata: &std::fs::Metadata) -> FileAttr {
    use std::os::unix::fs::MetadataExt;

    let kind = if metadata.is_dir() {
        FileType::Directory
    } else if metadata.is_symlink() {
        FileType::Symlink
    } else {
        FileType::RegularFile
    };

    FileAttr {
        ino,
        size: metadata.len(),
        blocks: metadata.blocks(),
        atime: UNIX_EPOCH + Duration::from_secs(metadata.atime() as u64),
        mtime: UNIX_EPOCH + Duration::from_secs(metadata.mtime() as u64),
        ctime: UNIX_EPOCH + Duration::from_secs(metadata.ctime() as u64),
        crtime: UNIX_EPOCH,
        kind,
        perm: (metadata.mode() & 0o7777) as u16,
        nlink: metadata.nlink() as u32,
        uid: metadata.uid(),
        gid: metadata.gid(),
        rdev: metadata.rdev() as u32,
        blksize: metadata.blksize() as u32,
        flags: 0,
    }
}

impl Filesystem for GhFs {
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        // Virtual inodes - check if it exists in our virtual_nodes map
        if InodeTable::is_virtual(ino) {
            if self.virtual_nodes.contains_key(&ino) {
                reply.attr(&TTL, &self.dir_attr(ino));
            } else {
                reply.error(libc::ENOENT);
            }
            return;
        }

        // Passthrough inode - get info from table
        let info = match self.inodes.get(ino) {
            Some(info) => info,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Stat the underlying file
        match std::fs::symlink_metadata(&info.path) {
            Ok(metadata) => {
                let attr = metadata_to_attr(ino, &metadata);
                reply.attr(&TTL, &attr);
            }
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Root directory lookups - dynamically create owner nodes
        if parent == ROOT_INO {
            if !Self::is_valid_github_name(name_str) {
                reply.error(libc::ENOENT);
                return;
            }

            // Create or get owner node
            let owner_ino = match self.get_or_create_owner(name_str) {
                Ok(ino) => ino,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };
            reply.entry(&TTL, &self.dir_attr(owner_ino), 0);
            return;
        }

        // Check if parent is a virtual node
        let parent_node = match self.virtual_nodes.get(&parent) {
            Some(node) => node.clone(),
            None => {
                // Not a virtual node, check passthrough
                if !InodeTable::is_virtual(parent) {
                    // Passthrough lookup
                    let parent_info = match self.inodes.get(parent) {
                        Some(info) => info,
                        None => {
                            reply.error(libc::ENOENT);
                            return;
                        }
                    };

                    let child_path = parent_info.path.join(name);
                    match std::fs::symlink_metadata(&child_path) {
                        Ok(metadata) => {
                            let key =
                                underlying_key_from_metadata(&metadata, parent_info.key.generation);
                            let (ino, _) = self.inodes.get_or_insert(child_path, key, parent);
                            let attr = metadata_to_attr(ino, &metadata);
                            reply.entry(&TTL, &attr, 0);
                        }
                        Err(_) => reply.error(libc::ENOENT),
                    }
                    return;
                }
                reply.error(libc::ENOENT);
                return;
            }
        };

        match parent_node {
            VirtualNode::Owner(owner) => {
                // Looking up a repo under an owner
                if !Self::is_valid_github_name(name_str) {
                    reply.error(libc::ENOENT);
                    return;
                }

                // Create or get repo node
                let repo_ino = match self.get_or_create_repo(parent, &owner, name_str) {
                    Ok(ino) => ino,
                    Err(e) => {
                        reply.error(e);
                        return;
                    }
                };
                reply.entry(&TTL, &self.dir_attr(repo_ino), 0);
            }
            VirtualNode::Repo { owner, repo, .. } => {
                // Looking up inside a repo - this triggers materialization
                let (gen_path, gen_id) = match self.ensure_repo_materialized(&owner, &repo) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::EIO);
                        return;
                    }
                };

                let child_path = gen_path.join(name);
                match std::fs::symlink_metadata(&child_path) {
                    Ok(metadata) => {
                        let key = underlying_key_from_metadata(&metadata, gen_id);
                        let (ino, _) = self.inodes.get_or_insert(child_path, key, parent);
                        let attr = metadata_to_attr(ino, &metadata);
                        reply.entry(&TTL, &attr, 0);
                    }
                    Err(_) => reply.error(libc::ENOENT),
                }
            }
            VirtualNode::Root => {
                // Should have been handled above
                reply.error(libc::ENOENT);
            }
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
        if ino == ROOT_INO {
            // List cached owners
            let cached_owners = self.list_cached_owners();

            let mut entries: Vec<(u64, FileType, String)> = vec![
                (ROOT_INO, FileType::Directory, ".".to_string()),
                (ROOT_INO, FileType::Directory, "..".to_string()),
            ];

            for owner in cached_owners {
                if let Ok(owner_ino) = self.get_or_create_owner(&owner) {
                    entries.push((owner_ino, FileType::Directory, owner));
                }
            }

            for (i, (entry_ino, kind, name)) in
                entries.into_iter().enumerate().skip(offset as usize)
            {
                if reply.add(entry_ino, (i + 1) as i64, kind, &name) {
                    break;
                }
            }
            reply.ok();
            return;
        }

        // Check if it's a virtual node
        let node = match self.virtual_nodes.get(&ino) {
            Some(n) => n.clone(),
            None => {
                // Not a virtual node, check passthrough
                if !InodeTable::is_virtual(ino) {
                    // Passthrough readdir
                    let info = match self.inodes.get(ino) {
                        Some(info) => info,
                        None => {
                            reply.error(libc::ENOENT);
                            return;
                        }
                    };

                    let read_dir = match std::fs::read_dir(&info.path) {
                        Ok(rd) => rd,
                        Err(_) => {
                            reply.error(libc::EIO);
                            return;
                        }
                    };

                    let parent_ino = ino;
                    let mut entries: Vec<(u64, FileType, String)> = vec![
                        (ino, FileType::Directory, ".".to_string()),
                        (parent_ino, FileType::Directory, "..".to_string()),
                    ];

                    for entry in read_dir.flatten() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        let child_path = info.path.join(&name);
                        if let Ok(metadata) = std::fs::symlink_metadata(&child_path) {
                            let key = underlying_key_from_metadata(&metadata, info.key.generation);
                            let (child_ino, _) = self.inodes.get_or_insert(child_path, key, ino);
                            let file_type = if metadata.is_dir() {
                                FileType::Directory
                            } else if metadata.is_symlink() {
                                FileType::Symlink
                            } else {
                                FileType::RegularFile
                            };
                            entries.push((child_ino, file_type, name));
                        }
                    }

                    for (i, (entry_ino, kind, name)) in
                        entries.into_iter().enumerate().skip(offset as usize)
                    {
                        if reply.add(entry_ino, (i + 1) as i64, kind, &name) {
                            break;
                        }
                    }
                    reply.ok();
                    return;
                }
                reply.error(libc::ENOENT);
                return;
            }
        };

        match node {
            VirtualNode::Root => {
                // Should have been handled above
                reply.error(libc::ENOENT);
            }
            VirtualNode::Owner(owner) => {
                // List cached repos for this owner
                let cached_repos = self.list_cached_repos(&owner);

                let mut entries: Vec<(u64, FileType, String)> = vec![
                    (ino, FileType::Directory, ".".to_string()),
                    (ROOT_INO, FileType::Directory, "..".to_string()),
                ];

                for repo in cached_repos {
                    if let Ok(repo_ino) = self.get_or_create_repo(ino, &owner, &repo) {
                        entries.push((repo_ino, FileType::Directory, repo));
                    }
                }

                for (i, (entry_ino, kind, name)) in
                    entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(entry_ino, (i + 1) as i64, kind, &name) {
                        break;
                    }
                }
                reply.ok();
            }
            VirtualNode::Repo {
                owner,
                repo,
                parent: repo_parent,
            } => {
                // Reading repo directory - triggers materialization
                let (gen_path, gen_id) = match self.ensure_repo_materialized(&owner, &repo) {
                    Some(p) => p,
                    None => {
                        reply.error(libc::EIO);
                        return;
                    }
                };

                let read_dir = match std::fs::read_dir(&gen_path) {
                    Ok(rd) => rd,
                    Err(_) => {
                        reply.error(libc::EIO);
                        return;
                    }
                };

                let mut entries: Vec<(u64, FileType, String)> = vec![
                    (ino, FileType::Directory, ".".to_string()),
                    (repo_parent, FileType::Directory, "..".to_string()),
                ];

                for entry in read_dir.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    let child_path = gen_path.join(&name);
                    if let Ok(metadata) = std::fs::symlink_metadata(&child_path) {
                        let key = underlying_key_from_metadata(&metadata, gen_id);
                        let (child_ino, _) = self.inodes.get_or_insert(child_path, key, ino);
                        let file_type = if metadata.is_dir() {
                            FileType::Directory
                        } else if metadata.is_symlink() {
                            FileType::Symlink
                        } else {
                            FileType::RegularFile
                        };
                        entries.push((child_ino, file_type, name));
                    }
                }

                for (i, (entry_ino, kind, name)) in
                    entries.into_iter().enumerate().skip(offset as usize)
                {
                    if reply.add(entry_ino, (i + 1) as i64, kind, &name) {
                        break;
                    }
                }
                reply.ok();
            }
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        // Only allow read-only opens
        let access_mode = flags & libc::O_ACCMODE;
        if access_mode != libc::O_RDONLY {
            reply.error(libc::EROFS);
            return;
        }

        // Virtual inodes can't be opened as files
        if InodeTable::is_virtual(ino) {
            reply.error(libc::EISDIR);
            return;
        }

        // Get the underlying path
        let info = match self.inodes.get(ino) {
            Some(info) => info,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Open the underlying file
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
            Err(e) => reply.error(e.raw_os_error().unwrap_or(libc::EIO)),
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
        let mut files = match self.open_files.lock() {
            Ok(f) => f,
            Err(_) => {
                reply.error(libc::EIO);
                return;
            }
        };
        let file = match files.get_mut(&fh) {
            Some(f) => f,
            None => {
                reply.error(libc::EBADF);
                return;
            }
        };

        // Seek to offset
        if file.seek(SeekFrom::Start(offset as u64)).is_err() {
            reply.error(libc::EIO);
            return;
        }

        // Read data
        let mut buf = vec![0u8; size as usize];
        match file.read(&mut buf) {
            Ok(n) => reply.data(&buf[..n]),
            Err(_) => reply.error(libc::EIO),
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
        // Virtual inodes aren't symlinks
        if InodeTable::is_virtual(ino) {
            reply.error(libc::EINVAL);
            return;
        }

        // Get the underlying path
        let info = match self.inodes.get(ino) {
            Some(info) => info,
            None => {
                reply.error(libc::ENOENT);
                return;
            }
        };

        // Read the symlink target
        match std::fs::read_link(&info.path) {
            Ok(target) => {
                // Return the target as bytes
                reply.data(target.as_os_str().as_encoded_bytes());
            }
            Err(e) => reply.error(e.raw_os_error().unwrap_or(libc::EIO)),
        }
    }
}
