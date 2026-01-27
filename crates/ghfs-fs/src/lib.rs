//! GHFS FUSE filesystem implementation.

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory, ReplyEmpty,
    ReplyEntry, ReplyOpen, Request,
};
use ghfs_cache::{CachePaths, RepoCache};
use ghfs_types::GenerationId;
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

/// Owner inode (octocat)
const OWNER_INO: u64 = 2;
/// Repo inode (Hello-World)
const REPO_INO: u64 = 3;

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
    #[allow(dead_code)] // Will be used when materializing repos
    cache: Arc<RepoCache>,
    /// Inode table for managing passthrough inodes
    inodes: InodeTable,
    /// For MVP: current generation (root_path, generation_id)
    current_generation: Mutex<Option<(PathBuf, GenerationId)>>,
    /// Cached UID for file attributes
    uid: u32,
    /// Cached GID for file attributes
    gid: u32,
    /// Open file handles
    open_files: Mutex<HashMap<u64, File>>,
    /// Next file handle to assign
    next_fh: AtomicU64,
}

impl GhFs {
    pub fn new(cache: Arc<RepoCache>) -> Self {
        // Cache uid/gid at startup to avoid repeated unsafe calls
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        Self {
            cache,
            inodes: InodeTable::new(),
            current_generation: Mutex::new(None),
            uid,
            gid,
            open_files: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
        }
    }

    pub fn with_default_cache() -> Self {
        let paths = CachePaths::default();
        let cache = Arc::new(RepoCache::new(paths));
        Self::new(cache)
    }

    /// Ensure the hardcoded repo is materialized and return its generation path and ID.
    fn ensure_repo(&self) -> Option<(PathBuf, GenerationId)> {
        let mut cached = match self.current_generation.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                // Recover from poisoned mutex - another thread panicked while holding the lock
                log::warn!("Recovering from poisoned mutex in ensure_repo");
                poisoned.into_inner()
            }
        };
        if cached.is_none() {
            // Hardcoded for MVP
            let key: ghfs_types::RepoKey = match "octocat/Hello-World".parse() {
                Ok(k) => k,
                Err(_) => return None,
            };
            match self.cache.ensure_current(&key) {
                Ok(g) => *cached = Some((g.path, g.generation)),
                Err(e) => {
                    log::error!("Failed to materialize repo: {}", e);
                    return None;
                }
            }
        }
        cached.clone()
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
        // Virtual inodes
        if InodeTable::is_virtual(ino) {
            match ino {
                ROOT_INO | OWNER_INO | REPO_INO => reply.attr(&TTL, &self.dir_attr(ino)),
                _ => reply.error(libc::ENOENT),
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

        // Root directory lookups
        if parent == ROOT_INO {
            match name_str {
                "octocat" => {
                    // Hardcoded owner inode for MVP
                    reply.entry(&TTL, &self.dir_attr(OWNER_INO), 0);
                }
                _ => reply.error(libc::ENOENT),
            }
            return;
        }

        // Owner directory lookups (inode 2 = octocat)
        if parent == OWNER_INO {
            match name_str {
                "Hello-World" => {
                    // Hardcoded repo inode for MVP
                    reply.entry(&TTL, &self.dir_attr(REPO_INO), 0);
                }
                _ => reply.error(libc::ENOENT),
            }
            return;
        }

        // Handle REPO_INO - this is where passthrough starts
        if parent == REPO_INO {
            let (gen_path, gen_id) = match self.ensure_repo() {
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
                    let (ino, _) = self.inodes.get_or_insert(child_path, key);
                    let attr = metadata_to_attr(ino, &metadata);
                    reply.entry(&TTL, &attr, 0);
                }
                Err(_) => reply.error(libc::ENOENT),
            }
            return;
        }

        // Passthrough lookup for non-virtual inodes
        if !InodeTable::is_virtual(parent) {
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
                    let key = underlying_key_from_metadata(&metadata, parent_info.key.generation);
                    let (ino, _) = self.inodes.get_or_insert(child_path, key);
                    let attr = metadata_to_attr(ino, &metadata);
                    reply.entry(&TTL, &attr, 0);
                }
                Err(_) => reply.error(libc::ENOENT),
            }
            return;
        }

        reply.error(libc::ENOENT);
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
            // Hardcoded entries for MVP
            // Format: (inode, type, name)
            let entries: Vec<(u64, FileType, &str)> = vec![
                (ROOT_INO, FileType::Directory, "."),
                (ROOT_INO, FileType::Directory, ".."),
                // Placeholder owner - will be replaced with actual cached owners later
                (OWNER_INO, FileType::Directory, "octocat"),
            ];

            for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
                // buffer full means reply.add returns true
                if reply.add(ino, (i + 1) as i64, kind, name) {
                    break;
                }
            }
            reply.ok();
            return;
        }

        // Owner directory (inode 2 = octocat)
        if ino == OWNER_INO {
            let entries: Vec<(u64, FileType, &str)> = vec![
                (OWNER_INO, FileType::Directory, "."),
                (ROOT_INO, FileType::Directory, ".."),
                (REPO_INO, FileType::Directory, "Hello-World"),
            ];

            for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
                if reply.add(ino, (i + 1) as i64, kind, name) {
                    break;
                }
            }
            reply.ok();
            return;
        }

        // Repo directory (inode 3 = Hello-World) - passthrough to underlying repo
        if ino == REPO_INO {
            let (gen_path, gen_id) = match self.ensure_repo() {
                Some(p) => p,
                None => {
                    reply.error(libc::EIO);
                    return;
                }
            };

            // Read the underlying directory
            let read_dir = match std::fs::read_dir(&gen_path) {
                Ok(rd) => rd,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };

            // Collect entries: . and .. plus directory contents
            let mut entries: Vec<(u64, FileType, String)> = vec![
                (REPO_INO, FileType::Directory, ".".to_string()),
                (OWNER_INO, FileType::Directory, "..".to_string()),
            ];

            for entry in read_dir.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let child_path = gen_path.join(&name);
                if let Ok(metadata) = std::fs::symlink_metadata(&child_path) {
                    let key = underlying_key_from_metadata(&metadata, gen_id);
                    let (child_ino, _) = self.inodes.get_or_insert(child_path, key);
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

            for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
                if reply.add(ino, (i + 1) as i64, kind, &name) {
                    break;
                }
            }
            reply.ok();
            return;
        }

        // Passthrough readdir for non-virtual inodes
        if !InodeTable::is_virtual(ino) {
            let info = match self.inodes.get(ino) {
                Some(info) => info,
                None => {
                    reply.error(libc::ENOENT);
                    return;
                }
            };

            // Read the underlying directory
            let read_dir = match std::fs::read_dir(&info.path) {
                Ok(rd) => rd,
                Err(_) => {
                    reply.error(libc::EIO);
                    return;
                }
            };

            // Get parent inode - for simplicity, use the current ino for ".."
            // In a full implementation, we'd track parent relationships
            let parent_ino = ino; // Simplified: actual parent tracking would be better

            // Collect entries
            let mut entries: Vec<(u64, FileType, String)> = vec![
                (ino, FileType::Directory, ".".to_string()),
                (parent_ino, FileType::Directory, "..".to_string()),
            ];

            for entry in read_dir.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                let child_path = info.path.join(&name);
                if let Ok(metadata) = std::fs::symlink_metadata(&child_path) {
                    let key = underlying_key_from_metadata(&metadata, info.key.generation);
                    let (child_ino, _) = self.inodes.get_or_insert(child_path, key);
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
                self.open_files.lock().unwrap().insert(fh, file);
                reply.opened(fh, 0);
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
        let mut files = self.open_files.lock().unwrap();
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
        self.open_files.lock().unwrap().remove(&fh);
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
