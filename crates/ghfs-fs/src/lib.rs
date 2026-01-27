//! GHFS FUSE filesystem implementation.

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyDirectory, ReplyEntry, Request,
};
use ghfs_cache::{CachePaths, RepoCache};
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

mod inode;

pub use inode::InodeTable;

const TTL: Duration = Duration::from_secs(1);

/// Root inode is always 1 in FUSE
const ROOT_INO: u64 = 1;
/// Owner inode (octocat)
const OWNER_INO: u64 = 2;
/// Repo inode (Hello-World)
const REPO_INO: u64 = 3;

/// The GHFS filesystem.
pub struct GhFs {
    #[allow(dead_code)] // Will be used when materializing repos
    cache: Arc<RepoCache>,
    /// For MVP: hardcoded repo that gets materialized on first access
    #[allow(dead_code)] // Will be used when materializing repos
    repo_generation: Mutex<Option<PathBuf>>,
    /// Cached UID for file attributes
    uid: u32,
    /// Cached GID for file attributes
    gid: u32,
}

impl GhFs {
    pub fn new(cache: Arc<RepoCache>) -> Self {
        // Cache uid/gid at startup to avoid repeated unsafe calls
        let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
        Self {
            cache,
            repo_generation: Mutex::new(None),
            uid,
            gid,
        }
    }

    pub fn with_default_cache() -> Self {
        let paths = CachePaths::default();
        let cache = Arc::new(RepoCache::new(paths));
        Self::new(cache)
    }

    /// Ensure the hardcoded repo is materialized and return its generation path.
    #[allow(dead_code)] // Will be used when materializing repos
    fn ensure_repo(&self) -> Option<PathBuf> {
        let mut cached = match self.repo_generation.lock() {
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
                Ok(g) => *cached = Some(g.path),
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

impl Filesystem for GhFs {
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match ino {
            ROOT_INO | OWNER_INO | REPO_INO => reply.attr(&TTL, &self.dir_attr(ino)),
            _ => reply.error(libc::ENOENT),
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

        // Repo directory (inode 3 = Hello-World) - empty for now, will show actual files later
        if ino == REPO_INO {
            let entries: Vec<(u64, FileType, &str)> = vec![
                (REPO_INO, FileType::Directory, "."),
                (OWNER_INO, FileType::Directory, ".."),
            ];

            for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
                if reply.add(ino, (i + 1) as i64, kind, name) {
                    break;
                }
            }
            reply.ok();
            return;
        }

        reply.error(libc::ENOENT);
    }
}
