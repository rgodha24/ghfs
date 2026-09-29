//! Linux frontend: FUSE via `fuser`. The FUSE session thread only dispatches;
//! every request runs as a tokio task that owns its reply, so a slow fetch in
//! one directory never blocks other requests.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, Request,
};
use tokio::runtime::Handle;

use crate::vfs::{Attr, FileKind, Vfs};

/// Keep file contents in the page cache across opens; blobs are immutable.
const FOPEN_KEEP_CACHE: u32 = 1 << 1;

pub struct FuseFs {
    vfs: Arc<Vfs>,
    rt: Handle,
}

impl FuseFs {
    pub fn new(vfs: Arc<Vfs>, rt: Handle) -> Self {
        Self { vfs, rt }
    }

    /// Mount in a background session; unmounts when the returned session drops.
    pub fn mount(self, mountpoint: &Path) -> std::io::Result<fuser::BackgroundSession> {
        let options = [
            MountOption::FSName("ghfs".to_string()),
            MountOption::Subtype("ghfs".to_string()),
            MountOption::RO,
            MountOption::NoDev,
            MountOption::NoSuid,
            MountOption::DefaultPermissions,
        ];
        fuser::spawn_mount2(self, mountpoint, &options)
    }
}

fn kind(kind: FileKind) -> FileType {
    match kind {
        FileKind::Directory => FileType::Directory,
        FileKind::File => FileType::RegularFile,
        FileKind::Symlink => FileType::Symlink,
    }
}

fn to_fuse(attr: &Attr) -> FileAttr {
    FileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.size.div_ceil(512),
        atime: attr.mtime,
        mtime: attr.mtime,
        ctime: attr.mtime,
        crtime: UNIX_EPOCH,
        kind: kind(attr.kind),
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

impl Filesystem for FuseFs {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), libc::c_int> {
        let _ = config.set_max_readahead(1 << 20);
        let _ = config.set_max_background(64);
        let _ = config.set_congestion_threshold(48);
        Ok(())
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let vfs = self.vfs.clone();
        let name = name.as_bytes().to_vec();
        self.rt.spawn(async move {
            let result = async {
                let ino = vfs.lookup(parent, &name).await?;
                vfs.getattr(ino).await
            }
            .await;
            match result {
                Ok(attr) => {
                    let ttl = vfs.volatility(attr.ino).ttl();
                    reply.entry(&ttl, &to_fuse(&attr), 0)
                }
                // Cache misses briefly so probe storms stay local.
                Err(libc::ENOENT) => reply.entry(&Duration::from_secs(5), &negative_attr(), 0),
                Err(e) => reply.error(e),
            }
        });
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let vfs = self.vfs.clone();
        self.rt.spawn(async move {
            match vfs.getattr(ino).await {
                Ok(attr) => reply.attr(&vfs.volatility(ino).ttl(), &to_fuse(&attr)),
                Err(e) => reply.error(e),
            }
        });
    }

    fn readlink(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyData) {
        let vfs = self.vfs.clone();
        self.rt.spawn(async move {
            match vfs.readlink(ino).await {
                Ok(target) => reply.data(&target),
                Err(e) => reply.error(e),
            }
        });
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            reply.error(libc::EROFS);
            return;
        }
        if !self.vfs.exists(ino) {
            reply.error(libc::ENOENT);
            return;
        }
        reply.opened(0, FOPEN_KEEP_CACHE);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
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
        let vfs = self.vfs.clone();
        self.rt.spawn(async move {
            match vfs.read(ino, offset as u64, size).await {
                Ok((data, _eof)) => reply.data(&data),
                Err(e) => reply.error(e),
            }
        });
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        reply.ok();
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
        let vfs = self.vfs.clone();
        self.rt.spawn(async move {
            let children = match vfs.readdir(ino).await {
                Ok(children) => children,
                Err(e) => {
                    reply.error(e);
                    return;
                }
            };
            let dots = [
                (ino, FileType::Directory, b".".as_slice()),
                (vfs.parent(ino), FileType::Directory, b"..".as_slice()),
            ];
            let entries = dots.into_iter().chain(
                children
                    .iter()
                    .map(|c| (c.ino, kind(c.kind), c.name.as_slice())),
            );
            for (i, (child, file_type, name)) in entries.enumerate().skip(offset as usize) {
                if reply.add(child, (i + 1) as i64, file_type, OsStr::from_bytes(name)) {
                    break;
                }
            }
            reply.ok();
        });
    }

    fn statfs(&mut self, _req: &Request<'_>, _ino: u64, reply: ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 4096, 255, 4096);
    }
}

/// FUSE negative entry: inode 0 with a TTL caches "doesn't exist".
fn negative_attr() -> FileAttr {
    FileAttr {
        ino: 0,
        size: 0,
        blocks: 0,
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind: FileType::RegularFile,
        perm: 0,
        nlink: 0,
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: 0,
        flags: 0,
    }
}
