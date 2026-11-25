//! Linux frontend: FUSE via `fuser`. The FUSE session thread only dispatches;
//! every request runs as a tokio task that owns its reply, so a slow fetch in
//! one directory never blocks other requests.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use dashmap::DashMap;

use fuser::{
    FileAttr, FileType, Filesystem, KernelConfig, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyStatfs, Request,
};
use tokio::runtime::Handle;

use crate::vfs::{Attr, FileKind, Vfs};

/// Keep file contents in the page cache across opens; blobs are immutable.
const FOPEN_KEEP_CACHE: u32 = 1 << 1;

/// One directory listing: `.`, `..`, then the entries.
type Listing = Arc<Vec<(u64, FileType, Vec<u8>)>>;

pub struct FuseFs {
    vfs: Arc<Vfs>,
    rt: Handle,
    /// Listing per open directory handle. The kernel reads a directory in
    /// pages of ~100 entries; building the listing once per `opendir` instead
    /// of per page keeps big directories linear and the pages consistent.
    listings: Arc<DashMap<u64, Listing>>,
    next_fh: u64,
}

impl FuseFs {
    pub fn new(vfs: Arc<Vfs>, rt: Handle) -> Self {
        Self {
            vfs,
            rt,
            listings: Arc::new(DashMap::new()),
            next_fh: 1,
        }
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
                // Counted before anything else can await, so the inode can't
                // be swept while its attributes are fetched.
                vfs.retain(ino);
                vfs.getattr(ino).await.inspect_err(|_| vfs.forget(ino, 1))
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

    fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
        self.vfs.forget(ino, nlookup);
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

    fn opendir(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if !self.vfs.exists(ino) {
            reply.error(libc::ENOENT);
            return;
        }
        let fh = self.next_fh;
        self.next_fh += 1;
        reply.opened(fh, 0);
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let vfs = self.vfs.clone();
        let listings = self.listings.clone();
        self.rt.spawn(async move {
            // Offset 0 starts a fresh pass (also after rewinddir).
            let cached = (offset > 0)
                .then(|| listings.get(&fh).map(|l| l.clone()))
                .flatten();
            let listing = match cached {
                Some(listing) => listing,
                None => {
                    let children = match vfs.readdir(ino).await {
                        Ok(children) => children,
                        Err(e) => {
                            reply.error(e);
                            return;
                        }
                    };
                    let mut listing = Vec::with_capacity(children.len() + 2);
                    listing.push((ino, FileType::Directory, b".".to_vec()));
                    listing.push((vfs.parent(ino).await, FileType::Directory, b"..".to_vec()));
                    listing.extend(children.into_iter().map(|c| (c.ino, kind(c.kind), c.name)));
                    let listing = Arc::new(listing);
                    listings.insert(fh, listing.clone());
                    listing
                }
            };
            for (i, (child, file_type, name)) in listing.iter().enumerate().skip(offset as usize) {
                if reply.add(*child, (i + 1) as i64, *file_type, OsStr::from_bytes(name)) {
                    break;
                }
            }
            reply.ok();
        });
    }

    fn releasedir(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        reply: ReplyEmpty,
    ) {
        self.listings.remove(&fh);
        reply.ok();
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
