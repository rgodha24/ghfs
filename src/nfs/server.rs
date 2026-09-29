//! NFSv3 + MOUNTv3 server over TCP (RFC 1813, RFC 5531), read-only.
//!
//! Every RPC runs as its own tokio task and replies are written as they
//! finish, so a slow request (a first-time fetch) never holds up the rest of
//! the mount. Kernel retransmissions of a request that's still running are
//! dropped instead of starting the work twice.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use super::xdr::{Reader, Writer, XdrError, XdrResult, opaque_size};
use crate::vfs::{Attr, FileKind, ROOT_INO, Vfs};

const NFS_PROGRAM: u32 = 100003;
const MOUNT_PROGRAM: u32 = 100005;

const MSG_CALL: u32 = 0;
const MSG_REPLY: u32 = 1;
const MSG_ACCEPTED: u32 = 0;
const MSG_DENIED: u32 = 1;
const RPC_MISMATCH: u32 = 0;

const SUCCESS: u32 = 0;
const PROG_UNAVAIL: u32 = 1;
const PROG_MISMATCH: u32 = 2;
const PROC_UNAVAIL: u32 = 3;
const GARBAGE_ARGS: u32 = 4;

const AUTH_NONE: u32 = 0;
const AUTH_UNIX: u32 = 1;
/// reject_stat for MSG_DENIED, and the auth_stat we send with it.
const AUTH_ERROR: u32 = 1;
const AUTH_TOOWEAK: u32 = 5;

/// Largest record we accept; NFS calls from a read-only client are tiny.
const MAX_RECORD: usize = 1 << 20;
/// Requests one connection may have in flight before we stop reading more.
const MAX_INFLIGHT_PER_CONN: usize = 128;
/// Largest READ we serve (advertised as rtmax).
const MAX_READ: u32 = 1 << 20;
const FH_LEN: usize = 16;
const FSID: u64 = 0x6768_6673; // "ghfs"

mod stat {
    pub const OK: u32 = 0;
    pub const PERM: u32 = 1;
    pub const NOENT: u32 = 2;
    pub const IO: u32 = 5;
    pub const ACCES: u32 = 13;
    pub const EXIST: u32 = 17;
    pub const NOTDIR: u32 = 20;
    pub const ISDIR: u32 = 21;
    pub const INVAL: u32 = 22;
    pub const ROFS: u32 = 30;
    pub const NAMETOOLONG: u32 = 63;
    pub const STALE: u32 = 70;
    pub const BADHANDLE: u32 = 10001;
    pub const BAD_COOKIE: u32 = 10003;
    pub const TOOSMALL: u32 = 10005;
}

mod access {
    pub const READ: u32 = 0x01;
    pub const LOOKUP: u32 = 0x02;
    pub const EXECUTE: u32 = 0x20;
}

fn errno_to_stat(errno: i32) -> u32 {
    match errno {
        libc::ENOENT => stat::NOENT,
        libc::EPERM => stat::PERM,
        libc::EACCES => stat::ACCES,
        libc::EEXIST => stat::EXIST,
        libc::ENOTDIR => stat::NOTDIR,
        libc::EISDIR => stat::ISDIR,
        libc::EINVAL => stat::INVAL,
        libc::EROFS => stat::ROFS,
        libc::ENAMETOOLONG => stat::NAMETOOLONG,
        _ => stat::IO,
    }
}

pub struct NfsServer {
    vfs: Arc<Vfs>,
    /// Embedded in file handles so handles from a previous daemon run come
    /// back as STALE instead of aliasing new inodes.
    boot: u64,
    /// Only serve NFS requests whose AUTH_UNIX credential is this uid (or
    /// root). The kernel stamps every request with the calling user's uid, so
    /// other local users going through the mount are refused. (A process
    /// talking to the port directly can forge the credential; source ports
    /// don't help either, since macOS lets anyone bind ports below 1024 and
    /// its NFS client doesn't use them by default.)
    owner_uid: Option<u32>,
}

impl NfsServer {
    /// A server that only answers the daemon's own user (and root).
    pub fn new(vfs: Arc<Vfs>) -> Arc<Self> {
        Self::build(vfs, Some(unsafe { libc::getuid() }))
    }

    /// A server that answers any credential. For tests.
    pub fn new_unrestricted(vfs: Arc<Vfs>) -> Arc<Self> {
        Self::build(vfs, None)
    }

    /// A server that only answers `uid` (and root).
    pub fn for_uid(vfs: Arc<Vfs>, uid: u32) -> Arc<Self> {
        Self::build(vfs, Some(uid))
    }

    fn build(vfs: Arc<Vfs>, owner_uid: Option<u32>) -> Arc<Self> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        Arc::new(Self {
            vfs,
            boot: nanos ^ ((std::process::id() as u64) << 32),
            owner_uid,
        })
    }

    /// Accept connections until the listener is dropped or the task aborted.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) {
        loop {
            match listener.accept().await {
                Ok((stream, peer)) => {
                    log::debug!("nfs: connection from {peer}");
                    let server = self.clone();
                    tokio::spawn(async move {
                        if let Err(e) = server.connection(stream).await {
                            log::debug!("nfs: connection from {peer} closed: {e}");
                        }
                    });
                }
                Err(e) => {
                    log::warn!("nfs: accept failed: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    }

    async fn connection(self: Arc<Self>, stream: TcpStream) -> std::io::Result<()> {
        let _ = stream.set_nodelay(true);
        let (mut rd, mut wr) = stream.into_split();
        let (tx, mut rx) = mpsc::channel::<Vec<u8>>(256);
        let writer = tokio::spawn(async move {
            while let Some(reply) = rx.recv().await {
                let header = 0x8000_0000u32 | reply.len() as u32;
                let mut out = Vec::with_capacity(4 + reply.len());
                out.extend_from_slice(&header.to_be_bytes());
                out.extend_from_slice(&reply);
                if wr.write_all(&out).await.is_err() {
                    break;
                }
            }
        });
        let inflight: Arc<Mutex<HashSet<u32>>> = Arc::default();
        let slots = Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT_PER_CONN));
        let result = loop {
            let record = match read_record(&mut rd).await {
                Ok(record) => record,
                Err(e) => break Err(e),
            };
            let Some(xid) = record
                .get(..4)
                .map(|b| u32::from_be_bytes(b.try_into().unwrap()))
            else {
                continue;
            };
            if !inflight.lock().expect("inflight poisoned").insert(xid) {
                log::debug!("nfs: dropping retransmission of xid {xid:#x}");
                continue;
            }
            let server = self.clone();
            let tx = tx.clone();
            let inflight = inflight.clone();
            // Backpressure: stop reading new requests while this many run.
            let slot = slots
                .clone()
                .acquire_owned()
                .await
                .expect("semaphore never closed");
            tokio::spawn(async move {
                let _slot = slot;
                if let Some(reply) = server.dispatch(&record).await {
                    let _ = tx.send(reply).await;
                }
                inflight.lock().expect("inflight poisoned").remove(&xid);
            });
        };
        drop(tx);
        let _ = writer.await;
        match result {
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(()),
            other => other,
        }
    }

    /// Handle one RPC call record, returning the reply record.
    pub async fn dispatch(&self, record: &[u8]) -> Option<Vec<u8>> {
        let mut r = Reader::new(record);
        let xid = r.u32().ok()?;
        let header = (|| -> XdrResult<(u32, u32, u32, u32, u32, Option<u32>)> {
            let msg_type = r.u32()?;
            let rpcvers = r.u32()?;
            let prog = r.u32()?;
            let vers = r.u32()?;
            let proc_ = r.u32()?;
            let flavor = r.u32()?;
            let cred = r.opaque(400)?;
            let uid = if flavor == AUTH_UNIX {
                auth_unix_uid(cred)
            } else {
                None
            };
            // verifier: flavor + opaque body
            r.u32()?;
            r.skip_opaque(400)?;
            Ok((msg_type, rpcvers, prog, vers, proc_, uid))
        })();
        let Ok((msg_type, rpcvers, prog, vers, proc_, uid)) = header else {
            return Some(accepted(xid, GARBAGE_ARGS).buf);
        };
        if msg_type != MSG_CALL {
            return None;
        }
        if rpcvers != 2 {
            let mut w = Writer::new();
            w.u32(xid)
                .u32(MSG_REPLY)
                .u32(MSG_DENIED)
                .u32(RPC_MISMATCH)
                .u32(2)
                .u32(2);
            return Some(w.buf);
        }
        // NULL (a ping) and MOUNT (which only hands out the root handle) are
        // fine for anyone; everything that reads data needs the owner.
        if let Some(owner) = self.owner_uid
            && prog == NFS_PROGRAM
            && proc_ != 0
            && !matches!(uid, Some(u) if u == owner || u == 0)
        {
            log::debug!("nfs: refusing proc {proc_} for uid {uid:?}");
            let mut w = Writer::new();
            w.u32(xid)
                .u32(MSG_REPLY)
                .u32(MSG_DENIED)
                .u32(AUTH_ERROR)
                .u32(AUTH_TOOWEAK);
            return Some(w.buf);
        }
        let reply = match (prog, vers) {
            (NFS_PROGRAM, 3) => self.nfs3(xid, proc_, &mut r).await,
            (MOUNT_PROGRAM, 3) => self.mount3(xid, proc_, &mut r),
            (NFS_PROGRAM, _) | (MOUNT_PROGRAM, _) => {
                let mut w = accepted(xid, PROG_MISMATCH);
                w.u32(3).u32(3);
                Ok(w)
            }
            _ => Ok(accepted(xid, PROG_UNAVAIL)),
        };
        Some(match reply {
            Ok(w) => w.buf,
            Err(XdrError) => accepted(xid, GARBAGE_ARGS).buf,
        })
    }

    fn fh(&self, ino: u64) -> [u8; FH_LEN] {
        let mut out = [0u8; FH_LEN];
        out[..8].copy_from_slice(&self.boot.to_be_bytes());
        out[8..].copy_from_slice(&ino.to_be_bytes());
        out
    }

    fn read_fh(&self, r: &mut Reader<'_>) -> XdrResult<Result<u64, u32>> {
        let raw = r.opaque(64)?;
        if raw.len() != FH_LEN {
            return Ok(Err(stat::BADHANDLE));
        }
        let boot = u64::from_be_bytes(raw[..8].try_into().unwrap());
        let ino = u64::from_be_bytes(raw[8..].try_into().unwrap());
        if boot != self.boot || !self.vfs.exists(ino) {
            return Ok(Err(stat::STALE));
        }
        Ok(Ok(ino))
    }

    fn mount3(&self, xid: u32, proc_: u32, r: &mut Reader<'_>) -> XdrResult<Writer> {
        match proc_ {
            0 => Ok(accepted(xid, SUCCESS)),
            // MNT: every path mounts the root.
            1 => {
                let path = r.opaque(1024)?;
                log::info!("nfs: mount request for {:?}", String::from_utf8_lossy(path));
                let mut w = accepted(xid, SUCCESS);
                w.u32(0)
                    .opaque(&self.fh(ROOT_INO))
                    .u32(2)
                    .u32(AUTH_UNIX)
                    .u32(AUTH_NONE);
                Ok(w)
            }
            // DUMP: no mount list.
            2 => {
                let mut w = accepted(xid, SUCCESS);
                w.bool(false);
                Ok(w)
            }
            // UMNT / UMNTALL
            3 | 4 => Ok(accepted(xid, SUCCESS)),
            // EXPORT: "/" to everyone.
            5 => {
                let mut w = accepted(xid, SUCCESS);
                w.bool(true).opaque(b"/").bool(false).bool(false);
                Ok(w)
            }
            _ => Ok(accepted(xid, PROC_UNAVAIL)),
        }
    }

    async fn nfs3(&self, xid: u32, proc_: u32, r: &mut Reader<'_>) -> XdrResult<Writer> {
        let mut w = accepted(xid, SUCCESS);
        match proc_ {
            0 => {}
            1 => self.getattr(r, &mut w).await?,
            3 => self.lookup(r, &mut w).await?,
            4 => self.access(r, &mut w).await?,
            5 => self.readlink(r, &mut w).await?,
            6 => self.read(r, &mut w).await?,
            16 => self.readdir(r, &mut w, false).await?,
            17 => self.readdir(r, &mut w, true).await?,
            18 => self.fsstat(r, &mut w)?,
            19 => self.fsinfo(r, &mut w)?,
            20 => self.pathconf(r, &mut w)?,
            // SETATTR, WRITE, CREATE, MKDIR, SYMLINK, MKNOD, REMOVE, RMDIR, COMMIT:
            // ROFS with empty wcc_data.
            2 | 7 | 8 | 9 | 10 | 11 | 12 | 13 | 21 => {
                w.u32(stat::ROFS);
                wcc_empty(&mut w);
            }
            // RENAME: two wcc_data
            14 => {
                w.u32(stat::ROFS);
                wcc_empty(&mut w);
                wcc_empty(&mut w);
            }
            // LINK: post_op_attr + wcc_data
            15 => {
                w.u32(stat::ROFS).bool(false);
                wcc_empty(&mut w);
            }
            _ => return Ok(accepted(xid, PROC_UNAVAIL)),
        }
        Ok(w)
    }

    async fn getattr(&self, r: &mut Reader<'_>, w: &mut Writer) -> XdrResult<()> {
        let ino = match self.read_fh(r)? {
            Ok(ino) => ino,
            Err(status) => {
                w.u32(status);
                return Ok(());
            }
        };
        match self.vfs.getattr(ino).await {
            Ok(attr) => {
                w.u32(stat::OK);
                write_fattr(w, &attr);
            }
            Err(e) => {
                w.u32(errno_to_stat(e));
            }
        }
        Ok(())
    }

    async fn local_attr(&self, ino: u64) -> Option<Attr> {
        self.vfs.getattr_local(ino).await.ok().flatten()
    }

    async fn lookup(&self, r: &mut Reader<'_>, w: &mut Writer) -> XdrResult<()> {
        let dir = self.read_fh(r)?;
        let name = r.opaque(1024)?;
        let dir = match dir {
            Ok(dir) => dir,
            Err(status) => {
                w.u32(status).bool(false);
                return Ok(());
            }
        };
        if name.len() > 255 {
            w.u32(stat::NAMETOOLONG);
            post_op_attr(w, None);
            return Ok(());
        }
        match self.vfs.lookup(dir, name).await {
            Ok(ino) => {
                let obj = self.local_attr(ino).await;
                let dir_attr = self.local_attr(dir).await;
                w.u32(stat::OK).opaque(&self.fh(ino));
                post_op_attr(w, obj.as_ref());
                post_op_attr(w, dir_attr.as_ref());
            }
            Err(e) => {
                let dir_attr = self.local_attr(dir).await;
                w.u32(errno_to_stat(e));
                post_op_attr(w, dir_attr.as_ref());
            }
        }
        Ok(())
    }

    async fn access(&self, r: &mut Reader<'_>, w: &mut Writer) -> XdrResult<()> {
        let ino = self.read_fh(r)?;
        let wanted = r.u32()?;
        match ino {
            Ok(ino) => {
                let attr = self.local_attr(ino).await;
                let granted = match self.vfs.mode(ino) {
                    Some((FileKind::Directory, _)) => {
                        access::READ | access::LOOKUP | access::EXECUTE
                    }
                    Some((_, perm)) if perm & 0o100 != 0 => access::READ | access::EXECUTE,
                    _ => access::READ,
                };
                w.u32(stat::OK);
                post_op_attr(w, attr.as_ref());
                w.u32(wanted & granted);
            }
            Err(status) => {
                w.u32(status).bool(false);
            }
        }
        Ok(())
    }

    async fn readlink(&self, r: &mut Reader<'_>, w: &mut Writer) -> XdrResult<()> {
        let ino = match self.read_fh(r)? {
            Ok(ino) => ino,
            Err(status) => {
                w.u32(status).bool(false);
                return Ok(());
            }
        };
        match self.vfs.readlink(ino).await {
            Ok(target) => {
                let attr = self.local_attr(ino).await;
                w.u32(stat::OK);
                post_op_attr(w, attr.as_ref());
                w.opaque(&target);
            }
            Err(e) => {
                w.u32(errno_to_stat(e)).bool(false);
            }
        }
        Ok(())
    }

    async fn read(&self, r: &mut Reader<'_>, w: &mut Writer) -> XdrResult<()> {
        let ino = self.read_fh(r)?;
        let offset = r.u64()?;
        let count = r.u32()?.min(MAX_READ);
        let ino = match ino {
            Ok(ino) => ino,
            Err(status) => {
                w.u32(status).bool(false);
                return Ok(());
            }
        };
        match self.vfs.read(ino, offset, count).await {
            Ok((data, eof)) => {
                let attr = self.local_attr(ino).await;
                w.u32(stat::OK);
                post_op_attr(w, attr.as_ref());
                w.u32(data.len() as u32).bool(eof).opaque(&data);
            }
            Err(e) => {
                w.u32(errno_to_stat(e)).bool(false);
            }
        }
        Ok(())
    }

    async fn readdir(&self, r: &mut Reader<'_>, w: &mut Writer, plus: bool) -> XdrResult<()> {
        let dir = self.read_fh(r)?;
        let cookie = r.u64()?;
        let _verf = r.fixed(8)?;
        let (dircount, maxcount) = if plus {
            (r.u32()? as usize, r.u32()? as usize)
        } else {
            let count = r.u32()? as usize;
            (usize::MAX, count)
        };
        let dir = match dir {
            Ok(dir) => dir,
            Err(status) => {
                w.u32(status).bool(false);
                return Ok(());
            }
        };
        let children = match self.vfs.readdir(dir).await {
            Ok(children) => children,
            Err(e) => {
                let attr = self.local_attr(dir).await;
                w.u32(errno_to_stat(e));
                post_op_attr(w, attr.as_ref());
                return Ok(());
            }
        };
        let mut entries: Vec<(u64, &[u8])> = Vec::with_capacity(children.len() + 2);
        entries.push((dir, b"."));
        entries.push((self.vfs.parent(dir), b".."));
        entries.extend(children.iter().map(|c| (c.ino, c.name.as_slice())));

        let start = cookie as usize;
        if start > entries.len() {
            let attr = self.local_attr(dir).await;
            w.u32(stat::BAD_COOKIE);
            post_op_attr(w, attr.as_ref());
            return Ok(());
        }

        let dir_attr = self.local_attr(dir).await;
        let mut body = Writer::new();
        body.u32(stat::OK);
        post_op_attr(&mut body, dir_attr.as_ref());
        body.fixed(&[0u8; 8]); // cookieverf: listings are stable per inode
        // status + attrs + verf so far, plus the list terminator and eof.
        let mut size = body.len() + 8;
        let mut dir_size = 0usize;
        let mut emitted = 0usize;
        for (i, (ino, name)) in entries.iter().enumerate().skip(start) {
            let base = 4 + 8 + opaque_size(name.len()) + 8;
            let mut entry = Writer::new();
            entry.bool(true).u64(*ino).opaque(name).u64(i as u64 + 1);
            if plus {
                let attr = self.local_attr(*ino).await;
                post_op_attr(&mut entry, attr.as_ref());
                entry.bool(true).opaque(&self.fh(*ino));
            }
            if size + entry.len() > maxcount || dir_size + base > dircount {
                break;
            }
            size += entry.len();
            dir_size += base;
            body.fixed(&entry.buf);
            emitted += 1;
        }
        if emitted == 0 && start < entries.len() {
            w.u32(stat::TOOSMALL);
            post_op_attr(w, dir_attr.as_ref());
            return Ok(());
        }
        let eof = start + emitted >= entries.len();
        body.bool(false).bool(eof);
        w.fixed(&body.buf);
        Ok(())
    }

    fn fsstat(&self, r: &mut Reader<'_>, w: &mut Writer) -> XdrResult<()> {
        match self.read_fh(r)? {
            Ok(_) => {
                w.u32(stat::OK).bool(false);
                w.u64(0).u64(0).u64(0).u64(0).u64(0).u64(0).u32(0);
            }
            Err(status) => {
                w.u32(status).bool(false);
            }
        }
        Ok(())
    }

    fn fsinfo(&self, r: &mut Reader<'_>, w: &mut Writer) -> XdrResult<()> {
        match self.read_fh(r)? {
            Ok(_) => {
                w.u32(stat::OK).bool(false);
                w.u32(MAX_READ) // rtmax
                    .u32(MAX_READ) // rtpref
                    .u32(4096) // rtmult
                    .u32(MAX_READ) // wtmax
                    .u32(MAX_READ) // wtpref
                    .u32(4096) // wtmult
                    .u32(64 * 1024) // dtpref
                    .u64(u64::MAX) // maxfilesize
                    .u32(1)
                    .u32(0) // time_delta
                    .u32(0x2 | 0x8); // FSF3_SYMLINK | FSF3_HOMOGENEOUS
            }
            Err(status) => {
                w.u32(status).bool(false);
            }
        }
        Ok(())
    }

    fn pathconf(&self, r: &mut Reader<'_>, w: &mut Writer) -> XdrResult<()> {
        match self.read_fh(r)? {
            Ok(_) => {
                w.u32(stat::OK).bool(false);
                w.u32(1) // linkmax
                    .u32(255) // name_max
                    .bool(true) // no_trunc
                    .bool(true) // chown_restricted
                    .bool(false) // case_insensitive
                    .bool(true); // case_preserving
            }
            Err(status) => {
                w.u32(status).bool(false);
            }
        }
        Ok(())
    }
}

/// uid from an AUTH_UNIX credential body: stamp, machinename, uid, ...
fn auth_unix_uid(body: &[u8]) -> Option<u32> {
    let mut r = Reader::new(body);
    r.u32().ok()?;
    r.skip_opaque(255).ok()?;
    r.u32().ok()
}

fn accepted(xid: u32, accept_stat: u32) -> Writer {
    let mut w = Writer::new();
    w.u32(xid)
        .u32(MSG_REPLY)
        .u32(MSG_ACCEPTED)
        .u32(AUTH_NONE)
        .u32(0)
        .u32(accept_stat);
    w
}

fn wcc_empty(w: &mut Writer) {
    w.bool(false).bool(false);
}

fn post_op_attr(w: &mut Writer, attr: Option<&Attr>) {
    match attr {
        Some(attr) => {
            w.bool(true);
            write_fattr(w, attr);
        }
        None => {
            w.bool(false);
        }
    }
}

fn write_fattr(w: &mut Writer, attr: &Attr) {
    let ftype = match attr.kind {
        FileKind::File => 1,
        FileKind::Directory => 2,
        FileKind::Symlink => 5,
    };
    let (secs, nanos) = attr
        .mtime
        .duration_since(UNIX_EPOCH)
        .map(|d| (d.as_secs().min(u32::MAX as u64) as u32, d.subsec_nanos()))
        .unwrap_or((0, 0));
    w.u32(ftype)
        .u32(attr.perm as u32)
        .u32(attr.nlink)
        .u32(attr.uid)
        .u32(attr.gid)
        .u64(attr.size)
        .u64(attr.size)
        .u32(0)
        .u32(0) // rdev
        .u64(FSID)
        .u64(attr.ino);
    for _ in 0..3 {
        w.u32(secs).u32(nanos); // atime, mtime, ctime
    }
}

/// Read one RPC record (possibly several fragments).
async fn read_record<R: AsyncReadExt + Unpin>(rd: &mut R) -> std::io::Result<Vec<u8>> {
    let mut record = Vec::new();
    loop {
        let header = rd.read_u32().await?;
        let last = header & 0x8000_0000 != 0;
        let len = (header & 0x7fff_ffff) as usize;
        if record.len() + len > MAX_RECORD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "rpc record too large",
            ));
        }
        let start = record.len();
        record.resize(start + len, 0);
        rd.read_exact(&mut record[start..]).await?;
        if last {
            return Ok(record);
        }
    }
}
