//! NFSv3-over-localhost mount lifecycle, adapted from smfs-core.
//!
//! This module owns the NFS adapter, smfs-style listener setup, native mount
//! command, and RAII unmount handle.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, specdata3,
};
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};

use super::{FsKind, GhFs, NodeAttr, ROOT_INO};

const DEFAULT_NFS_PORT: u16 = 11111;
const MAX_PORT_SCAN: u16 = 100;

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

fn kind_to_nfs(kind: FsKind) -> ftype3 {
    match kind {
        FsKind::Directory => ftype3::NF3DIR,
        FsKind::RegularFile => ftype3::NF3REG,
        FsKind::Symlink => ftype3::NF3LNK,
    }
}

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
                .map(|attr| attr.to_nfs_attr())
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

#[derive(Clone, Debug)]
pub struct MountOpts {
    pub mountpoint: PathBuf,
    pub lazy_unmount: bool,
}

impl MountOpts {
    pub fn new(mountpoint: PathBuf) -> Self {
        Self {
            mountpoint,
            lazy_unmount: false,
        }
    }
}

/// Handle to a live NFS mount. Dropping it unmounts and stops the server.
#[derive(Debug)]
pub struct MountHandle {
    mountpoint: PathBuf,
    lazy_unmount: bool,
    server_handle: tokio::task::JoinHandle<()>,
}

impl MountHandle {
    pub fn mountpoint(&self) -> &Path {
        &self.mountpoint
    }
}

impl Drop for MountHandle {
    fn drop(&mut self) {
        // A cwd inside the mount makes macOS report EBUSY.
        let _ = std::env::set_current_dir("/");

        if let Err(e) = unmount_nfs(&self.mountpoint, self.lazy_unmount) {
            log::error!(
                "failed to unmount NFS at {}: {e}",
                self.mountpoint.display()
            );
        }
        self.server_handle.abort();
    }
}

/// Bind an in-process NFSv3 server, mount it with macOS's native client, and
/// return an RAII handle that owns both sides of the mount.
pub async fn mount_nfs<F>(fs: F, opts: MountOpts) -> std::io::Result<MountHandle>
where
    F: NFSFileSystem + Send + Sync + 'static,
{
    if !opts.mountpoint.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("mountpoint does not exist: {}", opts.mountpoint.display()),
        ));
    }

    let mountpoint = std::fs::canonicalize(&opts.mountpoint).map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!(
                "failed to canonicalize mountpoint {}: {e}",
                opts.mountpoint.display()
            ),
        )
    })?;

    let port = find_free_port(DEFAULT_NFS_PORT)?;
    let bind_addr = format!("127.0.0.1:{port}");
    let listener = NFSTcpListener::bind(&bind_addr, fs).await.map_err(|e| {
        std::io::Error::new(
            e.kind(),
            format!("failed to bind NFS listener on {bind_addr}: {e}"),
        )
    })?;

    let server_handle = tokio::spawn(async move {
        if let Err(e) = listener.handle_forever().await {
            log::error!("NFS server task ended unexpectedly: {e}");
        }
    });

    // Match smfs-core: let the accept loop start before the kernel connects.
    tokio::time::sleep(Duration::from_millis(100)).await;

    // mount_nfs waits for RPC responses, so never block a Tokio worker on it.
    let mountpoint_for_command = mountpoint.clone();
    let mount_result =
        tokio::task::spawn_blocking(move || nfs_mount_command(port, &mountpoint_for_command))
            .await
            .map_err(|e| std::io::Error::other(format!("mount command task panicked: {e}")))?;

    if let Err(e) = mount_result {
        server_handle.abort();
        return Err(e);
    }

    log::info!("NFS mount ready at {} (port {port})", mountpoint.display());

    Ok(MountHandle {
        mountpoint,
        lazy_unmount: opts.lazy_unmount,
        server_handle,
    })
}

fn nfs_mount_command(port: u16, mountpoint: &Path) -> std::io::Result<()> {
    // These are the docfs/smfs-core options. In particular, the old
    // actimeo=120 mount hid GHFS generation swaps for two minutes.
    let options = format!(
        "locallocks,vers=3,tcp,port={port},mountport={port},soft,timeo=100,retrans=3,acregmin=1,acregmax=5"
    );
    let output = std::process::Command::new("/sbin/mount_nfs")
        .arg("-o")
        .arg(options)
        .arg("127.0.0.1:/")
        .arg(mountpoint)
        .output()
        .map_err(|e| {
            std::io::Error::new(e.kind(), format!("failed to execute /sbin/mount_nfs: {e}"))
        })?;

    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "mount_nfs failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }

    Ok(())
}

fn unmount_nfs(mountpoint: &Path, _lazy: bool) -> std::io::Result<()> {
    let output = std::process::Command::new("/sbin/umount")
        .arg(mountpoint)
        .output()
        .map_err(|e| std::io::Error::new(e.kind(), format!("failed to execute umount: {e}")))?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let forced = std::process::Command::new("/sbin/umount")
        .arg("-f")
        .arg(mountpoint)
        .output()?;
    if !forced.status.success() {
        return Err(std::io::Error::other(format!(
            "failed to unmount {}: {}",
            mountpoint.display(),
            stderr.trim()
        )));
    }

    Ok(())
}

fn find_free_port(start: u16) -> std::io::Result<u16> {
    for offset in 0..MAX_PORT_SCAN {
        let port = start.saturating_add(offset);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return Ok(port);
        }
    }

    Err(std::io::Error::new(
        std::io::ErrorKind::AddrNotAvailable,
        format!(
            "could not find a free port in range {}-{}",
            start,
            start.saturating_add(MAX_PORT_SCAN)
        ),
    ))
}

pub fn is_mount_active(mountpoint: &Path) -> bool {
    let Ok(output) = std::process::Command::new("/sbin/mount").output() else {
        return true;
    };

    if !output.status.success() {
        return true;
    }

    let needle = mountpoint.to_string_lossy();
    let mounts = String::from_utf8_lossy(&output.stdout);
    mounts.lines().any(|line| line.contains(needle.as_ref()))
}
