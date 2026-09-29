//! macOS frontend: an in-process NFSv3 server on localhost, mounted with the
//! system `mount_nfs`. No kernel extension needed.
//!
//! The server itself is portable (and tested on Linux); only mounting is
//! macOS-specific.

pub mod server;
pub mod xdr;

pub use server::NfsServer;

use std::path::Path;

use tokio::net::TcpListener;

/// Bind the NFS listener on an ephemeral localhost port.
pub async fn bind() -> std::io::Result<(TcpListener, u16)> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    Ok((listener, port))
}

/// Mount options for the macOS NFS client.
///
/// - `nobrowse` + `soft`: the kernel never shows the "Server connections
///   interrupted" dialog for this mount (see `nfs_down` in Apple's NFS kext).
/// - `dumbtimer,timeo=300,retrans=10`: without `dumbtimer` the kernel derives
///   LOOKUP/GETATTR/READ timeouts from the measured round-trip time, which is
///   microseconds on localhost, and a soft mount gives up after ~7s. With it,
///   a slow first fetch gets about 90s before the kernel returns ETIMEDOUT.
/// - short directory attribute caching so a moved HEAD shows up quickly;
///   files are immutable once seen, so their attributes can be cached longer.
pub fn mount_options(port: u16) -> String {
    format!(
        "rdonly,nobrowse,locallocks,vers=3,tcp,port={port},mountport={port},\
         soft,intr,dumbtimer,timeo=300,retrans=10,\
         acdirmin=1,acdirmax=5,acregmin=30,acregmax=3600"
    )
}

#[cfg(target_os = "macos")]
pub async fn mount(port: u16, mountpoint: &Path) -> std::io::Result<()> {
    let output = tokio::process::Command::new("/sbin/mount_nfs")
        .arg("-o")
        .arg(mount_options(port))
        .arg("127.0.0.1:/")
        .arg(mountpoint)
        .output()
        .await?;
    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "mount_nfs failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}

/// Unmount, forcing if a plain unmount fails (e.g. a shell has its cwd inside).
pub async fn unmount(mountpoint: &Path) -> std::io::Result<()> {
    let plain = tokio::process::Command::new("/sbin/umount")
        .arg(mountpoint)
        .output()
        .await?;
    if plain.status.success() {
        return Ok(());
    }
    let forced = tokio::process::Command::new("/sbin/umount")
        .arg("-f")
        .arg(mountpoint)
        .output()
        .await?;
    if forced.status.success() {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "umount failed: {}",
        String::from_utf8_lossy(&plain.stderr).trim()
    )))
}

/// Whether `mountpoint` is currently an NFS mount.
#[cfg(target_os = "macos")]
pub fn is_mounted(mountpoint: &Path) -> bool {
    use std::ffi::{CStr, CString};
    use std::os::unix::ffi::OsStrExt;

    let Ok(path) = CString::new(mountpoint.as_os_str().as_bytes()) else {
        return false;
    };
    let mut st: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(path.as_ptr(), &mut st) } != 0 {
        return false;
    }
    let fstype = unsafe { CStr::from_ptr(st.f_fstypename.as_ptr()) };
    let on = unsafe { CStr::from_ptr(st.f_mntonname.as_ptr()) };
    fstype.to_bytes() == b"nfs" && on.to_bytes() == mountpoint.as_os_str().as_bytes()
}
