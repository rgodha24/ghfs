//! The NFSv3 server over real TCP, driven by a minimal RPC client.

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Env, Remote};
use ghfs::nfs::NfsServer;
use ghfs::nfs::xdr::{Reader, Writer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

const NFS: u32 = 100003;
const MOUNT: u32 = 100005;
const NOENT: u32 = 2;
const NOTDIR: u32 = 20;
const ROFS: u32 = 30;
const STALE: u32 = 70;
const BADHANDLE: u32 = 10001;

struct Client {
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    next_xid: u32,
    /// Replies that arrived while waiting for a different xid.
    parked: HashMap<u32, Vec<u8>>,
}

struct Reply {
    accept: u32,
    body: Vec<u8>,
}

impl Client {
    async fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let (rd, wr) = stream.into_split();
        Self {
            rd,
            wr,
            next_xid: 1,
            parked: HashMap::new(),
        }
    }

    async fn send(&mut self, prog: u32, proc_: u32, args: &[u8]) -> u32 {
        let xid = self.next_xid;
        self.next_xid += 1;
        self.send_xid(xid, prog, 3, proc_, args).await;
        xid
    }

    async fn send_xid(&mut self, xid: u32, prog: u32, vers: u32, proc_: u32, args: &[u8]) {
        let mut w = Writer::new();
        w.u32(xid).u32(0).u32(2).u32(prog).u32(vers).u32(proc_);
        // AUTH_UNIX credential, AUTH_NONE verifier.
        let mut cred = Writer::new();
        cred.u32(0).opaque(b"test").u32(501).u32(20).u32(0);
        w.u32(1).opaque(&cred.buf).u32(0).u32(0);
        w.fixed(args);
        let mut record = (0x8000_0000u32 | w.buf.len() as u32).to_be_bytes().to_vec();
        record.extend_from_slice(&w.buf);
        self.wr.write_all(&record).await.unwrap();
    }

    async fn recv(&mut self, xid: u32) -> Reply {
        let raw = self.recv_raw(xid).await;
        let mut r = Reader::new(&raw);
        assert_eq!(r.u32().unwrap(), xid);
        assert_eq!(r.u32().unwrap(), 1, "REPLY");
        assert_eq!(r.u32().unwrap(), 0, "MSG_ACCEPTED");
        r.u32().unwrap();
        r.opaque(400).unwrap();
        let accept = r.u32().unwrap();
        Reply {
            accept,
            body: r.rest().to_vec(),
        }
    }

    async fn recv_raw(&mut self, xid: u32) -> Vec<u8> {
        match self.parked.remove(&xid) {
            Some(raw) => raw,
            None => loop {
                let header = self.rd.read_u32().await.unwrap();
                assert!(header & 0x8000_0000 != 0, "single-fragment replies");
                let mut buf = vec![0u8; (header & 0x7fff_ffff) as usize];
                self.rd.read_exact(&mut buf).await.unwrap();
                let got = u32::from_be_bytes(buf[..4].try_into().unwrap());
                if got == xid {
                    break buf;
                }
                self.parked.insert(got, buf);
            },
        }
    }

    async fn call(&mut self, prog: u32, proc_: u32, args: &[u8]) -> Reply {
        let xid = self.send(prog, proc_, args).await;
        self.recv(xid).await
    }

    async fn mount(&mut self) -> Vec<u8> {
        let mut args = Writer::new();
        args.opaque(b"/");
        let reply = self.call(MOUNT, 1, &args.buf).await;
        let mut r = Reader::new(&reply.body);
        assert_eq!(r.u32().unwrap(), 0);
        let fh = r.opaque(64).unwrap().to_vec();
        assert!(r.u32().unwrap() >= 1, "auth flavors");
        fh
    }

    async fn lookup(&mut self, dir: &[u8], name: &str) -> Result<Vec<u8>, u32> {
        let mut args = Writer::new();
        args.opaque(dir).opaque(name.as_bytes());
        let reply = self.call(NFS, 3, &args.buf).await;
        let mut r = Reader::new(&reply.body);
        match r.u32().unwrap() {
            0 => Ok(r.opaque(64).unwrap().to_vec()),
            status => Err(status),
        }
    }

    async fn walk(&mut self, root: &[u8], path: &str) -> Vec<u8> {
        let mut fh = root.to_vec();
        for part in path.split('/').filter(|p| !p.is_empty()) {
            fh = self
                .lookup(&fh, part)
                .await
                .unwrap_or_else(|e| panic!("lookup {part}: {e}"));
        }
        fh
    }

    /// (ftype, size, fileid)
    async fn getattr(&mut self, fh: &[u8]) -> Result<(u32, u64, u64), u32> {
        let mut args = Writer::new();
        args.opaque(fh);
        let reply = self.call(NFS, 1, &args.buf).await;
        let mut r = Reader::new(&reply.body);
        match r.u32().unwrap() {
            0 => Ok(read_fattr(&mut r)),
            status => Err(status),
        }
    }

    async fn read(&mut self, fh: &[u8], offset: u64, count: u32) -> (Vec<u8>, bool) {
        let mut args = Writer::new();
        args.opaque(fh).u64(offset).u32(count);
        let reply = self.call(NFS, 6, &args.buf).await;
        let mut r = Reader::new(&reply.body);
        assert_eq!(r.u32().unwrap(), 0);
        skip_post_op_attr(&mut r);
        let count = r.u32().unwrap();
        let eof = r.bool().unwrap();
        let data = r.opaque(1 << 21).unwrap().to_vec();
        assert_eq!(data.len() as u32, count);
        (data, eof)
    }

    /// Full listing via repeated READDIR(PLUS) calls with a small page size.
    async fn list(&mut self, dir: &[u8], plus: bool, page: u32) -> Vec<String> {
        let mut names = Vec::new();
        let mut cookie = 0u64;
        loop {
            let mut args = Writer::new();
            args.opaque(dir).u64(cookie).fixed(&[0; 8]);
            if plus {
                args.u32(page).u32(page);
            } else {
                args.u32(page);
            }
            let reply = self.call(NFS, if plus { 17 } else { 16 }, &args.buf).await;
            let mut r = Reader::new(&reply.body);
            assert_eq!(r.u32().unwrap(), 0, "readdir status");
            skip_post_op_attr(&mut r);
            r.fixed(8).unwrap();
            let mut got = 0;
            while r.bool().unwrap() {
                r.u64().unwrap();
                names.push(String::from_utf8(r.opaque(255).unwrap().to_vec()).unwrap());
                cookie = r.u64().unwrap();
                if plus {
                    skip_post_op_attr(&mut r);
                    if r.bool().unwrap() {
                        r.opaque(64).unwrap();
                    }
                }
                got += 1;
            }
            let eof = r.bool().unwrap();
            if eof {
                return names;
            }
            assert!(got > 0, "non-eof page must make progress");
        }
    }
}

fn read_fattr(r: &mut Reader<'_>) -> (u32, u64, u64) {
    let ftype = r.u32().unwrap();
    for _ in 0..4 {
        r.u32().unwrap(); // mode nlink uid gid
    }
    let size = r.u64().unwrap();
    r.u64().unwrap(); // used
    r.u64().unwrap(); // rdev
    r.u64().unwrap(); // fsid
    let fileid = r.u64().unwrap();
    for _ in 0..6 {
        r.u32().unwrap();
    }
    (ftype, size, fileid)
}

fn skip_post_op_attr(r: &mut Reader<'_>) {
    if r.bool().unwrap() {
        read_fattr(r);
    }
}

async fn start(env: &Env) -> u16 {
    let (listener, port) = ghfs::nfs::bind().await.unwrap();
    tokio::spawn(NfsServer::new_unrestricted(env.vfs.clone()).serve(listener));
    port
}

fn sample(remote: &Remote) {
    let mut files: Vec<(String, Vec<u8>)> = vec![
        ("README.md".into(), b"# hello\n".to_vec()),
        ("src/main.rs".into(), b"fn main() {}\n".to_vec()),
        (
            "big.bin".into(),
            (0..3_000_000u32).map(|i| (i % 251) as u8).collect(),
        ),
    ];
    for i in 0..300 {
        files.push((
            format!("many/file-with-a-long-name-{i:04}.txt"),
            vec![b'x'; i],
        ));
    }
    let refs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(p, d)| (p.as_str(), d.as_slice()))
        .collect();
    let fx = remote.repo("octo", "hello", &refs);
    std::os::unix::fs::symlink("src/main.rs", fx.work.join("link")).unwrap();
    fx.commit("link");
    fx.push("main");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn protocol_basics() {
    let remote = Remote::new();
    sample(&remote);
    let env = Env::new(remote);
    let port = start(&env).await;
    let mut c = Client::connect(port).await;

    assert_eq!(c.call(NFS, 0, &[]).await.accept, 0, "NULL");
    let root = c.mount().await;
    assert_eq!(c.getattr(&root).await.unwrap().0, 2, "root is a directory");

    let readme = c.walk(&root, "octo/hello/README.md").await;
    let (ftype, size, _) = c.getattr(&readme).await.unwrap();
    assert_eq!((ftype, size), (1, 8));
    assert_eq!(
        c.read(&readme, 0, 4096).await,
        (b"# hello\n".to_vec(), true)
    );
    assert_eq!(c.read(&readme, 2, 3).await, (b"hel".to_vec(), false));

    // Large file read in 1 MiB chunks.
    let big = c.walk(&root, "octo/hello/big.bin").await;
    assert_eq!(c.getattr(&big).await.unwrap().1, 3_000_000);
    let mut data = Vec::new();
    loop {
        let (chunk, eof) = c.read(&big, data.len() as u64, 1 << 20).await;
        data.extend_from_slice(&chunk);
        if eof {
            break;
        }
    }
    assert_eq!(data.len(), 3_000_000);
    assert!(data.iter().enumerate().all(|(i, b)| *b == (i % 251) as u8));

    // Symlinks.
    let link = c.walk(&root, "octo/hello/link").await;
    assert_eq!(c.getattr(&link).await.unwrap().0, 5);
    let mut args = Writer::new();
    args.opaque(&link);
    let reply = c.call(NFS, 5, &args.buf).await;
    let mut r = Reader::new(&reply.body);
    assert_eq!(r.u32().unwrap(), 0);
    skip_post_op_attr(&mut r);
    assert_eq!(r.opaque(1024).unwrap(), b"src/main.rs");

    // Paginated listings (both flavors) agree and include . and ..
    let hello = c.walk(&root, "octo/hello").await;
    let many = c.walk(&root, "octo/hello/many").await;
    for plus in [false, true] {
        let names = c.list(&many, plus, 1024).await;
        assert_eq!(names.len(), 302, "plus={plus}");
        assert_eq!(&names[..2], &[".", ".."]);
        assert_eq!(names[2], "file-with-a-long-name-0000.txt");
    }
    let top = c.list(&hello, true, 8192).await;
    assert_eq!(
        top,
        vec![".", "..", "README.md", "big.bin", "link", "many", "src"]
    );
    let root_names = c.list(&root, false, 4096).await;
    assert_eq!(root_names, vec![".", "..", "by-ref", "octo"]);

    // Errors.
    assert_eq!(c.lookup(&hello, "nope").await, Err(NOENT));
    assert_eq!(c.lookup(&readme, "x").await, Err(NOTDIR));
    assert_eq!(c.lookup(&root, "._octo").await, Err(NOENT));
    let mut stale = readme.clone();
    stale[0] ^= 0xff;
    assert_eq!(c.getattr(&stale).await, Err(STALE));
    assert_eq!(c.getattr(&[1, 2, 3]).await, Err(BADHANDLE));

    // Writes are refused.
    let mut args = Writer::new();
    args.opaque(&readme).u64(0).u32(1).u32(0).opaque(b"x");
    let reply = c.call(NFS, 7, &args.buf).await;
    assert_eq!(Reader::new(&reply.body).u32().unwrap(), ROFS);
    let mut args = Writer::new();
    args.opaque(&hello).opaque(b"new").u32(0);
    let reply = c.call(NFS, 9, &args.buf).await;
    assert_eq!(Reader::new(&reply.body).u32().unwrap(), ROFS);

    // ACCESS never grants modify/extend/delete, and execute only where it applies.
    let mut args = Writer::new();
    args.opaque(&readme).u32(0x3f);
    let reply = c.call(NFS, 4, &args.buf).await;
    let mut r = Reader::new(&reply.body);
    assert_eq!(r.u32().unwrap(), 0);
    skip_post_op_attr(&mut r);
    assert_eq!(r.u32().unwrap(), 0x01, "plain file: read only");
    let mut args = Writer::new();
    args.opaque(&hello).u32(0x3f);
    let reply = c.call(NFS, 4, &args.buf).await;
    let mut r = Reader::new(&reply.body);
    assert_eq!(r.u32().unwrap(), 0);
    skip_post_op_attr(&mut r);
    assert_eq!(
        r.u32().unwrap(),
        0x01 | 0x02 | 0x20,
        "directory: read/lookup/search"
    );

    // FSINFO / PATHCONF / FSSTAT succeed.
    for proc_ in [18, 19, 20] {
        let mut args = Writer::new();
        args.opaque(&root);
        let reply = c.call(NFS, proc_, &args.buf).await;
        assert_eq!(Reader::new(&reply.body).u32().unwrap(), 0, "proc {proc_}");
    }

    // Unknown program / version.
    assert_eq!(c.call(424242, 0, &[]).await.accept, 1, "PROG_UNAVAIL");
    c.send_xid(9999, NFS, 4, 0, &[]).await;
    assert_eq!(c.recv(9999).await.accept, 2, "PROG_MISMATCH");
}

/// The bug this rewrite exists for: one slow request (a first-time fetch from
/// an unresponsive remote) must not block other requests on the same
/// connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_request_does_not_block_others() {
    let remote = Remote::new();
    sample(&remote);
    let cache = tempfile::tempdir().unwrap();
    let paths = ghfs::paths::CachePaths::new(cache.path());
    // Warm the cache for octo/hello through a working remote.
    {
        let repos = ghfs::repos::Repos::new(paths.clone(), remote.cli());
        let vfs = ghfs::vfs::Vfs::new(repos);
        let owner = vfs.lookup(ghfs::vfs::ROOT_INO, b"octo").await.unwrap();
        let repo = vfs.lookup(owner, b"hello").await.unwrap();
        let file = vfs.lookup(repo, b"README.md").await.unwrap();
        vfs.read(file, 0, 100).await.unwrap();
    }

    // A "GitHub" that accepts connections and never answers.
    let hang = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hang_port = hang.local_addr().unwrap().port();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((sock, _)) = hang.accept().await {
            held.push(sock);
        }
    });
    let repos = ghfs::repos::Repos::new(
        paths,
        ghfs::git::GitCli::with_base_url(format!("http://127.0.0.1:{hang_port}")),
    );
    let vfs = ghfs::vfs::Vfs::new(repos);
    let (listener, port) = ghfs::nfs::bind().await.unwrap();
    tokio::spawn(NfsServer::new_unrestricted(vfs).serve(listener));

    let mut c = Client::connect(port).await;
    let root = c.mount().await;
    let octo = c.walk(&root, "octo").await;

    // Slow: first lookup of an uncached repo hangs on the remote.
    let mut args = Writer::new();
    args.opaque(&octo).opaque(b"never-answers");
    let slow = c.send(NFS, 3, &args.buf).await;
    // Retransmit it, like the kernel does after a timeout; must not start
    // a second fetch or produce a second reply.
    c.send_xid(slow, NFS, 3, 3, &args.buf).await;

    // Fast: everything on the cached repo, pipelined behind the slow call.
    let started = Instant::now();
    let readme = c.walk(&root, "octo/hello/README.md").await;
    assert_eq!(c.read(&readme, 0, 100).await.0, b"# hello\n");
    assert_eq!(
        c.list(&root, true, 4096).await,
        vec![".", "..", "by-ref", "octo"]
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(2),
        "fast requests waited {elapsed:?} behind the slow one"
    );
    // The slow request is still outstanding.
    assert!(!c.parked.contains_key(&slow));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_clients() {
    let remote = Remote::new();
    sample(&remote);
    let env = Env::new(remote);
    let port = start(&env).await;
    let env = Arc::new(env);
    let mut tasks = Vec::new();
    for i in 0..16 {
        tasks.push(tokio::spawn(async move {
            let mut c = Client::connect(port).await;
            let root = c.mount().await;
            let name = format!("octo/hello/many/file-with-a-long-name-{:04}.txt", i * 7);
            let fh = c.walk(&root, &name).await;
            let (data, eof) = c.read(&fh, 0, 4096).await;
            assert!(eof);
            assert_eq!(data, vec![b'x'; i * 7]);
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    // Every file in many/ arrived in one directory batch.
    assert!(env.pack_count("octo", "hello") <= 3);
}

/// Other local users going through the mount are refused: the kernel stamps
/// each request with the caller's uid (AUTH_UNIX).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refuses_other_users() {
    let remote = Remote::new();
    sample(&remote);
    let env = Env::new(remote);
    let (listener, port) = ghfs::nfs::bind().await.unwrap();
    // The test client sends uid 501.
    tokio::spawn(NfsServer::for_uid(env.vfs.clone(), 4242).serve(listener));
    let mut c = Client::connect(port).await;
    assert_eq!(c.call(NFS, 0, &[]).await.accept, 0, "NULL is always fine");
    let root = c.mount().await;
    let mut args = Writer::new();
    args.opaque(&root);
    let xid = c.send(NFS, 1, &args.buf).await;
    let raw = c.recv_raw(xid).await;
    let mut r = Reader::new(&raw);
    assert_eq!(r.u32().unwrap(), xid);
    assert_eq!(r.u32().unwrap(), 1, "REPLY");
    assert_eq!(r.u32().unwrap(), 1, "MSG_DENIED");
    assert_eq!(r.u32().unwrap(), 1, "AUTH_ERROR");

    // The owner (and root) get through.
    let (listener, port) = ghfs::nfs::bind().await.unwrap();
    tokio::spawn(NfsServer::for_uid(env.vfs.clone(), 501).serve(listener));
    let mut c = Client::connect(port).await;
    let root = c.mount().await;
    assert_eq!(c.getattr(&root).await.unwrap().0, 2);
}
