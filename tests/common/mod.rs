//! Hermetic fixtures: bare "GitHub" repos on disk served over `file://`,
//! configured to allow the partial/shallow fetches GitHub allows.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use ghfs::git::GitRemote;
use ghfs::paths::CachePaths;
use ghfs::repos::Repos;
use ghfs::vfs::Vfs;

pub fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args([
            "-c",
            "user.name=ghfs",
            "-c",
            "user.email=ghfs@test",
            "-c",
            "init.defaultBranch=main",
            "-c",
            "commit.gpgsign=false",
            "-c",
            "tag.gpgsign=false",
        ])
        .args(args)
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

pub struct Remote {
    pub root: PathBuf,
    _tmp: tempfile::TempDir,
}

pub struct Fixture {
    /// Working checkout the test commits into, then pushes to the bare remote.
    pub work: PathBuf,
    pub bare: PathBuf,
}

impl Remote {
    pub fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        Self {
            root: tmp.path().join("remote"),
            _tmp: tmp,
        }
    }

    pub fn base_url(&self) -> String {
        format!("file://{}", self.root.display())
    }

    pub fn cli(&self) -> GitRemote {
        GitRemote::with_transport(self.base_url(), Arc::new(UploadPack))
    }

    /// Create `<owner>/<repo>.git` with the given files (path, contents) on main.
    pub fn repo(&self, owner: &str, repo: &str, files: &[(&str, &[u8])]) -> Fixture {
        let bare = self.root.join(owner).join(format!("{repo}.git"));
        let work = self.root.join("_work").join(owner).join(repo);
        std::fs::create_dir_all(&work).unwrap();
        git(&work, &["init", "-q", "-b", "main"]);
        let fx = Fixture { work, bare };
        fx.write(files);
        fx.commit("initial");
        std::fs::create_dir_all(fx.bare.parent().unwrap()).unwrap();
        git(
            fx.bare.parent().unwrap(),
            &[
                "clone",
                "-q",
                "--bare",
                fx.work.to_str().unwrap(),
                fx.bare.to_str().unwrap(),
            ],
        );
        git(&fx.bare, &["config", "uploadpack.allowFilter", "true"]);
        git(
            &fx.bare,
            &["config", "uploadpack.allowAnySHA1InWant", "true"],
        );
        git(
            &fx.work,
            &["remote", "add", "origin", fx.bare.to_str().unwrap()],
        );
        fx
    }
}

impl Fixture {
    pub fn write(&self, files: &[(&str, &[u8])]) {
        for (path, data) in files {
            let full = self.work.join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(full, data).unwrap();
        }
    }

    pub fn commit(&self, msg: &str) -> String {
        git(&self.work, &["add", "-A"]);
        git(&self.work, &["commit", "-q", "--allow-empty", "-m", msg]);
        git(&self.work, &["rev-parse", "HEAD"])
    }

    pub fn push(&self, refspec: &str) {
        git(&self.work, &["push", "-q", "-f", "origin", refspec]);
    }
}

pub struct Env {
    pub remote: Remote,
    pub cache: tempfile::TempDir,
    pub repos: Arc<Repos>,
    pub git: GitRemote,
    pub vfs: Arc<Vfs>,
}

impl Env {
    pub fn new(remote: Remote) -> Self {
        let _ = env_logger::builder().is_test(true).try_init();
        // All tests share one process (and fd table); the daemon raises its
        // limit the same way.
        ghfs::daemon::raise_fd_limit();
        let cache = tempfile::tempdir().unwrap();
        let paths = CachePaths::new(cache.path());
        paths.create_dirs().unwrap();
        let git = remote.cli();
        let repos = Repos::new(paths, git.clone());
        let vfs = Vfs::new(repos.clone());
        Self {
            remote,
            git,
            cache,
            repos,
            vfs,
        }
    }

    /// Resolve a slash-separated path from the root to an inode.
    pub async fn walk(&self, path: &str) -> Result<u64, i32> {
        let mut ino = ghfs::vfs::ROOT_INO;
        for part in path.split('/').filter(|p| !p.is_empty()) {
            ino = self.vfs.lookup(ino, part.as_bytes()).await?;
        }
        Ok(ino)
    }

    pub async fn cat(&self, path: &str) -> Vec<u8> {
        let ino = self
            .walk(path)
            .await
            .unwrap_or_else(|e| panic!("walk {path}: {e}"));
        let attr = self.vfs.getattr(ino).await.unwrap();
        let (data, eof) = self.vfs.read(ino, 0, attr.size as u32 + 10).await.unwrap();
        assert!(eof);
        assert_eq!(data.len() as u64, attr.size);
        data
    }

    pub async fn ls(&self, path: &str) -> Vec<String> {
        let ino = self.walk(path).await.unwrap();
        self.vfs
            .readdir(ino)
            .await
            .unwrap()
            .into_iter()
            .map(|e| String::from_utf8(e.name).unwrap())
            .collect()
    }

    pub fn fetch_count(&self) -> u64 {
        self.git.stats().fetch_requests
    }

    pub fn assert_loose_only(&self) {
        for key in self.repos.paths().cached_repos() {
            let dir = self.repos.paths().repo_dir(&key);
            let packs = dir.join("objects/pack");
            assert!(
                std::fs::read_dir(packs).unwrap().next().is_none(),
                "retained packs/indexes"
            );
            let staging = dir.join("ghfs-staging");
            if staging.is_dir() {
                assert!(
                    std::fs::read_dir(staging).unwrap().next().is_none(),
                    "staging not cleaned"
                );
            }
        }
    }
}

/// The library never shells out to git. Only this fixture transport does.
#[derive(Debug)]
pub struct UploadPack;

impl ghfs::git::transport::Transport for UploadPack {
    fn discover<'a>(&'a self, url: &'a str) -> ghfs::git::transport::TransportFuture<'a> {
        Box::pin(upload_pack(url, None))
    }

    fn command<'a>(
        &'a self,
        url: &'a str,
        body: bytes::Bytes,
    ) -> ghfs::git::transport::TransportFuture<'a> {
        Box::pin(upload_pack(url, Some(body)))
    }
}

async fn upload_pack(
    url: &str,
    body: Option<bytes::Bytes>,
) -> Result<ghfs::git::transport::Response, ghfs::git::GitError> {
    use ghfs::git::transport::Response;
    use std::os::unix::process::CommandExt;
    use std::process::Stdio;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let path = url
        .strip_prefix("file://")
        .ok_or_else(|| ghfs::git::GitError::Transport("fixture expected file://".into()))?;
    let discovery = body.is_none();
    if !std::path::Path::new(path).join("objects").is_dir() {
        return Ok(Response {
            status: 404,
            content_type: None,
            body: Box::pin(std::io::Cursor::new(Vec::<u8>::new())),
        });
    }
    let mut command = tokio::process::Command::new("git");
    command.arg("upload-pack").arg("--stateless-rpc");
    if discovery {
        command.arg("--advertise-refs");
    }
    command
        .arg(path)
        .env("GIT_PROTOCOL", "version=2")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = command.spawn()?;
    let pid = child.id();
    let stdout = child.stdout.take().unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let writer = tokio::spawn(async move {
        if let Some(body) = body {
            let _ = stdin.write_all(&body).await;
        }
        let _ = stdin.shutdown().await;
    });
    let stderr = child.stderr.take().unwrap();
    let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
    let task_errors = errors.clone();
    let stderr_task = tokio::spawn(async move {
        let mut stderr = stderr;
        let mut buffer = [0; 4096];
        while let Ok(n) = stderr.read(&mut buffer).await {
            if n == 0 {
                break;
            }
            let mut errors = task_errors.lock().unwrap();
            let remaining = 64 * 1024 - errors.len();
            errors.extend_from_slice(&buffer[..n.min(remaining)]);
        }
    });
    let exit = tokio::spawn(async move { child.wait().await });
    Ok(Response {
        status: 200,
        content_type: Some(
            if discovery {
                "application/x-git-upload-pack-advertisement"
            } else {
                "application/x-git-upload-pack-result"
            }
            .into(),
        ),
        body: Box::pin(UploadBody {
            stdout,
            pid,
            writer,
            stderr_task,
            exit,
            errors,
            done: false,
        }),
    })
}

struct UploadBody {
    stdout: tokio::process::ChildStdout,
    pid: Option<u32>,
    writer: tokio::task::JoinHandle<()>,
    stderr_task: tokio::task::JoinHandle<()>,
    exit: tokio::task::JoinHandle<std::io::Result<std::process::ExitStatus>>,
    errors: Arc<std::sync::Mutex<Vec<u8>>>,
    done: bool,
}

impl tokio::io::AsyncRead for UploadBody {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::future::Future;
        use std::pin::Pin;
        use std::task::Poll;
        if self.done || buffer.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let before = buffer.filled().len();
        match Pin::new(&mut self.stdout).poll_read(cx, buffer) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) if buffer.filled().len() != before => return Poll::Ready(Ok(())),
            Poll::Ready(Ok(())) => {}
        }
        match Pin::new(&mut self.exit).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Ok(status))) => {
                self.done = true;
                if status.success() {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(std::io::Error::other(format!(
                        "upload-pack: {}",
                        String::from_utf8_lossy(&self.errors.lock().unwrap())
                    ))))
                }
            }
            Poll::Ready(error) => {
                self.done = true;
                Poll::Ready(Err(std::io::Error::other(format!(
                    "upload-pack wait: {error:?}"
                ))))
            }
        }
    }
}

impl Drop for UploadBody {
    fn drop(&mut self) {
        if !self.done
            && let Some(pid) = self.pid
        {
            unsafe {
                libc::kill(-(pid as libc::pid_t), libc::SIGKILL);
            }
        }
        self.writer.abort();
        self.stderr_task.abort();
        self.exit.abort();
    }
}
