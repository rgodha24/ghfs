//! Hermetic fixtures: bare "GitHub" repos on disk served over `file://`,
//! configured to allow the partial/shallow fetches GitHub allows.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use ghfs::git::GitCli;
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

    pub fn cli(&self) -> GitCli {
        GitCli::with_base_url(self.base_url())
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
        let repos = Repos::new(paths, remote.cli());
        let vfs = Vfs::new(repos.clone());
        Self {
            remote,
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

    pub fn pack_count(&self, owner: &str, repo: &str) -> usize {
        let key = format!("{owner}/{repo}").parse().unwrap();
        ghfs::git::odb::pack_count(&CachePaths::new(self.cache.path()).repo_dir(&key))
    }
}
