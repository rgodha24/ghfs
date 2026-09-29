//! End-to-end tests of the filesystem core against local `file://` remotes.

mod common;

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use common::{Env, Remote};
use ghfs::vfs::FileKind;

fn sample(remote: &Remote) -> common::Fixture {
    let fx = remote.repo(
        "octo",
        "hello",
        &[
            ("README.md", b"# hello\n"),
            ("src/main.rs", b"fn main() {}\n"),
            ("src/lib.rs", b"pub fn lib() {}\n"),
            ("src/nested/deep.txt", b"deep\n"),
            ("bin/run.sh", b"#!/bin/sh\necho hi\n"),
        ],
    );
    std::fs::set_permissions(
        fx.work.join("bin/run.sh"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    std::os::unix::fs::symlink("src/main.rs", fx.work.join("link")).unwrap();
    fx.commit("exec + symlink");
    fx.push("main");
    fx
}

#[tokio::test]
async fn browse_default_branch() {
    let remote = Remote::new();
    sample(&remote);
    let env = Env::new(remote);

    assert_eq!(env.ls("").await, vec!["by-ref"], "nothing cached yet");
    assert_eq!(env.cat("octo/hello/README.md").await, b"# hello\n");
    assert_eq!(env.ls("").await, vec!["by-ref", "octo"]);
    assert_eq!(env.ls("octo").await, vec!["hello"]);
    assert_eq!(
        env.ls("octo/hello").await,
        vec!["README.md", "bin", "link", "src"]
    );
    assert_eq!(
        env.ls("octo/hello/src").await,
        vec!["lib.rs", "main.rs", "nested"]
    );
    assert_eq!(env.cat("octo/hello/src/nested/deep.txt").await, b"deep\n");

    let exe = env
        .vfs
        .getattr(env.walk("octo/hello/bin/run.sh").await.unwrap())
        .await
        .unwrap();
    // Owner-only: the mount is private to the user running the daemon.
    assert_eq!(exe.perm, 0o500);
    assert_eq!(exe.kind, FileKind::File);

    let link = env.walk("octo/hello/link").await.unwrap();
    assert_eq!(env.vfs.getattr(link).await.unwrap().kind, FileKind::Symlink);
    assert_eq!(env.vfs.readlink(link).await.unwrap(), b"src/main.rs");

    let dir = env
        .vfs
        .getattr(env.walk("octo/hello/src").await.unwrap())
        .await
        .unwrap();
    assert_eq!(dir.kind, FileKind::Directory);

    // Partial reads.
    let readme = env.walk("octo/hello/README.md").await.unwrap();
    assert_eq!(
        env.vfs.read(readme, 2, 3).await.unwrap(),
        (b"hel".to_vec(), false)
    );
    assert_eq!(
        env.vfs.read(readme, 6, 100).await.unwrap(),
        (b"o\n".to_vec(), true)
    );
    assert_eq!(env.vfs.read(readme, 100, 10).await.unwrap(), (vec![], true));
}

#[tokio::test]
async fn missing_things_are_enoent() {
    let remote = Remote::new();
    sample(&remote);
    let env = Env::new(remote);

    assert_eq!(env.walk("octo/nope").await, Err(libc::ENOENT));
    assert_eq!(env.walk("octo/hello/nope.txt").await, Err(libc::ENOENT));
    assert_eq!(env.walk("octo/hello/README.md/x").await, Err(libc::ENOTDIR));
    // macOS metadata probes never reach git.
    assert_eq!(env.walk("._octo").await, Err(libc::ENOENT));
    assert_eq!(env.walk("octo/._hello").await, Err(libc::ENOENT));
    assert_eq!(env.walk("octo/.DS_Store").await, Err(libc::ENOENT));
    assert_eq!(
        env.walk("by-ref/octo/hello/._main").await,
        Err(libc::ENOENT)
    );
    assert_eq!(
        env.walk("by-ref/octo/hello/.DS_Store").await,
        Err(libc::ENOENT)
    );
    // The unknown repo never got a cache dir.
    assert_eq!(env.ls("octo").await, vec!["hello"]);
}

#[tokio::test]
async fn by_ref_branches_tags_and_commits() {
    let remote = Remote::new();
    let fx = sample(&remote);
    let first = common::git(&fx.work, &["rev-parse", "HEAD"]);
    common::git(&fx.work, &["tag", "-a", "v1", "-m", "v1"]);
    common::git(&fx.work, &["checkout", "-q", "-b", "feature/x"]);
    fx.write(&[("README.md", b"# feature\n")]);
    fx.commit("feature");
    fx.push("feature/x");
    fx.push("v1");
    let env = Env::new(remote);

    let refs = env.ls("by-ref/octo/hello").await;
    assert_eq!(refs, vec!["feature%2Fx", "main", "v1"]);
    assert_eq!(
        env.cat("by-ref/octo/hello/feature%2Fx/README.md").await,
        b"# feature\n"
    );
    assert_eq!(
        env.cat("by-ref/octo/hello/main/README.md").await,
        b"# hello\n"
    );
    assert_eq!(
        env.cat("by-ref/octo/hello/v1/README.md").await,
        b"# hello\n"
    );
    assert_eq!(
        env.cat(&format!("by-ref/octo/hello/{first}/README.md"))
            .await,
        b"# hello\n"
    );
    assert_eq!(env.walk("by-ref/octo/hello/nope").await, Err(libc::ENOENT));
    // Unknown commit ids fail at lookup, not later.
    assert_eq!(
        env.walk("by-ref/octo/hello/0000000000000000000000000000000000000000")
            .await,
        Err(libc::ENOENT)
    );
}

#[tokio::test]
async fn head_moves_are_picked_up() {
    let remote = Remote::new();
    let fx = sample(&remote);
    let env = Env::new(remote);

    assert_eq!(env.cat("octo/hello/README.md").await, b"# hello\n");
    let old = env.walk("octo/hello/README.md").await.unwrap();

    fx.write(&[("README.md", b"# v2\n"), ("NEW.md", b"new\n")]);
    fx.commit("v2");
    fx.push("main");

    // Cached HEAD is served until a refresh.
    assert_eq!(env.cat("octo/hello/README.md").await, b"# hello\n");
    let key = "octo/hello".parse().unwrap();
    let head_dir = env.walk("octo/hello").await.unwrap();
    let before = env.vfs.getattr(head_dir).await.unwrap().mtime;
    env.repos.get(&key).refresh_head().await.unwrap();
    let after = env.vfs.getattr(head_dir).await.unwrap().mtime;
    assert!(
        after > before,
        "HEAD dir mtime must change so kernels revalidate"
    );

    assert_eq!(env.cat("octo/hello/README.md").await, b"# v2\n");
    assert_eq!(env.cat("octo/hello/NEW.md").await, b"new\n");
    // Old handles keep reading the old commit.
    assert_eq!(env.vfs.read(old, 0, 100).await.unwrap().0, b"# hello\n");
}

#[tokio::test]
async fn cached_head_survives_restart_and_offline() {
    let remote = Remote::new();
    sample(&remote);
    let cache = tempfile::tempdir().unwrap();
    let paths = ghfs::paths::CachePaths::new(cache.path());
    {
        let repos = ghfs::repos::Repos::new(paths.clone(), remote.cli());
        let vfs = ghfs::vfs::Vfs::new(repos);
        let root = ghfs::vfs::ROOT_INO;
        let owner = vfs.lookup(root, b"octo").await.unwrap();
        let repo = vfs.lookup(owner, b"hello").await.unwrap();
        let file = vfs.lookup(repo, b"README.md").await.unwrap();
        vfs.read(file, 0, 100).await.unwrap();
    }
    // New daemon, remote gone: everything already fetched still works.
    let offline = ghfs::git::GitCli::with_base_url("file:///nonexistent");
    let repos = ghfs::repos::Repos::new(paths, offline);
    let vfs = ghfs::vfs::Vfs::new(repos);
    let owner = vfs.lookup(ghfs::vfs::ROOT_INO, b"octo").await.unwrap();
    let repo = vfs.lookup(owner, b"hello").await.unwrap();
    let file = vfs.lookup(repo, b"README.md").await.unwrap();
    assert_eq!(vfs.read(file, 0, 100).await.unwrap().0, b"# hello\n");
}

#[tokio::test]
async fn directory_blobs_are_fetched_in_one_batch() {
    let remote = Remote::new();
    let files: Vec<(String, Vec<u8>)> = (0..200)
        .map(|i| {
            (
                format!("many/f{i:03}.txt"),
                format!("file {i}\n").into_bytes(),
            )
        })
        .collect();
    let refs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(p, d)| (p.as_str(), d.as_slice()))
        .collect();
    remote.repo("octo", "wide", &refs);
    let env = Arc::new(Env::new(remote));

    env.ls("octo/wide/many").await;
    let packs_after_listing = env.pack_count("octo", "wide");
    // Read every file concurrently.
    let mut tasks = Vec::new();
    for i in 0..200 {
        let env = env.clone();
        tasks.push(tokio::spawn(async move {
            let data = env.cat(&format!("octo/wide/many/f{i:03}.txt")).await;
            assert_eq!(data, format!("file {i}\n").into_bytes());
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let packs = env.pack_count("octo", "wide");
    assert!(
        packs <= packs_after_listing + 1,
        "200 files should arrive in one batch, got {packs} packs (was {packs_after_listing})"
    );
}

#[tokio::test]
async fn concurrent_first_access_shares_one_fetch() {
    let remote = Remote::new();
    sample(&remote);
    let env = Arc::new(Env::new(remote));
    let mut tasks = Vec::new();
    for _ in 0..20 {
        let env = env.clone();
        tasks.push(tokio::spawn(async move {
            env.cat("octo/hello/src/lib.rs").await
        }));
    }
    for t in tasks {
        assert_eq!(t.await.unwrap(), b"pub fn lib() {}\n");
    }
    // One commit fetch + one blob batch for src/.
    assert!(env.pack_count("octo", "hello") <= 2);
}

#[tokio::test]
async fn repack_consolidates_packs() {
    let remote = Remote::new();
    let fx = sample(&remote);
    let env = Env::new(remote);
    for i in 0..5 {
        fx.write(&[("README.md", format!("v{i}\n").as_bytes())]);
        let sha = fx.commit(&format!("v{i}"));
        fx.push("main");
        assert_eq!(
            env.cat(&format!("by-ref/octo/hello/{sha}/README.md")).await,
            format!("v{i}\n").into_bytes()
        );
    }
    assert!(env.pack_count("octo", "hello") > 5);
    let key = "octo/hello".parse().unwrap();
    env.repos.get(&key).repack().await;
    assert_eq!(env.pack_count("octo", "hello"), 1);
    // Everything still readable after the repack.
    for dir in ["", "src", "src/nested"] {
        env.ls(&format!("octo/hello/{dir}")).await;
    }
    assert_eq!(env.cat("octo/hello/src/nested/deep.txt").await, b"deep\n");
}

#[tokio::test]
async fn listing_prefetches_the_subtree() {
    let remote = Remote::new();
    sample(&remote);
    let env = Env::new(remote);

    env.ls("octo/hello").await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !env.repos.activity().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "prefetch never finished"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let packs = env.pack_count("octo", "hello");
    // Deep files were fetched by the root listing's prefetch: no new fetches.
    assert_eq!(env.cat("octo/hello/src/nested/deep.txt").await, b"deep\n");
    assert_eq!(
        env.cat("octo/hello/bin/run.sh").await,
        b"#!/bin/sh\necho hi\n"
    );
    assert_eq!(env.pack_count("octo", "hello"), packs);
}

/// Regression: parallel shallow fetches in one repo collided on git's
/// `shallow.lock` (9 of 10 failed), and a lock left by a killed fetch broke
/// every later commit fetch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_commit_fetches_and_stale_shallow_lock() {
    let remote = Remote::new();
    let fx = sample(&remote);
    let mut shas = Vec::new();
    for i in 0..10 {
        fx.write(&[("README.md", format!("v{i}\n").as_bytes())]);
        shas.push((i, fx.commit(&format!("v{i}"))));
    }
    fx.push("main");
    let env = Arc::new(Env::new(remote));
    // Init the repo, then leave a lock behind like a SIGKILLed fetch would.
    env.cat("octo/hello/README.md").await;
    let key: ghfs::types::RepoKey = "octo/hello".parse().unwrap();
    let dir = env.repos.get(&key).dir().to_path_buf();
    std::fs::write(dir.join("shallow.lock"), b"").unwrap();

    let mut tasks = Vec::new();
    for (i, sha) in shas {
        let env = env.clone();
        tasks.push(tokio::spawn(async move {
            let got = env.cat(&format!("by-ref/octo/hello/{sha}/README.md")).await;
            assert_eq!(got, format!("v{i}\n").into_bytes());
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

/// Regression: every read of a not-yet-cached file loaded the whole blob and
/// wrote its own copy. Now concurrent readers share one streamed write.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reads_share_one_blob_file() {
    let remote = Remote::new();
    // Over the 8 MiB threshold, so this goes through the streamed path.
    let big: Vec<u8> = (0..9_000_000u32).map(|i| (i % 251) as u8).collect();
    remote.repo("octo", "big", &[("big.bin", &big)]);
    let env = Arc::new(Env::new(remote));
    let ino = env.walk("octo/big/big.bin").await.unwrap();
    let mut tasks = Vec::new();
    for chunk in 0..72u64 {
        let env = env.clone();
        tasks.push(tokio::spawn(async move {
            let offset = chunk * 128 * 1024;
            let (data, _) = env.vfs.read(ino, offset, 128 * 1024).await.unwrap();
            (offset, data)
        }));
    }
    for t in tasks {
        let (offset, data) = t.await.unwrap();
        let start = (offset as usize).min(big.len());
        let end = (start + data.len()).min(big.len());
        assert_eq!(data, &big[start..end]);
    }
    let blobs = env.cache.path().join("v2/blobs");
    let files: Vec<_> = walk_files(&blobs);
    assert_eq!(
        files.len(),
        1,
        "one materialized file, no temp leftovers: {files:?}"
    );
}

fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        if entry.file_type().unwrap().is_dir() {
            out.extend(walk_files(&entry.path()));
        } else {
            out.push(entry.path());
        }
    }
    out
}

/// Directories too big for one batch: reading a file right after listing
/// works (fetching just that blob) while the whole directory arrives in
/// background chunks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn huge_directory() {
    let remote = Remote::new();
    let files: Vec<(String, Vec<u8>)> = (0..1500)
        .map(|i| (format!("huge/f{i:04}"), format!("{i}\n").into_bytes()))
        .collect();
    let refs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(p, d)| (p.as_str(), d.as_slice()))
        .collect();
    remote.repo("octo", "huge", &refs);
    let env = Env::new(remote);
    assert_eq!(env.ls("octo/huge/huge").await.len(), 1500);
    assert_eq!(env.cat("octo/huge/huge/f1234").await, b"1234\n");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !env.repos.activity().is_empty() {
        assert!(
            std::time::Instant::now() < deadline,
            "prefetch never finished"
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let packs = env.pack_count("octo", "huge");
    assert_eq!(env.cat("octo/huge/huge/f0007").await, b"7\n");
    assert_eq!(env.pack_count("octo", "huge"), packs, "already prefetched");
}

/// Regression: on macOS the kernel can keep using cached names and only send
/// GETATTR, so GETATTR on `<owner>/<repo>` must be enough to notice a stale
/// HEAD.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn getattr_alone_refreshes_a_stale_head() {
    let remote = Remote::new();
    let fx = sample(&remote);
    let env = Env::new(remote);
    env.repos.set_head_ttl(std::time::Duration::from_secs(3600));
    env.cat("octo/hello/README.md").await;
    let head = env.walk("octo/hello").await.unwrap();

    fx.write(&[("README.md", b"# moved\n")]);
    fx.commit("moved");
    fx.push("main");
    let before = env.vfs.getattr(head).await.unwrap().mtime;

    // From here on only GETATTR: no LOOKUP or READDIR on the HEAD dir.
    env.repos.set_head_ttl(std::time::Duration::ZERO);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        if env.vfs.getattr(head).await.unwrap().mtime > before {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "HEAD never refreshed");
    }
    env.repos.set_head_ttl(std::time::Duration::from_secs(3600));
    assert_eq!(env.cat("octo/hello/README.md").await, b"# moved\n");
}

#[tokio::test]
async fn case_variants_share_one_repo() {
    let remote = Remote::new();
    sample(&remote);
    let env = Env::new(remote);
    assert_eq!(env.cat("Octo/Hello/README.md").await, b"# hello\n");
    assert_eq!(env.cat("octo/hello/README.md").await, b"# hello\n");
    assert_eq!(env.ls("").await, vec!["by-ref", "octo"]);
    assert_eq!(env.ls("Octo").await, vec!["hello"]);
}

#[tokio::test]
async fn non_commit_ids_are_enoent() {
    let remote = Remote::new();
    let fx = sample(&remote);
    let tree = common::git(&fx.work, &["rev-parse", "HEAD^{tree}"]);
    let env = Env::new(remote);
    env.cat("octo/hello/README.md").await;
    assert_eq!(
        env.walk(&format!("by-ref/octo/hello/{tree}")).await,
        Err(libc::ENOENT)
    );
}

/// A cached blob file truncated by a crash (we don't fsync) is detected by
/// size and rewritten instead of served.
#[tokio::test]
async fn truncated_cached_blob_is_rewritten() {
    let remote = Remote::new();
    sample(&remote);
    let cache = tempfile::tempdir().unwrap();
    let paths = ghfs::paths::CachePaths::new(cache.path());
    let read_readme = |paths: ghfs::paths::CachePaths, cli| async move {
        let repos = ghfs::repos::Repos::new(paths, cli);
        let vfs = ghfs::vfs::Vfs::new(repos);
        let owner = vfs.lookup(ghfs::vfs::ROOT_INO, b"octo").await.unwrap();
        let repo = vfs.lookup(owner, b"hello").await.unwrap();
        let file = vfs.lookup(repo, b"README.md").await.unwrap();
        vfs.read(file, 0, 100).await.unwrap().0
    };
    assert_eq!(read_readme(paths.clone(), remote.cli()).await, b"# hello\n");
    for f in walk_files(&cache.path().join("v2/blobs")) {
        std::fs::write(f, b"").unwrap();
    }
    // A new run (fresh verification state) must not serve the empty file.
    assert_eq!(read_readme(paths, remote.cli()).await, b"# hello\n");
}
