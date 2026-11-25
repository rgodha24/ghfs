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
    let offline = ghfs::git::GitRemote::with_base_url("file:///nonexistent");
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
    let fetches_after_listing = env.fetch_count();
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
    let fetches = env.fetch_count();
    assert!(
        fetches <= fetches_after_listing + 1,
        "200 files should arrive in one batch, got {fetches} requests (was {fetches_after_listing})"
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
    assert!(env.fetch_count() <= 2);
}

#[tokio::test]
async fn repeated_fetches_retain_only_loose_objects() {
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
    assert!(env.fetch_count() > 5);
    let key = "octo/hello".parse().unwrap();
    assert!(env.repos.get(&key).dir().join("ghfs-complete").is_dir());
    env.assert_loose_only();
    // Historical fetches and later reads require no consolidation.
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
    let fetches = env.fetch_count();
    // Deep files were fetched by the root listing's prefetch: no new fetches.
    assert_eq!(env.cat("octo/hello/src/nested/deep.txt").await, b"deep\n");
    assert_eq!(
        env.cat("octo/hello/bin/run.sh").await,
        b"#!/bin/sh\necho hi\n"
    );
    assert_eq!(env.fetch_count(), fetches);
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
    let blobs = env.cache.path().join("v3/blobs");
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
    let fetches = env.fetch_count();
    assert_eq!(env.cat("octo/huge/huge/f0007").await, b"7\n");
    assert_eq!(env.fetch_count(), fetches, "already prefetched");
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
    for f in walk_files(&cache.path().join("v3/blobs")) {
        std::fs::write(f, b"").unwrap();
    }
    // A new run (fresh verification state) must not serve the empty file.
    assert_eq!(read_readme(paths, remote.cli()).await, b"# hello\n");
}

/// A persisted marker is advisory until the whole tree closure has been hashed
/// this run. Missing/corrupt descendants must be repaired, not hidden by HEAD.
#[tokio::test]
async fn restart_repairs_missing_and_corrupt_trees_beneath_a_completion_marker() {
    let remote = Remote::new();
    let fx = sample(&remote);
    let commit: git2::Oid = common::git(&fx.work, &["rev-parse", "HEAD"])
        .parse()
        .unwrap();
    let child: git2::Oid = common::git(&fx.work, &["rev-parse", "HEAD:src/nested"])
        .parse()
        .unwrap();
    let cache = tempfile::tempdir().unwrap();
    let paths = ghfs::paths::CachePaths::new(cache.path());
    let key = "octo/hello".parse().unwrap();
    let object = paths
        .repo_dir(&key)
        .join("objects")
        .join(&child.to_string()[..2])
        .join(&child.to_string()[2..]);
    let marker = paths
        .repo_dir(&key)
        .join("ghfs-complete")
        .join(commit.to_string());
    {
        let repos = ghfs::repos::Repos::new(paths.clone(), remote.cli());
        repos.get(&key).ensure_commit(commit).await.unwrap();
        assert!(marker.is_file());
        repos.shutdown().await;
    }
    for corrupt in [false, true] {
        if corrupt {
            std::fs::write(&object, b"corrupt zlib bytes").unwrap();
        } else {
            std::fs::remove_file(&object).unwrap();
        }
        let git = remote.cli();
        let repos = ghfs::repos::Repos::new(paths.clone(), git.clone());
        let handle = repos.get(&key);
        handle.ensure_commit(commit).await.unwrap();
        assert_eq!(
            git.stats().fetch_requests,
            1,
            "restart must refetch the incomplete closure"
        );
        handle.tree(child).await.unwrap();
        assert!(marker.is_file());
        handle.ensure_commit(commit).await.unwrap();
        assert_eq!(
            git.stats().fetch_requests,
            1,
            "validated markers are cached this run"
        );
        repos.shutdown().await;
    }
}

#[tokio::test]
async fn annotated_tag_object_ids_are_peeled_without_fetching_history() {
    let remote = Remote::new();
    let fx = sample(&remote);
    common::git(&fx.work, &["tag", "-a", "annotated", "-m", "annotated"]);
    fx.push("annotated");
    let tag = common::git(&fx.work, &["rev-parse", "annotated"]);
    let env = Env::new(remote);
    assert_eq!(
        env.cat(&format!("by-ref/octo/hello/{tag}/README.md")).await,
        b"# hello\n"
    );
    env.assert_loose_only();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_large_materialization_can_be_retried_without_temp_races() {
    use std::io::Write;
    let remote = Remote::new();
    let fx = sample(&remote);
    let mut file = std::fs::File::create(fx.work.join("big.bin")).unwrap();
    let chunk = vec![b'x'; 1 << 20];
    for _ in 0..64 {
        file.write_all(&chunk).unwrap();
    }
    drop(file);
    let commit: git2::Oid = fx.commit("large streaming file").parse().unwrap();
    fx.push("main");
    let blob: git2::Oid = common::git(&fx.work, &["rev-parse", "HEAD:big.bin"])
        .parse()
        .unwrap();
    let env = Env::new(remote);
    let handle = env.repos.get(&"octo/hello".parse().unwrap());
    let (tree, _) = handle.ensure_commit(commit).await.unwrap();
    assert_eq!(handle.blob_size(tree, blob).await.unwrap(), 64 << 20);
    let writer = handle.clone();
    let task = tokio::spawn(async move { writer.materialize(tree, blob).await });
    let directory = env.repos.paths().blobs_dir().join(&blob.to_string()[..2]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let writing = std::fs::read_dir(&directory).ok().is_some_and(|entries| {
            entries
                .flatten()
                .any(|entry| entry.file_name().to_string_lossy().starts_with('.'))
        });
        if writing || task.is_finished() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "writer never started");
        tokio::task::yield_now().await;
    }
    task.abort();
    let _ = task.await;
    let path = handle.materialize(tree, blob).await.unwrap();
    use std::io::Read;
    let mut input = std::fs::File::open(path).unwrap();
    let mut buffer = [0; 65536];
    let mut bytes = 0;
    loop {
        let n = input.read(&mut buffer).unwrap();
        if n == 0 {
            break;
        }
        assert!(buffer[..n].iter().all(|byte| *byte == b'x'));
        bytes += n;
    }
    assert_eq!(bytes, 64 << 20);
    env.repos.shutdown().await;
    assert_eq!(env.git.stats().available_operation_slots, 24);
    assert_eq!(env.git.stats().available_decoder_slots, 2);
    assert!(
        std::fs::read_dir(directory)
            .unwrap()
            .flatten()
            .all(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
    );
    env.assert_loose_only();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corrupt_loose_blobs_are_dropped_and_refetched() {
    use std::io::Write;
    let remote = Remote::new();
    let fx = sample(&remote);
    let mut file = std::fs::File::create(fx.work.join("big.bin")).unwrap();
    for i in 0..10u8 {
        file.write_all(&vec![b'a' + i; 1 << 20]).unwrap();
    }
    drop(file);
    let commit: git2::Oid = fx.commit("large file").parse().unwrap();
    fx.push("main");
    let big = std::fs::read(fx.work.join("big.bin")).unwrap();
    let cases: [(&str, &[u8]); 2] = [("README.md", b"# hello\n"), ("big.bin", &big)];
    let cache = tempfile::tempdir().unwrap();
    let paths = ghfs::paths::CachePaths::new(cache.path());
    let key = "octo/hello".parse().unwrap();
    for truncate in [true, false] {
        for (name, expected) in cases {
            let blob: git2::Oid = common::git(&fx.work, &["rev-parse", &format!("HEAD:{name}")])
                .parse()
                .unwrap();
            let hex = blob.to_string();
            let object = paths
                .repo_dir(&key)
                .join("objects")
                .join(&hex[..2])
                .join(&hex[2..]);
            let repos = ghfs::repos::Repos::new(paths.clone(), remote.cli());
            let handle = repos.get(&key);
            let (tree, _) = handle.ensure_commit(commit).await.unwrap();
            handle.blob_size(tree, blob).await.unwrap();
            let loose = std::fs::read(&object).unwrap();
            // Simulate an unsynced write lost to a crash: the object still
            // "exists" but cannot be inflated or fails hash verification.
            let damaged = if truncate {
                loose[..loose.len() / 2].to_vec()
            } else {
                b"not a zlib stream".to_vec()
            };
            std::fs::write(&object, damaged).unwrap();
            let _ = std::fs::remove_file(repos.paths().blob_path(&hex));
            let path = handle.materialize(tree, blob).await.unwrap();
            assert_eq!(
                std::fs::read(path).unwrap(),
                expected,
                "{name}, truncate={truncate}"
            );
            assert_eq!(std::fs::read(&object).unwrap(), loose);
            repos.shutdown().await;
        }
    }
}

#[tokio::test]
async fn forgotten_inodes_are_swept_and_come_back() {
    use std::time::Duration;
    let remote = Remote::new();
    sample(&remote);
    let env = Env::new(remote);
    let vfs = &env.vfs;

    let file = env.walk("octo/hello/src/main.rs").await.unwrap();
    let src = vfs.parent(file).await;
    vfs.retain(src);
    vfs.retain(file);
    // Listed but never looked up: nothing holds these.
    env.ls("octo/hello").await;
    let before = vfs.len();

    // Held inodes survive; unheld idle ones go.
    let swept = vfs.sweep(Duration::ZERO);
    assert!(swept > 0, "listed-only inodes are swept");
    assert_eq!(vfs.len(), before - swept);
    assert!(vfs.exists(file) && vfs.exists(src));
    assert_eq!(vfs.read(file, 0, 100).await.unwrap().0, b"fn main() {}\n");

    // Once the kernel forgets it, the same path gets a fresh inode that
    // still works.
    vfs.forget(file, 1);
    vfs.sweep(Duration::ZERO);
    assert!(!vfs.exists(file));
    let again = vfs.lookup(src, b"main.rs").await.unwrap();
    assert_ne!(again, file);
    assert_eq!(vfs.read(again, 0, 100).await.unwrap().0, b"fn main() {}\n");

    // Recently used inodes are kept even when unheld.
    assert!(vfs.sweep(Duration::from_secs(3600)) == 0);
    assert!(vfs.exists(again));
}

#[tokio::test]
async fn identical_objects_at_different_paths_stay_distinct() {
    use std::time::Duration;
    let remote = Remote::new();
    // `a/x` and `b/x` are the same tree; `f` and `g` the same blob.
    remote.repo(
        "octo",
        "twins",
        &[
            ("a/x/f", b"same\n"),
            ("a/x/g", b"same\n"),
            ("a/marker", b"a\n"),
            ("b/x/f", b"same\n"),
            ("b/x/g", b"same\n"),
            ("b/marker", b"b\n"),
        ],
    );
    let env = Env::new(remote);
    let vfs = &env.vfs;
    let handle = |ino| vfs.handle(ino).unwrap();
    let ax = handle(env.walk("octo/twins/a/x").await.unwrap());
    let bx = handle(env.walk("octo/twins/b/x").await.unwrap());
    let f = handle(env.walk("octo/twins/a/x/f").await.unwrap());
    let g = handle(env.walk("octo/twins/a/x/g").await.unwrap());
    assert_ne!(ax, bx);
    assert_ne!(f, g);

    // Rebuilt from their handles, each `x` finds its own parent again.
    assert!(vfs.sweep(Duration::ZERO) > 0);
    let ax = vfs.resolve(&ax).await.unwrap();
    let bx = vfs.resolve(&bx).await.unwrap();
    assert_ne!(ax, bx);
    for (x, want) in [(ax, b"a\n"), (bx, b"b\n")] {
        let up = vfs.parent(x).await;
        let marker = vfs.lookup(up, b"marker").await.unwrap();
        assert_eq!(vfs.read(marker, 0, 10).await.unwrap().0, want);
        // Remembered, not searched for again.
        assert_eq!(vfs.parent(x).await, up);
    }
    let f = vfs.resolve(&f).await.unwrap();
    let g = vfs.resolve(&g).await.unwrap();
    assert_ne!(f, g);
}
