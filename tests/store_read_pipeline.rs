//! Integration test: the full object-backed read pipeline against a real
//! GitHub blobless clone.
//!
//! Exercises the same `Store` methods the FUSE/NFS backends call:
//!   resolve_head → root_tree → tree_entries → tree_entry → hydrate_blob
//!
//! Run with `GHFS_RUN_NETWORK_TESTS=1`.

use ghfs::cache::CachePaths;
use ghfs::store::{Store, StoreError};
use ghfs::types::RepoKey;
use tempfile::TempDir;

fn network_tests_enabled() -> bool {
    match std::env::var("GHFS_RUN_NETWORK_TESTS") {
        Ok(v) => {
            let v = v.to_ascii_lowercase();
            v == "1" || v == "true" || v == "yes"
        }
        Err(_) => false,
    }
}

#[test]
fn read_pipeline_lists_tree_and_hydrates_a_file() {
    if !network_tests_enabled() {
        eprintln!("skipping network integration test");
        return;
    }
    let dir = TempDir::new().unwrap();
    let paths = CachePaths::new(dir.path().to_path_buf());
    std::fs::create_dir_all(paths.mirrors_dir()).unwrap();
    std::fs::create_dir_all(paths.blobs_dir()).unwrap();
    std::fs::create_dir_all(paths.locks_dir()).unwrap();

    let store = Store::new(paths);
    let key: RepoKey = "octocat/Hello-World".parse().unwrap();

    // 1. Resolve default-branch HEAD to a commit OID.
    let head = store.resolve_head(&key).expect("resolve_head");
    let commit = head.to_string();
    assert!(commit.len() == 40, "full hex commit: {commit}");

    // 2. Get the commit's root tree.
    let root = store.root_tree(&key, head).expect("root_tree");

    // 3. List the root directory: must include README and at least one entry.
    let entries = store.tree_entries(&key, root).expect("tree_entries");
    assert!(!entries.is_empty(), "Hello-World root has entries");
    let readme = entries
        .iter()
        .find(|e| e.name == b"README")
        .or_else(|| entries.iter().find(|e| e.name.starts_with(b"README")))
        .expect("found a README-like entry");
    assert!(
        matches!(
            readme.kind,
            ghfs::store::EntryKind::Blob | ghfs::store::EntryKind::Executable
        ),
        "README is a regular file: {:?}",
        readme.kind
    );

    // 4. Single-entry lookup of README returns the same OID as the listing.
    let looked = store
        .tree_entry(&key, root, &readme.name)
        .expect("tree_entry")
        .expect("README present");
    assert_eq!(looked.oid, readme.oid);

    // 5. Hydrate the blob: fetch + write to the content-addressed cache, return
    // the cached file path + size.
    let (path, size) = store.hydrate_blob(&key, readme.oid).expect("hydrate_blob");
    assert!(size > 0, "README is non-empty");
    assert!(path.exists(), "cached blob on disk");
    let body = std::fs::read(&path).expect("read cached blob");
    assert_eq!(body.len() as u64, size, "reported size matches file bytes");

    // 6. Idempotency: second hydration is the disk fast path, same path.
    let (path2, size2) = store.hydrate_blob(&key, readme.oid).unwrap();
    assert_eq!(path, path2);
    assert_eq!(size, size2);
}

#[test]
fn by_ref_selector_resolves_branch_and_hydrates() {
    if !network_tests_enabled() {
        eprintln!("skipping network integration test");
        return;
    }
    let dir = TempDir::new().unwrap();
    let paths = CachePaths::new(dir.path().to_path_buf());
    std::fs::create_dir_all(paths.mirrors_dir()).unwrap();
    std::fs::create_dir_all(paths.blobs_dir()).unwrap();
    std::fs::create_dir_all(paths.locks_dir()).unwrap();
    let store = Store::new(paths);
    let key: RepoKey = "octocat/Hello-World".parse().unwrap();

    // HEAD and "master" should peel to the same commit.
    let head = store.resolve_head(&key).unwrap();
    let master = store
        .resolve_revision(&key, "master")
        .expect("resolve master");
    assert_eq!(head, master, "HEAD == master on Hello-World");

    // The commit-selector path produces the same root tree.
    let root_head = store.root_tree(&key, head).unwrap();
    let root_master = store.root_tree(&key, master).unwrap();
    assert_eq!(root_head, root_master);

    // Commit-OID selector also resolves identically.
    let by_commit = store
        .resolve_revision(&key, &head.to_string())
        .expect("resolve by commit OID");
    assert_eq!(by_commit, head);

    // Listing branches must include "master"; tags may be present.
    let branches = store.list_branches(&key).expect("list_branches");
    assert!(
        branches.iter().any(|b| b == "master"),
        "branches: {branches:?}"
    );
    let _tags = store.list_tags(&key).expect("list_tags");

    // Missing ref errors.
    let res = store.resolve_revision(&key, "no-such-branch-xyz");
    assert!(matches!(res, Err(StoreError::Git(_))));
}

#[test]
fn selector_codec_roundtrips_through_the_store() {
    if !network_tests_enabled() {
        eprintln!("skip");
        return;
    }
    let dir = TempDir::new().unwrap();
    let paths = CachePaths::new(dir.path().to_path_buf());
    std::fs::create_dir_all(paths.mirrors_dir()).unwrap();
    let store = Store::new(paths);
    let key: RepoKey = "octocat/Hello-World".parse().unwrap();

    // The store accepts the raw decoded ref; the FS layer encodes into one
    // path component via encode_ref. Confirm store-facing raw names round-trip
    // through the codec and the store can still resolve them.
    use ghfs::store::{decode_ref, encode_ref};
    let raw = "master";
    let enc = encode_ref(raw);
    let dec = decode_ref(enc.as_bytes()).unwrap();
    assert_eq!(dec, raw);
    store.resolve_revision(&key, &dec).unwrap();
}
