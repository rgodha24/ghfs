//! Object-backed tree reader.
//!
//! Serves directory structure directly from git tree objects in the blobless
//! mirror. Tree objects are immutable and content-addressed, so a parsed tree
//! is cached by its OID forever (well, up to an LRU cap sized to keep the
//! process under ~50 MB of tree cache in typical use). The OS page cache
//! already keeps the on-disk packfile hot; this cache only avoids re-parsing
//! the same tree on repeated traversals.

use git2::{Oid, Repository, Tree};
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::sync::{Arc, Mutex};
use thiserror::Error;

use super::git::GitError;

/// Maximum number of parsed trees retained in memory. Each entry is roughly
/// `name.len() + 40 (Oid) + ~16 (overhead)` bytes; at ~70 B/entry and an
/// average tree of ~40 entries that's ~2.8 KB per tree, so ~600_000 trees fit
/// in ~45 MB. Most workloads use far fewer. This is a soft cap, not a hard
/// budget — large repos (e.g. nixpkgs) will simply evict and fall back to
/// libgit2 pack reads, which work, just more slowly.
const MAX_CACHED_TREES: usize = 600_000;

/// Errors returned by tree operations.
#[derive(Error, Debug)]
pub enum TreeError {
    #[error(transparent)]
    Git(#[from] GitError),
    #[error("path component is not valid UTF-8")]
    InvalidPath,
    #[error("expected a tree at {0}")]
    NotATree(String),
}

/// Kind of a git tree entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
    Tree,
    Blob,
    Symlink,
    Executable,
    /// A gitlink (submodule commit reference). GHFS surfaces these as empty
    /// read-only regular files since the pointed-to commit isn't in the
    /// mirror and we don't recurse into submodules.
    Gitlink,
}

/// A single entry in a git tree: a name plus its object identity.
#[derive(Debug, Clone)]
pub struct TreeEntry {
    /// Child name as bytes. Directory entries use OsStr at the FS layer.
    pub name: Vec<u8>,
    /// Object OID of the child (tree, blob, or commit).
    pub oid: Oid,
    /// Discrete kind derived from the git mode.
    pub kind: EntryKind,
    /// Raw git file mode bits.
    pub mode: u32,
}

/// Cache of parsed trees keyed by tree OID, shared across all commits/refs.
#[derive(Clone)]
pub struct TreeCache {
    inner: Arc<Mutex<lru::LruCache<Oid, Vec<TreeEntry>>>>,
}

impl TreeCache {
    /// Create an empty tree cache sized to [`MAX_CACHED_TREES`].
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(lru::LruCache::new(
                std::num::NonZeroUsize::new(MAX_CACHED_TREES).expect("nonzero cap"),
            ))),
        }
    }

    /// Parse a tree object into entries, or return a cached copy.
    ///
    /// The returned [`Arc`] is cheap to clone and lets the caller hold a
    /// snapshot of the entries without holding the cache lock while iterating.
    fn get_or_parse(&self, repo: &Repository, oid: Oid) -> Result<Arc<[TreeEntry]>, TreeError> {
        // Fast path: cache hit. Clone the entries into a shared allocation so
        // the caller doesn't hold the lock while iterating / returning to the
        // FS layer.
        {
            let mut guard = self.inner.lock().expect("tree cache poisoned");
            if let Some(entries) = guard.get(&oid) {
                return Ok(entries.iter().cloned().collect());
            }
        }

        // Miss: parse the tree outside the lock, then insert.
        let tree = repo.find_tree(oid).map_err(|e| map_tree_lookup(e, oid))?;
        let parsed = parse_tree(&tree)?;

        {
            let mut guard = self.inner.lock().expect("tree cache poisoned");
            guard.put(oid, parsed.clone());
        }
        Ok(parsed.into())
    }
}

impl Default for TreeCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Object-backed tree reader bound to a single repository mirror.
///
/// Thin wrapper over a libgit2 [`Repository`] plus a shared [`TreeCache`]. All
/// methods are read-only and operate on local objects (commits + trees),
/// never contacting the promisor remote.
pub struct TreeReader<'a> {
    repo: &'a Repository,
    cache: &'a TreeCache,
}

impl<'a> TreeReader<'a> {
    /// Create a reader for `repo` using `cache` for parsed-tree memoization.
    pub fn new(repo: &'a Repository, cache: &'a TreeCache) -> Self {
        Self { repo, cache }
    }

    /// Resolve a commit's root tree OID.
    pub fn root_tree(&self, commit: Oid) -> Result<Oid, TreeError> {
        let commit = self
            .repo
            .find_commit(commit)
            .map_err(|e| map_commit_lookup(e, commit))?;
        Ok(commit.tree_id())
    }

    /// Return all direct children of a tree, as a shared slice snapshot. Used
    /// by `readdir`. Name order matches git's on-disk tree ordering (already
    /// sorted in git's canonical order, which is fine for directory listings).
    pub fn entries(&self, tree: Oid) -> Result<Arc<[TreeEntry]>, TreeError> {
        self.cache.get_or_parse(self.repo, tree)
    }

    /// Look up a single named child of a tree. The hot path for path
    /// traversal: a linear scan over the (small) cached entries. Returns
    /// `None` when no entry matches `name`, not an error.
    pub fn entry(&self, tree: Oid, name: &[u8]) -> Result<Option<TreeEntry>, TreeError> {
        let entries = self.cache.get_or_parse(self.repo, tree)?;
        Ok(entries.iter().find(|e| e.name.as_slice() == name).cloned())
    }

    /// Walk a sequence of path components from a tree down, returning the
    /// terminal entry. Empty `components` yields the starting tree's own
    /// sentinel: callers pass the root-tree OID directly for `/`.
    ///
    /// Returns the entry's OID + [`EntryKind`]. Intermediate components must
    /// name tree entries; a non-directory intermediate aborts with
    /// [`TreeError::NotATree`].
    pub fn walk(
        &self,
        start_tree: Oid,
        components: &[&[u8]],
    ) -> Result<Option<(Oid, EntryKind, u32)>, TreeError> {
        let mut current = start_tree;
        let mut current_kind = EntryKind::Tree;
        let mut current_mode = 0o040000u32;

        for component in components {
            if current_kind != EntryKind::Tree {
                return Err(TreeError::NotATree(format!("{}", current)));
            }
            let Some(entry) = self.entry(current, component)? else {
                return Ok(None);
            };
            current = entry.oid;
            current_kind = entry.kind;
            current_mode = entry.mode;
        }

        Ok(Some((current, current_kind, current_mode)))
    }
}

fn parse_tree(tree: &Tree<'_>) -> Result<Vec<TreeEntry>, TreeError> {
    let mut out = Vec::with_capacity(tree.len());
    for entry in tree.iter() {
        let name_bytes = entry.name_bytes().to_vec();
        let oid = entry.id();
        // git stores modes as octal in a small int; `filemode_raw` returns
        // that raw value. Normalise to u32.
        let mode = entry.filemode_raw() as u32;
        let kind = classify_entry(mode);
        out.push(TreeEntry {
            name: name_bytes,
            oid,
            kind,
            mode,
        });
    }
    Ok(out)
}

/// Classify a git tree entry into a discrete [`EntryKind`] from its raw mode
/// bytes. We avoid libgit2's `ObjectType` here because blobless clones can't
/// always resolve submodule gitlinks, and the mode alone is authoritative for
/// fs shape decisions.
fn classify_entry(mode: u32) -> EntryKind {
    // Git on-disk modes:
    //   0o040000 - directory (tree)
    //   0o100644 - regular file
    //   0o100755 - executable file
    //   0o120000 - symlink
    //   0o160000 - gitlink (submodule)
    match mode & 0o170000 {
        0o040000 => EntryKind::Tree,
        0o100000 if (mode & 0o111) != 0 => EntryKind::Executable,
        0o100000 => EntryKind::Blob,
        0o120000 => EntryKind::Symlink,
        0o160000 => EntryKind::Gitlink,
        _ => EntryKind::Blob,
    }
}

fn map_tree_lookup(e: git2::Error, oid: Oid) -> TreeError {
    if e.code() == git2::ErrorCode::NotFound {
        TreeError::NotATree(oid.to_string())
    } else {
        TreeError::Git(GitError::Git(e))
    }
}

fn map_commit_lookup(e: git2::Error, oid: Oid) -> TreeError {
    if e.code() == git2::ErrorCode::NotFound {
        TreeError::Git(GitError::NotFound(oid.to_string()))
    } else {
        TreeError::Git(GitError::Git(e))
    }
}

/// Convenience: build an [`OsStr`] reference from cached entry name bytes.
pub fn entry_name_osstr(entry: &TreeEntry) -> &OsStr {
    OsStr::from_bytes(&entry.name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use tempfile::tempdir;

    fn check_git_available() -> bool {
        Command::new("git")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    fn make_repo() -> (tempfile::TempDir, Repository, Oid) {
        assert!(check_git_available(), "git CLI required for store tests");
        let dir = tempdir().unwrap();
        let run = |args: &[&str]| {
            let status = Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(args)
                .env("GIT_TERMINAL_PROMPT", "0")
                .status()
                .unwrap();
            assert!(status.success(), "git {:?} failed", args);
        };
        run(&["init", "-q"]);
        run(&["config", "user.email", "t@t.com"]);
        run(&["config", "user.name", "T"]);
        run(&["config", "commit.gpgsign", "false"]);
        run(&["config", "tag.gpgsign", "false"]);

        std::fs::write(dir.path().join("a.txt"), "hello\n").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub").join("b"), "world\n").unwrap();
        std::fs::write(dir.path().join("run.sh"), "#!/bin/sh\n").unwrap();
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            dir.path().join("run.sh"),
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        std::os::unix::fs::symlink("a.txt", dir.path().join("link")).unwrap();
        run(&["add", "."]);
        run(&["commit", "-q", "-m", "first"]);

        let repo = Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap().id();
        (dir, repo, head)
    }

    #[test]
    fn root_tree_resolves() {
        let (_dir, repo, head) = make_repo();
        let cache = TreeCache::new();
        let reader = TreeReader::new(&repo, &cache);
        let root = reader.root_tree(head).unwrap();
        assert!(repo.find_tree(root).is_ok());
    }

    #[test]
    fn entries_lists_all_children() {
        let (_dir, repo, head) = make_repo();
        let cache = TreeCache::new();
        let reader = TreeReader::new(&repo, &cache);
        let root = reader.root_tree(head).unwrap();
        let entries = reader.entries(root).unwrap();
        let names: Vec<Vec<u8>> = entries.iter().map(|e| e.name.clone()).collect();
        assert!(names.iter().any(|n| n == b"a.txt"));
        assert!(names.iter().any(|n| n == b"sub"));
        assert!(names.iter().any(|n| n == b"run.sh"));
        assert!(names.iter().any(|n| n == b"link"));
    }

    #[test]
    fn entry_kinds_are_classified() {
        let (_dir, repo, head) = make_repo();
        let cache = TreeCache::new();
        let reader = TreeReader::new(&repo, &cache);
        let root = reader.root_tree(head).unwrap();
        let entries = reader.entries(root).unwrap();

        assert_eq!(
            entries.iter().find(|e| e.name == b"sub").unwrap().kind,
            EntryKind::Tree
        );
        assert_eq!(
            entries.iter().find(|e| e.name == b"a.txt").unwrap().kind,
            EntryKind::Blob
        );
        assert_eq!(
            entries.iter().find(|e| e.name == b"run.sh").unwrap().kind,
            EntryKind::Executable
        );
        assert_eq!(
            entries.iter().find(|e| e.name == b"link").unwrap().kind,
            EntryKind::Symlink
        );
    }

    #[test]
    fn entry_lookup_finds_and_misses() {
        let (_dir, repo, head) = make_repo();
        let cache = TreeCache::new();
        let reader = TreeReader::new(&repo, &cache);
        let root = reader.root_tree(head).unwrap();
        assert!(reader.entry(root, b"a.txt").unwrap().is_some());
        assert!(reader.entry(root, b"missing").unwrap().is_none());
    }

    #[test]
    fn walk_descends_into_subdir() {
        let (_dir, repo, head) = make_repo();
        let cache = TreeCache::new();
        let reader = TreeReader::new(&repo, &cache);
        let root = reader.root_tree(head).unwrap();
        let (oid, kind, _mode) = reader
            .walk(root, &[b"sub", b"b"])
            .unwrap()
            .expect("sub/b should exist");
        assert_eq!(kind, EntryKind::Blob);
        let blob = repo.find_blob(oid).unwrap();
        assert_eq!(blob.content(), b"world\n");
    }

    #[test]
    fn walk_through_non_dir_aborts() {
        let (_dir, repo, head) = make_repo();
        let cache = TreeCache::new();
        let reader = TreeReader::new(&repo, &cache);
        let root = reader.root_tree(head).unwrap();
        assert!(matches!(
            reader.walk(root, &[b"a.txt", b"deep"]),
            Err(TreeError::NotATree(_))
        ));
    }

    #[test]
    fn walk_missing_component_returns_none() {
        let (_dir, repo, head) = make_repo();
        let cache = TreeCache::new();
        let reader = TreeReader::new(&repo, &cache);
        let root = reader.root_tree(head).unwrap();
        assert!(reader.walk(root, &[b"nope"]).unwrap().is_none());
    }

    #[test]
    fn cache_hit_avoids_reparse() {
        let (_dir, repo, head) = make_repo();
        let cache = TreeCache::new();
        let reader = TreeReader::new(&repo, &cache);
        let root = reader.root_tree(head).unwrap();
        let e1 = reader.entries(root).unwrap();
        let e2 = reader.entries(root).unwrap();
        assert_eq!(e1.len(), e2.len());
    }
}
