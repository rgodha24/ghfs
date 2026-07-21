//! Inode table for the store-backed filesystem.
//!
//! Inodes are fully synthesized from git object identity; there is no
//! passthrough of an underlying worktree filesystem anymore. Two ranges:
//!
//! - Virtual range (reserved small inodes): the dynamic owner/repo/ref
//!   discovery tree plus the reserved `/by-ref` root.
//! - Path range (allocated from `PASSTHROUGH_INO_START` upward): one inode
//!   per `(repo, commit_oid, repo-relative path)`. Directories carry their
//!   git tree OID so descending is a single `tree_entry` lookup; files carry
//!   their blob OID for hydration.

use crate::store::EntryKind;
use crate::types::{Owner, RepoKey};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Reserved inode for the filesystem root.
pub const ROOT_INO: u64 = 1;
/// Reserved inode for the `/by-ref` parallel root.
pub const BY_REF_INO: u64 = 2;
/// First dynamically-allocated virtual inode.
pub const VIRTUAL_INO_START: u64 = 3;
/// Last virtual inode (inclusive).
pub const VIRTUAL_INO_END: u64 = 1000;
/// First inode allocated to real repository paths.
pub const PASSTHROUGH_INO_START: u64 = 1001;

/// Identity of a resolved repository path inode: `(repo, commit, path)`.
/// Immutability of git objects makes this a stable, content-defined key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PathKey {
    pub repo: RepoKey,
    pub commit: String,
    pub path: Vec<u8>,
}

/// Data stored per inode in the forward table.
#[derive(Debug, Clone)]
pub enum InodeData {
    /// `/`
    Root,
    /// `/by-ref`
    ByRefRoot,
    /// `/owner` (top-level)
    Owner(Owner),
    /// `/by-ref/owner`
    RefOwner(Owner),
    /// `/by-ref/owner/repo` — a directory of encoded ref selectors.
    RefRepo(RepoKey),
    /// `/<owner>/<repo>` (default HEAD) or
    /// `/by-ref/<owner>/<repo>/<ref>` (resolved selector): the commit root of
    /// a repository. `root_tree` is the commit's root tree OID; all path
    /// descent starts from here.
    Repo {
        key: RepoKey,
        /// Decoded selector that resolved to `commit`, or `None` for the
        /// default-branch (HEAD) alias. Used for attribute fallback only.
        selector: Option<String>,
        commit: String,
        root_tree: String,
    },
    /// Any path below a commit root. `oid` is the directory's tree OID for
    /// [`EntryKind::Tree`], or the blob OID for files/symlinks.
    Path {
        repo: RepoKey,
        commit: String,
        path: Vec<u8>,
        oid: String,
        kind: EntryKind,
        parent: u64,
    },
}

impl InodeData {
    /// Whether this node lives in the discovery hierarchy (not a real path).
    pub fn is_virtual(&self) -> bool {
        matches!(
            self,
            Self::Root | Self::ByRefRoot | Self::Owner(_) | Self::RefOwner(_) | Self::RefRepo(_)
        )
    }

    /// Tree OID used to descend into this node as a directory, if it is one.
    pub fn dir_tree_oid(&self) -> Option<&str> {
        match self {
            Self::Repo { root_tree, .. } => Some(root_tree),
            Self::Path { oid, kind, .. } if *kind == EntryKind::Tree => Some(oid),
            _ => None,
        }
    }

    /// [`EntryKind`] for attribute synthesis. Virtual dirs report Tree.
    pub fn kind(&self) -> EntryKind {
        match self {
            Self::Path { kind, .. } => *kind,
            _ => EntryKind::Tree,
        }
    }
}

pub struct InodeTable {
    next_virtual: AtomicU64,
    next_path: AtomicU64,
    forward: DashMap<u64, InodeData>,
    /// `(parent_ino, name_utf8)` → virtual child inode (for stable discovery).
    virtual_children: DashMap<(u64, String), u64>,
    /// `(repo, commit, path)` → path inode.
    path_reverse: DashMap<PathKey, u64>,
}

impl InodeTable {
    pub fn new() -> Self {
        let forward = DashMap::new();
        forward.insert(ROOT_INO, InodeData::Root);
        forward.insert(BY_REF_INO, InodeData::ByRefRoot);
        Self {
            next_virtual: AtomicU64::new(VIRTUAL_INO_START),
            next_path: AtomicU64::new(PASSTHROUGH_INO_START),
            forward,
            virtual_children: DashMap::new(),
            path_reverse: DashMap::new(),
        }
    }

    /// Look up an inode's data.
    pub fn get(&self, ino: u64) -> Option<InodeData> {
        self.forward.get(&ino).map(|r| r.clone())
    }

    /// Whether `ino` is within the reserved virtual range.
    pub fn is_virtual_ino(ino: u64) -> bool {
        ino < PASSTHROUGH_INO_START
    }

    /// Get or create a virtual child inode of `parent` named `name`, storing
    /// `data`. Returns the inode number. Allocation from the virtual range.
    pub fn get_or_alloc_virtual(
        &self,
        parent: u64,
        name: &str,
        data: InodeData,
    ) -> Result<u64, i32> {
        if let Some(ino) = self.virtual_children.get(&(parent, name.to_string())) {
            return Ok(*ino);
        }
        let ino = self.alloc_virtual()?;
        self.forward.insert(ino, data);
        self.virtual_children
            .insert((parent, name.to_string()), ino);
        Ok(ino)
    }

    /// Get or create a path inode for `(repo, commit, path)`.
    pub fn get_or_alloc_path(
        &self,
        key: PathKey,
        oid: String,
        kind: EntryKind,
        parent: u64,
    ) -> u64 {
        if let Some(ino) = self.path_reverse.get(&key) {
            return *ino;
        }
        let ino = self.next_path.fetch_add(1, Ordering::Relaxed);
        self.forward.insert(
            ino,
            InodeData::Path {
                repo: key.repo.clone(),
                commit: key.commit.clone(),
                path: key.path.clone(),
                oid,
                kind,
                parent,
            },
        );
        self.path_reverse.insert(key, ino);
        ino
    }

    /// Remove an inode by number (used on forget for path inodes).
    pub fn forget(&self, ino: u64) {
        if let Some((_, data)) = self.forward.remove(&ino) {
            if let InodeData::Path {
                repo, commit, path, ..
            } = &data
            {
                self.path_reverse.remove(&PathKey {
                    repo: repo.clone(),
                    commit: commit.clone(),
                    path: path.clone(),
                });
            }
        }
    }

    fn alloc_virtual(&self) -> Result<u64, i32> {
        self.next_virtual
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                if cur >= VIRTUAL_INO_END {
                    None
                } else {
                    Some(cur + 1)
                }
            })
            .map_err(|_| libc::ENOSPC)
    }
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}
