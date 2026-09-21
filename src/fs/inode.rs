//! Inode table for the store-backed filesystem.
//!
//! Inodes are fully synthesized from git object identity; there is no
//! passthrough of an underlying worktree filesystem anymore. Two kinds share
//! one monotonically increasing allocator (a u64 never wraps in practice):
//!
//! - Virtual inodes: the dynamic owner/repo/ref discovery tree plus the
//!   reserved `/by-ref` root. These are never reclaimed so discovery paths
//!   keep stable inode numbers for the life of the mount.
//! - Path inodes: one per `(repo, commit_oid, repo-relative path)`.
//!   Directories carry their git tree OID so descending is a single
//!   `tree_entry` lookup; files carry their blob OID for hydration. Reclaimed
//!   on `forget`.

use crate::store::EntryKind;
use crate::types::{Owner, RepoKey};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Reserved inode for the filesystem root.
pub const ROOT_INO: u64 = 1;
/// Reserved inode for the `/by-ref` parallel root.
pub const BY_REF_INO: u64 = 2;
/// First dynamically-allocated inode (virtual or path).
pub const FIRST_DYNAMIC_INO: u64 = 3;

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
    next_ino: AtomicU64,
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
            next_ino: AtomicU64::new(FIRST_DYNAMIC_INO),
            forward,
            virtual_children: DashMap::new(),
            path_reverse: DashMap::new(),
        }
    }

    /// Look up an inode's data.
    pub fn get(&self, ino: u64) -> Option<InodeData> {
        self.forward.get(&ino).map(|r| r.clone())
    }

    /// Get or create a virtual child inode of `parent` named `name`, storing
    /// `data`. Returns the inode number.
    pub fn get_or_alloc_virtual(&self, parent: u64, name: &str, data: InodeData) -> u64 {
        if let Some(ino) = self.virtual_children.get(&(parent, name.to_string())) {
            return *ino;
        }
        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
        self.forward.insert(ino, data);
        self.virtual_children
            .insert((parent, name.to_string()), ino);
        ino
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
        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
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
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn virtual_inodes_are_not_capped() {
        // Listing `/by-ref/<owner>/<repo>` allocates one virtual inode per
        // branch and tag; big repos have thousands. Regression for the old
        // fixed 998-slot range that surfaced as ENOSPC on every later lookup.
        let table = InodeTable::new();
        let owner: Owner = "o".parse().unwrap();
        let mut seen = std::collections::HashSet::new();
        for i in 0..5000 {
            let ino = table.get_or_alloc_virtual(
                ROOT_INO,
                &format!("ref-{i}"),
                InodeData::Owner(owner.clone()),
            );
            assert!(seen.insert(ino), "duplicate inode {ino}");
        }
        // Same (parent, name) resolves to the same inode.
        assert_eq!(
            table.get_or_alloc_virtual(ROOT_INO, "ref-0", InodeData::Owner(owner.clone())),
            *seen.iter().min().unwrap()
        );
    }
}
