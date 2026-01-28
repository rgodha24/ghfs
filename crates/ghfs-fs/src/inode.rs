//! Inode table management for the FUSE filesystem.

use dashmap::DashMap;
use ghfs_types::GenerationId;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Key for underlying file identity: (device, inode, generation)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UnderlyingKey {
    /// Device ID from the underlying filesystem.
    pub dev: u64,
    /// Inode number from the underlying filesystem.
    pub ino: u64,
    /// Cache generation ID for disambiguating reused inodes.
    pub generation: GenerationId,
}

/// Information stored for each FUSE inode.
#[derive(Debug, Clone)]
pub struct InodeInfo {
    /// The path to the underlying file/directory.
    pub path: PathBuf,
    /// The underlying file's device and inode.
    pub key: UnderlyingKey,
    /// Parent inode for directory traversal ("..").
    pub parent: u64,
}

/// Manages mapping between FUSE inodes and underlying filesystem paths.
///
/// Virtual inodes (root, owners, repos) are allocated from a fixed range.
/// Passthrough inodes are allocated dynamically and map to real files.
pub struct InodeTable {
    /// Next inode to allocate
    next_ino: AtomicU64,

    /// Map from FUSE inode -> info
    inodes: DashMap<u64, InodeInfo>,

    /// Reverse map from underlying key -> FUSE inode
    reverse: DashMap<UnderlyingKey, u64>,
}

/// Reserved inode for the virtual filesystem root.
pub const ROOT_INO: u64 = 1;
/// First inode in the virtual inode range.
pub const VIRTUAL_INO_START: u64 = 2;
/// Last inode in the virtual inode range (inclusive).
pub const VIRTUAL_INO_END: u64 = 1000; // Reserve first 1000 for virtual nodes
/// First inode for passthrough (real) filesystem entries.
pub const PASSTHROUGH_INO_START: u64 = 1001;

impl InodeTable {
    /// Create a new inode table with empty mappings.
    pub fn new() -> Self {
        Self {
            next_ino: AtomicU64::new(PASSTHROUGH_INO_START),
            inodes: DashMap::new(),
            reverse: DashMap::new(),
        }
    }

    /// Get or create an inode for an underlying file.
    /// Returns (fuse_inode, is_new).
    pub fn get_or_insert(&self, path: PathBuf, key: UnderlyingKey, parent: u64) -> (u64, bool) {
        match self.reverse.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => (*entry.get(), false),
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
                entry.insert(ino);
                self.inodes.insert(ino, InodeInfo { path, key, parent });
                (ino, true)
            }
        }
    }

    /// Look up info for a FUSE inode.
    pub fn get(&self, ino: u64) -> Option<InodeInfo> {
        self.inodes.get(&ino).map(|r| r.clone())
    }

    /// Check if an inode is a virtual node (not passthrough).
    pub fn is_virtual(ino: u64) -> bool {
        ino < PASSTHROUGH_INO_START
    }

    /// Remove an inode (called from forget).
    pub fn remove(&self, ino: u64) {
        if let Some((_, info)) = self.inodes.remove(&ino) {
            self.reverse.remove(&info.key);
        }
    }

    /// Clear all passthrough inodes (useful when generation changes).
    pub fn clear_passthrough(&self) {
        // Collect keys to remove
        let to_remove: Vec<u64> = self
            .inodes
            .iter()
            .filter(|r| *r.key() >= PASSTHROUGH_INO_START)
            .map(|r| *r.key())
            .collect();

        for ino in to_remove {
            self.remove(ino);
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
    fn test_get_or_insert_new() {
        let table = InodeTable::new();
        let key = UnderlyingKey {
            dev: 1,
            ino: 100,
            generation: GenerationId::new(1),
        };
        let (ino, is_new) = table.get_or_insert("/test/path".into(), key, ROOT_INO);

        assert!(is_new);
        assert!(ino >= PASSTHROUGH_INO_START);
    }

    #[test]
    fn test_get_or_insert_existing() {
        let table = InodeTable::new();
        let key = UnderlyingKey {
            dev: 1,
            ino: 100,
            generation: GenerationId::new(1),
        };

        let (ino1, _) = table.get_or_insert("/test/path".into(), key, ROOT_INO);
        let (ino2, is_new) = table.get_or_insert("/test/path".into(), key, ROOT_INO);

        assert!(!is_new);
        assert_eq!(ino1, ino2);
    }

    #[test]
    fn test_different_generations_get_different_inodes() {
        let table = InodeTable::new();
        let key1 = UnderlyingKey {
            dev: 1,
            ino: 100,
            generation: GenerationId::new(1),
        };
        let key2 = UnderlyingKey {
            dev: 1,
            ino: 100,
            generation: GenerationId::new(2),
        };

        let (ino1, _) = table.get_or_insert("/gen1/path".into(), key1, ROOT_INO);
        let (ino2, _) = table.get_or_insert("/gen2/path".into(), key2, ROOT_INO);

        assert_ne!(ino1, ino2);
    }

    #[test]
    fn test_get_returns_info() {
        let table = InodeTable::new();
        let key = UnderlyingKey {
            dev: 1,
            ino: 100,
            generation: GenerationId::new(1),
        };
        let path: PathBuf = "/test/path".into();
        let (ino, _) = table.get_or_insert(path.clone(), key, ROOT_INO);

        let info = table.get(ino).expect("Should find inode info");
        assert_eq!(info.path, path);
        assert_eq!(info.key, key);
    }

    #[test]
    fn test_remove() {
        let table = InodeTable::new();
        let key = UnderlyingKey {
            dev: 1,
            ino: 100,
            generation: GenerationId::new(1),
        };
        let (ino, _) = table.get_or_insert("/test/path".into(), key, ROOT_INO);

        assert!(table.get(ino).is_some());
        table.remove(ino);
        assert!(table.get(ino).is_none());
        let (new_ino, is_new) = table.get_or_insert("/test/path".into(), key, ROOT_INO);
        assert!(is_new);
        assert_ne!(ino, new_ino);
    }

    #[test]
    fn test_clear_passthrough() {
        let table = InodeTable::new();

        let key1 = UnderlyingKey {
            dev: 1,
            ino: 100,
            generation: GenerationId::new(1),
        };
        let key2 = UnderlyingKey {
            dev: 1,
            ino: 200,
            generation: GenerationId::new(1),
        };

        let (ino1, _) = table.get_or_insert("/path1".into(), key1, ROOT_INO);
        let (ino2, _) = table.get_or_insert("/path2".into(), key2, ROOT_INO);

        assert!(table.get(ino1).is_some());
        assert!(table.get(ino2).is_some());
        table.clear_passthrough();
        assert!(table.get(ino1).is_none());
        assert!(table.get(ino2).is_none());
    }
}
