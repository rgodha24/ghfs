//! Inode table management for the FUSE filesystem.

/// Manages mapping between FUSE inodes and underlying filesystem paths.
pub struct InodeTable {
    // Will be implemented in Phase 5
}

impl InodeTable {
    pub fn new() -> Self {
        Self {}
    }
}

impl Default for InodeTable {
    fn default() -> Self {
        Self::new()
    }
}
