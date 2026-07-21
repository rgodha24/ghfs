//! Cache directory layout + per-repo locking + negative repo cache.
//!
//! The heavy cache logic (blobless cloning, tree traversal, blob hydration)
//! lives in [`crate::store`]. This module keeps only the supporting pieces:
//! path layout ([`CachePaths`]), per-repo flock serialization ([`RepoLock`]),
//! and the negative cache of repos known not to exist ([`NegativeCache`]).

pub(crate) mod lock;
mod negative;
mod paths;

pub use lock::RepoLock;
pub use negative::NegativeCache;
pub use paths::CachePaths;
