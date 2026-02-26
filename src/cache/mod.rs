//! Git + cache manager

mod git;
mod lock;
mod managed;
mod negative;
mod paths;
mod repo;
mod staleness;
mod swap;

pub use git::open_repository;
pub use managed::ManagedCache;
pub use negative::NegativeCache;
pub use paths::CachePaths;
pub use repo::{CacheError, GenerationRef};
