//! Git + cache manager

mod git;
mod lock;
mod managed;
mod paths;
mod repo;
mod staleness;
mod swap;

pub use git::{
    GitCli, GitError, clone_bare_shallow, create_worktree, fetch_shallow, open_repository,
    repository_exists, resolve_default_branch,
};
pub use lock::RepoLock;
pub use managed::ManagedCache;
pub use paths::CachePaths;
pub use repo::{CacheError, GenerationRef, RepoCache};
pub use staleness::{is_stale, touch_symlink};
pub use swap::{atomic_symlink_swap, read_symlink_target};
