//! Git + cache manager

mod git;
mod lock;
mod managed;
mod negative;
mod paths;
mod repo;
mod staleness;
mod swap;

pub use git::{
    GitCli, GitError, clone_bare_full, clone_bare_shallow, create_worktree, fetch_full,
    fetch_reshallow, fetch_shallow, fetch_unshallow, is_shallow_repo, open_repository,
    repository_exists, resolve_default_branch,
};
pub use lock::RepoLock;
pub use managed::ManagedCache;
pub use negative::NegativeCache;
pub use paths::CachePaths;
pub use repo::{CacheError, GenerationRef, RepoCache};
pub use staleness::{is_stale, touch_symlink};
pub use swap::{atomic_symlink_swap, read_symlink_target};
