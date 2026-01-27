//! Git + cache manager

mod git;
mod paths;
mod staleness;
mod swap;

pub use git::{
    clone_bare_shallow, create_worktree, fetch_shallow, open_repository, repository_exists,
    resolve_default_branch, GitCli, GitError,
};
pub use paths::CachePaths;
pub use staleness::{is_stale, touch_symlink};
pub use swap::{atomic_symlink_swap, read_symlink_target};
