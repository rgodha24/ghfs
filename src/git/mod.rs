//! Git access: [`remote`] talks to GitHub through the `git` CLI (async), and
//! [`odb`] reads local objects through libgit2 (blocking, fast).

pub mod odb;
pub mod remote;

use std::time::Duration;

use thiserror::Error;

pub use odb::{EntryKind, TreeEntry};
pub use remote::{GitCli, RemoteRefs};

#[derive(Debug, Error)]
pub enum GitError {
    #[error("repository not found: {0}")]
    RepoNotFound(String),
    #[error("git command failed: {0}")]
    Command(String),
    #[error("git command timed out after {0:?}")]
    Timeout(Duration),
    #[error("failed to run git: {0}")]
    Spawn(#[source] std::io::Error),
    #[error("object not found locally: {0}")]
    MissingObject(git2::Oid),
    #[error("libgit2: {0}")]
    Libgit2(#[from] git2::Error),
}
