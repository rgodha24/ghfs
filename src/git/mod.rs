//! Git access: [`remote`] talks to GitHub through protocol v2 (async), and
//! [`odb`] reads local objects through libgit2 (blocking, fast).

pub(crate) mod loose;
pub mod odb;
pub(crate) mod operation;
pub(crate) mod pack;
pub mod remote;
pub mod transport;
mod wire;

use std::time::Duration;

use thiserror::Error;

pub use odb::{EntryKind, TreeEntry};
pub use remote::{GitRemote, RemoteRefs};

#[derive(Debug, Error)]
pub enum GitError {
    #[error("repository not found: {0}")]
    RepoNotFound(String),
    #[error("git operation failed: {0}")]
    Command(String),
    #[error("git operation timed out after {0:?}")]
    Timeout(Duration),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("git protocol error: {0}")]
    Protocol(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("resource limit: {0}")]
    ResourceLimit(String),
    #[error("staging quota exhausted: {0}")]
    Quota(String),
    #[error("operation cancelled")]
    Cancelled,
    #[error("object not found locally: {0}")]
    MissingObject(git2::Oid),
    #[error("libgit2: {0}")]
    Libgit2(#[from] git2::Error),
}
