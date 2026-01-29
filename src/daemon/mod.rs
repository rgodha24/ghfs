//! Daemon module for background sync operations.

pub mod state;
mod worker;

pub use state::{RepoState, State};
pub use worker::{WorkerHandle, WorkerRequest};
