//! Daemon module for background sync operations.

mod scheduler;
mod socket;
pub mod state;
mod worker;

pub use scheduler::SchedulerHandle;
pub use socket::{socket_path, SocketServerHandle};
pub use state::{RepoState, State};
pub use worker::{WorkerHandle, WorkerRequest};
