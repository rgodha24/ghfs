//! GHFS library target, used by integration tests in `tests/`.
//!
//! The binary at `src/main.rs` re-exports these modules via `pub use` so the
//! daemon and CLI share one implementation.

pub mod cache;
pub mod cli;
pub mod daemon;
pub mod fs;
pub mod protocol;
pub mod service;
pub mod store;
pub mod types;
