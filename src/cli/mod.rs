mod client;
mod status_tui;

pub use client::{socket_path, Client, ClientError};
pub use status_tui::run_status_tui;
