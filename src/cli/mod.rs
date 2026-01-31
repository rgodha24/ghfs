mod client;
mod status_tui;

pub use client::{Client, ClientError, socket_path};
pub use status_tui::run_status_tui;
