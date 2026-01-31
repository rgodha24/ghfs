mod client;
mod status;
mod tui;

pub use client::{Client, ClientError, socket_path};
pub use status::print_status;
pub use tui::run_tui;
