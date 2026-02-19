//! Text-based status command implementation.

use std::fs;
use std::path::Path;

use crate::cli::{Client, ClientError};

/// Information about a process with open files under a path.
#[derive(Debug)]
struct ProcessInfo {
    pid: u32,
    comm: String,
    paths: Vec<String>,
}

/// Find all processes that have files open under the given path.
/// This is a pure /proc-based implementation (no lsof).
fn find_open_files(target_path: &str) -> Vec<ProcessInfo> {
    let mut results = Vec::new();

    let Ok(proc_dir) = fs::read_dir("/proc") else {
        return results;
    };

    for entry in proc_dir.flatten() {
        let pid_str = entry.file_name();
        let pid_str = pid_str.to_string_lossy();

        // Skip non-numeric entries (not PIDs)
        let Ok(pid) = pid_str.parse::<u32>() else {
            continue;
        };

        // Skip our own process
        if pid == std::process::id() {
            continue;
        }

        let fd_dir = format!("/proc/{}/fd", pid);
        let Ok(fds) = fs::read_dir(&fd_dir) else {
            continue;
        };

        let mut matching_paths = Vec::new();

        for fd_entry in fds.flatten() {
            // Resolve the symlink to see what file is actually open
            if let Ok(link_target) = fs::read_link(fd_entry.path()) {
                let link_str = link_target.to_string_lossy();
                if link_str.starts_with(target_path) {
                    // Get relative path for display
                    let relative = link_str
                        .strip_prefix(target_path)
                        .unwrap_or(&link_str)
                        .trim_start_matches('/');
                    if !relative.is_empty() {
                        matching_paths.push(relative.to_string());
                    } else {
                        matching_paths.push("/".to_string());
                    }
                }
            }
        }

        if !matching_paths.is_empty() {
            // Read the process name
            let comm_path = format!("/proc/{}/comm", pid);
            let comm = fs::read_to_string(&comm_path)
                .map(|s| s.trim().to_string())
                .unwrap_or_else(|_| "?".to_string());

            // Deduplicate paths
            matching_paths.sort();
            matching_paths.dedup();

            results.push(ProcessInfo {
                pid,
                comm,
                paths: matching_paths,
            });
        }
    }

    // Sort by PID
    results.sort_by_key(|p| p.pid);
    results
}

/// Format uptime in a human-readable way.
fn format_uptime(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        let mins = secs / 60;
        let secs = secs % 60;
        format!("{}m {}s", mins, secs)
    } else if secs < 86400 {
        let hours = secs / 3600;
        let mins = (secs % 3600) / 60;
        format!("{}h {}m", hours, mins)
    } else {
        let days = secs / 86400;
        let hours = (secs % 86400) / 3600;
        format!("{}d {}h", days, hours)
    }
}

/// Print the daemon status.
pub fn print_status() -> Result<(), Box<dyn std::error::Error>> {
    // Try to connect to daemon
    let mut client = match Client::connect() {
        Ok(c) => c,
        Err(ClientError::NotRunning) => {
            println!("Daemon: not running");
            return Ok(());
        }
        Err(e) => return Err(Box::new(e)),
    };

    // Get status from daemon. If the daemon exits between connect and request,
    // treat it as not running instead of surfacing transport errors.
    let status = match client.status() {
        Ok(status) => status,
        Err(ClientError::NotRunning) => {
            println!("Daemon: not running");
            return Ok(());
        }
        Err(e) => return Err(Box::new(e)),
    };

    // Get repo list up front so output is consistent if daemon exits mid-command.
    let list = match client.list() {
        Ok(list) => list,
        Err(ClientError::NotRunning) => {
            println!("Daemon: not running");
            return Ok(());
        }
        Err(e) => return Err(Box::new(e)),
    };

    // Daemon status section
    println!("Daemon");
    println!("  Status:     running");
    println!("  PID:        {}", status.pid);
    println!("  Version:    {}", status.version);
    println!("  Uptime:     {}", format_uptime(status.uptime_secs));
    println!("  Mount:      {}", status.mount_point);
    println!();

    // Synced repos section
    println!("Repositories ({})", list.repos.len());
    if list.repos.is_empty() {
        println!("  (none)");
    } else {
        for repo in &list.repos {
            let name = format!("{}/{}", repo.owner, repo.repo);
            let gen_str = repo
                .generation
                .map(|g| format!("gen {}", g))
                .unwrap_or_else(|| "not synced".to_string());
            println!("  {:<40} {}", name, gen_str);
        }
    }
    println!();

    // Pending syncs
    if !status.pending_syncs.is_empty() {
        println!("Pending Syncs ({})", status.pending_syncs.len());
        for repo in &status.pending_syncs {
            println!("  {}", repo);
        }
        println!();
    }

    // Open files section
    let mount_point = &status.mount_point;
    if Path::new(mount_point).exists() {
        let open_files = find_open_files(mount_point);

        println!("Open Files");
        if open_files.is_empty() {
            println!("  (none)");
        } else {
            for proc in &open_files {
                // Show first few paths, truncate if many
                let paths_display = if proc.paths.len() <= 3 {
                    proc.paths.join(", ")
                } else {
                    format!(
                        "{}, ... (+{} more)",
                        proc.paths[..3].join(", "),
                        proc.paths.len() - 3
                    )
                };
                println!("  {:>6} {:<20} {}", proc.pid, proc.comm, paths_display);
            }
        }
    }

    Ok(())
}
