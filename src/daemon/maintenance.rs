//! Background upkeep: periodic repacks and `ghfs gc`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use crate::git::odb;
use crate::protocol::GcResult;
use crate::repos::Repos;

const REPACK_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Materialized blobs untouched for this long are dropped by `ghfs gc`; they
/// can be recreated from the local pack without the network.
const BLOB_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 3600);

/// Repack repos that have piled up packs (e.g. from a previous run), on
/// startup and then periodically.
pub async fn run(repos: Arc<Repos>) {
    let mut interval = tokio::time::interval(REPACK_INTERVAL);
    loop {
        interval.tick().await;
        for key in repos.paths().cached_repos() {
            repos.get(&key).maybe_repack();
        }
    }
}

pub async fn gc(repos: &Arc<Repos>) -> GcResult {
    let mut repacked = 0;
    for key in repos.paths().cached_repos() {
        let handle = repos.get(&key);
        if odb::pack_count(handle.dir()) > 1 {
            handle.repack().await;
            repacked += 1;
        }
    }
    let blobs_dir = repos.paths().blobs_dir();
    let (removed, freed) = tokio::task::spawn_blocking(move || remove_old_blobs(&blobs_dir))
        .await
        .unwrap_or((0, 0));
    GcResult {
        repos_repacked: repacked,
        blobs_removed: removed,
        bytes_freed: freed,
    }
}

fn remove_old_blobs(dir: &std::path::Path) -> (u64, u64) {
    let now = SystemTime::now();
    let (mut removed, mut freed) = (0, 0);
    let Ok(fans) = std::fs::read_dir(dir) else {
        return (0, 0);
    };
    for fan in fans.flatten() {
        let Ok(files) = std::fs::read_dir(fan.path()) else {
            continue;
        };
        for file in files.flatten() {
            let Ok(meta) = file.metadata() else { continue };
            let last_used = meta.accessed().or_else(|_| meta.modified());
            let old = last_used
                .ok()
                .and_then(|t| now.duration_since(t).ok())
                .is_some_and(|age| age > BLOB_MAX_AGE);
            if old && std::fs::remove_file(file.path()).is_ok() {
                removed += 1;
                freed += meta.len();
            }
        }
    }
    (removed, freed)
}
