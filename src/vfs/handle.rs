//! Self-describing file handles.
//!
//! NFS has no `forget`: a client may send back any handle it was ever given,
//! at any time. So a handle can't just be a key into the inode table (that
//! table could then never shrink). Instead it describes the node itself, and
//! [`super::Vfs::resolve`] rebuilds an inode from it when the table no longer
//! has one. Handles hold only content addresses and a hash of the repo name,
//! so they also stay valid across daemon restarts.
//!
//! NFSv3 handles are at most 64 bytes; the largest form here is 63.

use git2::Oid;
use sha1::{Digest, Sha1};

use crate::git::EntryKind;
use crate::types::RepoKey;

pub const MAX_LEN: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Desc {
    Root,
    ByRef,
    Owner(String),
    RefOwner(String),
    RefRepo(u64),
    Head(u64),
    Commit {
        repo: u64,
        commit: Oid,
        pinned: bool,
    },
    /// Anything inside a commit. `aux` is the containing directory's tree for
    /// files (blob fetches are batched per directory) and the commit for
    /// directories (to find their parent again for `..`).
    Path {
        repo: u64,
        kind: EntryKind,
        under_head: bool,
        /// Reached through `<owner>/<repo>` (HEAD) rather than `by-ref`.
        in_head: bool,
        /// Below a `by-ref` commit id (rather than a branch or tag).
        pinned: bool,
        /// Seconds since the epoch, clamped to `u32` (it's only an mtime).
        commit_time: u32,
        /// [`path_hash`] of the path within the commit: identical files or
        /// subtrees at different paths must stay different objects.
        path: u64,
        oid: Oid,
        aux: Oid,
    },
}

const ROOT: u8 = 1;
const BY_REF: u8 = 2;
const OWNER: u8 = 3;
const REF_OWNER: u8 = 4;
const REF_REPO: u8 = 5;
const HEAD: u8 = 6;
const COMMIT: u8 = 7;
const PATH: u8 = 8;

/// Stable id of a repo for handles: GitHub names are case-insensitive.
pub fn repo_id(key: &RepoKey) -> u64 {
    let key = key.normalized();
    let digest = Sha1::digest(format!("{}/{}", key.owner, key.repo));
    u64::from_be_bytes(digest[..8].try_into().unwrap())
}

/// Identity of a path within a commit: `parent` is the containing
/// directory's path hash (0 for the commit's root tree).
pub fn path_hash(parent: u64, name: &[u8]) -> u64 {
    let mut h = Sha1::new();
    h.update(parent.to_be_bytes());
    h.update(name);
    u64::from_be_bytes(h.finalize()[..8].try_into().unwrap())
}

/// Commit time as stored in a handle.
pub fn handle_time(secs: i64) -> u32 {
    secs.clamp(0, u32::MAX as i64) as u32
}

/// NFS fileid for a handle: the same object always gets the same number,
/// whichever inode currently backs it.
pub fn fileid(handle: &[u8]) -> u64 {
    let digest = Sha1::digest(handle);
    // 0 and the reserved low inode numbers stay unused.
    u64::from_be_bytes(digest[..8].try_into().unwrap()).max(16)
}

fn kind_code(kind: EntryKind) -> u8 {
    match kind {
        EntryKind::Tree => 0,
        EntryKind::Blob => 1,
        EntryKind::Executable => 2,
        EntryKind::Symlink => 3,
        EntryKind::Gitlink => 4,
    }
}

fn kind_from(code: u8) -> Option<EntryKind> {
    Some(match code {
        0 => EntryKind::Tree,
        1 => EntryKind::Blob,
        2 => EntryKind::Executable,
        3 => EntryKind::Symlink,
        4 => EntryKind::Gitlink,
        _ => return None,
    })
}

impl Desc {
    /// `None` if it doesn't fit (an owner name longer than GitHub allows).
    pub fn encode(&self) -> Option<Vec<u8>> {
        let mut out = Vec::with_capacity(MAX_LEN);
        match self {
            Self::Root => out.push(ROOT),
            Self::ByRef => out.push(BY_REF),
            Self::Owner(name) | Self::RefOwner(name) => {
                out.push(if matches!(self, Self::Owner(_)) {
                    OWNER
                } else {
                    REF_OWNER
                });
                out.extend_from_slice(name.as_bytes());
            }
            Self::RefRepo(repo) | Self::Head(repo) => {
                out.push(if matches!(self, Self::Head(_)) {
                    HEAD
                } else {
                    REF_REPO
                });
                out.extend_from_slice(&repo.to_be_bytes());
            }
            Self::Commit {
                repo,
                commit,
                pinned,
            } => {
                out.push(COMMIT);
                out.extend_from_slice(&repo.to_be_bytes());
                out.extend_from_slice(commit.as_bytes());
                out.push(*pinned as u8);
            }
            Self::Path {
                repo,
                kind,
                under_head,
                in_head,
                pinned,
                commit_time,
                path,
                oid,
                aux,
            } => {
                out.push(PATH);
                out.extend_from_slice(&repo.to_be_bytes());
                out.push(kind_code(*kind));
                out.push(*under_head as u8 | (*in_head as u8) << 1 | (*pinned as u8) << 2);
                out.extend_from_slice(&commit_time.to_be_bytes());
                out.extend_from_slice(&path.to_be_bytes());
                out.extend_from_slice(oid.as_bytes());
                out.extend_from_slice(aux.as_bytes());
            }
        }
        (out.len() <= MAX_LEN).then_some(out)
    }

    pub fn decode(raw: &[u8]) -> Option<Self> {
        let (&tag, rest) = raw.split_first()?;
        let u64_at = |at: usize| Some(u64::from_be_bytes(rest.get(at..at + 8)?.try_into().ok()?));
        let u32_at = |at: usize| Some(u32::from_be_bytes(rest.get(at..at + 4)?.try_into().ok()?));
        let oid_at = |at: usize| Oid::from_bytes(rest.get(at..at + 20)?).ok();
        let name = || {
            let name = std::str::from_utf8(rest).ok()?;
            (!name.is_empty()).then(|| name.to_owned())
        };
        let desc = match tag {
            ROOT if rest.is_empty() => Self::Root,
            BY_REF if rest.is_empty() => Self::ByRef,
            OWNER => Self::Owner(name()?),
            REF_OWNER => Self::RefOwner(name()?),
            REF_REPO if rest.len() == 8 => Self::RefRepo(u64_at(0)?),
            HEAD if rest.len() == 8 => Self::Head(u64_at(0)?),
            COMMIT if rest.len() == 29 => Self::Commit {
                repo: u64_at(0)?,
                commit: oid_at(8)?,
                pinned: match rest[28] {
                    0 => false,
                    1 => true,
                    _ => return None,
                },
            },
            PATH if rest.len() == 62 && rest[9] < 8 => Self::Path {
                repo: u64_at(0)?,
                kind: kind_from(rest[8])?,
                under_head: rest[9] & 1 != 0,
                in_head: rest[9] & 2 != 0,
                pinned: rest[9] & 4 != 0,
                commit_time: u32_at(10)?,
                path: u64_at(14)?,
                oid: oid_at(22)?,
                aux: oid_at(42)?,
            },
            _ => return None,
        };
        Some(desc)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_fits() {
        let a = Oid::from_bytes(&[7; 20]).unwrap();
        let b = Oid::from_bytes(&[9; 20]).unwrap();
        let all = [
            Desc::Root,
            Desc::ByRef,
            Desc::Owner("octo".into()),
            Desc::RefOwner("x".repeat(39)),
            Desc::RefRepo(42),
            Desc::Head(u64::MAX),
            Desc::Commit {
                repo: 1,
                commit: a,
                pinned: true,
            },
            Desc::Path {
                repo: 3,
                kind: EntryKind::Executable,
                under_head: true,
                in_head: true,
                pinned: true,
                commit_time: u32::MAX,
                path: 11,
                oid: a,
                aux: b,
            },
        ];
        for desc in all {
            let raw = desc.encode().unwrap();
            assert!(raw.len() <= MAX_LEN);
            assert_eq!(Desc::decode(&raw), Some(desc));
        }
        assert_eq!(Desc::decode(&[]), None);
        assert_eq!(Desc::decode(&[PATH, 1, 2]), None);
        assert_eq!(Desc::decode(&[99]), None);
        assert_eq!(Desc::Owner("o".repeat(64)).encode(), None);
    }

    #[test]
    fn path_hashes_differ_by_place() {
        let a = path_hash(path_hash(0, b"a"), b"x");
        let b = path_hash(path_hash(0, b"b"), b"x");
        assert_ne!(a, b);
        assert_eq!(a, path_hash(path_hash(0, b"a"), b"x"));
        assert_eq!(handle_time(-5), 0);
        assert_eq!(handle_time(i64::MAX), u32::MAX);
    }

    #[test]
    fn repo_ids_ignore_case() {
        let a: RepoKey = "Octo/Hello".parse().unwrap();
        let b: RepoKey = "octo/hello".parse().unwrap();
        assert_eq!(repo_id(&a), repo_id(&b));
    }
}
