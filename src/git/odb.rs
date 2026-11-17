//! Local object reads (trees and commits through libgit2, blobs through our
//! bounded loose reader). Everything here is blocking but only
//! touches the local object database, never the network; callers run it on
//! tokio's blocking pool.

use git2::{ErrorCode, Oid, Repository};

use super::GitError;
use super::loose::{self, Kind};

/// Kind of a git tree entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EntryKind {
    Tree,
    Blob,
    Executable,
    Symlink,
    /// Submodule pointer. Shown as an empty file.
    Gitlink,
}

impl EntryKind {
    fn from_mode(mode: i32) -> Option<Self> {
        match mode {
            0o040000 => Some(Self::Tree),
            0o100644 | 0o100664 => Some(Self::Blob),
            0o100755 => Some(Self::Executable),
            0o120000 => Some(Self::Symlink),
            0o160000 => Some(Self::Gitlink),
            _ => None,
        }
    }

    /// Whether the entry's content lives in a blob.
    pub fn has_blob(self) -> bool {
        matches!(self, Self::Blob | Self::Executable | Self::Symlink)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEntry {
    pub name: Vec<u8>,
    pub oid: Oid,
    pub kind: EntryKind,
}

fn missing(e: git2::Error, oid: Oid) -> GitError {
    if e.code() == ErrorCode::NotFound {
        GitError::MissingObject(oid)
    } else {
        GitError::Libgit2(e)
    }
}

pub fn open(dir: &std::path::Path) -> Result<Repository, GitError> {
    Ok(Repository::open_bare(dir)?)
}

/// Root tree and committer time of a commit (peeling tags), if present locally.
pub fn commit_info(repo: &Repository, commit: Oid) -> Result<(Oid, i64), GitError> {
    let obj = repo
        .find_object(commit, None)
        .map_err(|e| missing(e, commit))?;
    // An id that names a tree/blob/tag-to-non-commit is "no such commit".
    let commit = obj
        .peel_to_commit()
        .map_err(|_| GitError::MissingObject(commit))?;
    let tree = commit.tree_id();
    // A commit whose root tree is absent is a half-finished fetch.
    if !repo.odb()?.exists(tree) {
        return Err(GitError::MissingObject(tree));
    }
    Ok((tree, commit.time().seconds()))
}

pub fn read_tree(repo: &Repository, tree: Oid) -> Result<Vec<TreeEntry>, GitError> {
    let tree_obj = repo.find_tree(tree).map_err(|e| missing(e, tree))?;
    let mut out = Vec::with_capacity(tree_obj.len());
    for entry in tree_obj.iter() {
        let Some(kind) = EntryKind::from_mode(entry.filemode()) else {
            continue;
        };
        out.push(TreeEntry {
            name: entry.name_bytes().to_vec(),
            oid: entry.id(),
            kind,
        });
    }
    Ok(out)
}

/// Size of a blob if it's in the local object database. A corrupt local
/// copy is dropped and reported absent, so it gets refetched.
pub fn blob_size(repo: &Repository, blob: Oid) -> Result<Option<u64>, GitError> {
    let dir = repo.path();
    match loose::drop_if_corrupt(dir, blob, loose::read_header(dir, blob)) {
        Ok((Kind::Blob, size)) => Ok(Some(size)),
        Ok((other, _)) => Err(GitError::Command(format!(
            "{blob} is a {}, not a blob",
            other.name()
        ))),
        Err(GitError::MissingObject(_)) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Which of `oids` are missing from the local object database.
pub fn missing_objects(repo: &Repository, oids: &[Oid]) -> Result<Vec<Oid>, GitError> {
    let odb = repo.odb()?;
    Ok(oids
        .iter()
        .copied()
        .filter(|oid| !odb.exists(*oid))
        .collect())
}

/// Contents of a small (at most 8 MiB) local blob, read with our bounded,
/// hash-verifying loose reader rather than libgit2. A corrupt local copy is
/// dropped and reported missing, so callers refetch it.
pub fn read_blob(repo: &Repository, blob: Oid) -> Result<Vec<u8>, GitError> {
    let dir = repo.path();
    loose::drop_if_corrupt(dir, blob, loose::read_small_blob(dir, blob, 8 << 20))
}
