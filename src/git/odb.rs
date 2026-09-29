//! Local object reads through libgit2. Everything here is blocking but only
//! touches the local object database, never the network; callers run it on
//! tokio's blocking pool.

use git2::{ErrorCode, ObjectType, Oid, Repository};

use super::GitError;

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
    static EXTENSIONS: std::sync::Once = std::sync::Once::new();
    EXTENSIONS.call_once(|| {
        // Our repos are partial clones; libgit2 refuses unknown repository
        // extensions unless told they're fine. It only reads local objects,
        // which is all partialclone changes the meaning of.
        // SAFETY: called once, before any repository is opened.
        if let Err(e) = unsafe { git2::opts::set_extensions(&["partialclone"]) } {
            log::warn!("failed to register partialclone extension: {e}");
        }
    });
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

/// Size of a blob if it's in the local object database.
pub fn blob_size(repo: &Repository, blob: Oid) -> Result<Option<u64>, GitError> {
    match repo.odb()?.read_header(blob) {
        Ok((size, ObjectType::Blob)) => Ok(Some(size as u64)),
        Ok((_, other)) => Err(GitError::Command(format!(
            "{blob} is a {other}, not a blob"
        ))),
        Err(e) if e.code() == ErrorCode::NotFound => Ok(None),
        Err(e) => Err(e.into()),
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

pub fn read_blob(repo: &Repository, blob: Oid) -> Result<Vec<u8>, GitError> {
    let blob_obj = repo.find_blob(blob).map_err(|e| missing(e, blob))?;
    Ok(blob_obj.content().to_vec())
}

/// Number of pack files, used to decide when to repack.
pub fn pack_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir.join("objects/pack"))
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == "pack"))
                .count()
        })
        .unwrap_or(0)
}
