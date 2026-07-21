//! Object-backed revision store.
//!
//! Replaces the legacy shallow-clone + worktree-generation cache with a
//! blobless partial clone serving directory structures directly from git tree
//! objects and hydrating individual file blobs on demand. The store is
//! backend-agnostic: the FUSE and NFS adapters in [`crate::fs`] share the
//! same store implementation.

pub mod blob;
pub mod git;
pub mod ref_selector;
pub mod store;
pub mod tree;

pub use blob::{BlobCache, BlobError, Hydrator};
pub use git::{GitCli, GitError, resolve_head, resolve_revision};
pub use ref_selector::{BY_REF_ROOT, RefSelector, VirtualNode, decode_ref, encode_ref};
pub use store::{Store, StoreError};
pub use tree::{EntryKind, TreeCache, TreeEntry, TreeError, TreeReader};

/// Best-effort redaction of embedded credentials from a git error string.
///
/// URLs of the form `https://user:token@host/...` leak secrets into stderr
/// when a clone or fetch fails. This rewrites the `user:pass@` userinfo
/// segment to `***` so logged errors stay safe.
pub(crate) fn redact_creds(s: &str) -> String {
    // Scan for `https://` followed by `user:pass@`; replace the userinfo with
    // `***`. Everything else is copied verbatim.
    let needle = "https://";
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < s.len() {
        if s[i..].starts_with(needle)
            && let Some(at) = s[i + needle.len()..].find('@')
        {
            out.push_str("https://***@");
            i = i + needle.len() + at + 1;
        } else {
            // SAFETY: index `i` is a valid char boundary because we only ever
            // advance by whole-`start_with` chunks or by one byte (ASCII),
            // and the input is treated as bytes for the fast path.
            let c = s.as_bytes()[i] as char;
            out.push(c);
            i += 1;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_creds_replaces_userinfo() {
        assert_eq!(
            redact_creds("fatal: https://octo:token@github.com/x.git not found"),
            "fatal: https://***@github.com/x.git not found"
        );
    }

    #[test]
    fn redact_creds_leaves_clean_urls() {
        assert_eq!(
            redact_creds("https://github.com/octocat/Hello-World.git"),
            "https://github.com/octocat/Hello-World.git"
        );
    }
}
