//! Object-backed revision store.
//!
//! Replaces the legacy shallow-clone + worktree-generation cache with a
//! blobless partial clone serving directory structures directly from git tree
//! objects and hydrating individual file blobs on demand. The store is
//! backend-agnostic: the FUSE and NFS adapters in [`crate::fs`] share the
//! same store implementation.

pub mod git;

pub use git::{GitCli, GitError, resolve_head, resolve_revision};

/// Best-effort redaction of embedded credentials from a git error string.
///
/// URLs of the form `https://user:token@host/...` leak secrets into stderr
/// when a clone or fetch fails. This rewrites the `user:pass@` userinfo
/// segment to `***` so logged errors stay safe.
pub(crate) fn redact_creds(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'h'
            && s[i..].starts_with("https://")
            && let Some(rest_start) = s[i + 8..].find('@').map(|p| i + 8 + p + 1)
        {
            // Locate the host boundary we'll resume output from.
            out.push_str("https://***@");
            i = rest_start;
        } else {
            out.push(bytes[i] as char);
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
