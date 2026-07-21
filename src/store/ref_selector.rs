//! By-ref path codec and virtual-node shape.
//!
//! The filesystem exposes two parallel roots:
//!
//! ```text
//! /<owner>/<repo>/...                          # default branch (HEAD) alias
//! /by-ref/<owner>/<repo>/<encoded-ref>/...     # branch | tag | commit OID
//! ```
//!
//! A `<ref>` occupies a *single* path component, so any `/` inside a ref name
//! (e.g. `feature/new-cache`, `refs/heads/main`) is percent-encoded as `%2F`
//! and any literal `%` as `%25`. This keeps the path hierarchy unambiguous:
//! the FS splits on `/` first, then decodes each component.
//!
//! `/by-ref/<owner>/<repo>` is a *directory* whose `readdir` lists the
//! repo's encoded branch and tag names; `/by-ref/<owner>/<repo>/<ref>` is the
//! selected commit's root. The top-level `/<owner>/<repo>` is instead the
//! default-branch (HEAD) commit root — the two repo nodes differ in kind.
//!
//! An owner literally named `by-ref` remains reachable as
//! `/by-ref/by-ref/...` because `by-ref` is only reserved at the *root*
//! level as the first component.

use crate::types::{Owner, RepoKey};

/// Reserved first path component that selects the by-ref namespace.
pub const BY_REF_ROOT: &str = "by-ref";

/// Encode a raw ref name into a single path component.
///
/// `/` -> `%2F`, `%` -> `%25`. Everything else passes through. The result is
/// safe to use as one filesystem path segment and contains no `/` bytes.
pub fn encode_ref(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        match b {
            b'%' => out.push_str("%25"),
            b'/' => out.push_str("%2F"),
            // Path separators and null/control bytes never appear in a valid
            // git ref, but if they did they'd break the FS layer; escape them
            // defensively rather than reject, so equals-decode round-trips.
            0 => out.push_str("%00"),
            b if b < 0x20 => out.push_str(&format!("%{b:02X}")),
            _ => out.push(b as char),
        }
    }
    out
}

/// Decode an encoded path component back into a raw ref name.
///
/// Inverts [`encode_ref`]. Returns `None` if the input contains a malformed
/// percent escape (bad hex digits, a dangling `%`, or `%00` reappearing as a
/// null byte — refs never legitimately contain these).
pub fn decode_ref(component: &[u8]) -> Option<String> {
    let mut out = Vec::with_capacity(component.len());
    let mut i = 0;
    while i < component.len() {
        let b = component[i];
        if b != b'%' {
            out.push(b);
            i += 1;
            continue;
        }
        // Need two hex digits after `%`.
        if i + 2 >= component.len() {
            return None;
        }
        let hi = hex_digit(component[i + 1])?;
        let lo = hex_digit(component[i + 2])?;
        let decoded = (hi << 4) | lo;
        if decoded == 0 {
            return None;
        }
        out.push(decoded);
        i += 3;
    }
    String::from_utf8(out).ok()
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// A resolved ref selector paired with the commit it points at. Carried by
/// virtual nodes so the FS layer never re-resolves a moving ref mid-traversal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RefSelector {
    /// The raw, decoded ref name as understood by git (e.g.
    /// `feature/new-cache`, `v1.0`, or a full OID string).
    pub raw: String,
    /// The commit OID the selector resolved to at pin time.
    pub commit: String,
}

/// The virtual-node shape composed over the tree reader.
///
/// These are the inode identities the FS layer allocates. Everything below a
/// [`RefSelector`] (or the default-branch [`RepoDefault`]) is a real git path
/// resolved against the [`crate::store::tree::TreeReader`] and pinned to the
/// commit captured here.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VirtualNode {
    /// The filesystem root. `readdir` lists cached owners plus the reserved
    /// `by-ref` entry.
    Root,
    /// An owner directory at the top level. `readdir` lists cached repos.
    Owner(Owner),
    /// The default-branch repo root: `/<owner>/<repo>` resolves HEAD.
    RepoDefault { key: RepoKey, commit: String },
    /// The reserved `/by-ref` entry.
    ByRefRoot,
    /// `/by-ref/<owner>`: `readdir` lists repos.
    RefOwner(Owner),
    /// `/by-ref/<owner>/<repo>`: a *directory* whose children are encoded ref
    /// selectors. `readdir` lists encoded branch + tag names.
    RefRepo(RepoKey),
    /// `/by-ref/<owner>/<repo>/<ref>`: the selected commit's root.
    RefSelector { key: RepoKey, selector: RefSelector },
}

impl VirtualNode {
    /// Whether this node is one of the by-ref namespace roots (Root or
    /// ByRefRoot), used by the FS to decide handler dispatch on lookup.
    pub fn is_root_level(&self) -> bool {
        matches!(self, Self::Root | Self::ByRefRoot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decodes_slash_and_percent() {
        let raw = "feature/new-cache";
        assert_eq!(encode_ref(raw), "feature%2Fnew-cache");
        assert_eq!(decode_ref(b"feature%2Fnew-cache").unwrap(), raw);
    }

    #[test]
    fn encode_handles_full_ref() {
        let raw = "refs/heads/release";
        assert_eq!(encode_ref(raw), "refs%2Fheads%2Frelease");
        assert_eq!(decode_ref(b"refs%2Fheads%2Frelease").unwrap(), raw);
    }

    #[test]
    fn encode_neutral_for_plain_names() {
        assert_eq!(encode_ref("main"), "main");
        assert_eq!(encode_ref("v1.2.0"), "v1.2.0");
        assert_eq!(encode_ref("abc123def"), "abc123def");
    }

    #[test]
    fn escape_percent_literally() {
        let raw = "weird%name";
        assert_eq!(encode_ref(raw), "weird%25name");
        assert_eq!(decode_ref(b"weird%25name").unwrap(), raw);
    }

    #[test]
    fn roundtrip_mixed() {
        for raw in ["main", "a/b/c", "refs/tags/v1.0", "100%done", "x%2Fy"] {
            let enc = encode_ref(raw);
            assert!(
                !enc.contains('/'),
                "encoded ref must not contain a literal slash: {enc}"
            );
            assert_eq!(decode_ref(enc.as_bytes()).unwrap(), raw, "raw={raw}");
        }
    }

    #[test]
    fn decode_rejects_malformed() {
        assert!(decode_ref(b"foo%").is_none()); // dangling
        assert!(decode_ref(b"foo%2").is_none()); // truncated
        assert!(decode_ref(b"foo%2G").is_none()); // bad hex
        assert!(decode_ref(b"foo%00").is_none()); // null reappears
        assert!(decode_ref(b"\xff").is_none()); // invalid utf-8
    }
}
