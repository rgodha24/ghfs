//! `/by-ref/<owner>/<repo>/<ref>` path codec.
//!
//! A ref occupies one path component, so `/` inside a ref name is written as
//! `%2F` and a literal `%` as `%25`.

/// Reserved first path component that selects the by-ref namespace.
pub const BY_REF_ROOT: &str = "by-ref";

pub fn encode_ref(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        match b {
            b'%' => out.push_str("%25"),
            b'/' => out.push_str("%2F"),
            b if b < 0x20 => out.push_str(&format!("%{b:02X}")),
            _ => out.push(b as char),
        }
    }
    out
}

/// Inverse of [`encode_ref`]. `None` for malformed escapes or non-UTF-8.
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

/// Whether `name` can possibly be a ref selector. Git refuses ref components
/// starting with `.`, so this cheaply rejects macOS `._*` / `.DS_Store` probes
/// without a network round trip.
pub fn plausible_ref(name: &str) -> bool {
    !name.is_empty()
        && !name.ends_with('/')
        && !name.ends_with(".lock")
        && !name.contains("..")
        && name
            .split('/')
            .all(|c| !c.is_empty() && !c.starts_with('.'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        for raw in ["main", "a/b/c", "refs/tags/v1.0", "100%done", "x%2Fy"] {
            let enc = encode_ref(raw);
            assert!(!enc.contains('/'), "{enc}");
            assert_eq!(decode_ref(enc.as_bytes()).unwrap(), raw);
        }
        assert_eq!(encode_ref("feature/new-cache"), "feature%2Fnew-cache");
    }

    #[test]
    fn decode_rejects_malformed() {
        assert!(decode_ref(b"foo%").is_none());
        assert!(decode_ref(b"foo%2").is_none());
        assert!(decode_ref(b"foo%2G").is_none());
        assert!(decode_ref(b"foo%00").is_none());
        assert!(decode_ref(b"\xff").is_none());
    }

    #[test]
    fn rejects_dot_components() {
        assert!(plausible_ref("main"));
        assert!(plausible_ref("fix/thing"));
        assert!(!plausible_ref("._main"));
        assert!(!plausible_ref("._fix/thing"));
        assert!(!plausible_ref(".DS_Store"));
        assert!(!plausible_ref("a/.b"));
        assert!(!plausible_ref("x.lock"));
    }
}
