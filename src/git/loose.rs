//! Bounded loose-object IO and complete, hash-verified commit tree validation.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use flate2::{Decompress, FlushDecompress, Status};
use git2::{ObjectType, Oid};
use sha1::{Digest, Sha1};

use super::GitError;
use super::operation::{Limits, Work};

pub(crate) const BUFFER: usize = 64 * 1024;
const MAX_LOOSE_HEADER: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    Commit,
    Tree,
    Blob,
    Tag,
}

impl Kind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Commit => "commit",
            Self::Tree => "tree",
            Self::Blob => "blob",
            Self::Tag => "tag",
        }
    }

    pub fn from_pack(code: u8) -> Result<Self, GitError> {
        match code {
            1 => Ok(Self::Commit),
            2 => Ok(Self::Tree),
            3 => Ok(Self::Blob),
            4 => Ok(Self::Tag),
            _ => Err(protocol("invalid object type")),
        }
    }

    pub fn check_size(self, size: u64, limits: &Limits) -> Result<(), GitError> {
        let max = if self == Self::Blob {
            limits.max_object_bytes
        } else {
            limits
                .max_metadata_object_bytes
                .min(limits.max_object_bytes)
        };
        if size > max {
            return Err(GitError::ResourceLimit(format!(
                "{} object is {size} bytes (limit {max})",
                self.name()
            )));
        }
        Ok(())
    }
}

pub(crate) fn protocol(message: impl Into<String>) -> GitError {
    GitError::Protocol(message.into())
}

pub(crate) fn object_path(repo: &Path, oid: Oid) -> PathBuf {
    let hex = oid.to_string();
    repo.join("objects").join(&hex[..2]).join(&hex[2..])
}

pub(crate) fn marker_path(repo: &Path, oid: Oid) -> PathBuf {
    repo.join("ghfs-complete").join(oid.to_string())
}

/// Inflate exactly one zlib stream, retaining the next pack entry in `input`.
/// A low-level decoder lets us require StreamEnd, including the Adler checksum;
/// an EOF from a convenience Read adapter alone does not establish that.
pub(crate) fn inflate<R: BufRead, W: Write>(
    input: &mut R,
    output: &mut W,
    expected: Option<u64>,
    max: u64,
    work: &Work,
) -> Result<u64, GitError> {
    let mut z = Decompress::new(true);
    let mut buffer = [0u8; BUFFER];
    let mut written = 0u64;
    loop {
        work.check()?;
        let bytes = input.fill_buf()?;
        let before_in = z.total_in();
        let before_out = z.total_out();
        let status = z
            .decompress(bytes, &mut buffer, FlushDecompress::None)
            .map_err(|e| protocol(format!("invalid zlib stream: {e}")))?;
        let consumed = (z.total_in() - before_in) as usize;
        let produced = (z.total_out() - before_out) as usize;
        input.consume(consumed);
        written = written
            .checked_add(produced as u64)
            .ok_or_else(|| protocol("inflated length overflow"))?;
        if written > max || expected.is_some_and(|size| written > size) {
            return Err(protocol("inflated stream exceeds declared size"));
        }
        output.write_all(&buffer[..produced])?;
        if status == Status::StreamEnd {
            if expected.is_some_and(|size| written != size) {
                return Err(protocol("inflated stream is shorter than declared size"));
            }
            return Ok(written);
        }
        if consumed == 0 && produced == 0 {
            return Err(protocol("truncated or stalled zlib stream"));
        }
    }
}

struct LooseSink<'a, W> {
    output: W,
    header: Vec<u8>,
    parsed: Option<(Kind, u64)>,
    payload: u64,
    hash: Sha1,
    limits: &'a Limits,
    error: Option<GitError>,
}

impl<'a, W: Write> LooseSink<'a, W> {
    fn new(output: W, limits: &'a Limits) -> Self {
        Self {
            output,
            header: Vec::with_capacity(MAX_LOOSE_HEADER),
            parsed: None,
            payload: 0,
            hash: Sha1::new(),
            limits,
            error: None,
        }
    }

    fn accept(&mut self, mut bytes: &[u8]) -> Result<(), GitError> {
        if self.parsed.is_none() {
            let end = bytes.iter().position(|b| *b == 0);
            let header_len = end.map_or(bytes.len(), |n| n + 1);
            if self.header.len() + header_len > MAX_LOOSE_HEADER {
                return Err(protocol("loose header too long"));
            }
            self.header.extend_from_slice(&bytes[..header_len]);
            bytes = &bytes[header_len..];
            if end.is_none() {
                return Ok(());
            }
            let text = std::str::from_utf8(&self.header[..self.header.len() - 1])
                .map_err(|_| protocol("invalid loose header"))?;
            let (kind, size) = text
                .split_once(' ')
                .ok_or_else(|| protocol("invalid loose header"))?;
            let kind = match kind {
                "commit" => Kind::Commit,
                "tree" => Kind::Tree,
                "blob" => Kind::Blob,
                "tag" => Kind::Tag,
                _ => return Err(protocol("invalid loose object kind")),
            };
            if size.is_empty()
                || !size.bytes().all(|b| b.is_ascii_digit())
                || (size.len() > 1 && size.starts_with('0'))
            {
                return Err(protocol("noncanonical loose object size"));
            }
            let size = size
                .parse::<u64>()
                .map_err(|_| protocol("loose size overflow"))?;
            kind.check_size(size, self.limits)?;
            self.hash.update(&self.header);
            self.parsed = Some((kind, size));
        }
        self.payload = self
            .payload
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| protocol("loose payload length overflow"))?;
        if self.payload > self.parsed.expect("parsed header").1 {
            return Err(protocol("loose payload exceeds declared size"));
        }
        self.hash.update(bytes);
        self.output.write_all(bytes)?;
        Ok(())
    }

    fn finish(mut self, expected: Oid) -> Result<(Kind, u64), GitError> {
        if let Some(e) = self.error.take() {
            return Err(e);
        }
        let (kind, size) = self
            .parsed
            .ok_or_else(|| protocol("missing loose header"))?;
        if self.payload != size {
            return Err(protocol("truncated loose payload"));
        }
        if self.hash.finalize().as_slice() != expected.as_bytes() {
            return Err(protocol(format!("loose object {expected} has wrong hash")));
        }
        self.output.flush()?;
        Ok((kind, size))
    }
}

impl<W: Write> Write for LooseSink<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self.accept(bytes) {
            Ok(()) => Ok(bytes.len()),
            Err(e) => {
                self.error = Some(e);
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid loose object",
                ))
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

pub(crate) fn read_to<W: Write>(
    repo: &Path,
    oid: Oid,
    output: W,
    work: &Work,
    limits: &Limits,
) -> Result<(Kind, u64), GitError> {
    work.check()?;
    let file = File::open(object_path(repo, oid)).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            GitError::MissingObject(oid)
        } else {
            e.into()
        }
    })?;
    let mut input = BufReader::with_capacity(BUFFER, file);
    let mut sink = LooseSink::new(output, limits);
    let result = inflate(
        &mut input,
        &mut sink,
        None,
        limits
            .max_object_bytes
            .saturating_add(MAX_LOOSE_HEADER as u64),
        work,
    );
    if let Some(e) = sink.error.take() {
        return Err(e);
    }
    result?;
    if !input.fill_buf()?.is_empty() {
        return Err(protocol("trailing bytes after loose zlib stream"));
    }
    sink.finish(oid)
}

/// Kind and size from a loose object's header, inflating only its first
/// bytes. Bounded on any input, unlike libgit2's reader, which can spin on a
/// truncated stream.
pub(crate) fn read_header(repo: &Path, oid: Oid) -> Result<(Kind, u64), GitError> {
    let mut file = File::open(object_path(repo, oid)).map_err(|e| {
        if e.kind() == io::ErrorKind::NotFound {
            GitError::MissingObject(oid)
        } else {
            e.into()
        }
    })?;
    let mut input = Vec::with_capacity(1024);
    (&mut file).take(1024).read_to_end(&mut input)?;
    let mut output = [0u8; MAX_LOOSE_HEADER];
    let mut inflater = Decompress::new(true);
    // A failed or partial inflate simply leaves the header incomplete.
    let _ = inflater.decompress(&input, &mut output, FlushDecompress::Sync);
    let produced = inflater.total_out() as usize;
    let limits = Limits {
        max_object_bytes: u64::MAX,
        max_metadata_object_bytes: u64::MAX,
        ..Limits::default()
    };
    let mut sink = LooseSink::new(io::sink(), &limits);
    let end = output[..produced]
        .iter()
        .position(|b| *b == 0)
        .ok_or_else(|| protocol("truncated loose header"))?;
    sink.accept(&output[..=end])?;
    Ok(sink.parsed.expect("complete header was parsed"))
}

/// A whole small blob, hash-verified. Larger blobs must be streamed.
pub(crate) fn read_small_blob(repo: &Path, oid: Oid, max: u64) -> Result<Vec<u8>, GitError> {
    match read_header(repo, oid)? {
        (Kind::Blob, size) if size > max => {
            return Err(GitError::ResourceLimit(
                "blob requires streaming materialization".into(),
            ));
        }
        (Kind::Blob, _) => {}
        (kind, _) => return Err(protocol(format!("{oid} is a {}, not a blob", kind.name()))),
    }
    let mut data = Vec::new();
    let work = Work::new(std::time::Duration::from_secs(60));
    let (kind, _) = read_to(repo, oid, &mut data, &work, &Limits::default())?;
    if kind != Kind::Blob {
        return Err(protocol(format!("{oid} is not a blob")));
    }
    Ok(data)
}

/// Loose writes aren't fsynced, so a crash can leave an object that "exists"
/// but is truncated or corrupt. Drop it and report it missing so callers
/// refetch it instead of failing forever.
pub(crate) fn drop_if_corrupt<T>(
    repo: &Path,
    oid: Oid,
    result: Result<T, GitError>,
) -> Result<T, GitError> {
    match result {
        Err(GitError::Protocol(reason)) => {
            log::warn!("dropping corrupt loose object {oid}: {reason}");
            match fs::remove_file(object_path(repo, oid)) {
                Ok(()) => Err(GitError::MissingObject(oid)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Err(GitError::MissingObject(oid)),
                Err(e) => Err(e.into()),
            }
        }
        other => other,
    }
}

pub(crate) fn copy_blob(
    repo: &Path,
    oid: Oid,
    out: File,
    work: &Work,
    limits: &Limits,
) -> Result<(), GitError> {
    let (kind, _) = drop_if_corrupt(repo, oid, read_to(repo, oid, out, work, limits))?;
    if kind != Kind::Blob {
        return Err(protocol(format!("{oid} is not a blob")));
    }
    Ok(())
}

pub(crate) fn hash_payload(kind: Kind, path: &Path, work: &Work) -> Result<Oid, GitError> {
    let mut input = File::open(path)?;
    let size = input.metadata()?.len();
    let mut hash = Sha1::new();
    hash.update(format!("{} {size}\0", kind.name()).as_bytes());
    let mut buffer = [0u8; BUFFER];
    loop {
        work.check()?;
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(Oid::from_bytes(&hash.finalize())?)
}

/// The caller must bound concurrency. Only metadata objects enter libgit2, and
/// their kind, size and canonical hash are checked before parsing them there.
pub(crate) fn validate_commit(
    repo: &Path,
    requested: Oid,
    work: &Work,
    limits: &Limits,
) -> Result<(Oid, i64), GitError> {
    let repository = git2::Repository::open_bare(repo)?;
    let mut oid = requested;
    let mut tags = HashSet::new();
    let (root, time) = loop {
        work.check()?;
        if tags.len() > limits.max_delta_depth as usize || !tags.insert(oid) {
            return Err(GitError::ResourceLimit("tag chain depth/cycle".into()));
        }
        let (kind, _) = read_to(repo, oid, io::sink(), work, limits)?;
        match kind {
            Kind::Tag => {
                oid = repository.find_tag(oid)?.target_id();
            }
            Kind::Commit => {
                let commit = repository.find_commit(oid)?;
                break (commit.tree_id(), commit.time().seconds());
            }
            _ => return Err(GitError::MissingObject(requested)),
        }
    };
    let mut visited = HashSet::new();
    let mut pending = vec![root];
    visited.insert(root);
    while let Some(oid) = pending.pop() {
        work.check()?;
        let (kind, _) = read_to(repo, oid, io::sink(), work, limits)?;
        if kind != Kind::Tree {
            return Err(protocol(format!("{oid} is not a tree")));
        }
        let tree = repository.find_tree(oid)?;
        for entry in tree.iter() {
            work.check()?;
            // Only tree edges: blobs may be absent, and gitlinks name commits in
            // another repository which are never part of this closure.
            if entry.kind() == Some(ObjectType::Tree) && !visited.contains(&entry.id()) {
                if visited.len() >= limits.max_objects {
                    return Err(GitError::ResourceLimit("tree closure count".into()));
                }
                visited
                    .try_reserve(1)
                    .map_err(|_| GitError::ResourceLimit("tree closure allocation".into()))?;
                pending
                    .try_reserve(1)
                    .map_err(|_| GitError::ResourceLimit("tree closure allocation".into()))?;
                visited.insert(entry.id());
                pending.push(entry.id());
            }
        }
    }
    Ok((root, time))
}

pub(crate) fn mark_complete(repo: &Path, oid: Oid, work: &Work) -> Result<(), GitError> {
    work.check()?;
    let path = marker_path(repo, oid);
    let directory = path.parent().expect("marker parent");
    fs::create_dir_all(directory)?;
    let mut temp = tempfile::NamedTempFile::new_in(directory)?;
    temp.write_all(b"1\n")?;
    temp.flush()?;
    work.check()?;
    temp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::ZlibEncoder};
    use std::time::Duration;

    fn work() -> Work {
        Work::new(Duration::from_secs(30))
    }

    fn compressed(bytes: &[u8]) -> Vec<u8> {
        let mut z = ZlibEncoder::new(Vec::new(), Compression::fast());
        z.write_all(bytes).unwrap();
        z.finish().unwrap()
    }

    fn install(repo: &Path, bytes: &[u8]) -> Oid {
        let oid = Oid::from_bytes(&Sha1::digest(bytes)).unwrap();
        let path = object_path(repo, oid);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, compressed(bytes)).unwrap();
        oid
    }

    #[test]
    fn validates_canonical_header_payload_hash_and_stream_end() {
        let root = tempfile::tempdir().unwrap();
        let good = install(root.path(), b"blob 3\0abc");
        let mut output = Vec::new();
        assert_eq!(
            read_to(root.path(), good, &mut output, &work(), &Limits::default()).unwrap(),
            (Kind::Blob, 3)
        );
        assert_eq!(output, b"abc");
        let empty = install(root.path(), b"blob 0\0");
        assert_eq!(
            read_to(root.path(), empty, io::sink(), &work(), &Limits::default()).unwrap(),
            (Kind::Blob, 0)
        );
        for raw in [
            b"blob 03\0abc".as_slice(),
            b"blob -1\0".as_slice(),
            b"blob 1\0abc".as_slice(),
            b"blob 4\0abc".as_slice(),
            b"blob 18446744073709551616\0".as_slice(),
            b"strange 0\0".as_slice(),
            b"blob 0".as_slice(),
            &[b'x'; 65],
        ] {
            let oid = install(root.path(), raw);
            assert!(read_to(root.path(), oid, io::sink(), &work(), &Limits::default()).is_err());
        }
        let path = object_path(root.path(), good);
        fs::write(&path, compressed(b"blob 3\0xyz")).unwrap();
        assert!(matches!(
            read_to(root.path(), good, io::sink(), &work(), &Limits::default()),
            Err(GitError::Protocol(_))
        ));
        let mut bytes = compressed(b"blob 3\0abc");
        bytes.push(0);
        fs::write(&path, bytes).unwrap();
        assert!(matches!(
            read_to(root.path(), good, io::sink(), &work(), &Limits::default()),
            Err(GitError::Protocol(_))
        ));
        let mut bytes = compressed(b"blob 3\0abc");
        bytes.truncate(bytes.len() - 4);
        fs::write(&path, bytes).unwrap();
        assert!(read_to(root.path(), good, io::sink(), &work(), &Limits::default()).is_err());
    }

    #[test]
    fn rejects_large_metadata_before_delivering_payload_and_honors_cancellation() {
        let root = tempfile::tempdir().unwrap();
        let oid = install(root.path(), b"tree 4\0abcd");
        let mut output = Vec::new();
        assert!(matches!(
            read_to(
                root.path(),
                oid,
                &mut output,
                &work(),
                &Limits {
                    max_metadata_object_bytes: 3,
                    ..Limits::default()
                }
            ),
            Err(GitError::ResourceLimit(_))
        ));
        assert!(output.is_empty());
        struct CancelWriter<'a> {
            work: &'a Work,
            written: usize,
        }
        impl Write for CancelWriter<'_> {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.written += bytes.len();
                self.work.token.cancel();
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let raw = vec![b'a'; 4 * BUFFER];
        let data = compressed(&raw);
        let mut input = BufReader::new(data.as_slice());
        let work = work();
        let mut output = CancelWriter {
            work: &work,
            written: 0,
        };
        assert!(matches!(
            inflate(
                &mut input,
                &mut output,
                Some(raw.len() as u64),
                raw.len() as u64,
                &work
            ),
            Err(GitError::Cancelled)
        ));
        assert!(output.written <= BUFFER);
    }

    #[test]
    fn adjacent_zlib_streams_are_not_consumed_together() {
        let first = compressed(b"first");
        let second = compressed(b"second");
        let bytes = [first, second].concat();
        let mut input = BufReader::new(bytes.as_slice());
        let mut output = Vec::new();
        inflate(&mut input, &mut output, Some(5), 5, &work()).unwrap();
        assert_eq!(output, b"first");
        output.clear();
        inflate(&mut input, &mut output, Some(6), 6, &work()).unwrap();
        assert_eq!(output, b"second");
        assert!(input.fill_buf().unwrap().is_empty());
    }

    #[test]
    fn copy_blob_streams_and_checks_kind() {
        let root = tempfile::tempdir().unwrap();
        let raw = [b"blob 131072\0".as_slice(), &vec![b'x'; 2 * BUFFER]].concat();
        let oid = install(root.path(), &raw);
        let output = tempfile::NamedTempFile::new().unwrap();
        copy_blob(
            root.path(),
            oid,
            output.reopen().unwrap(),
            &work(),
            &Limits::default(),
        )
        .unwrap();
        assert_eq!(
            fs::metadata(output.path()).unwrap().len(),
            (2 * BUFFER) as u64
        );
        let tree = install(root.path(), b"tree 0\0");
        assert!(
            copy_blob(
                root.path(),
                tree,
                output.reopen().unwrap(),
                &work(),
                &Limits::default()
            )
            .is_err()
        );
        #[cfg(target_os = "linux")]
        {
            let full = File::options().write(true).open("/dev/full").unwrap();
            assert!(matches!(
                copy_blob(root.path(), oid, full, &work(), &Limits::default()),
                Err(GitError::Io(_))
            ));
        }
    }
}
