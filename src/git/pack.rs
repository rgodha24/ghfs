//! Transfer-local pack ingestion. Payloads and delta instructions stay on disk;
//! only capped metadata and fixed copy/zlib buffers live in memory.

use std::collections::{HashMap, VecDeque};
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use flate2::{Compression, write::ZlibEncoder};
use git2::Oid;
use sha1::{Digest, Sha1};

use super::GitError;
use super::loose::{self, BUFFER, Kind, protocol};
use super::operation::{Budget, Limits, Reservation, Work};

const METADATA_BUDGET: usize = 32 << 20;
// Covers Entry, both dependency arrays, queue, and the pre-reserved identity
// table. No per-entry paths, map growth or open files are retained.
const METADATA_PER_ENTRY: usize = 256;

#[derive(Clone, Copy, Debug)]
enum Base {
    None(Kind),
    Offset(u64),
    Ref(Oid),
}

struct Entry {
    offset: u64,
    size: u64,
    kind: Option<Kind>,
    oid: Option<Oid>,
    depth: u32,
}

struct Charge {
    reservation: Reservation,
    total: u64,
    max: u64,
}

impl Charge {
    fn add(&mut self, bytes: u64) -> Result<(), GitError> {
        let next = self
            .total
            .checked_add(bytes)
            .ok_or_else(|| GitError::ResourceLimit("expanded staging length overflow".into()))?;
        if next > self.max {
            return Err(GitError::ResourceLimit("expanded staging bytes".into()));
        }
        self.reservation.grow(bytes)?;
        self.total = next;
        Ok(())
    }
}

struct ChargedWriter<'a, W> {
    output: W,
    charge: &'a mut Charge,
    error: Option<GitError>,
}

impl<W: Write> Write for ChargedWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if let Err(e) = self.charge.add(bytes.len() as u64) {
            self.error = Some(e);
            return Err(io::Error::other("staging limit"));
        }
        self.output.write_all(bytes)?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}

fn byte(input: &mut impl Read, work: &Work) -> Result<u8, GitError> {
    work.check()?;
    let mut b = [0];
    input.read_exact(&mut b)?;
    Ok(b[0])
}

/// Pack sizes start with four low bits and then little-endian seven-bit chunks.
/// checked_shl alone is insufficient: it does not check discarded high bits.
fn entry_header(input: &mut impl Read, offset: u64, work: &Work) -> Result<(Base, u64), GitError> {
    let mut b = byte(input, work)?;
    let code = (b >> 4) & 7;
    let mut size = u64::from(b & 15);
    let mut factor = 16u64;
    let mut bytes = 1;
    while b & 128 != 0 {
        if bytes >= 10 {
            return Err(protocol("pack entry size header too long"));
        }
        b = byte(input, work)?;
        let part = u64::from(b & 127)
            .checked_mul(factor)
            .ok_or_else(|| protocol("pack entry size overflow"))?;
        size = size
            .checked_add(part)
            .ok_or_else(|| protocol("pack entry size overflow"))?;
        bytes += 1;
        if b & 128 != 0 {
            factor = factor
                .checked_mul(128)
                .ok_or_else(|| protocol("pack entry size overflow"))?;
        }
    }
    let base = match code {
        1..=4 => Base::None(Kind::from_pack(code)?),
        6 => {
            let mut b = byte(input, work)?;
            let mut distance = u64::from(b & 127);
            let mut bytes = 1;
            while b & 128 != 0 {
                if bytes >= 10 {
                    return Err(protocol("OFS distance too long"));
                }
                b = byte(input, work)?;
                distance = distance
                    .checked_add(1)
                    .and_then(|n| n.checked_mul(128))
                    .and_then(|n| n.checked_add(u64::from(b & 127)))
                    .ok_or_else(|| protocol("OFS distance overflow"))?;
                bytes += 1;
            }
            if distance == 0 {
                return Err(protocol("zero OFS distance"));
            }
            Base::Offset(
                offset
                    .checked_sub(distance)
                    .ok_or_else(|| protocol("OFS distance before pack"))?,
            )
        }
        7 => {
            let mut oid = [0; 20];
            input.read_exact(&mut oid)?;
            Base::Ref(Oid::from_bytes(&oid)?)
        }
        _ => return Err(protocol("invalid pack entry kind")),
    };
    Ok((base, size))
}

fn varint(input: &mut impl Read, work: &Work) -> Result<u64, GitError> {
    let mut number = 0u64;
    let mut factor = 1u64;
    for _ in 0..10 {
        let b = byte(input, work)?;
        let component = u64::from(b & 127)
            .checked_mul(factor)
            .ok_or_else(|| protocol("delta varint overflow"))?;
        number = number
            .checked_add(component)
            .ok_or_else(|| protocol("delta varint overflow"))?;
        if b & 128 == 0 {
            return Ok(number);
        }
        factor = factor
            .checked_mul(128)
            .ok_or_else(|| protocol("delta varint overflow"))?;
    }
    Err(protocol("delta varint too long"))
}

fn copy_exact(
    input: &mut impl Read,
    output: &mut impl Write,
    mut size: u64,
    work: &Work,
) -> Result<(), GitError> {
    let mut buffer = [0; BUFFER];
    while size > 0 {
        work.check()?;
        let n = size.min(BUFFER as u64) as usize;
        input.read_exact(&mut buffer[..n])?;
        output.write_all(&buffer[..n])?;
        size -= n as u64;
    }
    Ok(())
}

fn apply_delta(
    base: &Path,
    instructions: &Path,
    output: &Path,
    kind: Kind,
    work: &Work,
    limits: &Limits,
    charge: &mut Charge,
) -> Result<u64, GitError> {
    let mut base = File::open(base)?;
    let base_size = base.metadata()?.len();
    let instruction_file = File::open(instructions)?;
    // Git's patch_delta requires DELTA_SIZE_MIN=4, including the two size
    // varints. Empty objects are normally sent as ordinary entries.
    if instruction_file.metadata()?.len() < 4 {
        return Err(protocol("delta instruction stream is too short"));
    }
    let mut input = BufReader::with_capacity(BUFFER, instruction_file);
    if varint(&mut input, work)? != base_size {
        return Err(protocol("delta base size mismatch"));
    }
    let expected = varint(&mut input, work)?;
    kind.check_size(expected, limits)?;
    let mut output = ChargedWriter {
        output: File::create(output)?,
        charge,
        error: None,
    };
    let result = (|| {
        let mut written = 0u64;
        loop {
            work.check()?;
            let mut command = [0];
            if input.read(&mut command)? == 0 {
                break;
            }
            let command = command[0];
            let count = if command & 128 != 0 {
                let mut offset = 0u64;
                let mut length = 0u64;
                for index in 0..4 {
                    if command & (1 << index) != 0 {
                        offset |= u64::from(byte(&mut input, work)?) << (index * 8);
                    }
                }
                for index in 0..3 {
                    if command & (16 << index) != 0 {
                        length |= u64::from(byte(&mut input, work)?) << (index * 8);
                    }
                }
                if length == 0 {
                    length = 65536;
                }
                if offset.checked_add(length).is_none_or(|end| end > base_size) {
                    return Err(protocol("delta copy outside base"));
                }
                if written.checked_add(length).is_none_or(|end| end > expected) {
                    return Err(protocol("delta result exceeds declared size"));
                }
                base.seek(SeekFrom::Start(offset))?;
                copy_exact(&mut base, &mut output, length, work)?;
                length
            } else {
                let length = u64::from(command);
                if length == 0 {
                    return Err(protocol("reserved delta opcode"));
                }
                if written.checked_add(length).is_none_or(|end| end > expected) {
                    return Err(protocol("delta result exceeds declared size"));
                }
                copy_exact(&mut input, &mut output, length, work)?;
                length
            };
            written += count;
        }
        if written != expected {
            return Err(protocol("delta result shorter than declared size"));
        }
        output.flush()?;
        Ok(expected)
    })();
    if let Some(e) = output.error.take() {
        return Err(e);
    }
    result
}

fn verify_checksum(pack: &Path, work: &Work, limits: &Limits) -> Result<u64, GitError> {
    let mut input = File::open(pack)?;
    let length = input.metadata()?.len();
    if length > limits.max_pack_bytes {
        return Err(GitError::ResourceLimit("downloaded pack bytes".into()));
    }
    if length < 32 {
        return Err(protocol("pack is too short"));
    }
    let mut hash = Sha1::new();
    let mut left = length - 20;
    let mut buffer = [0; BUFFER];
    while left > 0 {
        work.check()?;
        let n = left.min(BUFFER as u64) as usize;
        input.read_exact(&mut buffer[..n])?;
        hash.update(&buffer[..n]);
        left -= n as u64;
    }
    let mut expected = [0; 20];
    input.read_exact(&mut expected)?;
    if hash.finalize().as_slice() != expected {
        return Err(protocol("pack trailer checksum mismatch"));
    }
    Ok(length)
}

fn payload(staging: &Path, index: usize) -> std::path::PathBuf {
    staging.join(format!("{index}.raw"))
}

fn publish(
    repo: &Path,
    staging: &Path,
    index: usize,
    entry: &Entry,
    work: &Work,
    charge: &mut Charge,
) -> Result<(), GitError> {
    work.check()?;
    let oid = entry.oid.expect("resolved entry");
    let kind = entry.kind.expect("resolved kind");
    let path = loose::object_path(repo, oid);
    fs::create_dir_all(path.parent().expect("object parent"))?;
    let mut temp = tempfile::NamedTempFile::new_in(path.parent().expect("object parent"))?;
    let mut input = File::open(payload(staging, index))?;
    let mut hash = Sha1::new();
    let header = format!("{} {}\0", kind.name(), entry.size);
    hash.update(header.as_bytes());
    {
        let writer = ChargedWriter {
            output: temp.as_file_mut(),
            charge,
            error: None,
        };
        let mut encoder = ZlibEncoder::new(writer, Compression::fast());
        let result = (|| {
            encoder.write_all(header.as_bytes())?;
            let mut buffer = [0; BUFFER];
            let mut actual = 0u64;
            loop {
                work.check()?;
                let n = input.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                actual += n as u64;
                hash.update(&buffer[..n]);
                encoder.write_all(&buffer[..n])?;
            }
            if actual != entry.size {
                return Err(protocol("payload changed during publication"));
            }
            encoder.try_finish()?;
            Ok(())
        })();
        if let Some(e) = encoder.get_mut().error.take() {
            return Err(e);
        }
        result?;
    }
    if hash.finalize().as_slice() != oid.as_bytes() {
        return Err(protocol("payload hash changed during publication"));
    }
    temp.flush()?;
    work.check()?;
    // Existing corrupt/truncated files must be repaired by a successful retry.
    // An atomic replacement is safe because the path identifies immutable bytes.
    temp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn ingest(
    repo: &Path,
    staging: &Path,
    pack_path: &Path,
    wants: &[Oid],
    commit: Option<Oid>,
    work: &Work,
    limits: &Limits,
    budget: &Arc<Budget>,
) -> Result<(), GitError> {
    work.check()?;
    if wants.len() > limits.max_wants {
        return Err(GitError::ResourceLimit("want count".into()));
    }
    let length = verify_checksum(pack_path, work, limits)?;
    let mut input = BufReader::with_capacity(BUFFER, File::open(pack_path)?);
    let mut header = [0; 12];
    input.read_exact(&mut header)?;
    if &header[..4] != b"PACK" {
        return Err(protocol("invalid pack signature"));
    }
    let version = u32::from_be_bytes(header[4..8].try_into().expect("four bytes"));
    if version != 2 && version != 3 {
        return Err(protocol("unsupported pack version"));
    }
    let count = u32::from_be_bytes(header[8..12].try_into().expect("four bytes")) as usize;
    if count > limits.max_objects || count > METADATA_BUDGET / METADATA_PER_ENTRY {
        return Err(GitError::ResourceLimit("pack object/metadata count".into()));
    }
    let mut entries = Vec::<Entry>::new();
    let mut ofs = Vec::<(u64, usize)>::new();
    let mut refs = Vec::<(Oid, usize)>::new();
    let mut ready = VecDeque::<usize>::new();
    let mut identities = HashMap::<Oid, usize>::new();
    identities
        .try_reserve(count)
        .map_err(|_| GitError::ResourceLimit("identity metadata allocation".into()))?;
    entries
        .try_reserve_exact(count)
        .map_err(|_| GitError::ResourceLimit("entry metadata allocation".into()))?;
    ofs.try_reserve_exact(count)
        .map_err(|_| GitError::ResourceLimit("OFS metadata allocation".into()))?;
    refs.try_reserve_exact(count)
        .map_err(|_| GitError::ResourceLimit("REF metadata allocation".into()))?;
    ready
        .try_reserve_exact(count)
        .map_err(|_| GitError::ResourceLimit("queue allocation".into()))?;
    let mut charge = Charge {
        reservation: budget.reserve(0)?,
        total: 0,
        max: limits.max_expanded_bytes,
    };
    for index in 0..count {
        work.check()?;
        let offset = input.stream_position()?;
        let (base, size) = entry_header(&mut input, offset, work)?;
        if size > limits.max_object_bytes {
            return Err(GitError::ResourceLimit("entry inflated bytes".into()));
        }
        let kind = match base {
            Base::None(kind) => {
                kind.check_size(size, limits)?;
                Some(kind)
            }
            Base::Offset(base_offset) => {
                if entries
                    .binary_search_by_key(&base_offset, |entry| entry.offset)
                    .is_err()
                {
                    return Err(protocol("OFS base is not a preceding entry start"));
                }
                ofs.push((base_offset, index));
                None
            }
            Base::Ref(oid) => {
                refs.push((oid, index));
                None
            }
        };
        let path = payload(staging, index);
        let mut output = ChargedWriter {
            output: File::create(&path)?,
            charge: &mut charge,
            error: None,
        };
        let result = loose::inflate(&mut input, &mut output, Some(size), size, work);
        if let Some(e) = output.error.take() {
            return Err(e);
        }
        result?;
        drop(output);
        let oid = kind
            .map(|kind| loose::hash_payload(kind, &path, work))
            .transpose()?;
        if let Some(oid) = oid
            && identities.insert(oid, index).is_some()
        {
            return Err(protocol("duplicate object id in pack"));
        }
        entries.push(Entry {
            offset,
            size,
            kind,
            oid,
            depth: 0,
        });
        if oid.is_some() {
            ready.push_back(index);
        }
    }
    if input.stream_position()? != length - 20 {
        return Err(protocol("pack count/trailing bytes mismatch"));
    }
    ofs.sort_unstable_by_key(|entry| entry.0);
    refs.sort_unstable_by_key(|entry| entry.0);
    while let Some(base_index) = ready.pop_front() {
        work.check()?;
        let base = &entries[base_index];
        let base_oid = base.oid.expect("ready OID");
        let base_offset = base.offset;
        let kind = base.kind.expect("ready kind");
        let depth = base.depth;
        let start_ofs = ofs.partition_point(|entry| entry.0 < base_offset);
        let end_ofs = ofs.partition_point(|entry| entry.0 <= base_offset);
        let start_ref = refs.partition_point(|entry| entry.0 < base_oid);
        let end_ref = refs.partition_point(|entry| entry.0 <= base_oid);
        for index in ofs[start_ofs..end_ofs]
            .iter()
            .map(|entry| entry.1)
            .chain(refs[start_ref..end_ref].iter().map(|entry| entry.1))
        {
            if entries[index].oid.is_some() {
                continue;
            }
            if depth >= limits.max_delta_depth {
                return Err(GitError::ResourceLimit("delta depth".into()));
            }
            let result_path = staging.join(format!("{index}.result"));
            let size = apply_delta(
                &payload(staging, base_index),
                &payload(staging, index),
                &result_path,
                kind,
                work,
                limits,
                &mut charge,
            )?;
            let oid = loose::hash_payload(kind, &result_path, work)?;
            if identities.insert(oid, index).is_some() {
                return Err(protocol("duplicate object id in pack"));
            }
            work.check()?;
            fs::rename(result_path, payload(staging, index))?;
            entries[index].oid = Some(oid);
            entries[index].kind = Some(kind);
            entries[index].size = size;
            entries[index].depth = depth + 1;
            ready.push_back(index);
        }
    }
    if entries.iter().any(|entry| entry.oid.is_none()) {
        return Err(protocol("missing/cyclic delta base"));
    }
    for want in wants {
        work.check()?;
        let index = *identities.get(want).ok_or(GitError::MissingObject(*want))?;
        let entry = &entries[index];
        match (commit, entry.kind.expect("resolved")) {
            (None, Kind::Blob) | (Some(_), Kind::Commit | Kind::Tag) => {}
            _ => return Err(GitError::MissingObject(*want)),
        }
    }
    // All transfer contents validate before any object is published. A later
    // filesystem failure can leave good objects, but never a completion marker.
    if let Some(oid) = commit {
        match fs::remove_file(loose::marker_path(repo, oid)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    for (index, entry) in entries.iter().enumerate() {
        publish(repo, staging, index, entry, work, &mut charge)?;
    }
    if let Some(oid) = commit {
        loose::validate_commit(repo, oid, work, limits)?;
        loose::mark_complete(repo, oid, work)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    fn work() -> Work {
        Work::new(Duration::from_secs(30))
    }

    fn oid(kind: Kind, bytes: &[u8]) -> Oid {
        let kind = match kind {
            Kind::Blob => git2::ObjectType::Blob,
            Kind::Tree => git2::ObjectType::Tree,
            Kind::Commit => git2::ObjectType::Commit,
            Kind::Tag => git2::ObjectType::Tag,
        };
        Oid::hash_object(kind, bytes).unwrap()
    }

    fn size_header(code: u8, mut size: u64) -> Vec<u8> {
        let mut bytes = vec![(code << 4) | (size as u8 & 15)];
        size >>= 4;
        while size > 0 {
            *bytes.last_mut().unwrap() |= 128;
            bytes.push(size as u8 & 127);
            size >>= 7;
        }
        bytes
    }

    fn var(mut value: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        while value >= 128 {
            bytes.push(value as u8 | 128);
            value >>= 7;
        }
        bytes.push(value as u8);
        bytes
    }

    fn distance(mut value: u64) -> Vec<u8> {
        let mut bytes = vec![value as u8 & 127];
        while value >> 7 != 0 {
            value = (value >> 7) - 1;
            bytes.insert(0, 128 | (value as u8 & 127));
        }
        bytes
    }

    fn compressed(raw: &[u8]) -> Vec<u8> {
        let mut z = ZlibEncoder::new(Vec::new(), Compression::fast());
        z.write_all(raw).unwrap();
        z.finish().unwrap()
    }

    fn append(pack: &mut Vec<u8>, code: u8, extra: &[u8], raw: &[u8]) -> u64 {
        let offset = pack.len() as u64;
        pack.extend(size_header(code, raw.len() as u64));
        pack.extend(extra);
        pack.extend(compressed(raw));
        offset
    }

    fn start(count: u32) -> Vec<u8> {
        let mut pack = b"PACK".to_vec();
        pack.extend(2u32.to_be_bytes());
        pack.extend(count.to_be_bytes());
        pack
    }

    fn finish(mut pack: Vec<u8>) -> Vec<u8> {
        let hash = Sha1::digest(&pack);
        pack.extend(hash);
        pack
    }

    struct Fixture {
        root: tempfile::TempDir,
        repo: std::path::PathBuf,
        stage: std::path::PathBuf,
        budget: Arc<Budget>,
    }

    impl Fixture {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            let repo = root.path().join("repo");
            git2::Repository::init_bare(&repo).unwrap();
            let stage = root.path().join("stage");
            fs::create_dir(&stage).unwrap();
            Self {
                root,
                repo,
                stage,
                budget: Budget::new("test", 256 << 20),
            }
        }

        fn ingest(
            &self,
            pack: &[u8],
            wants: &[Oid],
            commit: Option<Oid>,
            limits: &Limits,
        ) -> Result<(), GitError> {
            let path = self.root.path().join("transfer.pack");
            fs::write(&path, pack).unwrap();
            let result = ingest(
                &self.repo,
                &self.stage,
                &path,
                wants,
                commit,
                &work(),
                limits,
                &self.budget,
            );
            assert_eq!(self.budget.used(), 0, "all ingestion reservations released");
            result
        }

        fn check(&self, expected: Oid, bytes: &[u8]) {
            let repo = git2::Repository::open_bare(&self.repo).unwrap();
            let odb = repo.odb().unwrap();
            let object = odb.read(expected).unwrap();
            assert_eq!(object.data(), bytes);
            assert!(
                self.repo
                    .join("objects/pack")
                    .read_dir()
                    .unwrap()
                    .next()
                    .is_none()
            );
        }
    }

    fn git_accepts(fixture: &Fixture, pack: &[u8]) {
        let comparison = fixture.root.path().join("git-comparison");
        git2::Repository::init_bare(&comparison).unwrap();
        let mut child = Command::new("git")
            .arg("-C")
            .arg(comparison)
            .args(["index-pack", "--stdin", "--strict"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(pack).unwrap();
        let result = child.wait_with_output().unwrap();
        assert!(
            result.status.success(),
            "Git rejected fixture: {}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    #[test]
    fn metadata_layout_fits_admission_allowance() {
        // The remainder pays for the bounded closure visited set/stack and
        // allocator rounding; metadata object payloads have their own 8MiB cap.
        let per_entry = std::mem::size_of::<Entry>()
            + std::mem::size_of::<(u64, usize)>()
            + 3 * std::mem::size_of::<(Oid, usize)>() // REF array + conservative hash-table buckets
            + std::mem::size_of::<usize>();
        assert!(per_entry + 64 <= METADATA_PER_ENTRY);
    }

    #[test]
    fn forward_ref_backward_ofs_mixed_chain_matches_git() {
        let fixture = Fixture::new();
        let base = vec![b'a'; 65536];
        let base_oid = oid(Kind::Blob, &base);
        let mut middle = base.clone();
        middle.push(b'b');
        let middle_oid = oid(Kind::Blob, &middle);
        let leaf = b"end";
        let leaf_oid = oid(Kind::Blob, leaf);
        let mut leaf_delta = var(middle.len() as u64);
        leaf_delta.extend(var(3));
        leaf_delta.extend([3, b'e', b'n', b'd']);
        let mut pack = start(3);
        append(&mut pack, 7, middle_oid.as_bytes(), &leaf_delta);
        let base_offset = append(&mut pack, 3, &[], &base);
        let mut middle_delta = var(base.len() as u64);
        middle_delta.extend(var(middle.len() as u64));
        middle_delta.extend([128, 1, b'b']); // default 65,536-byte copy, then literal.
        let offset = pack.len() as u64;
        append(&mut pack, 6, &distance(offset - base_offset), &middle_delta);
        let pack = finish(pack);
        git_accepts(&fixture, &pack);
        fixture
            .ingest(
                &pack,
                &[leaf_oid, middle_oid, base_oid],
                None,
                &Limits::default(),
            )
            .unwrap();
        fixture.check(base_oid, &base);
        fixture.check(middle_oid, &middle);
        fixture.check(leaf_oid, leaf);
    }

    #[test]
    fn backward_ref_all_copy_operands_and_zero_object_match_git() {
        let fixture = Fixture::new();
        let mut base = vec![b'a'; 66000];
        base[257..260].copy_from_slice(b"xyz");
        let base_oid = oid(Kind::Blob, &base);
        let mut delta = var(base.len() as u64);
        delta.extend(var(3));
        delta.extend([255, 1, 1, 0, 0, 3, 0, 0]); // all offset/size operand flags.
        let wanted = oid(Kind::Blob, b"xyz");
        let empty = oid(Kind::Blob, b"");
        let mut pack = start(3);
        append(&mut pack, 3, &[], &base);
        append(&mut pack, 7, base_oid.as_bytes(), &delta);
        append(&mut pack, 3, &[], b"");
        let pack = finish(pack);
        git_accepts(&fixture, &pack);
        fixture
            .ingest(&pack, &[wanted, empty], None, &Limits::default())
            .unwrap();
        fixture.check(wanted, b"xyz");
        fixture.check(empty, b"");
    }

    #[test]
    fn duplicate_object_ids_are_rejected_before_publication() {
        let fixture = Fixture::new();
        let mut pack = start(2);
        append(&mut pack, 3, &[], b"abc");
        append(&mut pack, 3, &[], b"abc");
        assert!(matches!(
            fixture.ingest(&finish(pack), &[], None, &Limits::default()),
            Err(GitError::Protocol(_))
        ));
        assert!(!loose::object_path(&fixture.repo, oid(Kind::Blob, b"abc")).exists());
        let fixture = Fixture::new();
        let identity = oid(Kind::Blob, b"ab");
        let mut pack = start(2);
        append(&mut pack, 3, &[], b"ab");
        append(&mut pack, 7, identity.as_bytes(), &[2, 2, 0x90, 2]);
        assert!(matches!(
            fixture.ingest(&finish(pack), &[], None, &Limits::default()),
            Err(GitError::Protocol(_))
        ));
        assert!(!loose::object_path(&fixture.repo, identity).exists());
    }

    #[test]
    fn accepts_noncanonical_but_valid_size_header() {
        let fixture = Fixture::new();
        let mut pack = start(1);
        pack.extend([0xb3, 0]);
        pack.extend(compressed(b"abc"));
        let pack = finish(pack);
        git_accepts(&fixture, &pack);
        fixture
            .ingest(&pack, &[oid(Kind::Blob, b"abc")], None, &Limits::default())
            .unwrap();
    }

    #[test]
    fn malformed_integer_headers_and_offsets_fail_before_publication() {
        let bad = [
            vec![0xb0, 255, 255, 255, 255, 255, 255, 255, 255, 127],
            vec![0xb0, 128, 128, 128, 128, 128, 128, 128, 128, 128, 0],
            vec![0x60, 0],
            vec![0x60, 255, 255, 255, 255, 255, 255, 255, 255, 255, 127],
            vec![0x60, 1],
        ];
        for header in bad {
            let fixture = Fixture::new();
            let mut pack = start(1);
            pack.extend(header);
            pack.extend(compressed(b""));
            assert!(
                fixture
                    .ingest(&finish(pack), &[], None, &Limits::default())
                    .is_err()
            );
        }
        let fixture = Fixture::new();
        let mut pack = start(2);
        append(&mut pack, 3, &[], b"abc");
        append(&mut pack, 6, &[1], &[3, 0]);
        assert!(
            fixture
                .ingest(&finish(pack), &[], None, &Limits::default())
                .is_err()
        );
    }

    #[test]
    fn malformed_delta_graph_and_instructions_fail() {
        let base = b"ab";
        let base_oid = oid(Kind::Blob, base);
        let cases = [
            vec![2, 1, 0],          // reserved opcode
            vec![2, 1, 0x91, 3, 1], // outside base
            vec![2, 1, 1],          // truncated literal
            vec![2, 2, 1, b'x'],    // short result
            vec![3, 0],             // wrong base size
            vec![2, 0, 1, b'x'],    // result overflow
            vec![255; 11],          // varint overflow
        ];
        for instructions in cases {
            let fixture = Fixture::new();
            let mut pack = start(2);
            append(&mut pack, 7, base_oid.as_bytes(), &instructions);
            append(&mut pack, 3, &[], base);
            assert!(
                fixture
                    .ingest(&finish(pack), &[], None, &Limits::default())
                    .is_err()
            );
            assert!(!loose::object_path(&fixture.repo, base_oid).exists());
        }
        let fixture = Fixture::new();
        let mut pack = start(1);
        append(&mut pack, 7, base_oid.as_bytes(), &[2, 0]);
        assert!(matches!(
            fixture.ingest(&finish(pack), &[], None, &Limits::default()),
            Err(GitError::Protocol(_))
        ));
    }

    #[test]
    fn checksum_truncation_wrong_count_and_trailing_bytes_fail() {
        let mut good = start(1);
        append(&mut good, 3, &[], b"abc");
        let mut cases = Vec::new();
        let mut corrupt = finish(good.clone());
        *corrupt.last_mut().unwrap() ^= 1;
        cases.push(corrupt);
        let mut truncated_zlib = good.clone();
        truncated_zlib.truncate(truncated_zlib.len() - 4);
        cases.push(finish(truncated_zlib));
        let mut wrong_count = good.clone();
        wrong_count[11] = 2;
        cases.push(finish(wrong_count));
        let mut extra = good;
        extra.push(0);
        cases.push(finish(extra));
        for pack in cases {
            let fixture = Fixture::new();
            assert!(
                fixture
                    .ingest(&pack, &[], None, &Limits::default())
                    .is_err()
            );
            assert!(!loose::object_path(&fixture.repo, oid(Kind::Blob, b"abc")).exists());
        }
    }

    #[test]
    fn local_limits_are_distinct_from_global_quota_and_missing_wants() {
        let mut pack = start(1);
        append(&mut pack, 3, &[], b"abc");
        let pack = finish(pack);
        for limits in [
            Limits {
                max_pack_bytes: 1,
                ..Limits::default()
            },
            Limits {
                max_objects: 0,
                ..Limits::default()
            },
            Limits {
                max_object_bytes: 2,
                ..Limits::default()
            },
            Limits {
                max_expanded_bytes: 2,
                ..Limits::default()
            },
        ] {
            assert!(matches!(
                Fixture::new().ingest(&pack, &[], None, &limits),
                Err(GitError::ResourceLimit(_))
            ));
        }
        let mut fixture = Fixture::new();
        fixture.budget = Budget::new("small", 2);
        assert!(matches!(
            fixture.ingest(&pack, &[], None, &Limits::default()),
            Err(GitError::Quota(_))
        ));
        assert!(matches!(
            Fixture::new().ingest(
                &pack,
                &[oid(Kind::Blob, b"missing")],
                None,
                &Limits::default()
            ),
            Err(GitError::MissingObject(_))
        ));
    }

    fn tree_entry(mode: &str, name: &str, oid: Oid) -> Vec<u8> {
        let mut raw = format!("{mode} {name}\0").into_bytes();
        raw.extend(oid.as_bytes());
        raw
    }

    #[test]
    fn commit_closure_peels_tags_ignores_blobs_and_gitlinks_and_recovers_corruption() {
        let fixture = Fixture::new();
        let child = Vec::new();
        let child_oid = oid(Kind::Tree, &child);
        let mut root = tree_entry("40000", "child", child_oid);
        root.extend(tree_entry(
            "100644",
            "missing-blob",
            oid(Kind::Blob, b"absent"),
        ));
        root.extend(tree_entry(
            "160000",
            "submodule",
            Oid::from_str(&"1".repeat(40)).unwrap(),
        ));
        let root_oid = oid(Kind::Tree, &root);
        let commit = format!(
            "tree {root_oid}\nparent {}\nauthor A <a@b.c> 1 +0000\ncommitter A <a@b.c> 2 +0000\n\nmessage\n",
            "2".repeat(40)
        );
        let commit_oid = oid(Kind::Commit, commit.as_bytes());
        let tag =
            format!("object {commit_oid}\ntype commit\ntag v1\ntagger A <a@b.c> 3 +0000\n\ntag\n");
        let tag_oid = oid(Kind::Tag, tag.as_bytes());
        let mut pack = start(4);
        append(&mut pack, 4, &[], tag.as_bytes());
        append(&mut pack, 1, &[], commit.as_bytes());
        append(&mut pack, 2, &[], &root);
        append(&mut pack, 2, &[], &child);
        let pack = finish(pack);
        fixture
            .ingest(&pack, &[tag_oid], Some(tag_oid), &Limits::default())
            .unwrap();
        assert_eq!(
            loose::validate_commit(&fixture.repo, tag_oid, &work(), &Limits::default()).unwrap(),
            (root_oid, 2)
        );
        assert!(loose::marker_path(&fixture.repo, tag_oid).exists());
        fs::write(loose::object_path(&fixture.repo, child_oid), b"broken").unwrap();
        assert!(
            loose::validate_commit(&fixture.repo, tag_oid, &work(), &Limits::default()).is_err()
        );
        fixture
            .ingest(&pack, &[tag_oid], Some(tag_oid), &Limits::default())
            .unwrap();
        assert!(
            loose::validate_commit(&fixture.repo, tag_oid, &work(), &Limits::default()).is_ok()
        );
        fs::remove_file(loose::object_path(&fixture.repo, child_oid)).unwrap();
        assert!(
            loose::validate_commit(&fixture.repo, tag_oid, &work(), &Limits::default()).is_err()
        );
    }

    #[test]
    fn missing_tree_wrong_kind_and_metadata_cap_never_complete() {
        let fixture = Fixture::new();
        let child = oid(Kind::Tree, b"");
        let root = tree_entry("40000", "child", child);
        let root_oid = oid(Kind::Tree, &root);
        let commit = format!(
            "tree {root_oid}\nauthor A <a@b.c> 1 +0000\ncommitter A <a@b.c> 2 +0000\n\nm\n"
        );
        let commit_oid = oid(Kind::Commit, commit.as_bytes());
        let mut pack = start(2);
        append(&mut pack, 1, &[], commit.as_bytes());
        append(&mut pack, 2, &[], &root);
        assert!(
            fixture
                .ingest(
                    &finish(pack),
                    &[commit_oid],
                    Some(commit_oid),
                    &Limits::default()
                )
                .is_err()
        );
        assert!(!loose::marker_path(&fixture.repo, commit_oid).exists());
        let mut pack = start(1);
        append(&mut pack, 2, &[], &root);
        assert!(matches!(
            Fixture::new().ingest(
                &finish(pack),
                &[],
                None,
                &Limits {
                    max_metadata_object_bytes: 1,
                    ..Limits::default()
                }
            ),
            Err(GitError::ResourceLimit(_))
        ));
        let mut pack = start(1);
        append(&mut pack, 3, &[], b"abc");
        assert!(matches!(
            Fixture::new().ingest(
                &finish(pack),
                &[oid(Kind::Blob, b"abc")],
                Some(oid(Kind::Blob, b"abc")),
                &Limits::default()
            ),
            Err(GitError::MissingObject(_))
        ));
    }

    #[test]
    fn depth_limit_and_cancelled_worker_release_all_reservations() {
        let fixture = Fixture::new();
        let base_oid = oid(Kind::Blob, b"ab");
        let mut pack = start(2);
        append(&mut pack, 3, &[], b"ab");
        append(&mut pack, 7, base_oid.as_bytes(), &[2, 1, 1, b'x']);
        assert!(matches!(
            fixture.ingest(
                &finish(pack),
                &[],
                None,
                &Limits {
                    max_delta_depth: 0,
                    ..Limits::default()
                }
            ),
            Err(GitError::ResourceLimit(_))
        ));
        let work = work();
        work.token.cancel();
        assert!(matches!(
            ingest(
                &fixture.repo,
                &fixture.stage,
                Path::new("missing"),
                &[],
                None,
                &work,
                &Limits::default(),
                &fixture.budget
            ),
            Err(GitError::Cancelled)
        ));
        assert_eq!(fixture.budget.used(), 0);
    }
}
