use flate2::bufread::ZlibDecoder;
use gix_pack::data::{self, entry::Header};
use sha1::{Digest, Sha1};
use std::{
    fs::{self, File},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
const MAX: usize = 100_000;
const BUFFER: usize = 64 * 1024;
fn err(s: &str) -> io::Error {
    io::Error::other(s)
}
fn var(r: &mut impl Read) -> io::Result<u64> {
    let mut n = 0u64;
    for shift in (0..64).step_by(7) {
        let mut b = [0];
        r.read_exact(&mut b)?;
        let v = (b[0] & 127) as u64;
        if v > (u64::MAX >> shift) {
            return Err(err("varint overflow"));
        }
        n |= v << shift;
        if b[0] & 128 == 0 {
            return Ok(n);
        }
    }
    Err(err("varint overflow"))
}
fn byte(r: &mut impl Read) -> io::Result<u8> {
    let mut b = [0];
    r.read_exact(&mut b)?;
    Ok(b[0])
}
fn copy_exact(r: &mut impl Read, w: &mut impl Write, mut n: u64) -> io::Result<()> {
    let mut b = [0; BUFFER];
    while n > 0 {
        let k = (n.min(BUFFER as u64)) as usize;
        r.read_exact(&mut b[..k])?;
        w.write_all(&b[..k])?;
        n -= k as u64;
    }
    Ok(())
}
fn delta(base: &Path, instr: &Path, out: &Path) -> io::Result<()> {
    let mut base = File::open(base)?;
    let bs = base.metadata()?.len();
    let mut i = BufReader::new(File::open(instr)?);
    if var(&mut i)? != bs {
        return Err(err("base size"));
    }
    let expected = var(&mut i)?;
    let mut done = 0u64;
    let mut out = File::create(out)?;
    loop {
        let cmd = match byte(&mut i) {
            Ok(x) => x,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        };
        let n = if cmd & 128 != 0 {
            let mut offset = 0u64;
            let mut n = 0u64;
            for j in 0..4 {
                if cmd & (1 << j) != 0 {
                    offset |= (byte(&mut i)? as u64) << (8 * j)
                }
            }
            for j in 0..3 {
                if cmd & (16 << j) != 0 {
                    n |= (byte(&mut i)? as u64) << (8 * j)
                }
            }
            if n == 0 {
                n = 65536
            }
            if offset.checked_add(n).is_none_or(|end| end > bs) {
                return Err(err("copy out of range"));
            }
            if done.checked_add(n).is_none_or(|end| end > expected) {
                return Err(err("result too big"));
            }
            base.seek(SeekFrom::Start(offset))?;
            copy_exact(&mut base, &mut out, n)?;
            n
        } else {
            let n = cmd as u64;
            if n == 0 {
                return Err(err("reserved delta opcode"));
            }
            if done.checked_add(n).is_none_or(|end| end > expected) {
                return Err(err("result too big"));
            }
            copy_exact(&mut i, &mut out, n)?;
            n
        };
        done += n;
    }
    if done != expected {
        return Err(err("result too short"));
    }
    Ok(())
}
fn hash(path: &Path) -> io::Result<[u8; 20]> {
    let mut f = File::open(path)?;
    let mut h = Sha1::new();
    h.update(format!("blob {}\0", f.metadata()?.len()).as_bytes());
    let mut b = [0; BUFFER];
    loop {
        let n = f.read(&mut b)?;
        if n == 0 {
            break;
        }
        h.update(&b[..n])
    }
    Ok(h.finalize().into())
}
struct E {
    offset: u64,
    header: Header,
    path: PathBuf,
    oid: Option<[u8; 20]>,
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().collect::<Vec<_>>();
    let pack = Path::new(&args[1]);
    let root = Path::new(&args[2]);
    fs::create_dir_all(root)?;
    // Check checksum before publishing or trusting content. Bounded buffer, no mmap.
    let mut f = File::open(pack)?;
    let n = f.metadata()?.len();
    if n < 32 {
        return Err(err("pack too short").into());
    }
    let mut h = Sha1::new();
    let mut rem = n - 20;
    let mut b = [0; BUFFER];
    while rem > 0 {
        let take = rem.min(BUFFER as u64) as usize;
        f.read_exact(&mut b[..take])?;
        h.update(&b[..take]);
        rem -= take as u64
    }
    let mut expected = [0; 20];
    f.read_exact(&mut expected)?;
    if h.finalize()[..] != expected {
        return Err(err("pack checksum").into());
    }
    let mut r = BufReader::with_capacity(BUFFER, File::open(pack)?);
    let mut hdr = [0; 12];
    r.read_exact(&mut hdr)?;
    let (_, count) = data::header::decode(&hdr)?;
    if count as usize > MAX {
        return Err(err("object count limit").into());
    }
    let mut entries = Vec::with_capacity(count as usize);
    for index in 0..count {
        let offset = r.stream_position()?;
        let e = data::Entry::from_read(&mut r, offset, 20)?;
        let path = root.join(format!("{index}.raw"));
        let mut out = File::create(&path)?;
        let mut z = ZlibDecoder::new(&mut r);
        copy_exact(&mut z, &mut out, e.decompressed_size)?;
        if z.read(&mut [0])? != 0 {
            return Err(err("inflated entry too long").into());
        }
        drop(z);
        drop(out);
        let oid = match e.header {
            Header::Blob => Some(hash(&path)?),
            Header::OfsDelta { .. } | Header::RefDelta { .. } => None,
            _ => return Err(err("spike supports blobs only").into()),
        };
        entries.push(E {
            offset,
            header: e.header,
            path,
            oid,
        });
    }
    if r.stream_position()? != n - 20 {
        return Err(err("pack trailing data or count").into());
    }
    loop {
        let mut progress = false;
        let mut missing = 0;
        for idx in 0..entries.len() {
            if entries[idx].oid.is_some() {
                continue;
            }
            missing += 1;
            let base = match &entries[idx].header {
                Header::OfsDelta { base_distance } => {
                    let ofs = entries[idx]
                        .offset
                        .checked_sub(*base_distance)
                        .ok_or_else(|| err("ofs underflow"))?;
                    entries
                        .iter()
                        .position(|e| e.offset == ofs && e.oid.is_some())
                }
                Header::RefDelta { base_id } => entries.iter().position(|e| {
                    e.oid
                        .as_ref()
                        .is_some_and(|o| o.as_slice() == base_id.as_slice())
                }),
                _ => None,
            };
            if let Some(base) = base {
                let out = root.join(format!("{idx}.resolved"));
                delta(&entries[base].path, &entries[idx].path, &out)?;
                entries[idx].oid = Some(hash(&out)?);
                fs::remove_file(&entries[idx].path)?;
                entries[idx].path = out;
                progress = true;
            }
        }
        if missing == 0 {
            break;
        }
        if !progress {
            return Err(err("unresolved delta graph").into());
        }
    }
    for e in &entries {
        println!(
            "{} {}",
            e.oid
                .unwrap()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
            fs::metadata(&e.path)?.len()
        )
    }
    if let Ok(status) = fs::read_to_string("/proc/self/status") {
        for line in status.lines().filter(|l| l.starts_with("VmHWM:")) {
            eprintln!("{line}")
        }
    }
    Ok(())
}
