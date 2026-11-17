# Pure-Rust pack ingestion: source findings and executable spike

Researched 2026-11-16. Scope: ingest Git fetch packs into loose objects without
unbounded payload allocations, permanent packs, or subprocesses. This is a
research result, not production implementation.

## Conclusion

The design is feasible, but **gix-pack's ordinary streaming and object-decoding
APIs do not provide bounded-memory unpack-to-loose**. Keeping the memory contract
requires a small custom disk-backed unpacker using gix-pack's low-level entry
header parser, a streaming zlib decoder, and a streaming Git delta interpreter.
A scratch Rust program compiled and exercised this route successfully. The
simplest implementation downloads to a temporary pack outside `objects/pack`,
then expands it using bounded buffers and temporary payload files. Delete the
transfer directory afterwards; only loose objects remain permanently.

This deliberately revises “no packs” to “no persistent packs, index files, or
repacking”. Avoiding even temporary pack files is possible but complicates
checksum validation and cancellation before publication, without addressing the
incident more effectively.

## What the library actually does

Source inspection used the **published gix-pack 0.77.0** downloaded by Cargo;
its declared Rust version is 1.88. It compiled with this repository's Nix
shell, Rust 1.93.0. Gitoxide main at
`ab7c5dc0596943b42fe9294813a8cdc0383c403a` already declares Rust 1.95, so do not
silently substitute main for the released crate. Pin and compile an explicit
release before documenting signatures. [1]

- `data::input::BytesToEntriesIter::new_from_header(reader, mode, data_mode,
  object_hash)` measures each compressed entry. Its pass-through reader writes
  **all compressed bytes of that entry into a Vec**, including when
  `EntryDataMode::Ignore` is selected. Ignore clears/reuses the Vec afterwards;
  it does not impose an allocation cap. A large incompressible blob can consume
  approximately its compressed size in memory. [2]
- `data::File::decode_entry(entry, out: &mut Vec<u8>, inflate, resolve,
  delta_cache)` materializes the object. For delta chains the implementation
  holds all delta instructions plus two buffers sized for the largest base or
  result. `cache::Never` avoids cache growth but does not remove these buffers.
  `with_alloc_limit_bytes(Some(limit))` can make oversized decoding fail; it
  does not turn decoding into a stream. The limit is not an aggregate daemon
  memory budget. [3]
- `data::Entry::from_read(&mut dyn Read, pack_offset, hash_len)` is public and
  only parses an entry header, yielding its kind/base reference, declared
  decompressed size, and data offset. It is sufficient for incremental framing
  without either high-level API above. `data::header::decode(&[u8; 12])` parses
  pack version and entry count. [4]
- `flate2::bufread::ZlibDecoder` reads one zlib stream and then returns EOF while
  preserving following bytes in its underlying `BufRead`. This is the correct
  variant for adjacent entries in a pack; recover/reborrow the same BufReader
  after each stream. Track actual consumed compressed bytes and actual output
  bytes, rather than trusting the entry header. [5]
- gix-pack's delta implementation uses byte slices and is `pub(crate)`; it is
  not a public streaming interpreter. The authoritative pack format defines
  copy-from-base and insert-literal instructions; each appends to the result.
  Consequently the output can be sequential, while the base must support random
  reads. A regular payload file satisfies that requirement. [6]

## Concrete ingestion design

1. Allocate a unique transfer directory outside `objects/pack`. Download only
   sideband channel 1 data into a temporary file, with an explicit byte quota.
   Hash while writing using a rolling final-20-byte tail, or verify in a second
   fixed-buffer pass before decoding. A pack checksum is SHA-1 for this
   repository's SHA-1 object format. Validate the final checksum and exact
   framing; do not accept trailing bytes or a premature end. [6]
2. Read the 12-byte header and reject an excessive entry count **before**
   allocating metadata. Parse each entry with `Entry::from_read`. Inflate each
   ordinary object into a raw payload file, and each delta into a raw instruction
   file. Use a fixed-size copy buffer and enforce output quotas during copying,
   including one extra-byte probe to detect a stream exceeding its declared
   size. Validate zlib completion/checksum and exact declared size. Keep only
   compact metadata in memory: offset, kind/base reference, actual sizes, a file
   identifier, and resolution state. Derive filenames from identifiers instead
   of storing a PathBuf per entry.
3. Hash ordinary objects as `type SP decimal-length NUL payload`. Record both
   offset→resolved entry and OID→resolved entry associations. A REF_DELTA can
   refer to a base appearing **later** in the pack; processing strictly in pack
   order is insufficient. gix-pack explicitly supports such forward references
   in its indexing machinery. [7]
4. Resolve ready deltas with an iterative dependency queue. Parse the base-size
   and result-size varints from the instruction file. Require the base size to
   match the raw base file. For a copy instruction, validate the range, seek
   the base file, and copy in fixed chunks. For an insert instruction, copy its
   literal bytes. Reject opcode zero, overflow, truncated operands, out-of-base
   copies, output exceeding the promised size, and short output. A zero encoded
   copy length means 65,536 bytes. The result inherits its base's object kind.
   Compute the canonical OID while writing the result. [6]
5. Resolving a base wakes dependencies waiting on its offset or OID. A queue
   avoids repeated scans of every unresolved delta, which can become quadratic.
   If entries remain and no work is ready, fail as a missing-base/cyclic graph.
   Do not request thin packs; resolve only within this transfer, so old cache
   packs are not needed as delta bases. Do not assume that “no haves” prohibits
   REF_DELTA or implies dependency order.
6. Once the whole pack and dependency graph have validated, stream each raw
   payload through canonical-header hashing and zlib compression into a unique
   loose-object temporary file. Atomically publish to `objects/xx/yyyy...`.
   Hashing verifies the produced bytes/OID; the requested OID must actually be
   present, and fetch success must cover the requested commit's tree closure.
   Readers must not infer whole-fetch completion from one root object's early
   publication. Publish dependencies before their parents or maintain a durable
   completion marker/closure check. Individual object renames alone do not make
   a multi-object fetch atomic.
7. Remove all transfer files on success/failure. A cancellation flag is checked
   between bounded chunks and delta instructions. The blocking worker owns its
   transfer directory and resource permits until it exits, even if its caller
   stops awaiting it. On daemon start, remove abandoned transfer directories;
   the current blob-cache age-out does not implement this cleanup.

An optimization can stream ingestion while downloading, but start with the two
phases above. This keeps network cancellation separate from disk work and lets
checksum failure precede object publication. It adds local I/O proportional to
this fetch, without ever scanning/repacking the entire historical cache.

## Resource limits include more than payload buffers

These are implementation requirements, not measured production defaults:

- Bound transfer concurrency, decoder concurrency, compressed bytes per transfer,
  total temporary-disk reservations, expanded bytes, delta depth/work, entry
  count, and metadata allocation. A wants limit alone does not constrain any of
  these: one wanted commit includes its entire tree closure, and one blob can
  be large.
- Pre-reserve compact entry/dependency arrays under an explicit memory charge.
  HashMap capacity and allocator overhead must also be charged or avoided with
  fixed/preallocated tables. Reject oversized entry counts before reservation.
  Do not allocate an unbounded Vec from the pack's declared count.
- One 64 KiB byte-copy buffer plus bounded readers/zlib state per decoder is
  sufficient for payload processing. Admission should reserve a conservative
  **aggregate** worker allowance plus metadata space; 24 network slots do not
  imply 24 simultaneous large decoder workers. Start with two decoder workers,
  then measure CPU, I/O, and latency.
- Disk-backed decoding exchanges memory for temporary storage. Reserve/check
  both downloaded pack bytes and expanded raw payload/instruction bytes.
  Highly compressed inputs and delta results can expand far beyond transfer
  size. Check limits during writes, rather than after allocating/writing.
- Keep intermediates until their children are resolved; removing all raw files
  only at transfer completion is the simplest first implementation. An explicit
  aggregate temporary-disk quota prevents this policy from filling the disk.
- Fixed application buffers do not bound the entire service cgroup: file cache,
  HTTP/TLS allocations, libgit2 readers, and other daemon state also contribute.
  Measure the service under its 1 GiB cgroup limit before choosing final limits.

## Executable evidence and its limits

Retained standalone project: `docs/research/pure-rust-pack-spike/`, built using:

```sh
nix develop -c cargo build --release --locked --manifest-path docs/research/pure-rust-pack-spike/Cargo.toml --target-dir /tmp/ghfs-pack-spike-target
python3 docs/research/pure-rust-pack-spike/verify.py /tmp/ghfs-pack-spike-target/release/ghfs-pack-spike /tmp/ghfs-pack-spike-fixtures
```

Dependencies: `gix-pack = "=0.77.0"` with defaults disabled and `sha1` enabled,
`flate2 = "1"` (resolved 1.1.11), and `sha1 = "0.10"` (resolved 0.10.7).
The scratch code uses the exact public header/parser APIs above, fixed 64 KiB
buffers, raw disk payloads, and a custom disk delta interpreter. Its synthetic
pack contains a REF_DELTA **before** its base, a large base blob, and an OFS_DELTA
after the base. An independent Python generator calculates expected canonical
object OIDs and lengths.

| Base blob | Output OIDs and sizes | Process VmHWM from `/proc/self/status` |
|---|---|---|
| 16 MiB | all three match | 2,644 KiB |
| 128 MiB | all three match | 2,720 KiB |

An additional 16 MiB incompressible blob produced a 16,782,378-byte pack
and matched its independent OID/length at 2,652 KiB VmHWM. Git
`index-pack --strict` independently accepted both the forward-REF/OFS
16 MiB fixture and that incompressible fixture. The retained generator/verifier
includes the incompressible case.

A corrupted pack checksum failed. Additional valid-checksum fixtures with a
reserved delta opcode, an out-of-range base copy, a truncated insert, and a
short result all failed as expected. This demonstrates constant payload-memory
behavior for the tested path, and verifies real released-crate APIs.

**Not proven by this spike:** HTTP ownership/cancellation, async backpressure,
loose-object publication, large-scale metadata accounting, aggregate quotas,
performance on Linux's tree closure, all delta instructions/chain shapes, crash
recovery, fuzz safety, or old-pack reads. The spike caps entries at 100,000 but
uses a simple repeated-scan resolver and per-entry PathBufs; production must use
the bounded queue/metadata design above. It has no production disk quota or
cancellation implementation. The source and fixture generator are retained as a standalone research artifact,
not shipped code or part of the production regression suite.

The custom plumbing is more than a library adapter: roughly the delta
interpreter, dependency scheduler, quota checks, and publication semantics need
careful implementation. It is nevertheless a much smaller surface than general
Git fetch/index/repack machinery. Build and test the unpacker first, including
malformed packs and forward references, before replacing the existing transport.
If the project wants no custom delta interpreter, the honest alternative is
high-level gix decoding with enforced memory caps **and explicit rejection of
oversized objects**, or retaining temporary packs as the read source. Neither
is equivalent to the original unrestricted-size loose-object design.

## Sources

1. [Released gix-pack 0.77.0 manifest](https://docs.rs/crate/gix-pack/0.77.0/source/Cargo.toml);
   [inspected Gitoxide main commit](https://github.com/GitoxideLabs/gitoxide/tree/ab7c5dc0596943b42fe9294813a8cdc0383c403a).
2. [Released streaming input implementation](https://docs.rs/crate/gix-pack/0.77.0/source/src/data/input/bytes_to_entries.rs).
3. [Released entry decoder](https://docs.rs/crate/gix-pack/0.77.0/source/src/data/file/decode/entry.rs);
   [allocation limits](https://docs.rs/crate/gix-pack/0.77.0/source/src/data/file/init.rs).
4. [Released entry header parser](https://docs.rs/crate/gix-pack/0.77.0/source/src/data/entry/decode.rs);
   [pack header parser](https://docs.rs/crate/gix-pack/0.77.0/source/src/data/header.rs).
5. [flate2 ZlibDecoder documentation](https://docs.rs/flate2/1.1.11/flate2/bufread/struct.ZlibDecoder.html).
6. [Git pack format specification](https://git-scm.com/docs/gitformat-pack);
   [released gix delta implementation](https://docs.rs/crate/gix-pack/0.77.0/source/src/data/delta.rs).
7. [Released REF_DELTA lookup implementation and forward-reference contract](https://docs.rs/crate/gix-pack/0.77.0/source/src/data/input/lookup_ref_delta_objects.rs).
