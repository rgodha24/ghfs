# Pack ingestion research spike

A standalone proof of the bounded-payload-memory path discussed in
[the findings](../../pure-rust-pack-research.md). It parses pack headers with
`gix-pack`, inflates entries into files, and applies deltas by seeking the base
file and copying fixed chunks. It is not production code.

From the repository root:

```sh
nix develop -c cargo build --release --locked --manifest-path docs/research/pure-rust-pack-spike/Cargo.toml --target-dir /tmp/ghfs-pack-spike-target
python3 docs/research/pure-rust-pack-spike/verify.py /tmp/ghfs-pack-spike-target/release/ghfs-pack-spike /tmp/ghfs-pack-spike-fixtures
```

The generator makes 16 MiB and 128 MiB base blobs. Each pack includes a forward
REF_DELTA, its base blob, and a backward OFS_DELTA; expected canonical OIDs are
computed independently in Python. Invalid-checksum and four malformed-delta
cases must fail. Linux prints process VmHWM after each successful run.

An additional 16 MiB incompressible blob verifies that a large compressed
entry also stays out of memory. Git `index-pack --strict` independently accepted
the forward-REF/OFS fixture and the incompressible fixture during research.

Allow about 200 MiB of fixture/output disk plus Cargo build space. The pack
files compress heavily; expanded base blobs are deliberately large. Fixtures
and build artifacts use the scratch paths above.

Limitations: all objects are blobs; the resolver repeatedly scans entries;
metadata uses PathBufs and a 100,000-entry cap; there are no aggregate disk
quotas, cancellation checks, HTTP transport, loose-object publication, or
crash-recovery mechanisms. Production needs the bounded metadata/dependency
queue and quotas described in the findings. These measurements prove neither
a whole-daemon memory budget nor broad Git pack conformance.
