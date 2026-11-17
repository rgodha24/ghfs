# Independent design review: pure-Rust remote fetch

Reviewed 2026-11-16 against the handoff, both research documents, current
`src/git`, `src/repos.rs`, `src/singleflight.rs`, `src/paths.rs`, daemon startup,
and the locally downloaded **released** gix-pack 0.77.0 / gix-utils 0.5.2 source.
No daemon, mount, cache or production source was changed for this review.

## Verdict

Proceed with the proposed two-phase, disk-backed design. It removes the actual
failure mechanism: historical pack accumulation and repeated full repacks.
The released low-level gix APIs are usable, but are not a complete validation
boundary. The following details need explicit implementation and regression
tests before acceptance; none calls for changing the architecture.

## Concrete fixes and constraints

1. **Validate integer encodings around the gix header parser.** In 0.77.0,
   `streaming_parse_header_info` uses `u64::checked_shl`; that rejects an excessive
   shift count, but does not reject high bits shifted out of the value. The OFS
   reader delegated to gix-utils 0.5.2 similarly uses `checked_shl(7)`, so an
   overlong distance can wrap instead of reliably failing. Put a small bounded
   validating header reader in front of the parser, or decode these few fields
   with checked arithmetic. Require OFS distance > 0, checked subtraction, and
   an exact previously recorded entry-start offset. Always pass SHA-1 length
   20; the public API assumes a valid hash length. Bound header bytes and check
   cancellation there too. Test overlong/overflowing size and distance encodings
   with a valid pack checksum. Do not infer overflow safety from `checked_shl`
   appearing in dependency source. [1, 2]

   **Recommended simplification:** if these header APIs are the only gix-pack
   use, omit that dependency and own the small header decoder alongside the
   already-custom delta interpreter. Use checked multiplication/addition and
   bounded reads, and retain noncanonical-but-valid size-header fixtures. This
   is simpler to audit than parsing twice through a validation wrapper and
   does not change the architecture.

2. **Apply worker ownership to materialization as well as ingestion.** The
   existing `materialize` uses `.<oid>.<pid>.tmp`. Cancellation permits a
   replacement OnceCell initializer while a previous blocking writer remains
   alive. Reusing that filename can truncate or concurrently modify the same
   output. Use a unique owned temporary file per attempt and keep it, its
   cancellation state and permits alive through worker completion. Publish
   only after stream-end, size and OID checks. A fixed filename plus an async
   cleanup branch is insufficient because dropping the future skips that
   branch. Add cancel-and-immediately-retry tests for a large blob. [3, 4]

3. **Make completion checks exact and bounded.** Current `commit_info` peels
   tags, then checks only root-tree existence. Preserve annotated-tag behavior
   (including a full OID naming a tag), bound tag-chain traversal, and walk only
   tree edges: neither commit parents, missing blobs nor gitlinks belong to the
   closure. A marker must imply that all reachable trees have the expected
   kind and valid bytes, not just an ODB filename. On restart, verify canonical
   hashes when checking for corruption; `odb.exists` alone cannot do that.
   Remove/invalidate a marker after validation failure and ensure retry replaces
   corrupt objects rather than skipping an existing pathname forever. [3, 5]

4. **The resource budget must cover consumers of reconstructed objects.**
   Allowing a 512 MiB tree or commit through ingestion and then using libgit2
   to materialize it defeats an 8 MiB decoder budget. Choose a modest explicit
   limit for non-blob objects before publication/closure reads, or stream their
   validation. Charge visited-OID sets and closure work as metadata, and bound
   concurrent closure validation. Keep only a bounded number of open files;
   100,000 entries must not mean 100,000 open handles. Charge loose-output temps
   outside the transfer directory to staging until rename. These are direct
   consequences of the stated memory and disk guarantees, not optional tuning.
   [3, 5, 6]

   An initial **8 MiB non-blob object cap** is a reasonable concrete policy.
   Stream canonical hash validation, then use bounded libgit2 parsing; a fully
   streaming tree parser need not be added unless real repositories require
   raising that cap. Concurrent validation and visited sets still need bounds.

5. **Distinguish malformed input from admission failure.** Use typed resource
   failures for batch splitting. Clean up and release reservations before
   retries, split into strictly smaller groups, and preserve the original
   deadline across all attempts. Global quota pressure is not evidence that a
   batch is too large: do not recursively redownload every subset under global
   exhaustion. Per-transfer object/byte/count limits can trigger splitting;
   ENOSPC, cancellation and malformed packs should terminate. A single wanted
   commit with too many trees cannot be fixed by splitting. [6]

6. **Treat HTTP framing and credentials as an explicit boundary.** Disable
   redirects initially, or enforce the configured credential origin on every
   redirect, not only the initial request. Bound discovery, refs, progress and
   error packets as well as pack channel 1. Low-progress detection should
   measure useful response/pack progress: endless channel-2 traffic cannot
   keep a fetch alive indefinitely. Enforce the absolute deadline while waiting
   for operation/decoder permits and token refresh. Invalid-want errors should
   be object-not-found errors, not a negative cached repository result; 429,
   5xx, auth rejection and transport errors must not poison that cache. [6, 7]

7. **Make cleanup and shutdown sequencing observable.** Finish abandoned
   staging cleanup before admitting new transfers, or separate prior-run and
   current-run directories so cleanup cannot remove active files. Worker
   registration must happen before a caller can lose its handle; shutdown
   should stop admission, cancel, then join registered workers. Keep the
   operation and decoder permits owned by workers until they really exit.
   Leave `v2` outside the legacy-deletion list. [4, 6, 8]

## Implementation decomposition

1. Review/test and separately commit the existing stopgap changes as requested;
   accurately describe them as timeout cleanup, not arbitrary cancellation.
2. Implement cancellation/deadline, quota guards and worker supervision first,
   then the pack parser, disk delta resolver, and atomic loose publication.
   Keep these testable independently from HTTP and VFS.
3. Implement the transport trait, pkt-line/section state machine, capability
   discovery and ls-refs. Build both the test-owned upload-pack adapter and
   HTTP mock tests against the same parser.
4. Integrate fetch, v3 initialization, bounded closure validation/markers and
   streaming materialization; then remove repack and subprocess machinery.
5. Run the complete hermetic suite, followed by scratch Linux and macOS mounts.
   Keep acceptance evidence (commands, pinned OIDs, output hashes and resource
   measurements) in one document. Do not redeploy the installed daemon yet.

## Acceptance gates

- Pack fixtures accepted by Git and decoded identically: ordinary objects,
  forward/backward REF, OFS, mixed chains, duplicate OIDs, zero-length objects,
  all delta copy operands and the 65,536-byte default. Valid-checksum malformed
  cases must cover truncated zlib, extra bytes before/after trailer, integer
  overflow, wrong counts, invalid bases, wrong result size and depth/work caps.
- HTTP mocks exercise fragmentation at every framing boundary, direct and
  service-prefixed discovery, missing filter/shallow capabilities, SHA-256
  rejection, fatal sideband packets, stalls, redirects, auth refresh and status
  mapping. Public browsing works without `gh`; private access needs a real
  authorized test or must remain explicitly unverified.
- Fault injection interrupts download, inflate, delta application, loose
  publication, marker creation and large-blob materialization. Immediate retry
  and process restart recover, reservations return to zero, no active writer
  loses its permit/temp file, and missing/corrupt child trees are refetched.
- Linux FUSE and macOS NFS scratch mounts cover HEAD, branches/annotated tags,
  pinned Linux commit, parallel first reads, huge directories, subtree prefetch,
  submodules, symlinks, explicitly wanted blobs and a >8 MiB blob. Compare bytes
  against independent Git objects. Verify zero retained pack/index files and
  no git descendants. Preserve installed services and caches.
- Under Linux `MemoryMax=1G`, record cgroup peak/current memory, swap and I/O
  during concurrent cold reads and cancellation. Decoder VmHWM alone is not
  acceptance evidence. Record staging peaks and cleanup after shutdown/restart;
  tune concurrency downward if aggregate measurements require it.

## Sources

1. [gix-pack 0.77.0 entry decoder](https://docs.rs/crate/gix-pack/0.77.0/source/src/data/entry/decode.rs), inspected from Cargo's local released source.
2. [gix-utils 0.5.2 integer decoder](https://docs.rs/crate/gix-utils/0.5.2/source/src/decode.rs), and [Rust checked_shl contract](https://doc.rust-lang.org/std/primitive.u64.html#method.checked_shl).
3. [Current repository state/materialization](../src/repos.rs), `ensure_commit` and `materialize`.
4. [Current singleflight](../src/singleflight.rs); [Tokio spawn_blocking contract](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).
5. [Current object readers](../src/git/odb.rs), `commit_info`, `read_tree`, `missing_objects`.
6. [Implementation handoff](pure-rust-remote-handoff.md), sections 3–7; [pack research](pure-rust-pack-research.md).
7. [Remote protocol research](pure-rust-remote-research.md); [Git protocol v2 specification](https://git-scm.com/docs/gitprotocol-v2).
8. [Cache layout](../src/paths.rs) and [daemon startup](../src/daemon/mod.rs).
