# Handoff: replace the `git` CLI remote layer with a pure-Rust, lib-only fetcher

You are picking up a deliberate redesign. This doc contains the incident that
motivated it, the design decisions already made (with rationale — do not
re-litigate them unless you find something factually wrong), the interface you
must preserve, and the migration order. Read `AGENTS.md` first for conventions
(`nix develop -c cargo ...`, run **fmt** before committing, no compat concerns
before v0.2.0).

## 0. Recorded starting state (original handoff)

- `main` is up to date with `origin/main` (`b7d094e`).
- There are **uncommitted changes** in the working tree — interim stopgap
  fixes for the incident below. Review/test these and commit separately when
  implementing; the pure-Rust work then supersedes parts of them.
  The diff touches:
  - `src/git/remote.rs` — git subprocesses now run in their own process group
    (`cmd.as_std_mut().process_group(0)`) and `run()` kills the whole group
    (`kill(-pid, SIGKILL)`) on timeout, so `pack-objects`/`index-pack`
    children are killed on timeout too. Arbitrary caller cancellation still
    needs separate cleanup.
  - `src/repos.rs` — `maybe_repack` defers while fetches are in flight or the
    repo fetched within the last 120s (`FetchGuard`, `fetching`,
    `last_fetch_ms`, `REPACK_IDLE_AFTER`).
  - `src/daemon/maintenance.rs` — maintenance tick 30 min → 5 min.
- ghfs is deployed on this machine via a systemd **user** unit
  (`~/.config/systemd/user/ghfs.service`, `MemoryMax=1G`) running v0.1.6 from
  a nix store path. The daemon is currently **stopped**. Do not restart it
  until the new code is ready; e2e test with scratch paths (see AGENTS.md).

## 1. The incident (why we're doing this)

On 2026-11-16 an agent browsed `torvalds/linux` through the mount for ~7
hours. What happened:

1. ghfs's partial-clone design leaves one pack file per batch fetch
   (`git fetch --filter=blob:none origin --stdin`). 12,700 batch fetches left
   **811 packs** in the mirror.
2. `maybe_repack` (threshold: 50 packs) fired **mid-browse**. A full
   `repack -a -d` of a 5.3 GB pack set takes many minutes, races `git fetch`
   for `shallow.lock`, and hit the 1800 s timeout.
3. The timeout killed only the direct child. Orphaned `git pack-objects`
   processes survived and kept re-reading the whole pack set **for hours**.
   Net effect: 2.4 TB of disk reads, 9.1 GB of the cgroup swapped (the
   orphans live inside the service cgroup, so `MemoryMax=1G` force-swapped
   them), load average 40, SSH drops.
4. Every 30 min the maintenance loop fired another doomed repack, so orphan
   `pack-objects` piled up ~5/hour.

The stopgap fixes address timeout-orphans and gate repacks. This project removes
the mechanism: **no remote git subprocesses, persistent packs, or repack**.

## 2. Goal and researched design

Production remote operations use zero git subprocesses. Fetch protocol v2 over
HTTPS, ingest into loose objects, and keep libgit2 reads. **Temporary pack files
are permitted; persistent packs, indexes, and repacking are not.** This revises
the original absolute “no packs” requirement: bounded-memory delta resolution
needs random-access disk storage, and staging a transfer is much simpler than
streaming publication. Work stays proportional to the current transfer rather
than repeatedly scanning the historical cache.

Read [the protocol/resource research](pure-rust-remote-research.md) and
[the pack research](pure-rust-pack-research.md) for cited sources and experimental
evidence. A retained [standalone spike](research/pure-rust-pack-spike/README.md)
compiled with gix-pack 0.77.0 in the Nix devshell and reconstructed forward
REF_DELTA and backward OFS_DELTA packs with 16/128 MiB objects at roughly
2.6 MiB process peak RSS. This proves the payload-processing approach, not
production cancellation, quotas, or whole-daemon memory usage.

Only `gh auth token` may be spawned by the **remote layer**, at most hourly
normally, with one bounded forced refresh on authentication rejection. Git CLI
is allowed in test fixtures/research. Existing OS mount/service helpers remain
necessary and are outside this prohibition. Remove the production `git
--version` check in `src/main.rs::cmd_doctor` too.

## 3. Chosen architecture

### 3.1 Protocol client

Use reqwest with rustls, a shared Client, and a small explicit v2 pkt-line and
section parser. Implement ls-refs and fetch only; no negotiation, push, SSH,
ref updates, or automatic tag following. Inspect the server advertisement and
require `fetch` with `filter`, plus `shallow` for commit fetches. Support SHA-1
only initially and reject another object format. Do not silently fall back to
unfiltered fetches or full history.

- HEAD lookup: ls-refs with `symrefs`, `peel`, `ref-prefix HEAD`.
- Full lookup: additionally `ref-prefix refs/heads/` and
  `ref-prefix refs/tags/`; filter returned names locally too.
- Commit fetch: explicit `want <oid>`, `deepen 1`, `filter blob:none`,
  `ofs-delta`, `no-progress`, `done`.
- Blob fetch: explicit wants, `filter blob:none`, `ofs-delta`, `no-progress`,
  `done`. Explicitly wanted blobs are included despite the filter; verified
  against GitHub.
- Send no haves and **do not request thin-pack**, packfile-uris, sideband-all,
  include-tag, or want-ref. Handle REF_DELTA and OFS_DELTA within this transfer,
  including forward reference bases.

Retain ref caching and singleflight. Cache/discover capabilities per remote and
authentication identity. First discovery plus ls-refs takes two HTTP requests,
not one; later operations can reuse discovery.

### 3.2 Two-phase bounded ingestion

1. Create a unique owned transfer directory outside `objects/pack`.
2. Stream demultiplexed channel-1 bytes to a temporary pack file using fixed
   buffers, enforcing compressed-byte limits. Consume the full response,
   validate the SHA-1 trailer, and flush pending writes before decoding.
3. Acquire one of two decoder permits and run blocking ingestion. Use
   `gix_pack::data::header::decode` and `data::Entry::from_read` (verified in
   released gix-pack 0.77.0), plus `flate2::bufread::ZlibDecoder` to inflate each
   entry to disk. Check actual consumed/output lengths and zlib completion.
4. For deltas, use a custom bounded interpreter over an instruction file and a
   seekable raw base file. Apply copy/insert instructions into another raw file.
   Validate varints, base/result sizes, ranges, reserved opcodes, overflows,
   and exact output length. Results inherit their base's kind. A zero encoded
   copy length means 65,536 bytes.
5. Track offset and OID dependencies in capped compact metadata. Resolve with
   an iterative ready queue, including forward REF_DELTA. Reject missing bases,
   cycles, and excessive depth/work. Never use existing cache objects as bases.
6. Once every entry validates, stream canonical `type size\0payload` through
   SHA-1 and zlib into unique loose-object temporary files; rename atomically.
   Require the requested OIDs to exist and validate their expected kinds.
7. For commit fetches, verify the complete tree closure (not parent history or
   blobs), then atomically write a per-commit completion marker. `ensure_commit`
   may skip fetching only with that marker and valid local commit/root tree.
   On first use each daemon run, validate the marked tree closure too: without
   fsync ordering, a power loss can persist the marker before all object data.
   Cache that validation in memory; invalidate/refetch on a missing/corrupt tree.
   An interrupted publication must not leave a root tree that makes a partial
   fetch look complete. Valid individual objects may be reused on retry.
8. Remove operation staging on success/failure; remove abandoned staging at
   daemon startup. Existing blob-cache gc does not perform that cleanup.

**Do not use** gix-pack's convenient `BytesToEntriesIter`/`File::decode_entry`
for this path. The former buffers a complete compressed entry; the latter
allocates complete object/delta buffers. `EntryDataMode::Ignore` and disabling
the delta cache do not establish a bounded-memory contract.

Dependencies: reqwest (rustls + streaming), gix-pack pinned to a verified release,
flate2, sha1, bytes, and the async stream/reader adapters actually needed.
gix-object is optional; the loose header can be written directly. Do not pull
in the general gix fetch/index machinery.

### 3.3 Limits and cancellation

Keep 24 overall operation permits, but start with only **two blocking decoder
workers**. Move each operation's owned permit and staging directory into its
worker; neither is released until that worker exits. Caller drop sets an RAII
cancellation token. Check it and an absolute deadline between bounded chunks,
delta instructions, metadata work, and publication. Supervise worker handles
and join workers during shutdown.

`spawn_blocking` cannot be aborted after it starts. Dropping its JoinHandle
is insufficient. Dropping the HTTP body stops consumption for that request;
it need not close a shared HTTP/2 connection. Do not promise interruption of
an already-blocked OS filesystem call.

Initial limits to implement and validate, rather than unbounded allocations:

- Fixed 64 KiB payload-copy chunks; application-buffer allowances at most
  1 MiB/download and 8 MiB/decoder; a separate 32 MiB metadata allowance per
  decoder. Account for table capacity/allocator overhead or preallocate fixed
  compact arrays; reject oversized counts before allocating. Start with a
  100,000-entry cap and a 64-delta depth cap.
- At most 1,000 wants/request. Reject packs above 512 MiB and decoded objects
  above 512 MiB; cap expanded per-transfer staging at 1 GiB. These are explicit
  operational limits, not Git-format limits. A commit can exceed the entry
  cap regardless of its single want.
- Bound downloaded staging globally (start with 2 GiB) and decoder staging
  separately (4 GiB), counting intermediate payloads, instructions and staged
  loose output. Check/reserve space before writes. Fail growth reservations
  promptly rather than deadlocking while holding partial reservations. On a
  batch-size resource failure, clean up and split/retry smaller wants; a
  single over-limit want returns a clear resource-limit error.
- Keep finite connect, full-operation and low-progress deadlines, including
  token acquisition. Do not replace transport timeouts with infinite workers.

These starting settings need measured peak **cgroup memory and swap** under
concurrent Linux browsing before acceptance. Payload buffers do not bound
HTTP/TLS, libgit2, VFS, allocator, metadata or filesystem-cache overhead. Adjust
concurrency/caps downward if needed to stay comfortably below MemoryMax=1G.

### 3.4 Large local reads and cache rollout

Keep ordinary libgit2 reads. For `cat_blob_to_file`, stream the loose blob with
Rust zlib and fixed buffers, validating canonical header, size, stream end and
OID while writing the existing materialized-file temp. Keep the >8 MiB path
streaming; remove the earlier permission to load arbitrary large blobs into a
Vec. Small-blob reads remain as they are.

The installed git2 0.19.0 has `Odb::reader`, but libgit2 1.8.1 supports it for
loose objects only, and that implementation mmaps compressed input. Packed-only
streaming failed in a compiled probe. Therefore start a **fresh v3 layout**
with a small `src/paths.rs` version change; leave v2 untouched and unused.
Document the extra disk use and optional later manual v2 deletion. Do not wipe
the installed cache as part of development. No old-pack compatibility work is
required before v0.2.0.

New repos use `git2::Repository::init_bare` inside spawn_blocking. Preserve
`remote.origin.url` so fetch methods taking only a directory can recover the
remote without call-site changes. Delete promisor,
partialclone and Git-maintenance configuration in the new store. There is no
shallow file to update because all reads are by OID and history is not traversed.

## 4. Wire-format contract

See the research sources for full normative details. These specifics are easy
to get wrong:

- pkt-line has a four-byte hex length including its header. Recognize flush
  `0000`, delimiter `0001`, response-end `0002`; reject malformed/oversized
  lengths and truncation. Bound section bytes and line counts too.
- Discovery: GET `<repo>.git/info/refs?service=git-upload-pack`, header
  `Git-Protocol: version=2`. Accept a direct v2 advertisement or GitHub's
  observed `# service=git-upload-pack`, flush, then v2 advertisement.
- Command POST: `<repo>.git/git-upload-pack`, same Git-Protocol header,
  Content-Type `application/x-git-upload-pack-request`; response type
  `application/x-git-upload-pack-result`.
- Request: pkt(`command=fetch\n`), optional advertised capabilities,
  **`0001`**, argument pkt-lines, `0000`. ls-refs uses the same structure.
  The argument is **ref-prefix**, never ref-pattern.
- With `done`, acknowledgments are omitted. Parse optional shallow-info and
  delimiters, then `packfile` and sideband packets: channel 1 pack bytes,
  channel 2 progress, channel 3 fatal errors. Validate terminal framing too.
  Never assume an entry or checksum fits within one packet.
- Do not request packfile-uris. A compliant server will not send them;
  reject an unexpected section. This has nothing to do with blob:none.
- Use Basic auth with username `x-access-token`, token as password. The public
  endpoint accepted this in a live probe; private-repo access still needs a
  controlled acceptance test. Prefer env tokens, otherwise a singleflight
  hourly `gh auth token` refresh; cache missing credentials too. Public access
  must work without gh. Send credentials only to their configured GitHub
  origin and never log them.
- Preserve not-found behavior for 404/unknown wants; keep transient transport,
  429 and 5xx failures out of the negative repo cache. Keep or replace the
  timeout error variant; deadlines remain part of the contract.

## 5. Preserve the seam; remove obsolete machinery

Keep existing `GitCli` call sites initially (the name may be cleaned up later):
`ls_remote_head`, `ls_remote_all`, **`init_repo`**, `fetch_commit`,
`fetch_blobs`, `cat_blob_to_file`, `url`, and `with_base_url`.
`init_remote_config` is not a method in the current tree.

Keep HEAD/ref caching, directory/commit/blob singleflight and materialization.
Change `ensure_commit` to use completion markers. The old `commit_fetch` mutex
can be removed once shallow-file mutation disappears: independent server-side
upload-pack requests do not race a local shallow file. Per-commit singleflight
still handles duplicate requests.

Delete git command/run/process-group wrappers; repack/maybe_repack, fetch guards
and idle thresholds; pin refs; shallow-lock cleanup; pack-count helpers and
maintenance repack loops (including explicit gc repacking); stale pack-pool
generation machinery if it has no remaining purpose. Preserve blob age-out gc.
Remove/update repack status/result fields since compatibility is not required.
Update doctor and documentation to remove the git runtime dependency, keeping
OS mount/service helpers.

## 6. Hermetic transport and tests

Use a public injectable transport seam whose discovery/command responses are
**streamed** (e.g. boxed async futures returning boxed AsyncRead bodies plus
status/headers). Do not define a reqwest-specific return type or buffer whole
responses. HTTPS uses reqwest; the test implementation lives in
`tests/common/mod.rs` and is injected with a constructor such as
`GitCli::with_transport`.

Integration tests compile the library without cfg(test), so a FileTransport
hidden under cfg(test) in src cannot serve them. A shared library interface
plus a test-owned implementation avoids any production git command path.
The test adapter sets `GIT_PROTOCOL=version=2` and runs:

- discovery: `git upload-pack --stateless-rpc --advertise-refs <path>`;
- command: `git upload-pack --stateless-rpc <path>`, stdin carrying the exact
  pkt-line request and stdout streamed to the parser.

Drain stderr concurrently with a cap; clean up the test process group on
cancellation. The adapter needs no server-session state between HTTP-like
commands. These modes were exercised successfully with the current fixtures'
allowFilter/allowAnySHA1InWant configuration.

Update fixtures where necessary; do not promise all tests remain unchanged.
Replace pack-count batching/singleflight assertions with recorded fetch/want
counts, including asserting all requested OIDs. Replace repack assertions with
zero retained packs and readable loose objects. Keep behavior tests for huge
directories, subtree prefetch, refs/tags and parallel first reads.

Add meaningful tests for malformed pkt-lines/sections; real HTTP headers,
capabilities, errors, stalls and cancellation; OFS/REF forward/backward deltas,
chains and malformed instructions; quota/ENOSPC cleanup; cancellation while
inflating/applying/publishing; and restart after partial publication before a
completion marker. Cross-check pack/object fixtures with Git. The research
spike is evidence, not a replacement for these production tests.

## 7. Migration order and acceptance

Each implementation step should compile and pass the relevant tests:

1. Review and independently validate the existing stopgap diff, then commit it
   separately if proceeding with implementation. It kills groups on **timeout**;
   do not claim it also fixes arbitrary caller cancellation. Run fmt in the
   devshell before any commit. This research task does not commit that diff.
2. Build the bounded disk-backed ingester first, adapting the retained spike
   into production-quality code with limits, queues, all object kinds, loose
   publication, completion semantics and cancellation. Prove the difficult path
   before switching the remote transport.
3. Add streaming transport injection, pkt-line/capability handling, HTTPS and
   the test-owned upload-pack adapter. Replace ls-refs through the existing seam.
4. Wire commit/blob fetches and completion-marker recovery; change initialization
   to libgit2 and cache root to v3. Replace large-blob materialization.
5. Remove obsolete repack/subprocess state and update behavioral assertions,
   doctor, AGENTS.md, README, and docs/how-it-works.mdx as applicable.
6. Run `nix develop -c cargo fmt`, full tests, and check/build in the devshell.
7. Use scratch daemon mount/cache/socket paths (AGENTS.md) for real GitHub
   smoke tests: a small public repo and Linux pinned by commit, directory
   prefetch, an explicitly wanted blob, and a >8 MiB blob. Verify no git
   descendants and zero retained *.pack/*.idx files in **the new repo store**.
   Do not mistake an active temporary transfer pack for a retained cache pack.
8. Under a scratch cgroup with MemoryMax=1G, exercise concurrent browsing and
   cancellation while monitoring memory.current, peak memory, swap, CPU/disk,
   staging quotas, active worker permits, and cleanup. Test process restart
   after interrupted publication. Do not accept bounded-memory claims based
   solely on the small decoder's VmHWM measurement.

Do not restart/redeploy the installed daemon as part of implementation until
the scratch acceptance checks pass and deployment is authorized. Research has
left that daemon and the existing cache alone.
