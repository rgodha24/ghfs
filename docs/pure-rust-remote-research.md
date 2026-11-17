# Pure-Rust remote layer: research and implementation decisions

Investigated 2026-11-16 against Git's protocol/source, git2 0.19.0 +
libgit2 1.8.1 (the versions in this repo), Tokio, and gitoxide source. This
is design research, not a production implementation. The companion
[pack report](pure-rust-pack-research.md) records the decoder experiments;
[pure-rust-remote-handoff.md](pure-rust-remote-handoff.md) is the implementation brief.

## Decision

Proceed with protocol v2 over reqwest, disk-backed pack ingestion, and a
persistent loose-object store. Keep libgit2 for local reads. Allow temporary
pack files under an operation-owned staging directory; prohibit retained
packs, pack indexes, repacking, and git subprocesses in the remote layer.

The original proposal's difficult part is real: the convenient gix-pack
APIs buffer complete objects and delta data. They do not provide the stated
memory guarantee. A scratch Rust implementation using gix-pack's low-level
entry parser, streaming zlib, and file-backed delta application successfully
reconstructed 16 MiB and 128 MiB objects, including forward REF_DELTA and
OFS_DELTA. The companion report supplies source, measurements, and limits.
This proves feasibility, not production readiness or a whole-daemon RSS bound.

## What was verified live

An isolated scratch probe sent pkt-lines directly using Python urllib,
without using git as the network client. Git CLI calls only initialized and
inspected scratch object databases. The installed daemon was left stopped.

Against `https://github.com/octocat/Hello-World.git`:

- Discovery returned HTTP 200 and advertised `ls-refs=unborn`,
  `fetch=shallow wait-for-done filter`, and `object-format=sha1`.
- GitHub included `# service=git-upload-pack`, flush, then `version 2`.
  Git's current v2 documentation illustrates a response starting directly
  with `version 2`. Accept both, while requiring actual v2. [S1]
- `ls-refs` with `symrefs`, `peel`, and prefixes `HEAD`, `refs/heads/`,
  `refs/tags/` returned HEAD `7fd1a60b01f91b314f59955a4e4d4e80d8edf11d`
  and `symref-target:refs/heads/master`.
- Fetching that commit with `deepen 1`, `filter blob:none`, `ofs-delta`,
  `no-progress`, `done`, and **no haves or thin-pack** returned a 300-byte,
  two-object pack: commit and tree. Its README blob was absent, as intended.
- Fetching README OID `980a0d5f19a64b4b30a87d4206aade58726b60e3` with
  `filter blob:none`, `no-progress`, and `done` returned a 54-byte,
  one-object pack containing `Hello World!\n`. Explicit blob wants work
  with the filter. Both pack trailers matched independently computed SHA-1.
- A public `ls-refs` request using Basic auth with username `x-access-token`
  and the configured `gh auth token` succeeded. No credential was logged.
  **Private-repository access and token refresh were not tested.** GitHub
  documents using the personal access token in place of an HTTPS password. [S2]

Local `git upload-pack --stateless-rpc` with `GIT_PROTOCOL=version=2`
accepted the same command bytes and returned the expected HEAD and filtered
explicit blob. Discovery used the additional `--advertise-refs` argument.
The test repository enabled `uploadpack.allowFilter` and
`uploadpack.allowAnySHA1InWant`, as current fixtures already do. [S3]

These small probes did not exercise a Linux tree fetch, adverse network
conditions, or production cancellation. Those remain implementation acceptance
tests rather than claims made by this research.

## Correct protocol contract

Discovery is `GET <repo>.git/info/refs?service=git-upload-pack` with
`Git-Protocol: version=2`. Command requests POST to `<repo>.git/git-upload-pack`
with that header and Content-Type `application/x-git-upload-pack-request`;
expect `application/x-git-upload-pack-result`. Check status before parsing. [S1, S4]

Every command body is:

```text
pkt("command=<command>\n")
[advertised command-independent capabilities, if used]
0001
pkt("<argument>\n") ...
0000
```

Use **`ref-prefix`**, not `ref-pattern`. Filter replies locally too: prefix
filtering is an optimization, and the server may send extra refs. Request
HEAD alone for HEAD lookup; HEAD/heads/tags for the full listing, avoiding
GitHub's potentially large pull-ref namespace. Discovery plus the first
ls-refs is **two HTTP requests**; cached discovery avoids repeated GETs. [S1]

Require advertised `fetch` features `filter` and (for commit fetches)
`shallow`; reject unsupported servers rather than fetching full history/blobs.
Support SHA-1 initially and reject another `object-format` explicitly.
Send no `thin-pack`, `sideband-all`, `include-tag`, `want-ref`, or
`packfile-uris` argument. A compliant server sends URI sections only when
requested; `blob:none` does not control that. Reject an unexpected URI
section. Request `ofs-delta` because the decoder supports it. [S1]

With `done`, the acknowledgments section is omitted. Parse optional
shallow-info, delimiters, the required packfile header, sideband channel 1
bytes, channel 2 progress, channel 3 errors, and the response terminator.
Pkt-line boundaries do not coincide with pack-entry or trailer boundaries.
Bound packet lengths, section sizes and line counts; validate hex lengths,
truncation, unexpected sections, and terminal framing. A valid pack trailer
alone does not make a truncated protocol response successful. [S1, S5]

A durable-looking completion marker is not enough after power loss unless
object and directory fsyncs precede it. Prefer validating the marked tree
closure once per daemon run, then caching that result, to avoid fsyncing
thousands of loose objects during ingestion. Missing/corrupt trees invalidate
the marker and trigger a fresh fetch.

## Resource ownership and cancellation

Use two sequential phases instead of bridging async HTTP directly into a
blocking decoder:

1. Stream sideband-demultiplexed pack bytes to an owned temporary file with
   fixed application buffers. Compute/check the pack SHA-1 trailer and
   consume the complete protocol response. Flush pending file writes before
   handing the file to the decoder. Do not use `Response::bytes()`. [S6]
2. Acquire an ingestion permit, then `spawn_blocking` the disk-backed
   decoder. Move the operation's staging directory and owned global permit
   into that closure. Use a small, separate ingestion limit (start with 2)
   alongside the existing overall operation limit (24).

Tokio explicitly says running `spawn_blocking` tasks cannot be aborted.
Dropping a JoinHandle does not cancel the task. Use an operation cancellation
flag/token set by an RAII guard on caller cancellation, plus a deadline
checked inside every bounded inflate/copy loop and between metadata and
publication operations. Hold the semaphore permit until the worker exits,
including after the caller disappears. Supervise worker handles and join
workers during shutdown. This prevents cancellation from releasing capacity
while old work is still consuming resources. [S7]

There is no claim that a blocking filesystem syscall can be forcibly
interrupted. Nor is there a portable guarantee that dropping an HTTP body
closes the physical socket: HTTP/2 can reset just the stream. The guarantee
is stopping this operation's body consumption and cooperatively stopping
its CPU/disk work, with bounded outstanding work. [S7, S8]

Bound **all** retained data: object-count/metadata, packet/section buffers,
downloaded pack bytes, inflated staging bytes, delta depth, open files, and
published object sizes. Split oversized *batches* and retry smaller wants
where appropriate; a single object exceeding a configured size limit gets
a clear resource-limit error. Never silently allocate it in memory. Check
byte counters before growth; do not trust advertised pack counts or sizes.
Avoid mmap in the ingestion path. Concrete caps require the large-tree and
whole-daemon measurements in the handoff; 24 is not itself a memory budget.

Initial application-buffer target: at most 1 MiB per download and 8 MiB per
ingester, plus a separately capped 32 MiB metadata budget per ingester.
With 24 operation slots and two ingesters this leaves substantial headroom
under the incident's 1 GiB cgroup limit, but does **not** account for all
TLS/HTTP, allocator, libgit2, VFS, and filesystem-cache costs. Verify cgroup
memory and swap under concurrent browsing before accepting these settings.

## Large blob reads and existing cache

`git2::Odb::reader` exists in **the installed git2 0.19.0**. The matching
libgit2 loose backend supports it; the pack backend does not. The loose
backend mmaps the compressed file, so do not claim a fixed RSS bound from
this API alone. [S9, S10]

A separate scratch Rust program compiled in `nix develop` and confirmed:

- streaming a 22-byte loose blob succeeded;
- requesting the same API for a packed-only blob failed with
  `object not found - no matching loose object`.

The git2 reader's Read implementation requests the full supplied slice;
limit it with `reader.take(size as u64)` before `io::copy`, and check the
copied byte count and object kind. [S9]

Recommendation: use a Rust zlib loose-blob reader for the >8 MiB materialize
path. Validate header, declared length, stream termination and SHA-1 while
copying into the existing materialized-file temp. Keep ordinary libgit2
reads unchanged. This avoids both packed-reader limitations and large
compressed-file mmap exposure. Never fall back to reading an arbitrary
large blob into a Vec.

Start the new store in **v3**, leaving v2 untouched. A small layout-version
change is justified: it avoids importing 811 incident packs, old promisor
config, and old shallow state, without a migration parser or a packed large
blob fallback. Document the cold refetch and extra disk usage; deletion of
the old cache is a separate cleanup. No existing cache was modified during
research. The original claim that paths must be entirely untouched should
be relaxed for this one change.

## Completion, retries, and singleflight

The current `ensure_commit` checks only commit + root-tree existence.
That is insufficient after an interrupted sequence of loose-object writes:
subtrees may still be absent. Require a per-commit completion marker,
atomically written only after all entries are resolved, verified and
published. Check it on the fast path. Publish each immutable loose object
atomically using a unique same-directory temp; cancellation may leave
already verified objects, but never a completion marker for unfinished work.

The pack checksum must be validated before object publication. Hashing an
object gives its identity; it does not by itself prove the requested OID
was delivered. Verify requested object presence/type and validate commit
and tree closure before marking the fetch complete. Fault-injection tests
must interrupt publication between root and child trees and retry after
both cancellation and daemon restart. [S5; local `src/repos.rs::ensure_commit`]

Keep singleflight semantics, but audit cancellation of its OnceCell
initializer: a replacement caller can start work while the previous blocking
worker is winding down. Unique staging dirs, atomic object publication,
completion markers, and permits owned by the old worker make that safe.
The commit mutex can remain to simplify initial rollout; there is no
server-side shallow.lock race once these independent requests stop writing
local shallow files. [S1; local `src/singleflight.rs`]

## Test seam and necessary test changes

Put the `Transport` trait and injection constructor in the normal library.
Implement the git-spawning transport **inside `tests/common`**, not under
`#[cfg(test)]` in `src`: integration tests compile the library as an ordinary
dependency. Fixture creation changes to inject this transport; production
has no file:// git subprocess implementation. [S11]

The trait must represent both discovery and command responses, with an async
streaming body independent of reqwest. For example, use boxed Send futures
returning a response whose body implements AsyncRead, and adapt reqwest's
byte stream using tokio-util. A `post(Bytes) -> Vec<u8>` abstraction loses
both discovery and memory/cancellation properties. The test adapter spawns
`git upload-pack --stateless-rpc`, sets `GIT_PROTOCOL=version=2`, drains
stderr concurrently with a cap, and owns process cleanup on drop. [S3, S6]

Tests must change with the storage model:

- Replace pack-count batching assertions with recorded fetch requests/want
  counts. Zero packs cannot prove deduplication or batching.
- Replace repack tests with loose-object integrity, concurrent publication,
  completion-marker recovery, and no-persistent-pack assertions.
- Preserve parallel commit tests, replacing stale-lock mechanics with
  cancellation/restart coverage; retain an ignored stale lock in a fixture
  only if testing that it is irrelevant to the new transport.
- Add HTTP mock tests for real discovery, headers, response streaming,
  stalls, malformed framing, status mapping, and body cancellation. Local
  upload-pack does not exercise those behaviors.
- Add delta fixtures: backward OFS, backward/forward REF, chains, missing
  bases, cycles, oversized lengths, invalid copies, checksum/trailer failure,
  and ENOSPC/cancellation cleanup. Cross-check results against Git.

Keep finite connect, overall, and low-progress deadlines. A 404 maps to the
existing not-found behavior; 429/5xx/network failures must not poison the
not-found cache. Use env credentials first, then a singleflight hourly
`gh auth token` refresh, including caching absent credentials so public
browsing does not require gh. Token acquisition gets its own short timeout.
Only attach credentials to their configured GitHub origin; redact auth
headers and URLs. Retry an authentication rejection at most once after
refresh, respecting the overall deadline. These are implementation choices,
not behaviors proven by the live auth probe.

## Sources

- **S1:** [Git protocol v2 specification](https://github.com/git/git/blob/master/Documentation/gitprotocol-v2.adoc), HTTP transport, capabilities, ls-refs, fetch, and response sections.
- **S2:** [GitHub: creating a personal access token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#using-a-personal-access-token-on-the-command-line).
- **S3:** [git-upload-pack](https://git-scm.com/docs/git-upload-pack), stateless-rpc and advertise-refs; [Git v2.50.1 upload-pack source](https://github.com/git/git/blob/v2.50.1/upload-pack.c).
- **S4:** [Git HTTP protocol specification](https://github.com/git/git/blob/master/Documentation/gitprotocol-http.adoc).
- **S5:** [Git pack format and delta encoding](https://github.com/git/git/blob/master/Documentation/gitformat-pack.adoc).
- **S6:** [reqwest 0.12.28 Response](https://docs.rs/reqwest/0.12.28/reqwest/struct.Response.html), chunk/bytes_stream; [tokio-util StreamReader](https://docs.rs/tokio-util/latest/tokio_util/io/struct.StreamReader.html).
- **S7:** [Tokio spawn_blocking](https://docs.rs/tokio/1.49.0/tokio/task/fn.spawn_blocking.html), cancellation and concurrency guidance.
- **S8:** [HTTP/2 RST_STREAM](https://www.rfc-editor.org/rfc/rfc9113.html#name-rst_stream), stream-scoped cancellation.
- **S9:** [git2 0.19.0 Odb](https://docs.rs/git2/0.19.0/git2/struct.Odb.html#method.reader) and [its reader implementation](https://docs.rs/git2/0.19.0/src/git2/odb.rs.html).
- **S10:** libgit2 1.8.1 [loose backend](https://github.com/libgit2/libgit2/blob/v1.8.1/src/libgit2/odb_loose.c), [pack backend](https://github.com/libgit2/libgit2/blob/v1.8.1/src/libgit2/odb_pack.c), and [stream dispatch](https://github.com/libgit2/libgit2/blob/v1.8.1/src/libgit2/odb.c).
- **S11:** [Rust Book: integration tests](https://doc.rust-lang.org/book/ch11-03-test-organization.html#integration-tests).
