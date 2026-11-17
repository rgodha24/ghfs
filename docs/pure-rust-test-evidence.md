# Pure-Rust remote: test evidence

Final acceptance run for the pure-Rust remote/pack implementation
([plan](pure-rust-implementation-plan.md), [design review](pure-rust-astra-review.md)).
Date: 2026-11-17. Source: uncommitted working tree on top of `b7d094e`.
Installed services and caches on both machines were not touched.

## Hermetic suites

All cargo commands ran inside the repository devshell (`nix develop -c`).

| Platform | Where | Command | Result |
|---|---|---|---|
| Linux x86_64 | Tesser workbench `box_f5a6286fdfff540b` | `tesser exec -- bash .agents/skills/tesser/run.sh cargo test --all-targets` | lib 93, nfs 4, remote 5, vfs 20 passed |
| Linux | same | `cargo fmt --check`, `cargo build --release` | clean / built |
| macOS 15 (arm64) | `ssh mac`, isolated checkout `/private/tmp/ghfs-pure-rust.OdQ08y/src` | `nix develop -c cargo test --all-targets` | lib 93, nfs 4, remote 5, vfs 20 passed |
| macOS | same | `cargo build --release` | built |

`cargo clippy --all-targets` is clean.

## Real mounts: `tests/e2e_mount.py --github`

The harness builds a local fixture repository, serves it with
`git http-backend` over smart HTTP, and mounts a scratch daemon (its own mount,
cache, and socket). It checks every read against independent `git cat-file`
bytes. It then mounts against real GitHub: `octocat/Hello-World` at HEAD and
pinned to `7fd1a60`, and `torvalds/linux` pinned to the current HEAD commit.
For the Linux files it compares `README`, `Makefile`, `drivers/Makefile`,
`net/ipv4/tcp.c`, and lists `include/linux`. The Linux reference bytes come from
a `git clone --filter=blob:none` that the test itself runs.
(raw.githubusercontent.com returned 429 to the cloud IP on the first attempt.
This run uses `git` instead.)

Covered: 8 parallel cold first reads, branches including `feature%2Fone`,
annotated tags, full commit ids, an executable bit, a symlink, a gitlink
(an empty file), ENOENT, EROFS, a 1500-file directory (byte digest of every
file), 4 × 12 MiB blobs including a read near EOF, and want batches of at most
1000. Warm reads cause zero fetches. Restart repairs a corrupt and a missing
tree beneath a completion marker, and removes abandoned staging. It also covers
a SIGKILL during a stalled HTTP fetch, with cached reads still served while
stalled and recovery after restart. A `v2/` sentinel is preserved.

| | Linux FUSE (Tesser) | macOS NFS (`ssh mac`) |
|---|---|---|
| status | passed (21.5 s) | passed (18.8 s) |
| initial fetches / max wants | 10 / 1000 | 13 / 1000 |
| warm-read fetches | 0 | 0 |
| peak daemon RSS | 19.1 MiB | 23.5 MiB |
| git descendants of the daemon | none | none |
| retained `.pack`/`.idx` in `v3/repos` | none | none |
| loose objects after run | 7993 | 7993 |
| linux commit verified | `666539eecdf4` | `666539eecdf4` |
| `include/linux` entries | 1615 | 1615 |
| leftover daemon / mount after run | none | none |

### Linux under `MemoryMax=1G`

The whole harness ran inside `sudo systemd-run --scope -p MemoryMax=1G
-p MemorySwapMax=0`. It sampled the scope cgroup every second. The figures
cover everything in the scope: the daemon, the Python harness, `git
http-backend`, the reference `git clone` of Linux, and page cache.

- `memory.peak` 395,550,720 B (377 MiB); highest sampled `memory.current` 342 MiB
- `memory.swap.current` 0; `memory.events`: `max 0 oom 0 oom_kill 0`
- I/O on the main device: about 379 MB written, 0.3 MB read
- final cache size 139 MiB; no `.pack`/`.idx` files anywhere in the cache

An earlier attempt, before the reference-byte change, failed only at the GitHub
step (the 429). Its hermetic portion passed with `memory.peak` 392 MiB.

### macOS mount options

A scratch daemon was mounted by hand at `/private/tmp/ghfs-pure-rust.OdQ08y/opts/mnt`.
`nfsstat -m` shows `ro,nobrowse` and
`vers=3,tcp,soft,intr,locallocks,dumbtimer,timeo=300,retrans=10,deadtimeout=60`.
It read `octocat/Hello-World/README` correctly. After SIGTERM the daemon exited
0 and unmounted. The installed `com.ghfs.daemon` (PID 1036) and the
`/private/tmp/ghfs` mount were present before and after every Mac step.

## Final implementation review

The Astra (codex) review run died before writing anything. A Claude subagent
did the review instead, with the same brief. It found no high-severity issues
and confirmed that the design-review fixes are implemented. The fixes for its
findings:

- **Anonymous 401 was EIO.** Smart HTTP answers 401 to anonymous requests for
  both private and nonexistent repos. When no credential was sent, that now maps
  to not-found, so it gets negative-cached and returns ENOENT
  (`src/git/transport.rs`, `tests/remote.rs`).
- **One bad want stranded the rest of a split batch.** `fetch_blobs` now keeps
  fetching the remaining halves and reports the first per-want error at the end.
  `fetch_blob` also falls back to a single-blob fetch when the directory batch
  fails.
- **A corrupt loose blob was never repaired.** Loose writes aren't fsynced. Blob
  header, contents and streaming reads now drop a corrupt object and refetch it.
  Testing this exposed a worse problem: the bundled libgit2 **spins forever**
  inflating a truncated loose object (`read_loose_standard` →
  `git_zstream_get_output`, found with gdb). Blob reads now go only through our
  bounded, hash-verifying loose reader. Regression test:
  `corrupt_loose_blobs_are_dropped_and_refetched`, which covers truncated and
  garbage objects, both small and over 8 MiB.

Not changed: the per-window low-progress rule could kill a server that takes
more than about 20 s to start sending a huge tree. Concurrent huge transfers
fail fast with `Quota` rather than queueing. Both are intended for now.

After these fixes, both mount e2e runs (`--github`) were rerun and passed. On
Linux under 1G, `memory.peak` was 369 MiB with no OOM, swap 0, and daemon RSS
18.9 MiB. On macOS, daemon RSS was 22.8 MiB and the installed service was
untouched.

## Not verified

- Private repositories over real GitHub. The `gh auth token`/`GH_TOKEN` path
  is covered only by the mock-transport tests.
- No deployment: the installed services still run v0.1.6.
