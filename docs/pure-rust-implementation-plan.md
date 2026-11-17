# Pure-Rust remote implementation plan

Astra reviewed the handoff in [pure-rust-astra-review.md](pure-rust-astra-review.md).
User authorized implementation, substantial end-to-end testing, sensible subagent
use, and macOS testing through `ssh mac`. Installed services/caches stay untouched. Subsequent steering requires Linux
builds/tests on Tesser devboxes; actual Darwin checks remain native via ssh.
Workbench: `box_f5a6286fdfff540b`.

1. Establish cancellation/deadline, disk quota and worker-lifetime primitives.
2. In parallel, implement checked pack headers + disk-backed delta ingestion /
   loose streaming reads, and protocol v2 + injectable HTTPS transport.
   Omit gix-pack: its only proposed use was header parsing, and Astra identified
   arithmetic checks worth owning explicitly. Non-blob objects are capped at
   8 MiB before libgit2 reads. Keep blobs streaming up to explicit limits.
3. Integrate a fresh v3 cache, bounded commit/tag/tree closure checks and recovery
   markers; validate marked closures once per process. Use unique owned temps
   for materialization. Preserve refs, singleflight, batching and prefetch.
4. Remove runtime git/repack code, maintenance/status fields, and stale tests.
   Add request-count assertions, malformed-pack/protocol/quota tests and
   cancellation/immediate-retry/restart regressions.
5. Run devshell formatting, all-target check/test/build and focused failure tests.
   Run Linux FUSE scratch mounts against GitHub and controlled local HTTP remotes,
   with independent Git byte checks, parallel reads, huge directories, refs,
   submodules, symlinks, >8 MiB blobs, interruption and restart.
6. Copy the uncommitted source to an isolated Mac checkout, run the same hermetic
   suite, then real scratch NFS mounts. Preserve the Mac's existing launchd daemon.
7. Stress a scratch Linux daemon under MemoryMax=1G; record peak/current/swap/I/O,
   staging cleanup, absence of git children and retained packs, and cold/restart
   behavior. Fix defects and obtain a final independent implementation review.
8. Keep commands/results in a final test-evidence document and update runtime
   documentation. No deployment is part of this task.

Baseline: `nix develop -c cargo test --all-targets` passed 58 library + 4 NFS +
16 VFS tests. The original three-file stopgap patch is preserved at
`/tmp/ghfs-original-stopgap.patch`; the final implementation supersedes it.
