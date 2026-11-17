# macOS testing for the pure-Rust remote layer

Preflight: 2026-11-16. This records the actual remote environment and isolates
all testing from the user's installed daemon. Results are at the end.

## Test execution placement

Per the user’s later instruction, **Linux builds/tests run on Tesser devboxes**.
Native macOS compilation and real NFS mount checks still run on `ssh mac`,
which the user explicitly authorized; a Linux devbox cannot validate those.
All cargo build/test/fmt commands on either platform remain wrapped in
`nix develop -c`. Source snapshots are transferred from the shared working
tree so uncommitted new modules are included.

## Environment verified

- `ssh -o BatchMode=yes mac` reaches `rohans-laptop-2.local`, Apple Silicon,
  Darwin 24.6.0 (macOS 15 kernel), as `rohangodha`.
- SSH's noninteractive fish environment omits useful tool paths. Run commands
  through **`/bin/bash -lc`**; its login environment finds Nix, cargo, rustc,
  ripgrep, rsync, and gh.
- Nix: Determinate Nix 3.0.0 / Nix 2.26.3,
  `/nix/var/nix/profiles/default/bin/nix`.
- Login-shell cargo/rustc: 1.93.1. Use this repository's
  `nix develop -c cargo ...` for all actual builds, tests, and fmt.
- Apple clang is available via `xcrun`, under the installed Xcode toolchain.
- RAM: 16 GiB. `/private/tmp` has about **25 GiB available**; avoid multiple
  release targets or huge fixture sets and record remaining space during stress.
- Default file-descriptor soft limit is 256. The daemon's existing
  `raise_fd_limit` should increase it; check the scratch daemon's effective limit
  during concurrent tests.
- HTTPS GitHub discovery from this Mac succeeded: HTTP 200, Git v2 advertisement,
  against `octocat/Hello-World.git` with the Git-Protocol v2 header.
- `sudo -n true` requires a password. The existing daemon runs as the ordinary
  user and mounts with the unprivileged `/sbin/mount_nfs` path. No sudo is used
  by `src/nfs/mod.rs`; actual scratch mounting remains an acceptance check.

## Installed state: do not touch

The Mac's installed service **is running**, unlike the Linux daemon described
in the original handoff:

- launchd label: `com.ghfs.daemon`;
- observed PID: 1036, user `rohangodha`, parent 1;
- binary: `/nix/store/04iyg4sllav9p36fi9xkrfmgj4p8s4qp-ghfs-0.1.6/bin/ghfs`;
- NFS mount: **`/private/tmp/ghfs`** (`/tmp/ghfs` reaches the same location);
- existing checkout: `/Users/rohangodha/Developer/ghfs`, clean at
  `b7d094e655c86ec23ec87462ebecac1b49c7f93c` during preflight.

Do not stop/restart this service, unmount its path, modify its cache, or copy
work into its checkout. `/tmp` is a symlink to `/private/tmp`; pass canonical
`/private/tmp/...` paths to the scratch daemon so NFS mount detection agrees.

## Owned scratch space

Created and owned by this task:

```text
/private/tmp/ghfs-pure-rust.OdQ08y/
    src/       # working-tree snapshot goes here; currently empty
    mnt/       # dedicated NFS mount
    cache/     # dedicated cache
    logs/      # build, daemon, and e2e evidence
    envcheck/  # flake/Cargo metadata copied for Nix environment-only preflight
```

The Unix socket will be `/private/tmp/ghfs-pure-rust.OdQ08y/ghfs.sock`.
No installed path overlaps this root. Only remove paths under this task root
when cleaning up.

## Source transfer and build

Transfer the **current working tree**, including uncommitted new modules;
a clone of HEAD alone would miss implementation files. From the Linux repo:

```sh
rsync -a --delete --exclude=.git --exclude=target \
  --rsync-path=/Users/rohangodha/.nix-profile/bin/rsync \
  ./ mac:/private/tmp/ghfs-pure-rust.OdQ08y/src/
```

The explicit remote rsync path avoids SSH's minimal fish PATH. `--delete` is
safe only for this dedicated source directory; do not point it at the installed
checkout. Never sync local build outputs. The source snapshot intentionally
has no `.git`, so Nix sees all transferred new files.

Run each build command in a login bash on the Mac, from that source directory:

```sh
ssh mac "/bin/bash -lc 'cd /private/tmp/ghfs-pure-rust.OdQ08y/src && nix develop -c cargo test --all-targets'"
ssh mac "/bin/bash -lc 'cd /private/tmp/ghfs-pure-rust.OdQ08y/src && nix develop -c cargo build --release'"
```

Keep one target directory shared by successive source snapshots. Re-sync code
only after a Tesser/Linux test cycle is ready, then rerun affected checks and the final
full suite. Capture the build revision/snapshot and log beside every result.

## NFS acceptance sequence

Use the newly built scratch binary with all three paths explicitly set:

```sh
GHFS_MOUNT_POINT=/private/tmp/ghfs-pure-rust.OdQ08y/mnt \
GHFS_CACHE_DIR=/private/tmp/ghfs-pure-rust.OdQ08y/cache \
GHFS_SOCKET=/private/tmp/ghfs-pure-rust.OdQ08y/ghfs.sock \
RUST_LOG=ghfs=debug \
/private/tmp/ghfs-pure-rust.OdQ08y/src/target/release/ghfs daemon
```

Record **this scratch process PID**. Start with a cold cache and wait for its
own NFS mount and socket. Keep the user's service PID/mount recorded separately.

1. Read a small known file through both default-branch and pinned-by-ref paths;
   compare exact bytes/SHA-1 to the direct GitHub reference. Check listing,
   metadata, symlinks, tags, and a missing repository/path.
2. Browse Linux pinned to a full commit, list a wide directory, and read many
   files concurrently to exercise subtree prefetch and directory batching.
   Collect request counts/bytes from the daemon's instrumentation and verify
   complete output, not just successful directory listings.
3. Read a blob larger than 8 MiB in chunks and concurrently with small reads;
   compare the complete digest and test a near-EOF read.
4. Warm-cache repeat reads and a graceful daemon restart must preserve bytes
   and require no unnecessary object fetches.
5. Kill **only the scratch daemon** during download/ingestion/publication;
   restart against its scratch cache and verify successful recovery, no false
   completed commit, no abandoned staging after recovery, and correct contents.
6. Exercise stalled/failed fetches with the test transport/HTTP fixtures and
   confirm other NFS requests still finish. The hermetic integration suite
   must cover transport cancellation and quotas independently of kernel retries.
7. During concurrent operations, record `ps` RSS/CPU and available disk. Verify
   no git process is a descendant of the scratch daemon, and no persistent pack
   or index files exist in its new repository store. Temporary active transfer
   packs belong outside `objects/pack` and disappear after work completes.
8. Confirm macOS's `soft,nobrowse,dumbtimer,timeo=300,retrans=10` options on the
   actual scratch mount. Verify ordinary shell reads do not trigger a server
   interruption dialog or premature soft timeout on a cold fetch.

Cleanup: signal the scratch daemon gracefully, wait for its NFS unmount, and
force-unmount only `/private/tmp/ghfs-pure-rust.OdQ08y/mnt` if a killed process
left it mounted. Check the installed `/private/tmp/ghfs` mount and
`com.ghfs.daemon` are still present afterwards. Keep logs/test evidence until
reported; remove scratch build/fixture data when no longer needed.

## Nix devshell availability

The repo-pinned Darwin devshell was not already fully cached. An offline probe
turned substitutes off and attempted source bootstrapping, failing at the
retired `bmake-20251111.tar.gz` URL (404). This does not diagnose the normal
substituted build. A normal `nix build --dry-run` for
`devShells.aarch64-darwin.default` found binary-cache substitutes: 223.32 MiB
download / 1581.59 MiB unpacked, plus 18 Rust component/environment derivations
that fetch the pinned Rust 1.93.0 distribution.

A normal, isolated `nix develop --no-update-lock-file` preflight successfully
realized that environment under `envcheck`, without compiling or testing ghfs.
It reports cargo/rustc **1.93.0** and `mount_nfs: available`. The full log is
`logs/devshell-preflight.log`. About 22 GiB disk remains. Cache-only per-command
substituter/connect/stall settings avoided the offline bootstrap path; no global
Nix configuration changed.

## Results

2026-11-17: the hermetic suite, the release build, the scratch NFS e2e (including GitHub), and
the mount-option check all passed. See [pure-rust-test-evidence.md](pure-rust-test-evidence.md).
