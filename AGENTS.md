# GHFS

Read-only filesystem that exposes GitHub repos at `/<owner>/<repo>` (default branch) and `/by-ref/<owner>/<repo>/<ref>` (branch, tag, or full commit id). Linux mounts via FUSE (`src/fuse.rs`); macOS mounts our own NFSv3 server on localhost (`src/nfs/`) with the system `mount_nfs`.

## Key Flows

- Everything async runs on one tokio runtime. FUSE callbacks and NFS RPCs each run as their own task, so a slow fetch never blocks other requests.
- `src/vfs/` is the shared filesystem core; `src/repos.rs` owns per-repo state.
- HEAD comes from Git protocol-v2 `ls-refs` and is cached (memory + `ghfs-head` file); stale values are served immediately and refreshed in the background, so only the very first visit to a repo waits on the network.
- Each commit is fetched shallowly without blobs (`deepen 1`, `filter blob:none`). Blobs are fetched per directory in explicit-want batches; listing a directory prefetches its subtree in the background. HTTPS/protocol framing lives in `src/git/{transport,wire,remote}.rs`.
- Concurrent requests for the same commit/directory share one fetch (`src/singleflight.rs`). Fetches spool temporarily, then bounded disk-backed delta decoding writes only loose objects (`src/git/{pack,loose}.rs`). No persistent packs, git subprocesses, or repacks.
- Cache layout lives in `src/paths.rs`; the new store is `v3/`. Old `v2/` is retained unused; abandoned v3 transfers are removed before requests start.
- The NFS server only answers the daemon user's uid (AUTH_UNIX credential; root allowed). A process that connects to the port directly can forge that, so this only stops other users going through the mount.
- Blob contents are served from a content-addressed file cache (`v3/blobs`), written once per blob; files are checked against the object size on first use each run instead of being fsynced.
- macOS mount options matter: `soft,nobrowse` suppresses the "Server connections interrupted" dialog, `dumbtimer,timeo=300,retrans=10` keeps slow first fetches from timing out (see `src/nfs/mod.rs`).

## Testing

Linux builds/tests run on Tesser devboxes: see `.agents/skills/tesser/SKILL.md`.
macOS checks run natively through `ssh mac` in an isolated checkout.

`tests/` runs hermetically against `file://` remotes (no network). For a real mount without touching the installed service, point the daemon at scratch paths:

```bash
GHFS_MOUNT_POINT=/tmp/ghfs-e2e/mnt GHFS_CACHE_DIR=/tmp/ghfs-e2e/cache GHFS_SOCKET=/tmp/ghfs-e2e/ghfs.sock ghfs daemon
```

On NixOS, put `/run/wrappers/bin` first in `PATH` so FUSE uses the setuid `fusermount3`.

## commands

`flake.nix` devshell
wrap cargo commands with `nix develop -c`

```bash
nix develop -c cargo check
nix develop -c cargo test
```

## other stuff

this has no users yet. do NOT worry about 'breaking changes' and preserving compatibility until we hit v0.2.0.
always run `nix develop -c cargo fmt` before committing. YOU MUST RUN FMT IN THE DEVSHELL
