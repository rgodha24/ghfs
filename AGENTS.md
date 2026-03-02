# GHFS

Read-only FUSE filesystem that exposes GitHub repos via cached git worktrees. Uses a bare mirror plus per-generation worktrees so git metadata writes stay outside the mount. The cache refreshes by swapping a `current` symlink, keeping the visible tree immutable.

## Key Flows

- Entering `/<owner>/<repo>` triggers cache materialization via `RepoCache::ensure_current`.
- Cache uses a bare mirror + immutable worktree generations; `current` symlink swaps atomically.

## commands

`flake.nix` devshell
wrap cargo commands with `nix develop -c`

```bash
nix develop -c cargo check
nix develop -c cargo test
```

Network tests (git clone/fetch) are opt-in:

```bash
GHFS_RUN_NETWORK_TESTS=1 nix develop -c cargo test
```

## other stuff

this has no users yet. do NOT worry about 'breaking changes' and preserving compatibility until we hit v0.2.0.
always run `nix develop -c cargo fmt` before committing. YOU MUST RUN FMT IN THE DEVSHELL
