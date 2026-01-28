# GHFS - GitHub Filesystem

A FUSE-based filesystem that mounts GitHub repositories at `/mnt/github/<owner>/<repo>/...` with cached checkouts.

## Project Overview

GHFS is a read-only, loopback/passthrough FUSE filesystem that:

- Exposes GitHub repos as local directories
- Caches repos as git worktrees (refreshed every 24h)
- Allows `git log`, `git status`, etc. to work inside mounted directories
- Works on Linux and macOS

### Architecture

```
User access: /mnt/github/<owner>/<repo>/file.rs
                         |
                         v
                    FUSE layer (ghfs-fs)
                         |
                         v
               Cache manager (ghfs-cache)
                         |
                         v
        ~/.cache/ghfs/worktrees/<owner>/<repo>/current/file.rs
```

Key insight: We use `git worktree` with a bare mirror so git metadata writes go to the mirror (outside the mount), allowing read-only mounts while git commands still work.

## File Layout (Target Structure)

> **Note**: This is the planned structure. Implementation will build toward this incrementally.

```
ghfs/
├── agents.md              # This file - docs for AI agents
├── flake.nix              # Nix devshell configuration
├── Cargo.toml             # Workspace root
├── crates/
│   ├── ghfs/              # CLI binary
│   │   ├── Cargo.toml
│   │   └── src/
│   │       └── main.rs    # CLI entry point (clap)
│   ├── ghfs-fs/           # FUSE filesystem implementation
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── inode.rs   # Inode table management
│   │       └── ops.rs     # FUSE operation handlers
│   ├── ghfs-cache/        # Git + cache manager
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── git.rs     # GitRunner, clone, fetch, worktree
│   │       ├── paths.rs   # CachePaths
│   │       └── repo.rs    # RepoCache, locking, ensure_current
│   └── ghfs-types/        # Shared types
│       ├── Cargo.toml
│       └── src/
│           └── lib.rs     # Owner, Repo, RepoKey, GenerationRef
└── tests/                 # Integration tests
    └── integration/
```

### Cache Directory Layout

```
~/.cache/ghfs/
├── mirrors/
│   └── <owner>/
│       └── <repo>.git/           # Bare repo (shallow)
├── worktrees/
│   └── <owner>/
│       └── <repo>/
│           ├── gen-000001/       # Immutable generation
│           ├── gen-000002/       # Newer generation
│           └── current -> gen-000002/  # Atomic symlink
└── locks/
    └── <owner>__<repo>.lock      # flock-based locking
```

## Development Setup

### Prerequisites

- Nix with flakes enabled (recommended, Linux only for now)
- Or: Rust 1.85+ (edition 2024), libfuse3-dev (Linux) / macFUSE (macOS)

### Using Nix (recommended)

```bash
nix develop
```

This provides: `rustc`, `cargo`, `fuse3`, `pkg-config`, `git`

> **Note**: Nix devshell currently only provides fuse3 on Linux. macOS users should install macFUSE manually.

### Build & Test

```bash
nix develop -c cargo build                    # Build all crates
nix develop -c cargo test                     # Run all tests
nix develop -c cargo run -- --help            # Run CLI (once implemented)
```

## Commit Conventions

We use conventional commits with scope:

```
<type>(<scope>): <description>

[optional body]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `refactor`: Code restructuring (no behavior change)
- `test`: Adding/updating tests
- `docs`: Documentation only
- `chore`: Build, deps, tooling

### Scopes

- `types`: ghfs-types crate
- `cache`: ghfs-cache crate
- `fs`: ghfs-fs crate
- `cli`: ghfs binary/CLI
- `nix`: Nix/flake changes
- (none): Cross-cutting changes

### Examples

```
feat(cache): add shallow clone with depth=1
fix(fs): return EROFS for write operations
test(cache): add concurrent access test
refactor(types): rename RepoKey to RepoId
docs: update agents.md with new layout
chore(nix): add fuse3 to devshell
```

### Commit Atomicity

Each commit should:

1. Be a single logical change
2. Pass `cargo build` and `cargo test`
3. Be reviewable in isolation

## Code Review

After each step, use codex for review:

```bash
codex e 'review the changes in <file> for correctness and style'
```

Or for broader review:

```bash
codex e 'review recent changes, check for: error handling, unwrap usage, naming conventions'
```

## Implementation Phases

### Phase 1: Project Structure + Core Types

- Workspace skeleton
- `Owner`, `Repo`, `RepoKey` types
- `CachePaths` struct
- CLI scaffolding

### Phase 2: Git Operations

- `GitRunner` with hardening
- Clone, fetch, worktree commands
- Atomic symlink swap

### Phase 3: Cache Manager + Locking

- File-based locking (`fs2`)
- `RepoCache::ensure_current`
- Concurrent access safety

### Phase 4: Minimal FUSE Mount

- Basic `fuser` integration
- Root inode handling
- Single hardcoded repo

### Phase 5: Full Loopback Passthrough

- Inode table
- Virtual nodes for `/`, `/<owner>`, `/<owner>/<repo>`
- All read operations

### Phase 6: TTL + Generation Boundaries

- Entry TTL configuration
- Generation-aware inodes

### Phase 7: Write Denial + Git Compatibility

- `EROFS` for writes
- Verify git commands work

### Phase 8: CLI Polish

- Daemon mode
- `ghfs doctor`
- `ghfs prefetch`

## Error Handling Conventions

- Use `thiserror` for error types
- Propagate with `?`, don't `unwrap()` in library code
- CLI can `unwrap()` or use `anyhow` for top-level errors
- FUSE errors map to errno:
  - Missing repo: `ENOENT`
  - Network/git failure: `EIO`
  - Write attempt: `EROFS`

## Testing Strategy

### Unit Tests

- In each crate's `src/` as `#[cfg(test)]` modules
- Test parsing, path generation, inode allocation

### Integration Tests

- In `tests/integration/`
- Require FUSE (skip on CI without it)
- Test actual mount/read/unmount cycles

### Running Tests

```bash
cargo test                           # All tests
cargo test -p ghfs-types             # Single crate
cargo test --test integration        # Integration only
```

## Platform Notes

### Linux

- Requires `libfuse3` or `fuse3` package
- User must be in `fuse` group or use `allow_other` mount option

### macOS

- Requires macFUSE installed (https://osxfuse.github.io/)
- May need Security & Privacy approval + reboot
- macFUSE ships both libfuse2 and libfuse3

## Key Design Decisions

1. **Loopback FS, not symlinks**: Symlinks break `find` and tooling expectations
2. **git worktree**: Allows `.git` writes to go outside the read-only mount
3. **Generation-based updates**: Never mutate exposed tree; swap atomically
4. **Shallow clones**: `--depth=1` for MVP; can deepen later
5. **Blocking materialization**: MVP blocks on first access; background refresh later
