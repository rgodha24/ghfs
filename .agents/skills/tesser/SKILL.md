---
name: tesser
description: Run ghfs Linux builds, checks and tests on a Tesser workbench, including scratch FUSE end-to-end tests.
---

# ghfs on Tesser

Read `tesser --help` and the command's `--help` first. If tesser is not on PATH,
this machine currently installs it at `$HOME/.tesser/bin/tesser`.

Run from the local ghfs worktree; source stays local and exec syncs it:

```sh
tesser exec -- bash .agents/skills/tesser/run.sh cargo check --all-targets
tesser exec -- bash .agents/skills/tesser/run.sh cargo test --all-targets
tesser exec -- bash .agents/skills/tesser/run.sh cargo build --release
```

The run script prepares Nix/FUSE on a fresh Ubuntu workbench and always wraps
Cargo in the repository devshell. Never edit source on the box. To format,
run `cargo fmt` in that devshell, retrieve the resulting source files to the
local worktree before another sync, and review the diff. Likewise retrieve a
Cargo-generated lockfile before syncing the next command. Read files with
`tesser ssh <box> -- cat <path>`; `tesser exec` syncs first and can overwrite
unretrieved generated source.

Long commands run as the tool's background task, never nohup on the box.
Do not run the installed ghfs service: use unique scratch mount/cache/socket
paths and the newly built daemon. The workbench has `/dev/fuse`; put the
system `/usr/bin` ahead of Nix for its setuid fusermount3 if needed. Keep a
MemoryMax=1G cgroup around resource acceptance tests.

macOS NFS requires the actual Mac: `ssh mac "/bin/bash -lc '<command>'"`
restores its Nix PATH. Use an isolated scratch checkout and mount/cache/socket;
never touch the existing launchd service or `/tmp/ghfs` mount.

Record the workbench ID, commands, exit codes and evidence. Remove only boxes
this task created after retrieving logs/artifacts and finishing tests.
