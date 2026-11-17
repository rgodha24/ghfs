#!/usr/bin/env bash
set -euo pipefail
# Invoke only through tesser exec on this worktree's own workbench.
if ! command -v nix >/dev/null 2>&1; then
  sudo apt-get update -qq
  sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nix-bin pkg-config fuse3 libfuse3-dev git curl ca-certificates
fi
# Ubuntu's nix-bin defaults to daemon mode. A local user-owned store works
# without managing another daemon on the disposable workbench.
if [[ ! -w /nix ]]; then
  sudo mkdir -p /nix
  sudo chown -R "$(id -un):$(id -gn)" /nix
fi
export NIX_REMOTE=local
export NIX_CONFIG=$'experimental-features = nix-command flakes\nbuild-users-group =\nsandbox = false'
exec nix develop -c "$@"
