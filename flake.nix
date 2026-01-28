{
  description = "GHFS - GitHub Filesystem via FUSE";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        rust = pkgs.rust-bin.stable.latest.default.override {
          extensions = ["rust-src" "rust-analyzer"];
        };

        # FUSE is only available via nix on Linux
        fuseDeps = pkgs.lib.optionals pkgs.stdenv.isLinux [
          pkgs.fuse3
        ];
      in {
        packages.default = pkgs.rustPlatform.buildRustPackage {
          pname = "ghfs";
          version = "0.1.0";

          src = ./.;

          cargoLock.lockFile = ./Cargo.lock;

          nativeBuildInputs = [pkgs.pkg-config];

          buildInputs = fuseDeps;

          # Skip tests that require network
          checkFlags = [
            "--skip=clone_bare_shallow"
            "--skip=resolve_default_branch"
            "--skip=create_worktree"
            "--skip=fetch_shallow"
            "--skip=ensure_current"
            "--skip=concurrent"
          ];

          meta = with pkgs.lib; {
            description = "Mount GitHub repositories as a local filesystem";
            homepage = "https://github.com/rgodha24/ghfs";
            license = licenses.mit;
            maintainers = [];
            platforms = platforms.linux; # macOS requires macFUSE installed separately
          };
        };

        devShells.default = pkgs.mkShell {
          buildInputs =
            [
              rust
              pkgs.pkg-config
              pkgs.git
              pkgs.cargo-watch
              pkgs.cargo-nextest
            ]
            ++ fuseDeps;

          shellHook = ''
            echo "ghfs dev shell"
            echo "  rust: $(rustc --version)"
            echo "  fuse3: $(pkg-config --modversion fuse3 2>/dev/null || echo 'not found')"
          '';
        };
      }
    );
}
