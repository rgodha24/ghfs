{
  description = "GHFS - GitHub Filesystem (Linux FUSE, macOS NFS)";

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
    let
      perSystem = flake-utils.lib.eachDefaultSystem (
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

            nativeBuildInputs = pkgs.lib.optionals pkgs.stdenv.isLinux [pkgs.pkg-config];

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
              platforms = platforms.unix;
            };
          };

          devShells.default = pkgs.mkShell {
            buildInputs =
              [
                rust
                pkgs.git
                pkgs.cargo-watch
                pkgs.cargo-nextest
              ]
              ++ pkgs.lib.optionals pkgs.stdenv.isLinux [pkgs.pkg-config]
              ++ fuseDeps;

            shellHook = ''
              echo "ghfs dev shell"
              echo "  rust: $(rustc --version)"
              ${pkgs.lib.optionalString pkgs.stdenv.isLinux ''
                echo "  fuse3: $(pkg-config --modversion fuse3 2>/dev/null || echo 'not found')"
              ''}
              ${pkgs.lib.optionalString pkgs.stdenv.isDarwin ''
                echo "  mount_nfs: $(command -v mount_nfs >/dev/null && echo 'available' || echo 'not found')"
              ''}
            '';
          };
        }
      );
    in
      perSystem
      // {
        homeManagerModules.default = {pkgs, lib, ...}: {
          imports = [./nix/hm-module.nix];
          services.ghfs.package = lib.mkDefault self.packages.${pkgs.system}.default;
        };

        nixosModules.default = {pkgs, lib, ...}: {
          imports = [./nix/nixos-module.nix];
          services.ghfs.package = lib.mkDefault self.packages.${pkgs.system}.default;
        };
      };
}
