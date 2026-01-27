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

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        
        rust = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" ];
        };
        
        # FUSE is only available via nix on Linux
        # macOS users need to install macFUSE manually
        fuseDeps = pkgs.lib.optionals pkgs.stdenv.isLinux [
          pkgs.fuse3
        ];
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = [
            # Rust toolchain
            rust
            
            # Build dependencies
            pkgs.pkg-config
            pkgs.openssl
            
            # Git (for testing)
            pkgs.git
            
            # Useful dev tools
            pkgs.cargo-watch
            pkgs.cargo-nextest
          ] ++ fuseDeps;

          shellHook = ''
            echo "ghfs dev shell"
            echo "  rust: $(rustc --version)"
            echo "  fuse3: $(pkg-config --modversion fuse3 2>/dev/null || echo 'not found (install macFUSE on macOS)')"
          '';
        };
      }
    );
}
