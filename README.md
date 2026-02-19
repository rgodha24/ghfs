# ghfs

Mount GitHub repositories as a local filesystem.

`ghfs daemon` is the foreground daemon entry point. It does not daemonize itself; use your
platform service manager (`systemd` or `launchd`) for backgrounding and restarts.

## Non-Nix Service Management

For non-Nix installs, use `ghfs service` to install/manage a user service:

```bash
ghfs service install
ghfs service status
```

Available commands:

```bash
ghfs service install [--no-start]
ghfs service uninstall
ghfs service start
ghfs service stop [--force]
ghfs service restart
ghfs service status
ghfs service logs
```

`ghfs service install` is idempotent. Running it again rewrites service config with the current
binary path and restarts the daemon.

## Updating ghfs

After updating the `ghfs` binary, use one of:

```bash
ghfs service restart
```

or:

```bash
ghfs service install
```

`restart` is the lightweight option when binary path is unchanged. `install` always works (it
rewrites service files, then starts/restarts).

## Nix Flake Outputs

This flake exports:

- `packages.<system>.default` - `ghfs` package
- `devShells.<system>.default` - development shell
- `homeManagerModules.default` - home-manager module (Linux + macOS)
- `nixosModules.default` - NixOS module (system-level Linux daemon)

Both module wrappers default `services.ghfs.package` to
`self.packages.${pkgs.system}.default`, so the daemon binary always comes from this flake.

## Home-Manager Module (macOS + Linux)

Enable per-user service:

```nix
{
  services.ghfs.enable = true;
}
```

- Linux: creates `systemd.user.services.ghfs`
- macOS: creates `launchd.agents.ghfs`

### macOS with nix-darwin + home-manager

```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    nix-darwin.url = "github:LnL7/nix-darwin";
    home-manager.url = "github:nix-community/home-manager";
    ghfs.url = "github:rgodha24/ghfs";
  };

  outputs = { nix-darwin, home-manager, ghfs, ... }: {
    darwinConfigurations.myMac = nix-darwin.lib.darwinSystem {
      modules = [
        home-manager.darwinModules.home-manager
        {
          home-manager.sharedModules = [ ghfs.homeManagerModules.default ];
          home-manager.users.myuser = {
            services.ghfs.enable = true;
          };
        }
      ];
    };
  };
}
```

### NixOS with home-manager (per-user daemon)

```nix
{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    home-manager.url = "github:nix-community/home-manager";
    ghfs.url = "github:rgodha24/ghfs";
  };

  outputs = { nixpkgs, home-manager, ghfs, ... }: {
    nixosConfigurations.myBox = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        home-manager.nixosModules.home-manager
        {
          home-manager.sharedModules = [ ghfs.homeManagerModules.default ];
          home-manager.users.myuser = {
            services.ghfs.enable = true;
          };
        }
      ];
    };
  };
}
```

## NixOS Module (system-level daemon)

Enable system daemon:

```nix
{
  inputs.ghfs.url = "github:rgodha24/ghfs";

  outputs = { nixpkgs, ghfs, ... }: {
    nixosConfigurations.myBox = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        ghfs.nixosModules.default
        { services.ghfs.enable = true; }
      ];
    };
  };
}
```
