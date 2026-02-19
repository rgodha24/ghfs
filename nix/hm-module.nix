{ config, lib, pkgs, ... }:

let
  cfg = config.services.ghfs;
in {
  options.services.ghfs = {
    enable = lib.mkEnableOption "ghfs GitHub filesystem daemon";

    package = lib.mkOption {
      type = lib.types.package;
      description = "The ghfs package to use";
    };
  };

  config = lib.mkIf cfg.enable (lib.mkMerge [
    {
      home.packages = [cfg.package];
    }

    (lib.mkIf pkgs.stdenv.isLinux {
      systemd.user.services.ghfs = {
        Unit = {
          Description = "GHFS GitHub Filesystem";
          After = ["network-online.target"];
          Wants = ["network-online.target"];
        };
        Service = {
          ExecStart = "${cfg.package}/bin/ghfs daemon";
          Restart = "on-failure";
          RestartSec = 5;
          Environment = ["RUST_LOG=info"];
        };
        Install = {
          WantedBy = ["default.target"];
        };
      };
    })

    (lib.mkIf pkgs.stdenv.isDarwin {
      launchd.agents.ghfs = {
        enable = true;
        config = {
          Label = "com.ghfs.daemon";
          ProgramArguments = ["${cfg.package}/bin/ghfs" "daemon"];
          RunAtLoad = true;
          KeepAlive = true;
          StandardOutPath = "${config.home.homeDirectory}/Library/Logs/ghfs.log";
          StandardErrorPath = "${config.home.homeDirectory}/Library/Logs/ghfs.err.log";
          EnvironmentVariables = {
            RUST_LOG = "info";
          };
        };
      };
    })
  ]);
}
