{ config, lib, pkgs, ... }:

let
  cfg = config.services.ghfs;
in {
  options.services.ghfs = {
    enable = lib.mkEnableOption "ghfs system daemon";

    package = lib.mkOption {
      type = lib.types.package;
      description = "The ghfs package to use";
    };
  };

  config = lib.mkIf cfg.enable {
    systemd.services.ghfs = {
      description = "GHFS GitHub Filesystem";
      wantedBy = ["multi-user.target"];
      after = ["network-online.target"];
      wants = ["network-online.target"];

      serviceConfig = {
        ExecStart = "${cfg.package}/bin/ghfs daemon";
        Restart = "on-failure";
        RestartSec = 5;
        Environment = [
          "RUST_LOG=info"
          "PATH=/run/wrappers/bin:${lib.makeBinPath [ cfg.package pkgs.git pkgs.fuse3 ]}:/usr/bin:/bin:/usr/sbin:/sbin"
        ];
      };
    };

    environment.systemPackages = [cfg.package];
  };
}
