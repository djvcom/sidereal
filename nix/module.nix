# NixOS module for Sidereal platform
#
# This module provides a declarative way to run the Sidereal platform
# as a systemd service on NixOS.
#
# Example usage in configuration.nix:
#
#   services.sidereal = {
#     enable = true;
#     database.url = "postgres://sidereal@localhost/sidereal";
#     gateway.listenAddress = "0.0.0.0:8422";
#   };
#
{
  config,
  lib,
  pkgs,
  ...
}:

let
  cfg = config.services.sidereal;
  tomlFormat = pkgs.formats.toml { };

  # Generate the TOML configuration file
  configFile = tomlFormat.generate "sidereal.toml" {
    server = {
      inherit (cfg) mode;
      socket_dir = cfg.socketDir;
      data_dir = cfg.dataDir;
    };

    gateway = {
      enabled = cfg.gateway.enable;
      listen = cfg.gateway.listenAddress;
      api = {
        build_socket = "${cfg.socketDir}/build.sock";
        control_socket = "${cfg.socketDir}/control.sock";
      };
    };

    scheduler = {
      enabled = cfg.scheduler.enable;
    };

    control = {
      enabled = cfg.control.enable;
      inherit (cfg.control) provisioner;
    };

    build = {
      enabled = cfg.build.enable;
      inherit (cfg.build) workers;
      paths = {
        runtime = "${cfg.runtimePackage}/bin/sidereal-runtime";
      };
    }
    // lib.optionalAttrs (cfg.build.forgeAuth.type != "none") {
      forge_auth = {
        type = cfg.build.forgeAuth.type;
        key_path = cfg.build.forgeAuth.sshKeyPath;
      };
    };

    database = {
      inherit (cfg.database) url;
      max_connections = cfg.database.maxConnections;
    };

    valkey = {
      inherit (cfg.valkey) url;
    };

    storage = {
      inherit (cfg.storage) backend;
      inherit (cfg.storage) endpoint;
      inherit (cfg.storage) bucket;
    };
  };
in
{
  options.services.sidereal = {
    enable = lib.mkEnableOption "Sidereal platform";

    package = lib.mkOption {
      type = lib.types.package;
      default = pkgs.sidereal-server or (throw "sidereal-server package not found");
      defaultText = lib.literalExpression "pkgs.sidereal-server";
      description = "The sidereal-server package to use.";
    };

    runtimePackage = lib.mkOption {
      type = lib.types.package;
      default = pkgs.sidereal-runtime or (throw "sidereal-runtime package not found");
      defaultText = lib.literalExpression "pkgs.sidereal-runtime";
      description = "The sidereal-runtime package (runs inside Firecracker VMs).";
    };

    mode = lib.mkOption {
      type = lib.types.enum [
        "single-node"
        "distributed"
      ];
      default = "single-node";
      description = "Deployment mode. Single-node uses Unix sockets for inter-service communication.";
    };

    socketDir = lib.mkOption {
      type = lib.types.path;
      default = "/run/sidereal";
      description = "Directory for Unix domain sockets (single-node mode).";
    };

    dataDir = lib.mkOption {
      type = lib.types.path;
      default = "/var/lib/sidereal";
      description = "Directory for persistent data (build caches, artifacts).";
    };

    user = lib.mkOption {
      type = lib.types.str;
      default = "sidereal";
      description = "User account under which Sidereal runs.";
    };

    group = lib.mkOption {
      type = lib.types.str;
      default = "sidereal";
      description = "Group under which Sidereal runs.";
    };

    # Gateway configuration
    gateway = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Enable the gateway service.";
      };

      listenAddress = lib.mkOption {
        type = lib.types.str;
        default = "127.0.0.1:8422";
        description = "Address and port for the gateway to listen on.";
      };
    };

    # Scheduler configuration
    scheduler = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Enable the scheduler service.";
      };
    };

    # Control plane configuration
    control = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Enable the control plane service.";
      };

      provisioner = lib.mkOption {
        type = lib.types.enum [
          "mock"
          "firecracker"
        ];
        default = "mock";
        description = "Worker provisioner type.";
      };
    };

    # Build service configuration
    build = {
      enable = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Enable the build service.";
      };

      workers = lib.mkOption {
        type = lib.types.int;
        default = 2;
        description = "Number of concurrent build workers.";
      };

      forgeAuth = {
        type = lib.mkOption {
          type = lib.types.enum [
            "ssh"
            "none"
          ];
          default = "ssh";
          description = "Git forge authentication type.";
        };

        sshKeyPath = lib.mkOption {
          type = lib.types.path;
          default = "${cfg.dataDir}/ssh/id_ed25519";
          description = "Path to the SSH private key for git operations.";
        };
      };
    };

    # Database configuration
    database = {
      url = lib.mkOption {
        type = lib.types.str;
        default = "postgres://sidereal@localhost/sidereal";
        description = ''
          PostgreSQL connection URL.

          For local PostgreSQL with peer authentication:
            postgres://sidereal@localhost/sidereal

          For password authentication:
            postgres://user:password@host:5432/database
        '';
      };

      maxConnections = lib.mkOption {
        type = lib.types.int;
        default = 10;
        description = "Maximum database connection pool size.";
      };

      createLocally = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Whether to create a local PostgreSQL database.";
      };
    };

    # Valkey (Redis) configuration
    valkey = {
      url = lib.mkOption {
        type = lib.types.str;
        default = "redis://localhost:6379";
        description = "Valkey/Redis connection URL.";
      };

      createLocally = lib.mkOption {
        type = lib.types.bool;
        default = true;
        description = "Whether to enable a local Valkey instance.";
      };
    };

    # Object storage configuration
    storage = {
      backend = lib.mkOption {
        type = lib.types.enum [
          "filesystem"
          "s3"
        ];
        default = "filesystem";
        description = "Object storage backend type.";
      };

      endpoint = lib.mkOption {
        type = lib.types.nullOr lib.types.str;
        default = null;
        description = "S3-compatible endpoint URL (for s3 backend).";
      };

      bucket = lib.mkOption {
        type = lib.types.str;
        default = "sidereal";
        description = "Storage bucket name.";
      };

      credentialsFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = ''
          Path to a file containing AWS credentials.
          The file should contain:
            AWS_ACCESS_KEY_ID=...
            AWS_SECRET_ACCESS_KEY=...
        '';
      };
    };

    # Logging configuration
    logLevel = lib.mkOption {
      type = lib.types.enum [
        "error"
        "warn"
        "info"
        "debug"
        "trace"
      ];
      default = "info";
      description = "Log level for the Sidereal services.";
    };

    rustToolchain = lib.mkOption {
      type = lib.types.either lib.types.package (lib.types.listOf lib.types.package);
      default =
        if pkgs ? rust-bin then
          pkgs.rust-bin.stable.latest.default.override {
            targets = [ "x86_64-unknown-linux-musl" ];
          }
        else
          [
            pkgs.cargo
            pkgs.rustc
          ];
      defaultText = lib.literalExpression "pkgs.rust-bin.stable.latest.default.override { targets = [ \"x86_64-unknown-linux-musl\" ]; }";
      description = ''
        Rust toolchain package(s) to use for building.

        This should provide both rustc and cargo. Can be either:
        - A single package (e.g., from rust-overlay or fenix)
        - A list of packages (e.g., [ pkgs.cargo pkgs.rustc ])

        The default includes the x86_64-unknown-linux-musl target for static builds.
      '';
    };
  };

  config = lib.mkIf cfg.enable {
    # Create user and group
    users.users.${cfg.user} = {
      isSystemUser = true;
      inherit (cfg) group;
      home = cfg.dataDir;
      description = "Sidereal service user";
    };

    users.groups.${cfg.group} = { };

    systemd.tmpfiles.rules = [
      "d ${cfg.socketDir} 0750 ${cfg.user} ${cfg.group} -"
      "d ${cfg.dataDir} 0750 ${cfg.user} ${cfg.group} -"
      "d ${cfg.dataDir}/checkouts 0750 ${cfg.user} ${cfg.group} -"
      "d ${cfg.dataDir}/caches 0750 ${cfg.user} ${cfg.group} -"
      "d ${cfg.dataDir}/artifacts 0750 ${cfg.user} ${cfg.group} -"
      "d ${cfg.dataDir}/ssh 0700 ${cfg.user} ${cfg.group} -"
    ];

    # PostgreSQL setup (if createLocally is true)
    services.postgresql = lib.mkIf cfg.database.createLocally {
      enable = true;
      ensureDatabases = [ "sidereal" ];
      ensureUsers = [
        {
          name = "sidereal";
          ensureDBOwnership = true;
        }
      ];
    };

    # Valkey setup (if createLocally is true)
    # Note: Using redis service as valkey may not be in nixpkgs yet
    services.redis.servers.sidereal = lib.mkIf cfg.valkey.createLocally {
      enable = true;
      port = 6379;
      bind = "127.0.0.1";
    };

    # Main systemd service
    systemd.services.sidereal = {
      description = "Sidereal Platform";
      documentation = [ "https://github.com/your-org/sidereal" ];

      wantedBy = [ "multi-user.target" ];
      after = [
        "network.target"
      ]
      ++ lib.optional cfg.database.createLocally "postgresql.service"
      ++ lib.optional cfg.valkey.createLocally "redis-sidereal.service";

      requires =
        lib.optional cfg.database.createLocally "postgresql.service"
        ++ lib.optional cfg.valkey.createLocally "redis-sidereal.service";

      path = [
        pkgs.openssh
        pkgs.bubblewrap
        pkgs.gcc
        pkgs.musl
      ]
      ++ (if builtins.isList cfg.rustToolchain then cfg.rustToolchain else [ cfg.rustToolchain ]);

      environment = {
        RUST_LOG = cfg.logLevel;
        SIDEREAL_CONFIG = configFile;
        # Configure musl cross-compilation
        CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = "${pkgs.musl.dev}/bin/musl-gcc";
        CC_x86_64_unknown_linux_musl = "${pkgs.musl.dev}/bin/musl-gcc";
      };

      serviceConfig = {
        Type = "simple";
        User = cfg.user;
        Group = cfg.group;

        ExecStart = "${cfg.package}/bin/sidereal-server --config ${configFile}";

        # Restart policy
        Restart = "on-failure";
        RestartSec = "5s";

        # Security hardening
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        PrivateDevices = false;
        ProtectKernelTunables = false;
        ProtectKernelModules = true;
        ProtectControlGroups = true;
        RestrictSUIDSGID = true;
        RestrictNamespaces = "user mnt pid net ipc uts";
        LockPersonality = true;
        RestrictRealtime = true;

        # Allow write access to required directories
        ReadWritePaths = [
          cfg.socketDir
          cfg.dataDir
        ];

        # Capability restrictions
        CapabilityBoundingSet = "";
        AmbientCapabilities = "";

        # Load credentials from file if specified
        EnvironmentFile = lib.mkIf (cfg.storage.credentialsFile != null) cfg.storage.credentialsFile;
      };
    };

    # Firewall configuration (optional, only if gateway is public)
    networking.firewall.allowedTCPPorts = lib.mkIf (
      cfg.gateway.enable && lib.hasPrefix "0.0.0.0" cfg.gateway.listenAddress
    ) [ (lib.toInt (lib.last (lib.splitString ":" cfg.gateway.listenAddress))) ];
  };
}
