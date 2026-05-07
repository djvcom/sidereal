{
  description = "Sidereal - Self-hosted observability backend";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      crane,
    }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];

      forAllSystems =
        fn:
        nixpkgs.lib.genAttrs supportedSystems (
          system:
          fn (
            import nixpkgs {
              inherit system;
              overlays = [ rust-overlay.overlays.default ];
            }
          )
        );

      rustToolchain = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

      commonArgs =
        pkgs:
        let
          craneLib = (crane.mkLib pkgs).overrideToolchain (rustToolchain pkgs);
        in
        {
          pname = "sidereal-workspace";
          src = pkgs.lib.cleanSourceWith {
            src = ./.;
            filter =
              path: type:
              (craneLib.filterCargoSources path type) || (builtins.match ".*README\\.md$" path != null);
          };
          strictDeps = true;

          buildInputs = [ pkgs.openssl ];
          nativeBuildInputs = [ pkgs.pkg-config ];
        };

      buildSidereal =
        pkgs:
        let
          craneLib = (crane.mkLib pkgs).overrideToolchain (rustToolchain pkgs);
          args = commonArgs pkgs;
          cargoArtifacts = craneLib.buildDepsOnly args;
        in
        craneLib.buildPackage (
          args
          // {
            pname = "sidereal";
            inherit cargoArtifacts;
            cargoExtraArgs = "--package sidereal --features s3";

            meta = {
              description = "Self-hosted observability backend with OTLP ingestion and DataFusion queries";
              homepage = "https://github.com/djvcom/sidereal";
              license = pkgs.lib.licenses.mit;
              mainProgram = "sidereal";
            };
          }
        );

      buildSiderealAi =
        pkgs:
        let
          craneLib = (crane.mkLib pkgs).overrideToolchain (rustToolchain pkgs);
          args = commonArgs pkgs;
          cargoArtifacts = craneLib.buildDepsOnly args;
        in
        craneLib.buildPackage (
          args
          // {
            pname = "sidereal-ai";
            inherit cargoArtifacts;
            cargoExtraArgs = "--package sidereal-ai";

            meta = {
              description = "AI companion service for Sidereal";
              homepage = "https://github.com/djvcom/sidereal";
              license = pkgs.lib.licenses.mit;
              mainProgram = "sidereal-ai";
            };
          }
        );
    in
    {
      formatter = forAllSystems (pkgs: pkgs.nixfmt);

      overlays.default = nixpkgs.lib.composeManyExtensions [
        rust-overlay.overlays.default
        (final: _prev: {
          sidereal = buildSidereal final;
          sidereal-ai = buildSiderealAi final;
        })
      ];

      packages = forAllSystems (pkgs: {
        default = self.packages.${pkgs.stdenv.hostPlatform.system}.sidereal;
        sidereal = buildSidereal pkgs;
        sidereal-ai = buildSiderealAi pkgs;
      });

      nixosModules.sidereal =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.sidereal;

          configFile = pkgs.writeText "telemetry.toml" ''
            [server]
            grpc_addr = "${cfg.grpcListenAddress}"
            http_addr = "${cfg.httpListenAddress}"
            query_addr = "${cfg.queryListenAddress}"

            ${lib.optionalString (cfg.storage.type == "local") ''
              [storage]
              type = "local"
              path = "${cfg.storage.path}"
            ''}
            ${lib.optionalString (cfg.storage.type == "s3") ''
              [storage]
              type = "s3"
              bucket = "${cfg.storage.bucket}"
              region = "${cfg.storage.region}"
              ${lib.optionalString (cfg.storage.endpoint != "") ''endpoint = "${cfg.storage.endpoint}"''}
              force_path_style = ${lib.boolToString cfg.storage.forcePathStyle}
              allow_http = ${lib.boolToString cfg.storage.allowHttp}
            ''}
            ${lib.optionalString cfg.auth.oidc.enable ''
              [auth.oidc]
              issuer = "${cfg.auth.oidc.issuer}"
              audience = "${cfg.auth.oidc.audience}"
              jwks_refresh_secs = ${toString cfg.auth.oidc.jwksRefreshSecs}
            ''}
          '';

          environmentFiles =
            lib.optional (cfg.storage.credentialsFile != null) cfg.storage.credentialsFile
            ++ lib.optional (cfg.authKeyFile != null) cfg.authKeyFile;
        in
        {
          options.services.sidereal = {
            enable = lib.mkEnableOption "Sidereal observability backend";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.stdenv.hostPlatform.system}.sidereal;
              description = "The sidereal package to use.";
            };

            grpcListenAddress = lib.mkOption {
              type = lib.types.str;
              default = "127.0.0.1:4317";
              description = "Address on which the OTLP gRPC receiver listens.";
            };

            httpListenAddress = lib.mkOption {
              type = lib.types.str;
              default = "127.0.0.1:4318";
              description = "Address on which the OTLP HTTP receiver listens.";
            };

            queryListenAddress = lib.mkOption {
              type = lib.types.str;
              default = "127.0.0.1:3100";
              description = "Address on which the query API listens.";
            };

            storage = {
              type = lib.mkOption {
                type = lib.types.enum [
                  "local"
                  "s3"
                ];
                default = "local";
                description = "Storage backend to use.";
              };

              path = lib.mkOption {
                type = lib.types.str;
                default = "/var/lib/sidereal/data";
                description = "Path for local filesystem storage.";
              };

              bucket = lib.mkOption {
                type = lib.types.str;
                default = "";
                description = "S3 bucket name.";
              };

              region = lib.mkOption {
                type = lib.types.str;
                default = "";
                description = "S3 region (or custom region string for S3-compatible services).";
              };

              endpoint = lib.mkOption {
                type = lib.types.str;
                default = "";
                description = "Custom endpoint URL for S3-compatible services such as Garage.";
              };

              forcePathStyle = lib.mkOption {
                type = lib.types.bool;
                default = true;
                description = "Force path-style URLs; required for Garage and most S3-compatible services.";
              };

              allowHttp = lib.mkOption {
                type = lib.types.bool;
                default = false;
                description = "Allow plain HTTP connections to the storage endpoint.";
              };

              credentialsFile = lib.mkOption {
                type = lib.types.nullOr lib.types.path;
                default = null;
                description = ''
                  Path to an environment file containing AWS_ACCESS_KEY_ID and
                  AWS_SECRET_ACCESS_KEY for S3 or S3-compatible storage.
                '';
              };
            };

            authKeyFile = lib.mkOption {
              type = lib.types.nullOr lib.types.path;
              default = null;
              description = ''
                Path to an environment file containing TELEMETRY_AUTH_API_KEY.
                When set, all data endpoints require this key as a Bearer token
                or X-API-Key header.
              '';
            };

            auth.oidc = {
              enable = lib.mkEnableOption "OIDC JWT authentication for the query API";

              issuer = lib.mkOption {
                type = lib.types.str;
                default = "";
                description = "OIDC issuer URL. Used to discover the JWKS endpoint via standard OIDC discovery.";
                example = "https://auth.example.com/oauth2/openid/sidereal";
              };

              audience = lib.mkOption {
                type = lib.types.str;
                default = "";
                description = "Expected audience claim in validated JWTs; typically the OAuth2 client ID.";
                example = "sidereal";
              };

              jwksRefreshSecs = lib.mkOption {
                type = lib.types.int;
                default = 3600;
                description = "Interval in seconds at which the JWKS key cache is refreshed.";
              };
            };
          };

          config = lib.mkIf cfg.enable {
            environment.etc."sidereal/telemetry.toml".source = configFile;

            users.users.sidereal = {
              isSystemUser = true;
              group = "sidereal";
              description = "Sidereal service user";
            };

            users.groups.sidereal = { };

            systemd.services.sidereal = {
              description = "Sidereal observability backend";
              wantedBy = [ "multi-user.target" ];
              after = [ "network.target" ];

              serviceConfig = {
                ExecStart = "${cfg.package}/bin/sidereal";
                User = "sidereal";
                Group = "sidereal";
                WorkingDirectory = "/etc/sidereal";
                StateDirectory = "sidereal";
                ReadWritePaths = [ "/var/lib/sidereal" ];
                NoNewPrivileges = true;
                ProtectSystem = "strict";
                ProtectHome = true;
                PrivateTmp = true;
                PrivateDevices = true;
                ProtectKernelTunables = true;
                ProtectKernelModules = true;
                ProtectControlGroups = true;
                RestrictNamespaces = true;
                RestrictRealtime = true;
                RestrictSUIDSGID = true;
                LockPersonality = true;
              }
              // lib.optionalAttrs (environmentFiles != [ ]) {
                EnvironmentFile = environmentFiles;
              };
            };
          };
        };

      homeManagerModules.sidereal-ai =
        {
          config,
          lib,
          pkgs,
          ...
        }:
        let
          cfg = config.services.sidereal-ai;
        in
        {
          options.services.sidereal-ai = {
            enable = lib.mkEnableOption "Sidereal AI companion service";

            package = lib.mkOption {
              type = lib.types.package;
              default = self.packages.${pkgs.stdenv.hostPlatform.system}.sidereal-ai;
              description = "The sidereal-ai package to use.";
            };

            sidereal.url = lib.mkOption {
              type = lib.types.str;
              description = "URL of the remote Sidereal query API.";
              example = "http://terminus:3100";
            };

            listenAddress = lib.mkOption {
              type = lib.types.str;
              default = "127.0.0.1:3200";
              description = "Address on which the sidereal-ai service listens.";
            };
          };

          config = lib.mkIf cfg.enable {
            home.packages = [ cfg.package ];

            launchd.agents.sidereal-ai = lib.mkIf pkgs.stdenv.isDarwin {
              enable = true;
              config = {
                Label = "dev.sidereal.ai";
                ProgramArguments = [ "${cfg.package}/bin/sidereal-ai" ];
                EnvironmentVariables = {
                  SIDEREAL_URL = cfg.sidereal.url;
                  SIDEREAL_LISTEN_ADDRESS = cfg.listenAddress;
                };
                RunAtLoad = true;
                KeepAlive = true;
                StandardOutPath = "/tmp/sidereal-ai.log";
                StandardErrorPath = "/tmp/sidereal-ai.log";
              };
            };

            systemd.user.services.sidereal-ai = lib.mkIf pkgs.stdenv.isLinux {
              Unit = {
                Description = "Sidereal AI companion service";
                After = [ "network.target" ];
              };

              Service = {
                ExecStart = "${cfg.package}/bin/sidereal-ai";
                Environment = [
                  "SIDEREAL_URL=${cfg.sidereal.url}"
                  "SIDEREAL_LISTEN_ADDRESS=${cfg.listenAddress}"
                ];
                Restart = "on-failure";
              };

              Install.WantedBy = [ "default.target" ];
            };
          };
        };

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell (
          {
            packages = [
              (rustToolchain pkgs)
              pkgs.just
              pkgs.pkg-config
              pkgs.openssl
              pkgs.nixfmt
              pkgs.statix
              pkgs.deadnix
            ];
            RUST_BACKTRACE = "1";
          }
          // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
            CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS = "-Clinker-features=-lld -Clink-arg=-Wl,--copy-dt-needed-entries";
          }
        );
      });
    };
}
