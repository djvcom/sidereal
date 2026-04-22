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

      buildSidereal =
        pkgs:
        let
          craneLib = (crane.mkLib pkgs).overrideToolchain (rustToolchain pkgs);

          commonArgs = {
            pname = "sidereal";
            src = pkgs.lib.cleanSourceWith {
              src = ./.;
              filter =
                path: type:
                (craneLib.filterCargoSources path type) || (builtins.match ".*README\\.md$" path != null);
            };
            strictDeps = true;

            buildInputs = [
              pkgs.openssl
            ];

            nativeBuildInputs = [
              pkgs.pkg-config
            ];
          };

          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        in
        craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;

            meta = {
              description = "Self-hosted observability backend with OTLP ingestion and DataFusion queries";
              homepage = "https://github.com/djvcom/sidereal";
              license = pkgs.lib.licenses.mit;
              mainProgram = "sidereal";
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
        })
      ];

      packages = forAllSystems (pkgs: {
        default = self.packages.${pkgs.stdenv.hostPlatform.system}.sidereal;
        sidereal = buildSidereal pkgs;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            (rustToolchain pkgs)
            pkgs.just
            pkgs.pkg-config
            pkgs.openssl
            pkgs.nixfmt
            pkgs.statix
            pkgs.deadnix
          ];

          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS = "-Clinker-features=-lld -Clink-arg=-Wl,--copy-dt-needed-entries";
          RUST_BACKTRACE = "1";
        };
      });
    };
}
