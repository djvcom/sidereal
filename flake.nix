{
  description = "Sidereal - A platform for building and running applications in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
      fenix,
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

      rustToolchain =
        pkgs:
        pkgs.rust-bin.stable.latest.default.override {
          extensions = [
            "rust-src"
            "rust-analyzer"
          ];
          targets = [
            "wasm32-wasip1"
            "x86_64-unknown-linux-musl"
          ];
        };

      # Source filter that includes Cargo files and READMEs (for include_str!)
      srcFilter =
        craneLib: path: type:
        (craneLib.filterCargoSources path type) || (builtins.match ".*README\\.md$" path != null);

      # Build the sidereal-server package using crane
      buildSidereal =
        pkgs:
        let
          craneLib = (crane.mkLib pkgs).overrideToolchain (rustToolchain pkgs);

          # Common arguments for crane builds
          commonArgs = {
            pname = "sidereal-server";
            src = pkgs.lib.cleanSourceWith {
              src = ./.;
              filter = srcFilter craneLib;
            };
            strictDeps = true;

            buildInputs = [
              pkgs.openssl
            ];

            nativeBuildInputs = [
              pkgs.pkg-config
            ];
          };

          # Build workspace dependencies first (for caching)
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        in
        craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;

            # Only build the sidereal-server binary
            cargoExtraArgs = "-p sidereal-server";

            meta = {
              description = "Sidereal unified server for single-node deployments";
              homepage = "https://github.com/your-org/sidereal";
              license = pkgs.lib.licenses.mit;
              mainProgram = "sidereal-server";
            };
          }
        );

      # Build the sidereal-runtime package (statically linked for Firecracker VMs)
      buildSiderealRuntime =
        pkgs:
        let
          craneLib = (crane.mkLib pkgs).overrideToolchain (rustToolchain pkgs);

          # Common arguments for crane builds
          commonArgs = {
            pname = "sidereal-runtime";
            src = pkgs.lib.cleanSourceWith {
              src = ./.;
              filter = srcFilter craneLib;
            };
            strictDeps = true;

            # Build for musl target (static linking for Firecracker)
            CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";

            nativeBuildInputs = [
              pkgs.musl.dev
            ];

            # Use musl-gcc for linking (disable rust-lld which doesn't work with musl)
            CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = "${pkgs.musl.dev}/bin/musl-gcc";
            CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS = "-Clinker-features=-lld";
            CC_x86_64_unknown_linux_musl = "${pkgs.musl.dev}/bin/musl-gcc";
          };

          # Build workspace dependencies first (for caching)
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        in
        craneLib.buildPackage (
          commonArgs
          // {
            inherit cargoArtifacts;

            # Only build the sidereal-runtime binary
            cargoExtraArgs = "-p sidereal-runtime";

            # Install from the musl target directory
            installPhaseCommand = ''
              mkdir -p $out/bin
              cp target/x86_64-unknown-linux-musl/release/sidereal-runtime $out/bin/
            '';

            meta = {
              description = "Sidereal runtime for Firecracker VMs";
              homepage = "https://github.com/your-org/sidereal";
              license = pkgs.lib.licenses.mit;
              mainProgram = "sidereal-runtime";
            };
          }
        );
    in
    {
      formatter = forAllSystems (pkgs: pkgs.nixfmt-rfc-style);

      # NixOS module
      nixosModules = {
        default = self.nixosModules.sidereal;
        sidereal = import ./nix/module.nix;
      };

      # Overlay for adding sidereal packages to nixpkgs (includes rust-overlay)
      overlays.default = nixpkgs.lib.composeManyExtensions [
        rust-overlay.overlays.default
        (final: _prev: {
          sidereal-server = buildSidereal final;
          sidereal-runtime = buildSiderealRuntime final;
        })
      ];

      # Expose fenix for consuming flakes to use for rustToolchain
      fenixPackages = fenix.packages;

      # Packages
      packages = forAllSystems (pkgs: {
        default = self.packages.${pkgs.system}.sidereal-server;
        sidereal-server = buildSidereal pkgs;
        sidereal-runtime = buildSiderealRuntime pkgs;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            (rustToolchain pkgs)
            pkgs.just
            pkgs.wasmtime

            # Firecracker for local VM deployment
            pkgs.firecracker

            # musl for static cross-compilation
            pkgs.musl

            # Build dependencies for native crates
            pkgs.pkg-config
            pkgs.openssl

            # Sandbox runtime (bubblewrap)
            pkgs.bubblewrap

            # Nix tooling
            pkgs.nixfmt-rfc-style
            pkgs.statix
            pkgs.deadnix
          ];

          # Rust 1.90+ uses rust-lld by default, which lacks NixOS rpath handling.
          # Disable lld entirely and use the traditional GNU linker.
          # Also add --copy-dt-needed-entries to handle DSO ordering issues.
          RUSTFLAGS = "-Clinker-features=-lld -Clink-arg=-Wl,--copy-dt-needed-entries";

          RUST_BACKTRACE = "1";

          # Configure cargo for musl cross-compilation
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = "${pkgs.musl}/bin/musl-gcc";
        };
      });
    };
}
