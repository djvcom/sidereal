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
              homepage = "https://github.com/djvcom/sidereal";
              license = pkgs.lib.licenses.mit;
              mainProgram = "sidereal-server";
            };
          }
        );

      # Build the sidereal-runtime package (statically linked for Firecracker VMs)
      # Uses cargo-zigbuild for cross-compilation (Zig bundles musl)
      buildSiderealRuntime =
        pkgs:
        let
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain pkgs;
            rustc = rustToolchain pkgs;
          };
        in
        rustPlatform.buildRustPackage {
          pname = "sidereal-runtime";
          version = "0.1.0";

          src = pkgs.lib.cleanSource ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = [
            pkgs.cargo-zigbuild
            pkgs.zig
          ];

          # Disable cargo-auditable (Zig linker doesn't support --undefined flag)
          auditable = false;

          # Use cargo-zigbuild for cross-compilation (Zig handles linking)
          buildPhase = ''
            runHook preBuild

            # cargo-zigbuild needs a writable cache directory
            export HOME=$(mktemp -d)

            cargo zigbuild \
              --release \
              --target x86_64-unknown-linux-musl \
              --offline \
              --locked \
              -p sidereal-runtime

            runHook postBuild
          '';

          # Skip tests - they can't run cross-compiled
          doCheck = false;

          # Override install phase to copy from musl target directory
          installPhase = ''
            runHook preInstall
            mkdir -p $out/bin
            cp target/x86_64-unknown-linux-musl/release/sidereal-runtime $out/bin/
            runHook postInstall
          '';

          meta = {
            description = "Sidereal runtime for Firecracker VMs";
            homepage = "https://github.com/djvcom/sidereal";
            license = pkgs.lib.licenses.mit;
            mainProgram = "sidereal-runtime";
          };
        };

      # Build the sidereal-builder-runtime package (statically linked for builder VMs)
      # This runs as /sbin/init in builder VMs and executes cargo builds
      buildSiderealBuilderRuntime =
        pkgs:
        let
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rustToolchain pkgs;
            rustc = rustToolchain pkgs;
          };
        in
        rustPlatform.buildRustPackage {
          pname = "sidereal-builder-runtime";
          version = "0.1.0";

          src = pkgs.lib.cleanSource ./.;

          cargoLock = {
            lockFile = ./Cargo.lock;
          };

          nativeBuildInputs = [
            pkgs.cargo-zigbuild
            pkgs.zig
          ];

          # Disable cargo-auditable (Zig linker doesn't support --undefined flag)
          auditable = false;

          # Use cargo-zigbuild for cross-compilation (Zig handles linking)
          buildPhase = ''
            runHook preBuild

            # cargo-zigbuild needs a writable cache directory
            export HOME=$(mktemp -d)

            cargo zigbuild \
              --release \
              --target x86_64-unknown-linux-musl \
              --offline \
              --locked \
              -p sidereal-builder-runtime

            runHook postBuild
          '';

          # Skip tests - they can't run cross-compiled
          doCheck = false;

          # Override install phase to copy from musl target directory
          installPhase = ''
            runHook preInstall
            mkdir -p $out/bin
            cp target/x86_64-unknown-linux-musl/release/sidereal-builder-runtime $out/bin/
            runHook postInstall
          '';

          meta = {
            description = "Sidereal builder runtime for compilation VMs";
            homepage = "https://github.com/djvcom/sidereal";
            license = pkgs.lib.licenses.mit;
            mainProgram = "sidereal-builder-runtime";
          };
        };

      # Build the builder rootfs image
      buildBuilderRootfs =
        pkgs:
        import ./nix/builder-rootfs.nix {
          inherit pkgs;
          sidereal-builder-runtime = buildSiderealBuilderRuntime pkgs;
        };

      # Build the runtime rootfs image
      buildRuntimeRootfs =
        pkgs:
        import ./nix/runtime-rootfs.nix {
          inherit pkgs;
          sidereal-runtime = buildSiderealRuntime pkgs;
        };

      # Build custom kernel for Firecracker VMs
      buildFirecrackerKernel = pkgs: import ./nix/firecracker-kernel.nix { inherit pkgs; };
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
          sidereal-builder-runtime = buildSiderealBuilderRuntime final;
          sidereal-builder-rootfs = buildBuilderRootfs final;
          sidereal-runtime-rootfs = buildRuntimeRootfs final;
          sidereal-firecracker-kernel = buildFirecrackerKernel final;
        })
      ];

      # Expose fenix for consuming flakes to use for rustToolchain
      fenixPackages = fenix.packages;

      # Packages
      packages = forAllSystems (pkgs: {
        default = self.packages.${pkgs.system}.sidereal-server;
        sidereal-server = buildSidereal pkgs;
        sidereal-runtime = buildSiderealRuntime pkgs;
        sidereal-builder-runtime = buildSiderealBuilderRuntime pkgs;
        sidereal-builder-rootfs = buildBuilderRootfs pkgs;
        sidereal-runtime-rootfs = buildRuntimeRootfs pkgs;
        sidereal-firecracker-kernel = buildFirecrackerKernel pkgs;
      });

      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          packages = [
            (rustToolchain pkgs)
            pkgs.just

            # Firecracker for local VM deployment
            pkgs.firecracker

            # Cross-compilation via cargo-zigbuild (Zig bundles musl)
            pkgs.cargo-zigbuild
            pkgs.zig

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
          # Disable lld for native builds only.
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS = "-Clinker-features=-lld -Clink-arg=-Wl,--copy-dt-needed-entries";

          RUST_BACKTRACE = "1";
        };
      });
    };
}
