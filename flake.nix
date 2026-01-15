{
  description = "Sidereal - A platform for building and running applications in Rust";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      rust-overlay,
    }:
    let
      forAllSystems =
        fn:
        nixpkgs.lib.genAttrs nixpkgs.lib.systems.flakeExposed (
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
    in
    {
      formatter = forAllSystems (pkgs: pkgs.nixfmt-rfc-style);

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
