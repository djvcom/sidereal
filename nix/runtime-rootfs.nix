# Runtime VM rootfs image (~10-20MB)
#
# This is a minimal rootfs containing only:
# - sidereal-runtime as /sbin/init
# - Placeholder for user binary at /app/binary
#
# The user binary is injected at deploy time using the
# RootfsBuilder.build() method which uses mkfs.ext4 -d.
#
# Usage:
#   let
#     runtimeRootfs = import ./runtime-rootfs.nix {
#       inherit pkgs;
#       sidereal-runtime = pkgs.sidereal-runtime;
#     };
#   in runtimeRootfs
#
{ pkgs, sidereal-runtime }:

let
  makeExt4Fs = import ./make-ext4-fs.nix { inherit pkgs; };
in
makeExt4Fs {
  name = "sidereal-runtime-rootfs";
  size = "64M";
  label = "sidereal";

  contents = [
    # Init process (runtime)
    {
      source = "${sidereal-runtime}/bin/sidereal-runtime";
      target = "/sbin/init";
    }

    # Placeholder for user binary (will be replaced at deploy time)
    # The actual binary injection happens in the build service using
    # mkfs.ext4 -d, not by modifying this base image.
  ];
}
