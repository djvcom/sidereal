# Build an ext4 filesystem image from a set of contents.
#
# This derivation uses mkfs.ext4 -d to populate the filesystem directly
# from a staging directory, avoiding the need for root privileges or
# loop mounting.
#
# Usage:
#   make-ext4-fs {
#     name = "my-rootfs";
#     size = "64M";  # Size with suffix (K, M, G)
#     contents = [
#       { source = "${pkgs.hello}/bin/hello"; target = "/bin/hello"; }
#       { source = ./my-config; target = "/etc/config"; }
#     ];
#   }
#
{ pkgs }:

{
  name,
  size ? "64M",
  contents ? [ ],
  label ? "sidereal",
  # Bytes per inode ratio - lower values create more inodes (default is ~16K)
  # Set to 4096 for filesystems with many small files
  bytesPerInode ? null,
}:

let
  # Parse size into bytes
  parseSizeArg = s:
    let
      suffix = builtins.substring (builtins.stringLength s - 1) 1 s;
      numStr = builtins.substring 0 (builtins.stringLength s - 1) s;
      num = builtins.fromJSON numStr;
      multiplier =
        if suffix == "K" || suffix == "k" then 1024
        else if suffix == "M" || suffix == "m" then 1024 * 1024
        else if suffix == "G" || suffix == "g" then 1024 * 1024 * 1024
        else 1;
    in
    num * multiplier;

  sizeBytes = parseSizeArg size;
  sizeBlocks = sizeBytes / 1024; # mkfs.ext4 uses 1K blocks by default
in

pkgs.runCommand "${name}.ext4"
{
  nativeBuildInputs = [ pkgs.e2fsprogs ];

  # Make the content paths available
  passthru = { inherit contents size label; };
}
''
  # Create staging directory with rootfs structure
  staging=$(mktemp -d)

  # Create standard FHS directories
  mkdir -p "$staging"/{bin,sbin,lib,lib64,usr,etc,tmp,dev,proc,sys,var,run,home,root}
  mkdir -p "$staging"/usr/{bin,sbin,lib,share}
  chmod 1777 "$staging/tmp"
  chmod 0700 "$staging/root"

  ${builtins.concatStringsSep "\n" (map (entry: ''
    # Create parent directory for target
    mkdir -p "$staging/$(dirname "${entry.target}")"

    # Copy source to target (dereference symlinks to avoid dangling refs)
    if [ -d "${entry.source}" ]; then
      cp -aL "${entry.source}/." "$staging${entry.target}/"
    else
      cp -aL "${entry.source}" "$staging${entry.target}"
    fi
  '') contents)}

  # Create ext4 image from staging directory (no mount required)
  mkfs.ext4 \
    -d "$staging" \
    -L "${label}" \
    ${if bytesPerInode != null then "-i ${toString bytesPerInode}" else ""} \
    -q \
    "$out" \
    ${toString sizeBlocks}

  # Clean up (chmod first since nix store files are read-only)
  chmod -R u+w "$staging" 2>/dev/null || true
  rm -rf "$staging"
''
