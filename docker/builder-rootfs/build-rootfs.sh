#!/usr/bin/env bash
# Build the builder VM rootfs from Docker
#
# Usage:
#   ./build-rootfs.sh [output-path]
#
# Requires:
#   - Docker
#   - e2fsprogs (for mkfs.ext4)
#   - Built sidereal-builder-runtime binary

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
OUTPUT_PATH="${1:-${PROJECT_ROOT}/target/builder-rootfs.ext4}"
IMAGE_NAME="sidereal-builder-rootfs"
CONTAINER_NAME="sidereal-rootfs-tmp-$$"
ROOTFS_SIZE="3G"

# Colours for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[+]${NC} $*"; }
warn() { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[-]${NC} $*" >&2; }

# Check dependencies
check_deps() {
    local missing=()
    command -v docker >/dev/null || missing+=("docker")
    command -v mkfs.ext4 >/dev/null || missing+=("e2fsprogs (mkfs.ext4)")

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing dependencies: ${missing[*]}"
        exit 1
    fi
}

# Build or find the builder-runtime binary
get_builder_runtime() {
    local runtime_path="${PROJECT_ROOT}/target/x86_64-unknown-linux-musl/release/sidereal-builder-runtime"

    if [[ ! -f "${runtime_path}" ]]; then
        log "Building sidereal-builder-runtime..." >&2
        cargo build --release -p sidereal-builder-runtime --target x86_64-unknown-linux-musl >&2
    fi

    if [[ ! -f "${runtime_path}" ]]; then
        error "Failed to build sidereal-builder-runtime"
        exit 1
    fi

    echo "${runtime_path}"
}

# Build the Docker image
build_image() {
    log "Building Docker image..."
    docker build -t "${IMAGE_NAME}" "${SCRIPT_DIR}"
}

# Create rootfs ext4 image
create_rootfs() {
    local runtime_path="$1"
    local staging_dir
    staging_dir="$(mktemp -d)"

    # Cleanup function
    cleanup() {
        if [[ -n "${staging_dir:-}" && -d "${staging_dir}" ]]; then
            rm -rf "${staging_dir}"
        fi
        # Also clean up any leftover container
        docker rm "${CONTAINER_NAME}" 2>/dev/null || true
    }
    trap cleanup EXIT

    log "Creating container and exporting filesystem..."

    # Create container (don't start it)
    docker create --name "${CONTAINER_NAME}" "${IMAGE_NAME}" /bin/true

    # Export filesystem
    docker export "${CONTAINER_NAME}" | tar -C "${staging_dir}" -xf -

    # Clean up container
    docker rm "${CONTAINER_NAME}"

    # Copy builder-runtime as /sbin/init
    log "Installing builder-runtime as /sbin/init..."
    cp "${runtime_path}" "${staging_dir}/sbin/init"
    chmod 755 "${staging_dir}/sbin/init"

    # Remove unnecessary files to reduce size
    log "Cleaning up unnecessary files..."
    rm -rf "${staging_dir}/var/cache/apt" \
           "${staging_dir}/var/lib/apt/lists"/* \
           "${staging_dir}/usr/share/doc" \
           "${staging_dir}/usr/share/man" \
           "${staging_dir}/usr/share/locale" \
           2>/dev/null || true

    # Create ext4 image
    log "Creating ext4 filesystem (${ROOTFS_SIZE})..."

    # Parse size to bytes
    local size_bytes
    case "${ROOTFS_SIZE}" in
        *G) size_bytes=$(( ${ROOTFS_SIZE%G} * 1024 * 1024 * 1024 )) ;;
        *M) size_bytes=$(( ${ROOTFS_SIZE%M} * 1024 * 1024 )) ;;
        *K) size_bytes=$(( ${ROOTFS_SIZE%K} * 1024 )) ;;
        *)  size_bytes="${ROOTFS_SIZE}" ;;
    esac
    local size_blocks=$(( size_bytes / 1024 ))

    # Ensure output directory exists
    mkdir -p "$(dirname "${OUTPUT_PATH}")"

    # Create ext4 image from staging directory
    mkfs.ext4 -d "${staging_dir}" -L "builder" -q "${OUTPUT_PATH}" "${size_blocks}"

    log "Created rootfs: ${OUTPUT_PATH}"
    ls -lh "${OUTPUT_PATH}"
}

main() {
    check_deps

    log "Building builder VM rootfs..."

    local runtime_path
    runtime_path="$(get_builder_runtime)"
    log "Using builder-runtime: ${runtime_path}"

    build_image
    create_rootfs "${runtime_path}"

    log "Done! Rootfs image: ${OUTPUT_PATH}"
}

main "$@"
