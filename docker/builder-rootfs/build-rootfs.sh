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

check_deps() {
    local missing=()
    command -v docker >/dev/null || missing+=("docker")
    command -v mkfs.ext4 >/dev/null || missing+=("e2fsprogs (mkfs.ext4)")

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing dependencies: ${missing[*]}"
        exit 1
    fi
}

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

build_image() {
    log "Building Docker image..."
    docker build -t "${IMAGE_NAME}" "${SCRIPT_DIR}"
}

smoke_test() {
    log "Running smoke tests..."
    docker run --rm "${IMAGE_NAME}" /usr/bin/cargo --version
    docker run --rm "${IMAGE_NAME}" /usr/bin/rustc --version
    docker run --rm "${IMAGE_NAME}" /bin/bash -c 'echo "shell OK"'
    docker run --rm "${IMAGE_NAME}" /usr/bin/git --version
    log "Smoke tests passed"
}

validate_staging() {
    local staging="$1"
    local missing=()

    local required_paths=(
        /sbin/init
        /usr/bin/cargo
        /usr/bin/rustc
        /bin/bash
        /etc/ssl/certs
        /usr/bin/git
    )

    for path in "${required_paths[@]}"; do
        if [[ ! -e "${staging}${path}" ]]; then
            missing+=("${path}")
        fi
    done

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing required files in rootfs: ${missing[*]}"
        exit 1
    fi

    log "Rootfs validation passed"
}

create_rootfs() {
    local runtime_path="$1"
    local staging_dir
    staging_dir="$(mktemp -d)"

    cleanup() {
        if [[ -n "${staging_dir:-}" && -d "${staging_dir}" ]]; then
            rm -rf "${staging_dir}"
        fi
        docker rm "${CONTAINER_NAME}" 2>/dev/null || true
    }
    trap cleanup EXIT

    log "Creating container and exporting filesystem..."

    docker create --name "${CONTAINER_NAME}" "${IMAGE_NAME}" /bin/true
    docker export "${CONTAINER_NAME}" | tar -C "${staging_dir}" -xf -
    docker rm "${CONTAINER_NAME}"

    log "Installing builder-runtime as /sbin/init..."
    cp "${runtime_path}" "${staging_dir}/sbin/init"
    chmod 755 "${staging_dir}/sbin/init"

    log "Cleaning up unnecessary files..."
    rm -rf "${staging_dir}/var/cache/apt" \
           "${staging_dir}/var/lib/apt/lists"/* \
           "${staging_dir}/usr/share/doc" \
           "${staging_dir}/usr/share/man" \
           "${staging_dir}/usr/share/locale" \
           2>/dev/null || true

    validate_staging "${staging_dir}"

    log "Creating ext4 filesystem (${ROOTFS_SIZE})..."

    local size_bytes
    case "${ROOTFS_SIZE}" in
        *G) size_bytes=$(( ${ROOTFS_SIZE%G} * 1024 * 1024 * 1024 )) ;;
        *M) size_bytes=$(( ${ROOTFS_SIZE%M} * 1024 * 1024 )) ;;
        *K) size_bytes=$(( ${ROOTFS_SIZE%K} * 1024 )) ;;
        *)  size_bytes="${ROOTFS_SIZE}" ;;
    esac
    local size_blocks=$(( size_bytes / 1024 ))

    mkdir -p "$(dirname "${OUTPUT_PATH}")"

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
    smoke_test
    create_rootfs "${runtime_path}"

    log "Done! Rootfs image: ${OUTPUT_PATH}"
}

main "$@"
