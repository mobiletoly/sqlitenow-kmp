#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY_DIR="$ROOT_DIR/library-core/build/bin/linuxArm64/debugTest"
BINARY_PATH="$BINARY_DIR/test.kexe"
DOCKER_IMAGE="ubuntu:24.04"
SKIP_BUILD=0
TEST_ARGS=()

usage() {
  cat <<'EOF'
Usage: scripts/test-linux-arm64-on-mac.sh [--skip-build] [--image <docker-image>] [-- <test args>]

Builds the Linux ARM64 native test binary for :library-core and runs it inside a Docker
linux/arm64 container. Any arguments after `--` are forwarded to `test.kexe`.

Options:
  --skip-build           Reuse the existing binary instead of running Gradle.
  --image <docker-image> Override the Docker image. Default: ubuntu:24.04
  -h, --help             Show this help text.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --image)
      if [[ $# -lt 2 ]]; then
        echo "error: --image requires a value" >&2
        exit 1
      fi
      DOCKER_IMAGE="$2"
      shift 2
      ;;
    --)
      shift
      TEST_ARGS=("$@")
      break
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown argument: $1" >&2
      echo >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "error: docker is required for linuxArm64 runtime testing on macOS" >&2
  exit 1
fi

HOST_ARCH="$(uname -m)"
if [[ "$HOST_ARCH" != "arm64" && "$HOST_ARCH" != "aarch64" ]]; then
  echo "warning: host architecture is '$HOST_ARCH'; Docker will likely use emulation for linux/arm64" >&2
fi

if [[ "$SKIP_BUILD" -eq 0 ]]; then
  (
    cd "$ROOT_DIR"
    ./gradlew :library-core:linkDebugTestLinuxArm64
  )
fi

if [[ ! -x "$BINARY_PATH" ]]; then
  echo "error: expected binary not found at $BINARY_PATH" >&2
  exit 1
fi

DOCKER_CMD=(
  docker run --rm
  --platform linux/arm64
  -v "$BINARY_DIR:/work"
  -w /work
  "$DOCKER_IMAGE"
  ./test.kexe
)

if [[ ${#TEST_ARGS[@]} -gt 0 ]]; then
  DOCKER_CMD+=("${TEST_ARGS[@]}")
fi

exec "${DOCKER_CMD[@]}"
