#!/usr/bin/env bash
set -euo pipefail

if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
  echo "Flutter SampleSync realserver validation is local-only." >&2
  exit 64
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DART_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
APP_DIR="$DART_DIR/examples/flutter_samplesync"
HOST_BASE_URL="${SAMPLESYNC_HOST_BASE_URL:-http://127.0.0.1:8080}"
APP_BASE_URL="${SAMPLESYNC_APP_BASE_URL:-http://10.0.2.2:8080}"
DEVICE_ID="${SAMPLESYNC_ANDROID_DEVICE_ID:-emulator-5554}"
RUN_TOKEN_REFRESH="${SAMPLESYNC_RUN_TOKEN_REFRESH:-false}"

for tool in curl flutter grep; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Required tool not found on PATH: $tool" >&2
    exit 1
  fi
done

status_body="$(curl -fsS "${HOST_BASE_URL%/}/syncx/status")" || {
  cat >&2 <<EOF
SampleSync server is not reachable at ${HOST_BASE_URL%/}/syncx/status.
Start go-oversync/examples/samplesync_server with its PostgreSQL samplesync
database, or set SAMPLESYNC_HOST_BASE_URL.
EOF
  exit 1
}

if ! grep -Eq '"app_name"[[:space:]]*:[[:space:]]*"samplesync-server"' <<<"$status_body"; then
  echo "Expected app_name='samplesync-server', got: $status_body" >&2
  exit 1
fi

cd "$APP_DIR"
flutter pub get
flutter pub run sqlitenow_cli generate
flutter test integration_test/realserver_smoke_test.dart \
  -d "$DEVICE_ID" \
  --dart-define=SAMPLESYNC_BASE_URL="$APP_BASE_URL" \
  --dart-define=SAMPLESYNC_RUN_TOKEN_REFRESH="$RUN_TOKEN_REFRESH"
