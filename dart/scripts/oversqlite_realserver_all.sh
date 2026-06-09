#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$WORKSPACE_DIR"

export OVERSQLITE_REALSERVER_TESTS=true
export OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL="${OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL:-http://localhost:8080}"
export OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL="${OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL:-http://10.0.2.2:8080}"

for test_file in \
  packages/sqlitenow_oversqlite/test/realserver_conformance_test.dart \
  packages/sqlitenow_oversqlite/test/realserver_watch_test.dart \
  packages/sqlitenow_oversqlite/test/realserver_rich_schema_test.dart
do
  flutter test "$test_file"
done

if [[ "${OVERSQLITE_RUN_FLUTTER_ANDROID_SMOKE:-false}" == "true" ]]; then
  (
    cd examples/flutter_todo
    flutter_android_tests=(
      integration_test/realserver_smoke_test.dart
      integration_test/realserver_basic_lifecycle_test.dart
      integration_test/realserver_conflict_test.dart
      integration_test/realserver_rich_schema_test.dart
      integration_test/realserver_watch_test.dart
    )
    if [[ "${OVERSQLITE_REALSERVER_HEAVY:-false}" == "true" ]]; then
      flutter_android_tests+=(
        integration_test/realserver_heavy_test.dart
      )
    fi

    for test_file in "${flutter_android_tests[@]}"
    do
      flutter test "$test_file" \
        -d "${OVERSQLITE_ANDROID_DEVICE_ID:-emulator-5554}" \
        --dart-define=OVERSQLITE_REALSERVER_TESTS=true \
        --dart-define=OVERSQLITE_REALSERVER_HEAVY="${OVERSQLITE_REALSERVER_HEAVY:-false}" \
        --dart-define=OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL="$OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL"
    done
  )
fi
