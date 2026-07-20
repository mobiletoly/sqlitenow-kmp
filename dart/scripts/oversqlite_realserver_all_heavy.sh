#!/usr/bin/env bash
set -euo pipefail

if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
  echo "Oversqlite realserver heavy workloads are local-only and must not run in GitHub Actions." >&2
  exit 64
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$WORKSPACE_DIR"

export OVERSQLITE_REALSERVER_TESTS=true
export OVERSQLITE_REALSERVER_HEAVY=true
export OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL="${OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL:-http://localhost:8080}"
export OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL="${OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL:-http://10.0.2.2:8080}"

"$SCRIPT_DIR/oversqlite_realserver_all.sh"

flutter test packages/sqlitenow_oversqlite/test/realserver_heavy_test.dart
flutter test packages/sqlitenow_oversqlite/test/realserver_snapshot_profile_test.dart
