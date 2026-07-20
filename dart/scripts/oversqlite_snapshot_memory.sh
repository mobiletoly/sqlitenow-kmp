#!/usr/bin/env bash
set -euo pipefail

if [[ "${GITHUB_ACTIONS:-false}" == "true" ]]; then
  echo "Phase 4C snapshot memory workloads are local-only and must not run in GitHub Actions." >&2
  exit 64
fi

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 ARTIFACT_DIRECTORY" >&2
  exit 64
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$WORKSPACE_DIR"

dart run packages/sqlitenow_oversqlite/test/support/snapshot_memory_matrix.dart \
  --artifact-dir "$1"
