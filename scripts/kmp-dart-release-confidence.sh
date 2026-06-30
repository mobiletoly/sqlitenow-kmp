#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

GRADLE_FLAGS=(--no-daemon --no-configuration-cache -Dorg.gradle.vfs.watch=false)
RUN_REALSERVER=false
RUN_HEAVY=false
RUN_ALL_PLATFORMS=false
RUN_FLUTTER_ANDROID_REALSERVER=false

usage() {
  cat <<'USAGE'
Usage: scripts/kmp-dart-release-confidence.sh [options]

Runs the KMP plus Dart/Flutter release-confidence gate used before merging
the Swift branch back to main.

Options:
  --realserver                    Include KMP and Dart live realserver suites.
  --heavy                         Include heavy realserver stress tests.
  --all-platforms                 Include Android, iOS simulator, and macOS KMP platform lanes.
  --flutter-android-realserver    Include Flutter Android realserver integration tests.
  --help                          Show this help text.

Realserver lanes require go-oversync/examples/nethttp_server to be running.
Host tests default to http://localhost:8080. Android tests default to
http://10.0.2.2:8080. Override with OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL and
OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --realserver)
      RUN_REALSERVER=true
      ;;
    --heavy)
      RUN_HEAVY=true
      RUN_REALSERVER=true
      ;;
    --all-platforms)
      RUN_ALL_PLATFORMS=true
      ;;
    --flutter-android-realserver)
      RUN_FLUTTER_ANDROID_REALSERVER=true
      RUN_REALSERVER=true
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unsupported argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_gradle() {
  run ./gradlew "$@" "${GRADLE_FLAGS[@]}"
}

require_tool() {
  local tool="$1"
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Required tool not found on PATH: $tool" >&2
    exit 1
  fi
}

check_realserver() {
  local base_url="${OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL:-http://localhost:8080}"
  require_tool curl
  printf '\n==> Checking realserver at %s\n' "$base_url"
  if ! curl -fsS "${base_url%/}/syncx/health" >/dev/null; then
    cat >&2 <<EOF
Realserver is not reachable at ${base_url%/}/syncx/health.
Start go-oversync/examples/nethttp_server or set OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL.
EOF
    exit 1
  fi
}

run_kmp_local() {
  run_gradle :sqlitenow-compiler:test :sqlitenow-gradle-plugin:test
  run_gradle :library-core:jvmTest :library-core:jsTest :library-core:wasmJsBrowserTest
  run_gradle :library-oversqlite:jsNodeTest :library-oversqlite:wasmJsBrowserTest
  run_gradle \
    :platform-core-test:harness:generateLibraryTestDatabase \
    :platform-core-test:harness:generateMigrationFixtureDatabase
  run_gradle corePlatformJvm corePlatformJsNode corePlatformWasmBrowser
  run_gradle oversqliteComprehensive oversqlitePlatformJvm oversqlitePlatformJsNode oversqlitePlatformWasmBrowser

  if [[ "$RUN_ALL_PLATFORMS" == "true" ]]; then
    run_gradle :library-core:iosSimulatorArm64Test :library-core:macosArm64Test
    run_gradle corePlatformAndroid corePlatformIosSimulatorArm64 corePlatformMacosArm64
    run_gradle oversqlitePlatformAndroid oversqlitePlatformIosSimulatorArm64 oversqlitePlatformMacosArm64
  fi
}

run_dart_flutter_local() {
  require_tool dart
  require_tool flutter

  run_gradle :sqlitenow-compiler:syncDartCliCompilerJar

  pushd dart >/dev/null
  run dart pub get
  run dart packages/sqlitenow_cli/bin/sqlitenow_cli.dart generate -c packages/sqlitenow_runtime/sqlitenow.yaml
  run git diff --exit-code -- packages/sqlitenow_runtime/test/generated/dart_db.dart

  for target in packages/sqlitenow_runtime packages/sqlitenow_cli packages/sqlitenow_oversqlite examples/dart_console; do
    run dart analyze "$target"
  done

  run dart test packages/sqlitenow_runtime
  run dart test packages/sqlitenow_cli
  run dart test packages/sqlitenow_oversqlite
  run dart test examples/dart_console

  pushd examples/flutter_todo >/dev/null
  run flutter pub get
  run flutter analyze
  run flutter pub run sqlitenow_cli generate
  run flutter test
  popd >/dev/null
  popd >/dev/null
}

run_kmp_realserver() {
  if [[ "$RUN_ALL_PLATFORMS" == "true" ]]; then
    if [[ "$RUN_HEAVY" == "true" ]]; then
      run_gradle oversqliteRealserverAllHeavy
    else
      run_gradle oversqliteRealserverAll
    fi
    return
  fi

  run_gradle oversqliteRealserverJvm oversqliteRealserverJvmHarness oversqliteRealserverJsNode oversqliteRealserverWasmBrowser
  if [[ "$RUN_HEAVY" == "true" ]]; then
    run_gradle oversqliteRealserverJvmHeavy oversqliteRealserverJvmHarnessHeavy oversqliteRealserverJsNodeHeavy oversqliteRealserverWasmBrowserHeavy
  fi
}

run_dart_realserver() {
  require_tool flutter

  pushd dart >/dev/null
  if [[ "$RUN_FLUTTER_ANDROID_REALSERVER" == "true" ]]; then
    export OVERSQLITE_RUN_FLUTTER_ANDROID_SMOKE=true
  fi

  if [[ "$RUN_HEAVY" == "true" ]]; then
    run scripts/oversqlite_realserver_all_heavy.sh
  else
    run scripts/oversqlite_realserver_all.sh
  fi
  popd >/dev/null
}

run_kmp_local
run_dart_flutter_local

if [[ "$RUN_REALSERVER" == "true" ]]; then
  check_realserver
  run_kmp_realserver
  run_dart_realserver
fi

run git diff --check

printf '\nKMP + Dart/Flutter release-confidence gate completed.\n'
