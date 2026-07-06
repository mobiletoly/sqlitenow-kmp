# KMP And Dart Release Confidence Gate

Use this gate before merging the Swift branch back to `main`, and before making
claims that Swift changes preserved KMP and Dart behavior.

The executable entry point is:

```shell
scripts/kmp-dart-release-confidence.sh
```

The default gate runs host-side KMP, generated-code, Dart, and Flutter checks
without live-server or device-only platform requirements.

## Default Coverage

KMP:

- `:sqlitenow-compiler:test`
- `:sqlitenow-gradle-plugin:test`
- `:library-core:jvmTest`
- `:library-core:jsTest`
- `:library-core:wasmJsBrowserTest`
- `:library-oversqlite:jsNodeTest`
- `:library-oversqlite:wasmJsBrowserTest`
- `:platform-core-test:harness:generateLibraryTestDatabase`
- `:platform-core-test:harness:generateMigrationFixtureDatabase`
- `corePlatformJvm`
- `corePlatformJsNode`
- `corePlatformWasmBrowser`
- `oversqliteComprehensive`
- `oversqlitePlatformJvm`
- `oversqlitePlatformJsNode`
- `oversqlitePlatformWasmBrowser`

Dart and Flutter:

- `:sqlitenow-compiler:syncDartCliCompilerJar`
- `dart pub get`
- regenerate `packages/sqlitenow_runtime/test/generated/dart_db.dart`
- verify the regenerated Dart fixture has no diff
- `dart analyze` for `sqlitenow_runtime`, `sqlitenow_cli`,
  `sqlitenow_oversqlite`, and `examples/dart_console`
- package tests for `sqlitenow_runtime`, `sqlitenow_cli`,
  `sqlitenow_oversqlite`, and `examples/dart_console`
- `flutter pub get`, `flutter analyze`, generator run, and widget tests for
  `dart/examples/flutter_todo`

The script finishes with `git diff --check`.

## Optional Lanes

Use `--realserver` to include live KMP and Dart realserver suites. The server
must be running before the script starts.

Host realserver default:

```text
http://localhost:8080
```

Android realserver default:

```text
http://10.0.2.2:8080
```

Overrides:

- `OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL`
- `OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL`

Use `--heavy` to include heavy realserver stress tests. This implies
`--realserver`.

Use `--all-platforms` to add Android, iOS simulator, and macOS KMP platform
lanes. This requires the local machine to have the matching SDKs, simulator, and
emulator setup.

Use `--flutter-android-realserver` to include Flutter Android realserver
integration tests. This implies `--realserver` and requires a running Android
emulator or device.

## Interpreting Results

The default gate is the merge-prep smoke surface for KMP plus Dart/Flutter. It
does not prove every local-only runtime surface and does not prove live sync
server behavior.

When a change touches oversqlite sync, realserver behavior, platform-specific
SQLite persistence, or release claims, run the relevant optional lanes in
addition to the default gate.

For individual suite ownership and lower-level command details, see:

- `docs-engineering/sqlitenow_core_test.md`
- `docs-engineering/oversqlite_test.md`
