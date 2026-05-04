# SQLiteNow Dart

This workspace contains Dart-facing SQLiteNow packages.

Phase 3 introduces `sqlitenow_runtime`, the low-level runtime surface that
future generated Dart code will target. It is intentionally independent of the
Kotlin Multiplatform generator and runtime.

## Packages

- `packages/sqlitenow_runtime`: SQLite lifecycle, migrations,
  transactions, bind/read helpers, invalidation, and query runners for Dart VM
  and Flutter native targets.
- `packages/sqlitenow_cli`: direct Dart command for invoking the
  SQLiteNow compiler with the Dart backend.
- `examples/dart_console`: minimal non-Flutter Dart project using generated
  SQLiteNow Dart code.
