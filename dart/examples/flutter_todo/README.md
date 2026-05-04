# SQLiteNow Flutter Todo

This example is the first Flutter smoke app for the SQLiteNow Dart backend. It
uses handwritten SQL assets, generates Dart database code with
`sqlitenow_cli`, opens SQLite through `sqlitenow_runtime`, and drives
query, execute, transaction, adapter, and invalidation flows from normal Flutter
code.

## Generate

From this directory:

```shell
flutter pub get
flutter pub run sqlitenow_cli generate
```

The generated database lives under `lib/src/db/generated/`. The app code imports
that generated file directly, so Flutter does not apply the Gradle plugin or call
any Kotlin APIs.

The normal release path uses the compiler jar embedded in `sqlitenow_cli`.
Use `--compiler-jar` only when working inside this repository against a custom
local compiler artifact.

## Test

```shell
flutter test
flutter test integration_test -d <android-or-ios-device-id>
```

There is no separate `sqlitenow_flutter` package yet. The runtime remains a
Dart package; Flutter apps add their own app storage choice, such as
`path_provider`, around the generated database path.
