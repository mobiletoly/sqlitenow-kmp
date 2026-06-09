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

The app remains a core SQLiteNow example. It also carries an opt-in
generated-oversqlite live-server suite for Flutter Android runtime validation.
After starting `go-oversync/examples/nethttp_server`, run it on an Android
emulator from the Dart workspace with:

```shell
OVERSQLITE_RUN_FLUTTER_ANDROID_SMOKE=true scripts/oversqlite_realserver_all.sh
```

The compatibility flag name is historical; it now runs the expanded Flutter
Android live-server suite, not only the smoke test. The suite covers generated
metadata, basic convergence, lifecycle, conflict recovery, rich-schema rows,
BLOB keys, cascades, bundle-change watch, and practical Android device
transport behavior.

Run the scaled heavy Android stress cases with:

```shell
OVERSQLITE_RUN_FLUTTER_ANDROID_SMOKE=true scripts/oversqlite_realserver_all_heavy.sh
```

To run one file directly:

```shell
flutter test integration_test/realserver_smoke_test.dart -d emulator-5554 \
  --dart-define=OVERSQLITE_REALSERVER_TESTS=true \
  --dart-define=OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=http://10.0.2.2:8080
```

Set `OVERSQLITE_ANDROID_DEVICE_ID` for a non-default device and
`OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL` if the Android emulator should
reach the server through a different URL.

There is no separate `sqlitenow_flutter` package yet. The runtime remains a
Dart package; Flutter apps add their own app storage choice, such as
`path_provider`, around the generated database path.
