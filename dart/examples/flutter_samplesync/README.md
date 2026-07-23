# SQLiteNow Flutter SampleSync

This Android-first Flutter example mirrors the interactive
`samplesync-kmp` app. It keeps a persistent local SQLite database, supports
local-first person/address/comment changes, and synchronizes the generated
`business` schema through Oversqlite.

## Run

Start `go-oversync/examples/samplesync_server` against its PostgreSQL
`samplesync` database. Confirm that `http://127.0.0.1:8080/syncx/status`
reports `app_name: samplesync-server`.

Then run:

```shell
flutter pub get
flutter pub run sqlitenow_cli generate
flutter run -d emulator-5554 \
  --dart-define=SAMPLESYNC_BASE_URL=http://10.0.2.2:8080
```

The demo sign-in accepts a non-empty username such as `u10` and an empty
password. Choose Watch for server-sent bundle notifications with polling
fallback, or Polling for a ten-second interval. Skip leaves the local database
fully usable without a remote session.

Only the username and sync mode are persisted. Passwords and bearer tokens
remain in memory, and a restored session obtains a fresh token with the demo's
empty password.

## Test

Run the CI-safe host suite:

```shell
flutter analyze
flutter pub run sqlitenow_cli generate
flutter test
```

Run the local Android database smoke:

```shell
flutter test integration_test/app_smoke_test.dart -d emulator-5554
```

Run the explicit live SampleSync lane from the Dart workspace:

```shell
scripts/flutter_samplesync_realserver.sh
```

Environment overrides:

- `SAMPLESYNC_HOST_BASE_URL`: host-visible status URL, default
  `http://127.0.0.1:8080`
- `SAMPLESYNC_APP_BASE_URL`: emulator-visible server URL, default
  `http://10.0.2.2:8080`
- `SAMPLESYNC_ANDROID_DEVICE_ID`: Flutter device, default `emulator-5554`
- `SAMPLESYNC_RUN_TOKEN_REFRESH=true`: add the long live JWT-expiry check

The live lane verifies empty-password sign-in, initial/manual sync, Watch and
Polling delivery between two Flutter clients, durable session restore, and
sync-then-detach sign-out. It deliberately remains separate from
`oversqlite_realserver_all.sh`, which targets `nethttp-server-example`.
