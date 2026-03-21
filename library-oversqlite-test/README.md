# library-oversqlite-test

This module holds Android device tests for KMP `oversqlite`.

See also:

- [REAL_SERVER_E2E_TRACKER.md](./REAL_SERVER_E2E_TRACKER.md)

Binary payload note:

- non-key binary payload fields use standard base64 on the HTTP wire
- UUID-valued keys and UUID-valued key columns use dashed UUID text

## Opt-in Real-Server E2E Tests

The `dev.goquick.sqlitenow.oversqlite.e2e` Android device-test package talks to a manually started
`go-oversync` `nethttp_server`. These tests
are opt-in and skip unless you explicitly enable them with instrumentation arguments.

### Start the server

In `go-oversync`:

```bash
DATABASE_URL="postgres://postgres:postgres@localhost:5432/clisync_example?sslmode=disable" \
JWT_SECRET="your-secret-key-change-in-production" \
go run ./examples/nethttp_server
```

For Android emulator runs, the base URL should normally be `http://10.0.2.2:8080`.

### Run the opt-in tests

From `sqlitenow-kmp`:

```bash
./gradlew :library-oversqlite-test:composeApp:connectedAndroidDeviceTest \
  -Pandroid.testInstrumentationRunnerArguments.class=dev.goquick.sqlitenow.oversqlite.e2e.RealServerBasicContractTest \
  -Pandroid.testInstrumentationRunnerArguments.oversqliteRealServer=true \
  -Pandroid.testInstrumentationRunnerArguments.oversqliteE2EBaseUrl=http://10.0.2.2:8080 \
  --no-daemon
```

Optional arguments:

- `oversqliteE2ESchema=business`

### Notes

- these tests are not part of the normal automated suite
- they use fresh user IDs and source IDs per run to avoid cross-test contamination
- if `oversqliteRealServer` is not set, the class is skipped instead of trying to reach a server
