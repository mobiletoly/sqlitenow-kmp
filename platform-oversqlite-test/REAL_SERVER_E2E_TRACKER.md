# Android Realserver Tracker

This tracker covers Android runtime-harness `realserver` scenarios in
`platform-oversqlite-test`.

Module:

- `:platform-oversqlite-test:composeApp`

Primary package:

- `dev.goquick.sqlitenow.oversqlite.realserver`

Run a targeted Android realserver class:

```bash
./gradlew :platform-oversqlite-test:composeApp:connectedAndroidDeviceTest \
  -Pandroid.testInstrumentationRunnerArguments.class=dev.goquick.sqlitenow.oversqlite.realserver.RealServerBasicContractTest \
  -Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REALSERVER_TESTS=true \
  -Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=http://10.0.2.2:8080 \
  --no-daemon
```

Coverage groups:

- contract/lifecycle:
  - `RealServerBasicContractTest`
  - `RealServerLifecycleContractTest`
  - `RealServerSyncThenDetachContractTest`
  - `RealServerThreeDeviceConvergenceTest`
- richer schema and typed config:
  - `RealServerFkTopologyTest`
  - `RealServerBlobAndCascadeTest`
  - `RealServerGeneratedConfigTest`
  - `RealServerTypedRowsTest`
- recovery and transport:
  - `RealServerRecoveryTest`
  - `RealServerChunkedPushTransportTest`
- heavier scenarios:
  - `RealServerStressTest`
  - `RealServerRichSchemaStressTest`
  - `RealServerSharedConnectionStressTest`
  - `RealServerLongHorizonStaleFollowerStressTest`
  - `RealServerLongHorizonDivergentWriterStressTest`
  - `RealServerStaleFollowerPruneRecoveryStressTest`

Baseline expectations:

- tests are local-only and opt-in
- the suite uses `OVERSQLITE_REALSERVER_TESTS=true`
- Android emulators should target `http://10.0.2.2:8080`
- the suite resets the example server through `POST /test/reset`
- local sync keys remain covered for `TEXT PRIMARY KEY` and `BLOB PRIMARY KEY`
