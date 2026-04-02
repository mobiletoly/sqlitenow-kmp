# platform-oversqlite-test

This module is the runtime-harness home for oversqlite `platform` and `realserver` suites.

Prefer the root oversqlite suite entry points when running these lanes. The broad target tasks in
this module remain generic execution surfaces.

Suite model:

- `platform`: concrete runtime-surface validation without the live server
- `realserver`: live-server validation on concrete runtime surfaces

`comprehensive` stays in `:library`.

## Platform

Full platform sweep:

```bash
./gradlew oversqlitePlatformAll
```

Android emulator:

```bash
./gradlew oversqlitePlatformAndroid
```

JVM:

```bash
./gradlew oversqlitePlatformJvm
```

iOS simulator:

```bash
./gradlew oversqlitePlatformIosSimulatorArm64
```

macOS:

```bash
./gradlew oversqlitePlatformMacosArm64
```

JS Node:

```bash
./gradlew oversqlitePlatformJsNode
```

Wasm browser:

```bash
./gradlew oversqlitePlatformWasmBrowser
```

## Realserver

Start `go-oversync/examples/nethttp_server` first.

Full realserver sweep:

```bash
./gradlew oversqliteRealserverAll
```

Full realserver sweep plus JVM heavy scenario:

```bash
./gradlew oversqliteRealserverAllHeavy
```

Android emulator:

```bash
./gradlew oversqliteRealserverAndroid
```

Android emulator heavy mode:

```bash
./gradlew oversqliteRealserverAndroidHeavy
```

Target a specific Android realserver class:

```bash
./gradlew :platform-oversqlite-test:composeApp:connectedAndroidDeviceTest \
  -Pandroid.testInstrumentationRunnerArguments.class=dev.goquick.sqlitenow.oversqlite.realserver.RealServerBasicContractTest \
  -Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REALSERVER_TESTS=true \
  -Pandroid.testInstrumentationRunnerArguments.OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=http://10.0.2.2:8080 \
  --no-daemon
```

JVM:

```bash
./gradlew oversqliteRealserverJvmHarness
```

JVM heavy mode:

```bash
./gradlew oversqliteRealserverJvmHarnessHeavy
```

iOS simulator:

```bash
./gradlew oversqliteRealserverIosSimulatorArm64
```

iOS simulator heavy mode:

```bash
./gradlew oversqliteRealserverIosSimulatorArm64Heavy
```

macOS:

```bash
./gradlew oversqliteRealserverMacosArm64
```

macOS heavy mode:

```bash
./gradlew oversqliteRealserverMacosArm64Heavy
```

JS Node:

```bash
./gradlew oversqliteRealserverJsNode
```

JS Node heavy mode:

```bash
./gradlew oversqliteRealserverJsNodeHeavy
```

Wasm browser:

```bash
./gradlew oversqliteRealserverWasmBrowser
```

Wasm browser heavy mode:

```bash
./gradlew oversqliteRealserverWasmBrowserHeavy
```

Heavy realserver mode increases the intensity of heavy scenarios without acting as a separate suite
gate:

```bash
./gradlew oversqliteRealserverJvmHeavy
```

Default base URLs:

- host wrappers use `http://localhost:8080`
- Android wrappers use `http://10.0.2.2:8080`

Optional overrides:

- host wrappers: `OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL=...`
- Android wrappers: `OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL=...`

See [REAL_SERVER_E2E_TRACKER.md](./REAL_SERVER_E2E_TRACKER.md) for Android-specific realserver
scenario coverage notes.
