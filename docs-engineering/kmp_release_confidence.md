# KMP release confidence

Use this guide when a KMP change needs a bounded host-side confidence pass. It
does not replace platform, device, or live-server qualification required by a
governing specification.

## Default coverage

Run the compiler and Gradle-plugin tests separately, then cover the retained
KMP runtimes and generated consumers:

```shell
./gradlew :sqlitenow-compiler:test
./gradlew :sqlitenow-gradle-plugin:test
./gradlew :library-core:jvmTest
./gradlew :library-core:jsTest
./gradlew :library-core:wasmJsBrowserTest
./gradlew :library-oversqlite:jsNodeTest
./gradlew :library-oversqlite:wasmJsBrowserTest
./gradlew :platform-core-test:harness:generateLibraryTestDatabase
./gradlew :platform-core-test:harness:generateMigrationFixtureDatabase
./gradlew corePlatformJvm
./gradlew corePlatformJsNode
./gradlew corePlatformWasmBrowser
./gradlew oversqliteComprehensive
./gradlew oversqlitePlatformJvm
./gradlew oversqlitePlatformJsNode
./gradlew oversqlitePlatformWasmBrowser
```

Finish with `git diff --check`. A successful host run is confidence evidence,
not acceptance or proof for unrun platform and live-server lanes.

## Optional qualification lanes

Run KMP real-server wrappers only after confirming the expected server identity
and endpoint. The default host endpoint is `http://localhost:8080`; Android
uses `http://10.0.2.2:8080`. Configure explicit overrides with
`OVERSQLITE_REAL_SERVER_SMOKE_BASE_URL` and
`OVERSQLITE_ANDROID_REAL_SERVER_SMOKE_BASE_URL`.

Use the documented `oversqliteRealserver*` tasks for live coverage, adding
`*Heavy` only when the heavier scenario intensity is required. Platform
qualification includes Android, iOS simulator, macOS, JS Node, and Wasm browser
where the local SDKs and harnesses are available. Record unavailable lanes as
unrun; do not substitute a host suite or mock for them.

## Interpretation

The default set is a merge-preparation confidence surface. It does not prove
all local runtime targets, server behavior, or owner acceptance. Changes to
shared synchronization behavior require the focused fixture consumers and the
applicable platform or live-server evidence described in
[Oversqlite testing](oversqlite_test.md).
