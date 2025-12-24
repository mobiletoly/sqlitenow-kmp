---
layout: page
title: Kotlin/Wasm Integration
permalink: /documentation/kotlin-wasm/
---

SQLiteNow ships a Kotlin/Wasm target so your generated database code can run inside modern browsers
while sharing the same multiplatform API used on Android, iOS, JVM, and Kotlin/JS. If you already
consume the library from JS, the Wasm target feels identical—just with the newer toolchain.

## Enable the Wasm target

Add the Wasm target to your Gradle module alongside the other Kotlin targets:

```kotlin
kotlin {
    @OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)
    wasmJs {
        browser()
        binaries.library()
    }
}
```

The helper scripts bundled with SQLiteNow are wired in automatically, so `Gradle` takes care of
pulling the sql.js runtime and packaging it with your artifacts.

## Storage model

- **Native targets** (Android/iOS/JVM) persist directly to disk as soon as a transaction commits.
- **Kotlin/Wasm** keeps the live database inside the sql.js WebAssembly module. SQLiteNow detects the
  best browser persistence option at runtime:
  - **OPFS (Origin Private File System)** when available in a secure context. OPFS-backed storage
    delivers the same snapshot semantics today, with fast SyncAccessHandle support on our roadmap.
  - **IndexedDB** as a universal fallback for older browsers, non-secure contexts, or non-browser
    Wasm hosts. The behaviour matches the Kotlin/JS target.

Every successful statement outside a transaction (and each committed transaction) triggers an export
when `autoFlushPersistence` is enabled. You can still disable auto flush and call
`persistSnapshotNow()` manually.

## Controlling snapshots

Disable automatic exports when you want to batch writes, then flush explicitly:

```kotlin
val db = SampleDatabase()

db.connectionConfig = db.connectionConfig.copy(autoFlushPersistence = false)
db.open()

// … perform large batches …

db.persistSnapshotNow()
```

`persistSnapshotNow()` is a no-op on native targets, so you can call it from common code without
sprinkling platform checks. Provide your own `SqlitePersistence` implementation if you'd rather send
snapshots to another storage layer (for example OPFS or remote sync).

## Bundling sql.js assets

When you build the Wasm target, the SQLiteNow Gradle plugin copies the sql.js resources from the
`dev.goquick.sqlitenow:core` Wasm klib into your module’s generated resources. To run in production:

- Ensure your bundler serves `sql-wasm.wasm` alongside the compiled assets (the helper script loads
  it with `new URL("./sql-wasm.wasm", import.meta.url)`).
- Keep `sqlitenow-sqljs.js` and `sqlitenow-indexeddb.js` in the output bundle; they provide the glue
  code Kotlin uses at runtime.
- If your bundler complains about Node core modules (`fs`, `path`, `crypto`) referenced by sql.js,
  add Webpack fallbacks (recommended) or install browser polyfills.

### Webpack 5 fallbacks (recommended)

If you see errors like “Can’t resolve 'fs' in …/node_modules/sql.js/dist”, create a
`webpack.config.d/sqljs.js` file in the *consumer module* (the module that declares `wasmJs { browser() }`):

```js
config.resolve = config.resolve || {};
config.resolve.fallback = Object.assign({}, config.resolve.fallback, {
  fs: false,
  path: false,
  crypto: false,
});

config.module = config.module || {};
config.module.rules = Array.isArray(config.module.rules) ? config.module.rules : [];
config.module.rules.push({
  test: /sql\.js\/dist\/sql-wasm\.wasm$/,
  type: "asset/resource",
});

config.experiments = Object.assign({}, config.experiments, { asyncWebAssembly: true });
config.target = "web";
```

## Local testing

Use the Wasm browser test task to exercise your database end to end:

```shell
./gradlew :your-module:wasmJsBrowserTest
```

The task builds the Wasm binaries, launches a headless browser, and runs the Kotlin tests via Mocha.
It’s the quickest way to verify migrations, queries, and persistence without deploying to production.

## Troubleshooting

- **Snapshots missing after refresh** – confirm `autoFlushPersistence` is still `true`, or call
  `persistSnapshotNow()` before tearing down the database.
- **sql.js can’t find the `.wasm` file** – verify your bundler copies `sql-wasm.wasm` next to the
  helper script so the relative URL resolves.
- **Webpack errors for fs/path/crypto** – add fallbacks or browser-friendly polyfills; the Wasm
  runtime never uses those modules directly.
When you rely on OPFS in production, consider calling `navigator.storage.persist()` during startup so
the browser treats the origin as less disposable. If the capability test fails (for instance on Node
or plain HTTP contexts) the runtime seamlessly continues with IndexedDB snapshots.
