## Kotlin/Wasm JS Enablement Plan

### 1. Research & Environment Validation
- [X] Confirm Kotlin 2.3 toolchain supports the `wasmJs` target and review the official setup guide (Kotlin docs: https://kotlinlang.org/docs/wasm-overview.html) to ensure compliance with the current Beta requirements — inspected `kotlin-gradle-plugin-2.2.20-gradle813.jar` for `wasmJs` presets and captured Kotlin docs highlights (Beta label, browser minimum versions, Wasm GC/exception-handling requirements).
- [X] Audit Gradle 9 / Android Gradle Plugin 8.13 compatibility notes for Kotlin/Wasm to avoid conflicts with our existing toolchain — Kotlin Gradle plugin enforces only a minimum Gradle 7.6.3 (`GradleCompatibilityCheck.kt` in `kotlin` repo) and minimum AGP 7.3.1 (`AgpCompatibilityCheck.kt`), while Google’s AGP 8.13 table pairs plugin 8.13 with Gradle 8.13 (`developer.android.com/build/releases/gradle-plugin#compatibility`), so our Gradle 8.13 / AGP 8.13 combo is within supported ranges.
- [X] Inventory all direct library dependencies from `library/build.gradle.kts` and record which already publish `wasmJs` artifacts (check `.module` metadata in `~/.gradle/caches` or Maven Central) so we know where shims are required — coroutines (`kotlinx-coroutines-core-1.10.2.module`), datetime (`kotlinx-datetime-0.7.1.module`), serialization JSON (`kotlinx-serialization-json-1.9.0.module`), Ktor core/content-negotiation/auth/serialization (`ktor-client-core-3.3.1.module`, etc.), and Kermit (`kermit-2.0.8.module`) all ship `wasmJs` variants; platform-only dependencies (`androidx.sqlite:sqlite-bundled`, `ktor-client-okhttp`, `ktor-client-darwin`) remain scoped to their targets; npm `sql.js@1.13.0` continues to require bundler-managed assets.
- [X] Review sql.js documentation (https://sql.js.org/documentation) to confirm the recommended packaging process for WebAssembly and how it aligns with Kotlin/Wasm bundling expectations — the v1.13.0 README states wasm is loaded via `locateFile` and the `sql-wasm.wasm` asset must be copied or served alongside the JS bundle, matching our loader strategy.
- [X] Verify IndexedDB APIs availability in the Kotlin/Wasm JS runtime (`kotlinx.browser` coverage) and document any host restrictions (e.g., browser-only vs. Deno compatibility) — Kotlin/Wasm docs highlight that the standard library exposes browser DOM APIs for wasm targets (`wasm-overview.md`:74-83), enabling IndexedDB usage in browsers; non-DOM hosts (WASI/Node) lack these APIs, so persistence must be guarded or disabled when `window.indexedDB` is absent.

### 2. Gradle Configuration & Target Hierarchy
- [X] Update `library/build.gradle.kts` to add the `wasmJs` target using the official DSL, configure `browser` (and keep the option open for `nodejs`) so Gradle generates the Wasm-specific assemblies — Kotlin’s own templates (`kotlin-wasm-browser-template/build.gradle.kts`) and Compose Wasm samples annotate the script with `@file:OptIn(ExperimentalWasmDsl::class)` and declare `wasmJs { browser(); binaries.executable() }`, confirming the required opt-in and target block for Kotlin 2.3.
- [X] Define a `wasmJsMain` source set that initially depends on `jsMain` via the default hierarchy (`webMain`) and break the dependency only where Wasm-specific code is needed — Kotlin 2.3 adds the shared `webMain`/`webTest` parent sets when `applyDefaultHierarchyTemplate()` is used (`docs/topics/whatsnew/whatsnew2220.md`:618-658), so enabling that template lets us share implementations between `js` and `wasmJs` without ad-hoc wiring.
- [X] Configure `wasmJsTest` (and optional `wasmJsNodeTest`) source sets with Kotlin’s default testing harness; ensure mocha/jest-style configs are removed in favor of the K/Wasm runner — the JS/Wasm DSL reuses the same `browser()` instrumentation, which creates tasks such as `wasmJsBrowserTest` alongside production bundles (mirrored in the official Wasm browser template README and reinforced by changelog item `KT-76996`, noting JS tasks now trigger Wasm subtasks), so once the target is added we need to hook `wasmJsTest` into CI.
- [X] Wire the new target into the publication matrix by extending `mavenPublishing` so the Wasm artifact is produced alongside existing JVM/JS/Native variants — the Vanniktech Maven Publish plugin automatically packages all Kotlin Multiplatform variants; we still need to verify `publishToMavenLocal` exposes the Wasm `.klib` after the target is added and adjust signing if any new publication identifiers appear.

### 3. Expect/Actual Implementations
- [X] Catalogue all `expect` declarations in `commonMain` (e.g., `SqliteConnection`, dispatchers, persistence hooks) and determine reuse vs. rewrite strategies for `wasmJs` — surface area consists of path/platform helpers (`resolveDatabasePath`, `validateFileExists`, `platform`), dispatcher factories (`sqliteConnectionDispatcher`, `sqliteNetworkDispatcher`), persistence hooks (`sqliteDefaultPersistence`, `exportConnectionBytes`), the bundled opener (`openBundledSqliteConnection`), and the core SQL primitives (`SqliteConnection`, `SqliteStatement`). JS actuals for the lightweight helpers can likely be shared, while the connection/statement APIs demand a dedicated SQL.js-backed implementation.
- [X] Migrate shared web-friendly actuals (`Platform`, `Dispatchers`, default export helper, default persistence hook) into `webMain`, keeping JS-only dynamic code in `jsMain` and constraining Wasm-specific logic to `wasmJsMain`.
- [ ] Audit remaining JS implementations for `dynamic`/`js("...")`; refactor Wasm-bound code paths to use supported interop types (`JsAny`, `JsArray`, `Uint8Array`, `@JsFun`) while leaving legacy dynamic usage in JS-only source sets.
- [X] Provide Wasm dispatcher actuals via the shared web source set (`Dispatchers.Default` for both connection and network paths).

### 4. SQL.js Integration for Wasm
- [X] Rebuild `library/src/wasmJsMain/resources/sqlitenow-sqljs.js` as an ES module that stores SQL.js `Database`/`Statement` objects in handle maps and exports only primitives plus array-backed blobs (converted to `Array<Int>` for wasm interop).
- [X] Add a dedicated `SqlJsGlue.externals.kt` file with `@file:JsModule("./sqlitenow-sqljs.js")` externals mirroring the glue’s handle-based API.
- [X] Implement `SqlJsInterop.wasm.kt` to wrap those externals: handle classes, suspend `ensureSqlJsLoaded()` using coroutine `await`, JsArray-based byte bridging, and full `SqliteConnection`/`SqliteStatement` behavior (transactions, bindings, stepping, export cache).
- [X] Wire the loader to accept a `locateFile` callback compatible with Kotlin/Wasm’s Vite bundler and document required asset-copy settings (handled by `ensureSqlJsLoaded()` and Gradle resource copy tasks).
- [X] Confirm SQL.js persistence (`Database#export()`) works under Wasm, and document any performance or size considerations flagged by sql.js maintainers (see `SqliteWasmPersistenceTest`).

### 5. Persistence & Host Storage
- [X] Evaluate a wasm-safe persistence story: Wasm now shares `IndexedDbSqlitePersistence` via the `webMain` actual, so browser targets persist snapshots just like JS.
- [X] Add an OPFS-backed persistence path with automatic fallback to IndexedDB when OPFS is unavailable.
- [ ] Add runtime guards for environments without IndexedDB (e.g., Wasm+Node) and provide fallback persistence (in-memory) with clear logging.
- [ ] Add end-to-end tests that open a database, persist bytes to IndexedDB, reload, and verify integrity in both browser and headless Wasm contexts.

### 6. Tooling & Bundling
- [X] Configure the Wasm resources pipeline to copy `sql-wasm.wasm` from `node_modules/sql.js/dist` into the processed resources (see build.gradle `copySqlJsWasmForWasm`), keeping `locateFile` paths aligned with the served asset.
- [ ] Audit existing `webpack.config.d/sqljs.js` overrides and extract shared configuration applicable to both JS and Wasm builds without duplicating logic.
- [X] Ensure npm dependencies (`sql.js`, `copy-webpack-plugin`) are declared via `packageJson { }` blocks for both JS and Wasm targets per Kotlin’s guidance, enabling Gradle to manage install steps automatically.

### 7. Testing & Verification
- [X] Create smoke tests that execute schema creation, CRUD operations, and migration paths via the Wasm runtime to match existing JS coverage; assert snapshot export/import parity (see `SqliteWasmSmokeTest`).

### 8. Documentation & Samples
- [ ] Update README and module-level KDocs to advertise Wasm JS availability, including any feature gaps.
- [X] Provide a minimal Wasm sample (extend `sample-kmp` and `samplesync-kmp`) that demonstrates initializing the database in a browser via Kotlin/Wasm.
- [ ] Document known limitations (e.g., unsupported Node features, file system APIs) and link out to Kotlin’s Wasm status page for future updates.

