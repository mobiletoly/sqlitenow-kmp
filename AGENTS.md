# Agent Onboarding Notes

This repository hosts the SQLiteNow KMP libraries, sample apps, and the `sqlitenow-gradle-plugin` code generator.
The notes below capture the current mental model so the next agent can resume quickly.

## Key Projects
- `sqlitenow-gradle-plugin`: Gradle JVM module that generates Kotlin code from SQL assets.
- `library`: Shared runtime consumed by generated code (core, common logging, oversqlite sync helpers).
- `sample-kmp`, `daytempo-kmp`, `samplesync-kmp`: Example consumers showing plugin configuration.

## Gradle Code Generator Plugin
- Plugin ID `dev.goquick.sqlitenow`; defined in `sqlitenow-gradle-plugin/src/main/kotlin/dev/goquick/sqlitenow/gradle/SqliteNowPlugin.kt:1`.
- Adds a `sqliteNow { databases { create("DbName") { ... } } }` DSL in Kotlin Multiplatform modules; see `sample-kmp/composeApp/build.gradle.kts:110` for usage.
- For each registered database, the plugin registers `Generate<DbName>` tasks backed by `GenerateDatabaseFilesTask` and wires the generated sources into `commonMain`.

### Task Flow (`GenerateDatabaseFilesTask`)
1. Resources expected under `src/commonMain/sql/<DbName>/` with subfolders `schema`, `init`, `migration`, `queries`.
2. Uses a temp SQLite connection (file-backed when `schemaDatabaseFile` is supplied) to inspect schema and execute init batches.
3. Orchestrates codegen pipeline:
   - `SchemaInspector`, `SQLBatchInspector`, `MigrationInspector` parse respective directories.
   - `MigratorCodeGenerator` emits `VersionBasedDatabaseMigrations` implementation, wiring coroutine context helpers (extra tracing when `debug = true`).
   - `DataStructCodeGenerator` introspects CREATE statements, generates namespace objects, result classes, shared results.
   - `QueryCodeGenerator` produces per-query Kotlin extension functions (bind/execute/read) with optional debug logging & previews.
   - `DatabaseCodeGenerator` assembles a high-level database façade and adapter entry points.

### SQL Annotation Support
- SQL comments can include HOCON-style annotation blocks `@@{ ... }` handled by `Annotations.kt` for renaming, adapter selection, `queryResult` sharing, dynamic field mappings, etc.
- `SharedResultManager` enforces identical field shapes for shared result types and generates consolidated classes.
- Dynamic field alias paths now flow from the primary table alias through any joined view aliases: `StatementProcessingHelper.computeAliasPathForAlias` captures the prefix (e.g., `pkg -> act`) before appending transitive aliases gathered from nested views. `DynamicFieldUtils.computeSkipSet` uses these lower-cased paths to prune per-row/entity mappings that belong to a collection mapping, preventing stray properties such as `schedule` from escaping `ActivityPackageWithActivitiesDoc`.
- When annotating collection mappings, point `sourceTable` at the join alias surfaced in the current SELECT (e.g., `cdv` for `child_detailed_view` in `parent_with_children_view`). Inner aliases coming from nested views still show up via the appended alias path and keep skip logic aligned.
- Integration coverage: instrumentation tests under `library-test/composeApp/src/androidInstrumentedTest/kotlin/dev/goquick/sqlitenow/librarytest` include `ParentViewCollectionTest`, which seeds SQL fixtures and verifies view-based dynamic fields omit unintended top-level properties.

## Debug & Customization Tips
- Set `debug = true` in the DSL to enable verbose logging and tracing throughout generated code.
- Provide `schemaDatabaseFile` to persist the inspected schema DB for debugging migrations.
- Generated sources land in `build/generated/sqlitenow/code`; tasks clean this directory before regenerating when it contains `/generated/` in the path.

## Routine Commands
- After **any changes to `sqlitenow-gradle-plugin`**, always run plugin unit tests and fix failures: `./gradlew :sqlitenow-gradle-plugin:test`.
- To validate core runtime behavior, regenerate and test the dedicated library suite:
  - `./gradlew :library-test:composeApp:generateLibraryTestDatabase`
  - `./gradlew :library-test:composeApp:test`
  - `./gradlew :library-test:composeApp:compileDebugAndroidTestKotlin` (guards instrumentation stubs)
- For investigations in the complex `daytempo-kmp` sample:
  - Regenerate DB code: `./gradlew :daytempo-kmp:composeApp:generateDayTempoDatabase`
  - Verify compilation: `./gradlew :daytempo-kmp:composeApp:compileDebugUnitTestSources`

Keep this document updated when plugin behavior, verification steps, or expected SQL layout changes.
