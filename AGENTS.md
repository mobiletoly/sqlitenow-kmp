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
- Dynamic field alias paths now flow from the primary table alias through any joined view aliases: `StatementProcessingHelper.computeAliasPathForAlias` captures the prefix (e.g., `pkg -> act`) before appending transitive aliases gathered from nested views. `DynamicFieldUtils.computeSkipSet` uses these lower-cased paths to prune any mapping nested beneath a collection alias, so per-row/entity helpers stay scoped and even nested collections (e.g., `activities` in `ActivityBundleFullView`) no longer leak into the parent result type. `QueryCodeGenerator.addCollectionMappingExecuteAsListImplementation` now respects that skip set for collection fields too, keeping executeAsList constructors aligned with the slimmed data classes.
- `ResultMappingPlan`/`ResultMappingPlanner` (see `sqlitenow-gradle-plugin/src/main/kotlin/dev/goquick/sqlitenow/gradle/ResultMappingPlan.kt`) precompute regular fields, dynamic mapping roles, and skip sets once per SELECT. Both data-structure and query generators consume the plan, so we no longer maintain parallel pruning code when the schema changes.
- Instrumented coverage exists for DayTempo’s complex bundle hierarchy: `ActivityBundleIntegrationTest` seeds a full provider/bundle/package/activity/schedule stack and asserts the nested mapping emitted by `selectAllFullEnabled`. The test relies on `DayTempoTestDatabaseHelper` to configure adapters identical to production wiring; reuse that helper when adding new DayTempo Android tests.
- `DayTempoHeavyIntegrationTest` seeds multiple bundles/packages/activities with schedules, program items, and daily logs to stress nested mapping. It validates both `activityBundle.selectAllFullEnabled` on larger datasets and `dailyLog.selectAllDetailedByDate` to ensure schedule/program-item wiring remains intact.
- `DayTempoHeavyIntegrationTest` now also covers `activity.selectAllEnabledWithTimePointsByScheduleRepeat`, exercising schedule-repeat filtering and time-point hydration end-to-end.
- When annotating collection mappings, point `sourceTable` at the join alias surfaced in the current SELECT (e.g., `cdv` for `child_detailed_view` in `parent_with_children_view`). Inner aliases coming from nested views still show up via the appended alias path and keep skip logic aligned.
- `QueryCodeGenerator` now annotates every `readStatementResult` constructor argument (and the joined variants) with inline `//` comments describing the SQL type mapping plus the originating alias/table/column. Use these when tracing regressions in generated code—the emitted comments mirror alias paths, mapping hints, and `notNull` flags.
- Nested dynamic fields are resolved via `DataStructCodeGenerator.findSelectStatementByResultName`, letting the query generator reuse the referenced result type to build constructor arguments (including multi-level collections). `generateCollectionMappingCode` performs hierarchical `groupBy` so structures such as `ActivityBundleFullView` and `ActivityPackageWithActivitiesDoc` hydrate correctly.
- Alias-prefix handling has been tightened: `generateConstructorArgumentsFromMapping` normalises both the field alias and the original column name before resolving parameter names. We only strip prefixes when they are genuine alias artifacts, which prevents classifiers like `addressType` from losing their semantic prefix while still fixing cases such as `providerDocId`.
- `ResultMappingPlanner` now automatically filters out any SELECT columns that merely feed dynamic field mappings (including nested view aliases such as `joined__package__category__`). This keeps `_Joined` classes comprehensive while preventing top-level docs from gaining stray `joined*` constructor parameters.
- `generateDynamicFieldMappingCodeFromJoined` no longer assumes the first grouped row contains the full payload. It searches `rowsFor*` for the first non-null tuple before constructing per-row/entity mappings so left joins with intermittent nulls stop tripping the `notNull=true` guard.
- Constructor synthesis now consults table `propertyName` overrides for all mapping types (not just `entity`). Per-row projections like `PersonRow` therefore respect annotations such as `-- @@{ field=first_name, propertyName=myFirstName }` when wiring generated DTOs.
- For dynamic fields marked `notNull=true`, the generator skips the runtime null guard when the mapping originates from the primary alias; this removes the "condition is always false" warnings for base-entity projections while keeping protective checks on joined aliases.
- Integration coverage: instrumentation tests under `library-test/composeApp/src/androidInstrumentedTest/kotlin/dev/goquick/sqlitenow/librarytest` include `ParentViewCollectionTest`, which seeds SQL fixtures and verifies view-based dynamic fields omit unintended top-level properties.
- Parameter type inference now walks nested view columns (e.g., `activity_detailed_view` → `activity_schedule_to_join`) so placeholders inherit the same property types as their backing tables. Expect generated `Params` to surface enums like `ActivityScheduleRepeat` instead of falling back to `String`.

## Debug & Customization Tips
- Set `debug = true` in the DSL to enable verbose logging and tracing throughout generated code.
- Provide `schemaDatabaseFile` to persist the inspected schema DB for debugging migrations.
- Generated sources land in `build/generated/sqlitenow/code`; tasks clean this directory before regenerating when it contains `/generated/` in the path.
- When authoring views, make sure join predicates align with the logical foreign-key columns (example: `activity.category_doc_id = activity_category.doc_id`). SQLite will happily produce NULL for mismatched joins and the generator will escalate via the dynamic field `notNull` checks.

## Routine Commands
- After **any changes to `sqlitenow-gradle-plugin`**, always run plugin unit tests and fix failures: `./gradlew :sqlitenow-gradle-plugin:test`.
- To validate core runtime behavior, regenerate and test the dedicated library suite:
  - `./gradlew :library-test:composeApp:generateLibraryTestDatabase`
  - `./gradlew :library-test:composeApp:test`
  - `./gradlew :library-test:composeApp:compileDebugAndroidTestKotlin` (guards instrumentation stubs)
- For investigations in the complex `daytempo-kmp` sample:
  - Regenerate DB code: `./gradlew :daytempo-kmp:composeApp:generateDayTempoDatabase`
  - Verify compilation: `./gradlew :daytempo-kmp:composeApp:compileDebugUnitTestSources`
- During incremental refactors, run the full verification loop after **each** meaningful change:
  - `./gradlew :sqlitenow-gradle-plugin:test`
  - `./gradlew :library-test:composeApp:generateLibraryTestDatabase`
  - `./gradlew :library-test:composeApp:connectedAndroidTest`
  - `./gradlew :daytempo-kmp:composeApp:generateDayTempoDatabase`
  - `./gradlew :daytempo-kmp:composeApp:connectedAndroidTest`

Keep this document updated when plugin behavior, verification steps, or expected SQL layout changes.
