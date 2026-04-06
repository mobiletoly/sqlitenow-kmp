# Core And Oversqlite Module Split

## Summary

Split the current runtime `:library` module into two published modules:

- `:library-core` published as `dev.goquick.sqlitenow:core`
- `:library-oversqlite` published as `dev.goquick.sqlitenow:oversqlite`

`library-oversqlite` depends on `library-core`. It is not intended to be independent.

To keep the SQLitenow plugin compatible with that split, introduce a database-level DSL flag:

```kotlin
sqliteNow {
    databases {
        create("NowSampleSyncDatabase") {
            packageName = "dev.goquick.sqlitenow.samplesynckmp.db"
            oversqlite = true
        }
    }
}
```

The existing table-level SQL annotation `enableSync=true` remains the source of truth for which
tables participate in oversqlite sync. The new database-level `oversqlite` flag controls whether
the plugin emits oversqlite bridge APIs for that generated database.

## Problem

Today the runtime is published as `dev.goquick.sqlitenow:core`, but the artifact contains both:

- core database/runtime functionality
- oversqlite sync/runtime functionality

That creates three problems:

1. The published coordinates do not match the runtime surface.
2. Pure core consumers receive oversqlite and Ktor sync dependencies transitively.
3. The plugin currently emits oversqlite bridge code whenever it sees `enableSync=true` tables,
   which makes a clean runtime split impossible without additional codegen control.

## Goals

- publish core and oversqlite as separate runtime modules
- keep `core` as the minimal dependency for non-sync users
- make oversqlite bridge generation explicit at the database DSL level
- preserve table-level `enableSync=true` annotations as the sync-table declaration mechanism
- fail early with clear messages when a database is configured for oversqlite but the runtime
  dependency is missing or the schema does not define sync tables
- keep the generated oversqlite bridge API names stable where practical

## Non-Goals

- making oversqlite independent from core
- redesigning oversqlite runtime APIs
- changing sync protocol behavior
- auto-injecting the oversqlite dependency into consumer builds in this phase
- keeping the old unified `:library` module as a long-lived compatibility wrapper

## Public API Changes

### Runtime modules

- `dev.goquick.sqlitenow:core`
  contains only `dev.goquick.sqlitenow.core.*` and shared common utilities required by core
- `dev.goquick.sqlitenow:oversqlite`
  contains `dev.goquick.sqlitenow.oversqlite.*` and depends on `core`

### Plugin DSL

Add a new property to `DatabaseConfig`:

```kotlin
val oversqlite: Property<Boolean> = objects.property(Boolean::class.java).convention(false)
```

Usage:

```kotlin
sqliteNow {
    databases {
        create("AppDatabase") {
            packageName = "com.example.db"
        }

        create("SyncDatabase") {
            packageName = "com.example.sync.db"
            oversqlite = true
        }
    }
}
```

### Naming rationale

Use `oversqlite`, not `enabledSync`.

Reasons:

- `enableSync` already exists as a table-level SQL annotation
- `enabledSync` is too close semantically and visually to `enableSync`
- the new flag does not mark tables as sync-managed; it enables oversqlite-specific codegen for
  the whole database

## Proposed Behavior

### Source of truth split

- `enableSync=true` on tables:
  declares which tables are sync-managed
- `oversqlite = true` on the database:
  opts the generated database into oversqlite bridge generation

### Bridge generation matrix

| `oversqlite` | any `enableSync=true` tables | behavior |
| --- | --- | --- |
| `false` | `false` | generate core-only database code |
| `false` | `true` | generate core-only database code; validate sync annotations; emit a warning that oversqlite bridge code is disabled |
| `true` | `false` | fail generation with a clear error |
| `true` | `true` | validate sync annotations and generate oversqlite bridge code |

### Generated APIs when `oversqlite = true`

If at least one table is annotated with `enableSync=true`, the generated database keeps emitting:

- `companion object syncTables`
- `buildOversqliteConfig(...)`
- `newOversqliteClient(...)`

These remain generated from the table annotations exactly as today.

### Generated APIs when `oversqlite = false`

The generated database must not reference any type from `dev.goquick.sqlitenow.oversqlite`.

In particular, it must not emit:

- `syncTables`
- `buildOversqliteConfig(...)`
- `newOversqliteClient(...)`

This allows a core-only project to compile against `dev.goquick.sqlitenow:core` only.

## Validation Rules

### Schema validation

Validation tied to `enableSync=true` tables should still run even if `oversqlite = false`.

Rationale:

- invalid sync-table annotations should fail close to the schema authoring point
- users may temporarily disable bridge generation during migration work
- the meaning of `enableSync=true` should not become silently ignored

That validation includes the current sync-key checks such as:

- exactly one primary key column
- sync key must match the visible primary key
- sync key type must be supported for oversqlite
- reserved sync columns must not appear in local schema

### Dependency validation

If `oversqlite = true`, the plugin must validate that the oversqlite runtime is available on the
consumer compile classpath.

Accepted forms:

- external dependency on `dev.goquick.sqlitenow:oversqlite`
- project dependency on `:library-oversqlite`

If missing, generation should fail with an actionable message, for example:

```text
Database 'NowSampleSyncDatabase' sets oversqlite=true, but no oversqlite runtime dependency was found.
Add implementation("dev.goquick.sqlitenow:oversqlite:<version>") or depend on project(":library-oversqlite").
```

### Empty sync-table validation

If `oversqlite = true` but the schema contains no `enableSync=true` tables, generation should fail.

Rationale:

- the bridge would be misleading or useless without sync-managed tables
- the mismatch is almost certainly a configuration error

Example failure:

```text
Database 'NowSampleSyncDatabase' sets oversqlite=true, but no tables are annotated with enableSync=true.
```

## Module And Publication Layout

### Target module layout

- `:library-core`
  contains all current `dev.goquick.sqlitenow.core.*` sources and core tests
- `:library-oversqlite`
  contains all current `dev.goquick.sqlitenow.oversqlite.*` sources and oversqlite-specific tests

The old unified `:library` module is removed as part of this implementation rather than retained as
a compatibility wrapper.

### Dependency direction

- `library-oversqlite` depends on `library-core`

`api(...)` is preferred unless the oversqlite public surface is changed to hide core types from
consumers. Today the oversqlite runtime publicly exposes core types such as `SafeSQLiteConnection`,
so re-exporting core is the safer default.

### Publishing

- `:library-core` continues publishing artifact id `core`
- `:library-oversqlite` publishes artifact id `oversqlite`

This preserves the current published coordinate for pure core users while making sync support
explicit.

### Versioning decision

`core` and `oversqlite` use the same repository version and are released together.

This split does not introduce independent versioning between the two runtime modules. A given
release should publish matching coordinates such as:

- `dev.goquick.sqlitenow:core:0.x.y`
- `dev.goquick.sqlitenow:oversqlite:0.x.y`

Rationale:

- oversqlite remains layered directly on top of core
- the plugin and samples are easier to document with one version number
- consumers avoid compatibility ambiguity between runtime modules

### Artifact naming decision

Use the following root coordinates:

- `dev.goquick.sqlitenow:core`
- `dev.goquick.sqlitenow:oversqlite`

Expected target-specific publications follow the existing Kotlin Multiplatform naming pattern:

- `dev.goquick.sqlitenow:core-jvm`
- `dev.goquick.sqlitenow:core-js`
- `dev.goquick.sqlitenow:core-wasm-js`
- `dev.goquick.sqlitenow:core-android`
- `dev.goquick.sqlitenow:core-iosarm64`
- `dev.goquick.sqlitenow:core-iossimulatorarm64`
- `dev.goquick.sqlitenow:core-iosx64`
- `dev.goquick.sqlitenow:core-macosarm64`
- `dev.goquick.sqlitenow:core-macosx64`
- `dev.goquick.sqlitenow:core-linuxarm64`
- `dev.goquick.sqlitenow:core-linuxx64`
- `dev.goquick.sqlitenow:oversqlite-jvm`
- `dev.goquick.sqlitenow:oversqlite-js`
- `dev.goquick.sqlitenow:oversqlite-wasm-js`
- `dev.goquick.sqlitenow:oversqlite-android`
- `dev.goquick.sqlitenow:oversqlite-iosarm64`
- `dev.goquick.sqlitenow:oversqlite-iossimulatorarm64`
- `dev.goquick.sqlitenow:oversqlite-iosx64`
- `dev.goquick.sqlitenow:oversqlite-macosarm64`
- `dev.goquick.sqlitenow:oversqlite-macosx64`
- `dev.goquick.sqlitenow:oversqlite-linuxarm64`
- `dev.goquick.sqlitenow:oversqlite-linuxx64`

This keeps the module naming symmetric and makes the published artifact names match the runtime
surface.

## Plugin Implementation Plan

1. Extend `DatabaseConfig` with `oversqlite: Property<Boolean>` defaulting to `false`.
2. Extend `GenerateDatabaseFilesTask` with an `oversqlite` input property.
3. Pass the value through `SqliteNowPlugin` into the generation task.
4. Pass the flag into `DatabaseCodeGenerator`.
5. Refactor the current oversqlite generation block so it is gated by `oversqlite`.
6. Keep sync-table validation independent from bridge emission.
7. Add classpath validation for the oversqlite runtime when `oversqlite = true`.
8. Update samples, docs, and tests to use explicit core vs oversqlite dependencies.

## Execution Checklist

### Phase 1: Split runtime modules

- [x] Add `:library-core` and `:library-oversqlite` to `settings.gradle.kts`.
- [x] Remove the old unified `:library` module from `settings.gradle.kts`.
- [x] Create `library-core` build configuration based on the current core runtime surface.
- [x] Create `library-oversqlite` build configuration with a dependency on `:library-core`.
- [x] Move `dev.goquick.sqlitenow.core.*` runtime sources into `:library-core`.
- [x] Move `dev.goquick.sqlitenow.oversqlite.*` runtime sources into `:library-oversqlite`.
- [x] Move core-owned tests into `:library-core`.
- [x] Move oversqlite-owned tests into `:library-oversqlite`.
- [x] Ensure `:library-core` does not depend on oversqlite runtime code or oversqlite-only Ktor client wiring.

### Phase 2: Publishing

- [x] Configure `:library-core` to publish as `dev.goquick.sqlitenow:core`.
- [x] Configure `:library-oversqlite` to publish as `dev.goquick.sqlitenow:oversqlite`.
- [x] Confirm target-specific publications follow the expected `core-*` and `oversqlite-*` artifact naming pattern.
- [x] Keep `core` and `oversqlite` on the same repository version.
- [x] Verify generated publication metadata no longer uses the old `library` artifact naming.
- [x] Fix any remaining publication-version inconsistencies across targets before finalizing the split.

### Phase 3: Plugin DSL and code generation

- [x] Add `oversqlite: Property<Boolean>` to `DatabaseConfig` with default `false`.
- [x] Add an `oversqlite` input to `GenerateDatabaseFilesTask`.
- [x] Pass the database `oversqlite` flag from `SqliteNowPlugin` into the generation task.
- [x] Pass the `oversqlite` flag into `DatabaseCodeGenerator`.
- [x] Gate generation of `syncTables`, `buildOversqliteConfig(...)`, and `newOversqliteClient(...)` on `oversqlite = true`.
- [x] Keep `enableSync=true` table validation active even when `oversqlite = false`.
- [x] Make `oversqlite = true` fail when no tables are annotated with `enableSync=true`.
- [x] Add classpath validation so `oversqlite = true` fails with a clear message when the oversqlite runtime dependency is missing.
- [x] Ensure `oversqlite = false` generates code with no references to `dev.goquick.sqlitenow.oversqlite.*`.

### Phase 4: Existing test and sample module rewiring

- [x] Rewire `:platform-core-test:harness` to depend on `:library-core` only.
- [x] Rewire `:platform-oversqlite-test:composeApp` to depend on `:library-oversqlite`.
- [x] Rewire `:sample-kmp` to depend on `:library-core` only.
- [x] Rewire `:samplesync-kmp` to depend on `:library-oversqlite` instead of the old `:library` module.
- [x] Update `:samplesync-kmp` database DSL to set `oversqlite = true`.
- [x] Ensure `:samplesync-kmp` does not keep a stale direct dependency on the old unified runtime module.
- [x] Update any local project substitution or functional-test fixture that still points at the old `:library` module.
- [x] Reuse the existing separated plugin, sample, integration, and platform test surfaces instead of introducing new top-level test modules unless blocked.

### Phase 5: Documentation and repo notes

- [x] Update getting-started documentation so `core` is the default runtime dependency.
- [x] Update sync documentation to require both `dev.goquick.sqlitenow:oversqlite` and `oversqlite = true`.
- [x] Update sample snippets in docs to show the new DSL flag.
- [x] Update onboarding/repo notes where they still describe a single `library` runtime module.

### Phase 6: Verification

- [x] Run `./gradlew :sqlitenow-gradle-plugin:test`.
- [x] Run `./gradlew :library-core:jvmTest`.
- [x] Run `./gradlew :library-oversqlite:jvmTest`.
- [x] Run `./gradlew :platform-core-test:harness:jvmTest`.
- [x] Run `./gradlew :platform-oversqlite-test:composeApp:jvmTest`.
- [x] Run `./gradlew :sample-kmp:composeApp:compileKotlinDesktop`.
- [x] Run `./gradlew :samplesync-kmp:composeApp:compileCommonMainKotlinMetadata`.
- [x] Confirm the core-only harness and sample compile without any oversqlite dependency leakage.

## Consumer Migration

### Core-only database

Before:

```kotlin
dependencies {
    implementation("dev.goquick.sqlitenow:core:<version>")
}

sqliteNow {
    databases {
        create("AppDatabase") {
            packageName = "com.example.db"
        }
    }
}
```

After:

```kotlin
dependencies {
    implementation("dev.goquick.sqlitenow:core:<version>")
}

sqliteNow {
    databases {
        create("AppDatabase") {
            packageName = "com.example.db"
        }
    }
}
```

No DSL change is required for core-only databases.

### Oversqlite-enabled database

Before:

```kotlin
dependencies {
    implementation("dev.goquick.sqlitenow:core:<version>")
}

sqliteNow {
    databases {
        create("NowSampleSyncDatabase") {
            packageName = "dev.goquick.sqlitenow.samplesynckmp.db"
        }
    }
}
```

After:

```kotlin
dependencies {
    implementation("dev.goquick.sqlitenow:core:<version>")
    implementation("dev.goquick.sqlitenow:oversqlite:<version>")
}

sqliteNow {
    databases {
        create("NowSampleSyncDatabase") {
            packageName = "dev.goquick.sqlitenow.samplesynckmp.db"
            oversqlite = true
        }
    }
}
```

## Testing Plan

### Test scope decision

The repository already has sufficiently separated test surfaces for this split.

This work should not introduce new top-level test modules by default.

The preferred approach is:

- reuse the existing plugin, sample, integration, and platform test modules
- move or rewire existing runtime-owned tests to the new runtime modules
- update existing samples and platform harness modules to depend on the correct runtime
- add only narrowly targeted new tests if an existing test surface cannot express the regression

### Existing module rewiring

Update the current test and sample modules so they enforce the new runtime boundary:

- `:platform-core-test:harness` depends on `:library-core` only
- `:platform-oversqlite-test:composeApp` depends on `:library-oversqlite`
- `:sample-kmp` depends on `:library-core` only
- `:samplesync-kmp` depends on `:library-oversqlite` and sets `oversqlite = true`

Inside the split runtime modules:

- core tests move to `:library-core`
- oversqlite tests move to `:library-oversqlite`

The main regression to guard against is accidental oversqlite leakage into core-only consumers.
That is best caught by keeping the existing core-oriented modules free of any oversqlite dependency.

In other words, the current test separation is already a good match for the target architecture;
the main task is to preserve that separation at the dependency level after the runtime split.

For `samplesync-kmp`, that means updating its build scripts to point at the oversqlite runtime
module instead of the old unified `:library` dependency.

### Plugin unit tests

- `DatabaseCodeGenerator` emits no oversqlite bridge code when `oversqlite = false`
- `DatabaseCodeGenerator` emits bridge code when `oversqlite = true` and sync tables exist
- sync-table validation still runs when `oversqlite = false`
- `oversqlite = true` with no `enableSync=true` tables fails with the expected error

### Plugin functional tests

- fixture with `core` only and `oversqlite = false` compiles
- fixture with `oversqlite = true` and `oversqlite` dependency compiles
- fixture with `oversqlite = true` and missing `oversqlite` dependency fails with the expected message
- fixture with `enableSync=true` tables and `oversqlite = false` compiles without generated oversqlite APIs

Prefer adapting the existing functional test fixtures for these cases rather than creating a new
functional test suite solely for the split.

### Repo validation

- `./gradlew :sqlitenow-gradle-plugin:test`
- `./gradlew :library-core:jvmTest`
- `./gradlew :library-oversqlite:jvmTest`
- targeted verification of rewired modules after dependency updates:
  - `./gradlew :platform-core-test:harness:jvmTest`
  - `./gradlew :platform-oversqlite-test:composeApp:jvmTest`
  - `./gradlew :sample-kmp:composeApp:compileKotlinJvm`
  - `./gradlew :samplesync-kmp:composeApp:compileKotlinJvm`

Successful compilation of the core-only harness and sample without any oversqlite dependency is a
required acceptance check for the split.

## Documentation Updates

- update getting-started docs so `core` is the default runtime dependency
- update sync docs so oversqlite examples include both:
  - `implementation("dev.goquick.sqlitenow:oversqlite:<version>")`
  - `oversqlite = true` in the database DSL
- update sample apps:
  - `sample-kmp` depends on core only
  - `samplesync-kmp` depends on oversqlite, with `oversqlite = true`

## Decision

Adopt the split and make oversqlite bridge generation an explicit database-level opt-in through
`oversqlite = true`.

This keeps the runtime layering honest:

- core users get a smaller, cleaner dependency surface
- oversqlite users opt into both the runtime module and the generated bridge
- table-level `enableSync=true` remains the schema declaration mechanism
