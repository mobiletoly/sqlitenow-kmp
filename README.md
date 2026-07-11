# SQLiteNow

[![Kotlin Multiplatform](https://img.shields.io/badge/Kotlin-Multiplatform-blue?logo=kotlin)](https://kotlinlang.org/docs/multiplatform.html)
[![Maven Central](https://img.shields.io/maven-central/v/dev.goquick.sqlitenow/dev.goquick.sqlitenow.gradle.plugin?logo=apache-maven&label=Maven%20Central)](https://central.sonatype.com/artifact/dev.goquick.sqlitenow/dev.goquick.sqlitenow.gradle.plugin)
[![CI](https://img.shields.io/github/actions/workflow/status/mobiletoly/sqlitenow-kmp/gradle.yml?branch=main&logo=github&label=CI)](https://github.com/mobiletoly/sqlitenow-kmp/actions/workflows/gradle.yml)
[![License](https://img.shields.io/github/license/mobiletoly/sqlitenow-kmp?logo=apache&label=License)](LICENSE)

**If SQLiteNow saves you time, please consider starring ⭐ the repository - it helps
more KMP, Flutter/Dart, and Swift developers find it.**

SQLiteNow is SQL-first tooling for type-safe SQLite access in Kotlin
Multiplatform and Flutter/Dart apps, plus native Swift apps through a local
generated-package workflow. Write schema, migration, and query files in SQL,
then generate platform-native code with typed parameters, typed results,
migrations, transactions, and reactive invalidation.

Full documentation is available at https://mobiletoly.github.io/sqlitenow-kmp/.

## Contents

- [Why SQLiteNow](#why-sqlitenow)
- [Choose Your Platform](#choose-your-platform)
- [SQL-First Example](#sql-first-example)
- [Quick Start](#quick-start)
- [Multi-Device Synchronization](#multi-device-synchronization-optional)
- [Components](#components)
- [Documentation](#documentation)

## Why SQLiteNow

SQLiteNow keeps SQL as the source of truth while still giving application code a
typed API. It is focused exclusively on SQLite instead of abstracting over
multiple database engines, which leaves room for SQLite-specific behavior,
annotations, migrations, and sync-aware generated code.

The main goals are:

- **Pure SQL control** - Write normal `.sql` files for schema, migrations, and
  queries.
- **Type-safe generated APIs** - Get typed parameters, typed result models,
  transactions, migrations, and reactive invalidation.
- **No IDE plugin requirement** - Use any editor and regular SQL tooling.
- **SQLite-specific generation** - Optimize for SQLite features instead of a
  lowest-common-denominator SQL dialect.
- **Domain-shaped results** - Use comment-based annotations such as
  `-- @@{ queryResult=... }`, `dynamicField`, and `mapTo` to shape generated
  models.
- **Optional sync** - Use SQLiteNow without synchronization, or add Oversqlite
  when an offline-first multi-device app needs it.


## SQL-First Example

SQLiteNow can shape generated result models directly from SQL annotations:

```sql
-- @@{ queryResult=PersonWithAddresses }
SELECT p.id,
       p.first_name,
       p.last_name,
       p.email,
       p.created_at,

       a.address_type,
       a.postal_code,
       a.country,
       a.street,
       a.city,
       a.state

/* @@{ dynamicField=addresses,
       mappingType=collection,
       propertyType=List<Address>,
       sourceTable=a,
       collectionKey=address_id } */

FROM Person p
     LEFT JOIN PersonAddress a ON p.id = a.person_id
ORDER BY p.id, a.address_type
LIMIT :limit OFFSET :offset
```

This generates a `PersonWithAddresses` result model with
`addresses: List<Address>` plus typed query parameters for `limit` and `offset`.
Adapters can convert between SQLite values and domain types, and `mapTo` can
shape data further when the generated model should match application-layer
types.

Full examples are available in [`/sample-kmp`](./sample-kmp) for KMP,
[`/dart/examples`](./dart/examples) for Dart/Flutter, and
[`/swift/samples/core`](./swift/samples/core) for native Swift.


## Choose Your Platform

| Platform                   | Use this when                                                               | Status                                                                | Start here                                                                |
|----------------------------|-----------------------------------------------------------------------------|-----------------------------------------------------------------------|---------------------------------------------------------------------------|
| Kotlin Multiplatform       | You are building shared Kotlin apps for Android, iOS, desktop, JS, or Wasm. | Released Gradle plugin and KMP runtime libraries                      | [KMP guide](https://mobiletoly.github.io/sqlitenow-kmp/kmp/)              |
| Flutter/Dart               | You are building Flutter apps or pure Dart packages.                        | Released Dart runtime and CLI packages                                | [Flutter/Dart guide](https://mobiletoly.github.io/sqlitenow-kmp/flutter/) |
| Native Swift local package | You want Swift/Xcode code to consume a generated local SwiftPM package.     | Local generated package workflow; release artifacts still in progress | [Swift guide](https://mobiletoly.github.io/sqlitenow-kmp/swift/)          |

Supported target details:

- Kotlin Multiplatform: Android, iOS, `macosArm64`, `linuxX64`, `linuxArm64`,
  JVM desktop/server, JavaScript browser, and Kotlin/Wasm browser.
- Flutter/Dart: Flutter native runtimes through Dart VM and `package:sqlite3`,
  plus pure Dart packages through the Dart runtime.
- Native Swift local package: Apple platforms through generated SwiftPM
  packages with reusable arm64 runtime XCFramework artifacts.
- JavaScript uses SQL.js with optional IndexedDB persistence. Kotlin/Wasm uses
  the same SQL.js runtime with automatic OPFS or IndexedDB persistence.

Native Swift support currently uses local generated Swift packages on Apple
platforms. Core Swift apps can keep SQL in a Swift/Xcode repository, run the
`sqlitenow-generate` SwiftPM command plugin, and import the generated package
product. The generator still requires Java 17 or newer, and published runtime
binary artifacts, a Swift-native generator/runtime, and a public `SWIFT`
compiler backend are not released yet. Native Swift runtime artifacts are
arm64-only: `macosArm64`, `iosArm64`, and `iosSimulatorArm64`.


## Quick Start

Each platform has a complete setup guide. The short version is the same across
all runtimes: keep SQL in your app repository, configure one or more databases,
run the generator, and use the generated API from application code.

### Kotlin Multiplatform

Full setup: [KMP getting started](https://mobiletoly.github.io/sqlitenow-kmp/kmp/getting-started/)

KMP apps use the `dev.goquick.sqlitenow` Gradle plugin and the
`dev.goquick.sqlitenow:core` runtime dependency. Configure one or more
databases in Gradle:

```kotlin
sqliteNow {
    databases {
        create("SampleDatabase") {
            packageName.set("com.example.app.db")
        }
    }
}
```

Place SQL files under the matching database directory:

```text
src/commonMain/sql/SampleDatabase/
  schema/
  queries/
  init/
  migration/
```

Generate the database API with the task created from the database name:

```shell
./gradlew :composeApp:generateSampleDatabase
```

Replace `:composeApp` with your KMP module path and `SampleDatabase` with your
database name. Generated Kotlin is written under `build/generated/sqlitenow/code`
and is wired into `commonMain` by the Gradle plugin. Treat that directory as
generated output: edit SQL/configuration and rerun the task instead of
hand-editing the generated files.

For a complete app walkthrough, follow the
[Mood Tracker tutorial series](https://mobiletoly.github.io/sqlitenow-kmp/kmp/tutorials/)
and browse the
[sample project](https://github.com/mobiletoly/moodtracker-sample-kmp).

### Flutter/Dart

Full setup: [Flutter/Dart getting started](https://mobiletoly.github.io/sqlitenow-kmp/flutter/getting-started/)

Flutter and Dart apps use `sqlitenow_runtime` plus `sqlitenow_cli`:

```yaml
dependencies:
  sqlitenow_runtime: ^X.Y.Z

dev_dependencies:
  sqlitenow_cli: ^X.Y.Z
```

Configure generation in `sqlitenow.yaml`:

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
```

Generate Dart code:

```shell
flutter pub run sqlitenow_cli generate
```

For pure Dart packages, use:

```shell
dart run sqlitenow_cli generate
```

Generated Dart is written to the configured `output` directory, such as
`lib/db/generated/app_database.dart`. Edit SQL/configuration and regenerate
instead of hand-editing generated Dart.

### Native Swift Local Package

Full setup: [Swift getting started](https://mobiletoly.github.io/sqlitenow-kmp/swift/getting-started/)

Swift apps add the released SQLiteNow SwiftPM package to the Swift package that
owns the SQL files, then configure generation with `SQLiteNow.json`:

```json
{
  "schemaVersion": 1,
  "databases": [
    {
      "databaseName": "AppDatabase",
      "swiftPackageName": "AppDatabaseSQLiteNow",
      "swiftTargetName": "AppDatabaseSQLiteNow",
      "runtime": "core"
    }
  ]
}
```

By default, SQL is read from:

```text
SQLiteNow/databases/AppDatabase/
```

Generate the local Swift package from the Swift package root:

```shell
swift package plugin --allow-writing-to-package-directory sqlitenow-generate
```

By default, the generated package is written to:

```text
SQLiteNowGenerated/AppDatabaseSQLiteNow
```

Add that generated directory as a local SwiftPM dependency, link the generated
product to the app target, and import it from Swift:

```swift
import AppDatabaseSQLiteNow
```

Treat `SQLiteNowGenerated/` as generated output. Change SQL or
`SQLiteNow.json`, then rerun `sqlitenow-generate`.

## Multi-Device Synchronization (optional)

SQLiteNow includes Oversqlite, an optional synchronization system for
multi-device applications with conflict resolution and offline-first behavior.
Sync setup requires three explicit pieces on each client:

- the SQLiteNow runtime package
- the Oversqlite runtime package
- generated database configuration with Oversqlite enabled

Oversqlite canonical bytes use RFC 8785 JCS. Exact signed 64-bit values are JSON strings mapped
to ordinary SQLite `INTEGER`; exact arbitrary-precision decimals are JSON strings mapped to
ordinary SQLite `TEXT`; explicitly approximate values use SQLite `REAL` and finite binary64 JSON
numbers. Configure these columns with `SyncTable.numericColumns` and `NumericColumnKind.EXACT_INT64`,
`EXACT_DECIMAL`, or `APPROXIMATE`. These rules apply only to Oversqlite: SQLiteNow library-core and
generated output with `oversqlite = false` are unchanged.

For an `oversqlite = true` generated database, pass an overridden `syncTables` list to
`buildOversqliteConfig(...)` or `newOversqliteClient(...)` when exact numeric metadata is needed.
The default remains the generated `enableSync` table list, so ordinary generated clients require no
extra argument.

Only fresh Oversqlite databases using this canonicalization and numeric contract are supported.
There is no in-place durable-state migration, canonicalization or hash fallback, or mixed-version
mode. When aligning an existing deployment with this contract, release compatible server and
client versions together and recreate the client databases, discarding outboxes, checkpoints,
retry state, and offline work.

The sync system automatically handles:

- **Change tracking** for all sync-enabled tables
- **Conflict resolution** for concurrent writes across devices
- **Incremental sync** to minimize bandwidth usage
- **Chunked push upload** for large dirty sets without a total-row hard ceiling
- **Error handling** and retry logic
- **Authentication** via customizable HTTP clients

If you have a table to sync, annotate it with `enableSync=true`:

```sql
-- Enable sync for this table
-- @@{ enableSync=true }
CREATE TABLE person (
    id TEXT PRIMARY KEY NOT NULL,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
);
```

Sync-enabled tables must use exactly one local `PRIMARY KEY` column of type `TEXT` or `BLOB`.
`INTEGER`/`BIGINT` sync keys are rejected, and local sync-enabled tables must not model the
reserved server scope column `_sync_scope_id`.

### Kotlin Multiplatform

Add the KMP runtime artifacts and enable Oversqlite in the Gradle DSL:

```kotlin
commonMain.dependencies {
    implementation("dev.goquick.sqlitenow:core:<version>")
    implementation("dev.goquick.sqlitenow:oversqlite:<version>")
}

sqliteNow {
    databases {
        create("AppDatabase") {
            packageName = "com.example.app.db"
            oversqlite = true
        }
    }
}
```

Then use the generated sync client in your application:

```kotlin
// Create authenticated HTTP client with JWT token refresh and base URL
val httpClient = HttpClient {
    install(Auth) {
        bearer {
            loadTokens { /* load saved token */ }
            refreshTokens { /* refresh when expired */ }
        }
    }
    defaultRequest {
        url("https://api.myapp.com")
    }
}

// Create sync client
val syncClient = db.newOversqliteClient(
    schema = "myapp",
    httpClient = httpClient,
    resolver = ServerWinsResolver
)

// Open local runtime and attach the authenticated account.
syncClient.open().getOrThrow()
syncClient.attach(userId = "user123").getOrThrow()

// Perform full sync (upload local changes, download remote changes)
syncClient.sync().getOrThrow()

// Optional: start default-off automatic downloads.
// Bundle-change watch is only a wake-up hint; pullToStable() remains authoritative.
val automaticDownloads = coroutineScope.launch {
    syncClient.runAutomaticDownloads(
        db.buildOversqliteAutomaticDownloadConfig(
            bundleChangeWatchMode = BundleChangeWatchMode.AUTO,
        ),
    )
}
automaticDownloads.cancelAndJoin()
```

KMP sync example: [`/samplesync-kmp`](./samplesync-kmp).

### Flutter/Dart

Add the Dart runtime packages and enable Oversqlite in `sqlitenow.yaml`.
Replace `X.Y.Z` with the latest SQLiteNow release version.

```yaml
dependencies:
  sqlitenow_runtime: ^X.Y.Z
  sqlitenow_oversqlite: ^X.Y.Z

dev_dependencies:
  sqlitenow_cli: ^X.Y.Z
```

```yaml
databases:
  AppDatabase:
    input: lib/db/sql/AppDatabase
    output: lib/db/generated
    package: app.db
    runtime: dart
    oversqlite: true
```

Then use the generated sync client in your application:

```dart

final httpClient = IoOversqliteHttpClient(
  baseUri: Uri.parse('https://api.myapp.com'),
  defaultHeaders: {
    HttpHeaders.authorizationHeader: 'Bearer $token',
  },
);

final syncClient = db.newOversqliteClient(
  schema: 'myapp',
  httpClient: httpClient,
);

await syncClient.open();
await syncClient.attach('user123');
await syncClient.sync();

// Optional: start default-off automatic downloads.
// Bundle-change watch is only a wake-up hint; pullToStable() remains authoritative.
final automaticDownloads = syncClient.startAutomaticDownloads();
await automaticDownloads.stop();
```

Dart sync package and realserver coverage: [
`/dart/packages/sqlitenow_oversqlite`](./dart/packages/sqlitenow_oversqlite).

## Components

Client-side framework components:

- **SQLiteNow Generator** - Generates type-safe Kotlin, Dart, or native Swift
  package code from SQL files.
- **SQLiteNow Library** - Provides runtime APIs for database access,
  transactions, migrations, and reactive invalidation.
- **Oversqlite** - Adds synchronization for SQLite databases with conflict
  resolution, change tracking, and offline-first behavior.

Server-side component:

- **OverSync** - Sync server adapter library for data synchronization. The
  current implementation is
  [go-oversync](https://github.com/mobiletoly/go-oversync), written in Go with
  PostgreSQL as the server-side data store.

SQLiteNow Generator and SQLiteNow Library can be used without Oversqlite.
Oversqlite can also synchronize a SQLite database with PostgreSQL without using
SQLiteNow code generation.

## Documentation

Full documentation is available at https://mobiletoly.github.io/sqlitenow-kmp/.
