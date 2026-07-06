---
layout: page
title: Swift Getting Started
permalink: /swift/getting-started/
---

# Swift Getting Started

This guide sets up SQLiteNow in a SwiftPM or Xcode app using the released
SQLiteNow SwiftPM package. You will keep SQL files in your app repository,
generate a local Swift package, add that generated package to the app, and use
the generated typed API from Swift.

Replace `X.Y.Z` with the latest SQLiteNow release version. SwiftPM dependency
versions use the bare version, such as `X.Y.Z`; GitHub release asset URLs use
the `vX.Y.Z` tag.

## Requirements

- macOS with Xcode command line tools
- SwiftPM with `swift-tools-version: 6.0` support
- Java 17 or newer on `PATH`

Check Java with:

```shell
java -version
```

## Add SQLiteNow

Add the released SQLiteNow package to the Swift package that owns your SQL
files:

```swift
// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "MyAppleApp",
    platforms: [
        .iOS(.v15),
        .macOS(.v14),
    ],
    dependencies: [
        .package(url: "https://github.com/mobiletoly/sqlitenow-kmp.git", from: "X.Y.Z"),
    ],
    targets: [
        .target(name: "MyAppleApp"),
    ]
)
```

The dependency makes the `sqlitenow-generate` command plugin available to
`swift package plugin`. When the package dependency is added to an Xcode
project, SQLiteNow also appears in Xcode's package plugin UI.

## Add SQL Files

Place SQL files under a database directory in the Swift package:

```text
SQLiteNow/databases/AppDatabase/
  schema/person.sql
  init/
  migration/
  queries/person/insert.sql
  queries/person/selectAll.sql
```

SQLiteNow uses the same SQL folder names for Swift, Kotlin Multiplatform, and
Flutter/Dart:

- `schema`: schema objects such as tables, indexes, and views
- `init`: data or setup SQL for a fresh database
- `migration`: versioned migration SQL
- `queries`: SQL files that generate typed Swift APIs

A minimal schema and query set can look like this:

```sql
-- SQLiteNow/databases/AppDatabase/schema/person.sql
CREATE TABLE person (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);
```

```sql
-- SQLiteNow/databases/AppDatabase/queries/person/insert.sql
INSERT INTO person (id, name)
VALUES (:id, :name);
```

```sql
-- SQLiteNow/databases/AppDatabase/queries/person/selectAll.sql
SELECT *
FROM person
ORDER BY id;
```

## Configure SQLiteNow

Create `SQLiteNow.json` at the Swift package root:

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

The released SQLiteNow SwiftPM package supplies the matching runtime artifact
URL and checksum for its version. App configs only choose the runtime mode.

When `sqlDirectory` is omitted, SQLiteNow reads SQL from
`SQLiteNow/databases/<databaseName>` under the package root. When
`outputDirectory` is omitted, it writes the generated package to:

```text
SQLiteNowGenerated/AppDatabaseSQLiteNow
```

## Generate The Package

Run generation from the Swift package root:

```shell
swift package plugin --allow-writing-to-package-directory sqlitenow-generate
```

From Xcode, invoke the `sqlitenow-generate` package plugin command from the
package plugin UI after adding the SQLiteNow package dependency.

Xcode uses the Xcode project directory as the default root. If the Swift package
root is above the `.xcodeproj`, pass the package root and config path together:

```shell
--package-root /path/to/MyAppleApp --config Config/SQLiteNow.json
```

The same options can be passed from SwiftPM:

```shell
swift package plugin --allow-writing-to-package-directory sqlitenow-generate --package-root /path/to/MyAppleApp --config Config/SQLiteNow.json
```

First-run setup matters: generate the local package before adding it as an app
dependency, or commit previously generated output that matches the SQL sources.

## Add The Generated Package

For a SwiftPM app, add the generated package path as a local dependency and link
the generated product:

```swift
dependencies: [
    .package(url: "https://github.com/mobiletoly/sqlitenow-kmp.git", from: "X.Y.Z"),
    .package(path: "SQLiteNowGenerated/AppDatabaseSQLiteNow"),
],
targets: [
    .executableTarget(
        name: "MyAppleApp",
        dependencies: [
            .product(name: "AppDatabaseSQLiteNow", package: "AppDatabaseSQLiteNow"),
        ]
    ),
]
```

For an Xcode app, run generation first so the package directory contains
`Package.swift`, then add `SQLiteNowGenerated/AppDatabaseSQLiteNow` as a local
package dependency and link the `AppDatabaseSQLiteNow` product to the app
target.

App source imports the generated product:

```swift
import AppDatabaseSQLiteNow
```

## Open The Database

Create the parent directory before opening a file-backed database:

```swift
import AppDatabaseSQLiteNow
import Foundation

let supportURL = FileManager.default
    .urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]

try FileManager.default.createDirectory(
    at: supportURL,
    withIntermediateDirectories: true
)

let databaseURL = supportURL.appendingPathComponent("app.sqlite")

let db = AppDatabase(path: databaseURL)
try await db.open()
```

Close the database when the owning app object shuts down:

```swift
try await db.close()
```

## Insert And Query Rows

Generated execute methods are `async throws`. SELECT statements return typed
runners with `list()`, `one()`, `oneOrNull()`, and `stream()`.

```swift
try await db.person.insert(PersonInsertParams(id: 1, name: "Ada"))

let people: [PersonRow] = try await db.person.selectAll().list()
```

## Watch Query Results

```swift
for try await rows in db.person.selectAll().stream() {
    print("Person count: \(rows.count)")
}
```

Generated write methods report invalidation through SQLiteNow's runtime, so the
stream emits again after generated inserts, updates, and deletes touch the
query's tables.

## Use A Transaction

Transactions use a synchronous mutation builder for generated non-returning
execute statements:

```swift
try await db.transaction { tx in
    tx.person.insert(PersonInsertParams(id: 2, name: "Grace"))
}
```

If any operation in the transaction fails, SQLiteNow rolls back the batch and
throws a Swift error.

## Regenerate Safely

The generated package is disposable output. Change SQL files or
`SQLiteNow.json`, then rerun `sqlitenow-generate`. Do not hand-edit files under
`SQLiteNowGenerated/`; edits there are replaced by the next generation run.

For local development, rerun generation whenever SQL changes. For CI, choose one
of these project policies:

- Commit `SQLiteNowGenerated/` and fail CI when generated output is stale.
- Run `sqlitenow-generate` before building the app target.

The generator writes request files under `.build/sqlitenow/requests/` and
records package metadata under `.sqlitenow/package-manifest.json` inside the
generated package.
