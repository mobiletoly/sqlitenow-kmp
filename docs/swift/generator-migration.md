---
layout: page
title: Swift Generator Migration
permalink: /swift/generator-migration/
---

# Swift Generator Migration

SQLiteNow generates one Swift architecture for Core and Oversqlite. The compiler emits a Swift
product from a contract-v2 `swiftProductSource` or `swiftProductPackage` request. SwiftPM consumers
can use the `sqlitenow-generate` command plugin and `SQLiteNow.json` instead of constructing the
compiler request themselves.

The previous overlay generator and its app-specific Kotlin bridge have been removed. SQLiteNow
does not provide aliases, forwarding APIs, or a fallback mode for those surfaces.

## Replace Compiler And Gradle Configuration

Replace `SqliteNowCompilerInput.swiftExport` with `swiftProductExport` and a
`SqliteNowSwiftProductExportConfig`. Direct compiler users should send contract version 2 and one
of these modes:

- `swiftProductSource` writes Swift sources for a caller-owned build.
- `swiftProductPackage` writes the generated SwiftPM package and its ownership manifest.

Gradle users should configure the existing `swiftPackage {}` block. Remove the retired
`swiftOverlayOutputDir`, `swiftOverlayModuleName`, `swiftFrameworkModuleName`, and
`swiftBridgePackageName` task properties. Set `runtime` to `core` or `sync` and provide the matching
runtime artifact.

The compiler rejects retired fields before it parses SQL, opens a schema database, or cleans an
output directory. Regenerate after changing the configuration so stale overlay files do not remain
in an application target.

## Replace Application Imports And Types

Import the generated product, such as `AppDatabaseSQLiteNow`. Remove imports of the former
app-specific Kotlin framework. Use the generated database, query namespaces, row and parameter
types, transactions, observers, and `SQLiteNowError` values.

For sync databases, call the generated database-owned `openForSync` API. The returned opening owns
upgrade progression and publishes the ready sync client. Close through the opening, generated
database, or client according to the owner that your application holds.

Custom migration callbacks move to the generated database initializer:

```swift
let database = AppDatabase(
    path: databaseURL,
    onMigrationStep: { scope in
        if scope.fromVersion == 1 && scope.toVersion == 2 {
            try await scope.connection.execute(
                sql: "UPDATE person SET display_name = full_name",
                bindValues: []
            )
        }
    }
)
```

Keep numbered SQL under `migration/`. SQLiteNow runs the callback inside the same migration
transaction after that boundary's SQL.

## Regenerate And Verify

Run the command plugin from the package root:

```shell
swift package plugin --allow-writing-to-package-directory sqlitenow-generate
```

Build the generated package, then compile the application against its product. Remove old overlay
files and app-specific bridge framework references from the application project after the new
product compiles.
