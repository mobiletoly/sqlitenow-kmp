---
layout: page
title: Swift Configuration
permalink: /swift/configuration/
---

# Swift Configuration

`SQLiteNow.json` is the Swift generation config. By default the command plugin
reads `<package-root>/SQLiteNow.json`; pass `--config <path>` when the file
lives elsewhere.

```shell
swift package plugin --allow-writing-to-package-directory sqlitenow-generate --config Config/SQLiteNow.json
```

Relative paths in explicit config fields are resolved from the directory that
contains `SQLiteNow.json`. The default SQL input and generated package output
are resolved from the Swift package root.

Replace `X.Y.Z` with the latest SQLiteNow release version.

## Minimal Core Config

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

This generates a local Swift package at
`SQLiteNowGenerated/AppDatabaseSQLiteNow`. Because `sqlDirectory` is omitted,
SQL is read from `SQLiteNow/databases/AppDatabase`.

## Minimal Sync Config

For a sync database, use `"runtime": "sync"`:

```json
{
  "schemaVersion": 1,
  "databases": [
    {
      "databaseName": "AppSyncDatabase",
      "swiftPackageName": "AppSyncDatabaseSQLiteNow",
      "swiftTargetName": "AppSyncDatabaseSQLiteNow",
      "runtime": "sync"
    }
  ]
}
```

This generates a local Swift package at
`SQLiteNowGenerated/AppSyncDatabaseSQLiteNow` and links
`SQLiteNowSyncSupport` and `SQLiteNowSyncRuntime` from the released SQLiteNow
package. Because `sqlDirectory` is omitted, SQL is read from
`SQLiteNow/databases/AppSyncDatabase`. See the
[Swift sync guide]({{ site.baseurl }}/swift/sync/) for sync-managed table and
client setup.

## Top-Level Fields

| Field | Required | Description |
| --- | --- | --- |
| `schemaVersion` | yes | Must be `1`. |
| `databases` | yes | Non-empty array of database generation entries. |

Each database entry generates one local Swift package. `databaseName` values,
`swiftPackageName` values, and resolved output directories must be unique.

## Database Fields

| Field | Required | Description |
| --- | --- | --- |
| `databaseName` | yes | Database class name and compiler request name. |
| `sqlDirectory` | no | SQL directory containing `schema`, `init`, `migration`, and `queries` folders. Defaults to `SQLiteNow/databases/<databaseName>` from the package root. Explicit paths are relative to the config file. |
| `swiftPackageName` | yes | Generated Swift package and product name. |
| `swiftTargetName` | yes | Generated Swift target name. Usually the same as `swiftPackageName`. |
| `runtime` | yes | Use `"core"` for a local database package or `"sync"` for an Oversqlite-enabled package. |
| `outputDirectory` | no | Generated package destination. Defaults to `SQLiteNowGenerated/<swiftPackageName>` from the package root. Explicit paths are relative to the config file. |
| `runtimeArtifact` | no | Advanced/custom runtime artifact override. Released SQLiteNow SwiftPM packages provide runtime/support products automatically. Mutually exclusive with `runtimeXcframeworkDirectory`. |
| `runtimeXcframeworkDirectory` | no | Local-development-only unpacked runtime XCFramework directory. Mutually exclusive with `runtimeArtifact`. |
| `minimumPlatforms` | no | Swift package platform versions. Defaults to iOS 15 and macOS 14. |
| `requestedAppleTargets` | no | Runtime target list. Defaults to `macosArm64`, `iosArm64`, and `iosSimulatorArm64`. |
| `runtimeModuleName` | no | Advanced runtime module override. Defaults to `SQLiteNowCoreRuntime` for core and `SQLiteNowSyncRuntime` for sync. |
| `metadataPackageName` | no | Internal compiler metadata package name. Defaults from `databaseName`. |
| `debug` | no | Enables extra compiler debug behavior when `true`; the plugin writes the temporary schema database under `.build/sqlitenow/schema/`. |

The config rejects unknown fields so misspelled options fail fast.

## Runtime Artifacts

Released SQLiteNow SwiftPM packages expose matching support and runtime
products. During generation, SQLiteNow uses release metadata to write the
generated package `Package.swift` with a dependency on the same SQLiteNow
package version and the correct `.product(...)` entries. Normal app configs do
not include artifact URLs or checksums.

Use `runtimeArtifact.kind: "localZip"` only when you intentionally want a custom
runtime zip stored in your app repository or produced by a local development
workflow:

```json
{
  "runtime": "core",
  "runtimeArtifact": {
    "kind": "localZip",
    "path": "Artifacts/SQLiteNowCoreRuntime-X.Y.Z.xcframework.zip",
    "checksum": "<swift-package-compute-checksum-output>",
    "sqliteNowVersion": "X.Y.Z"
  }
}
```

Use `runtimeXcframeworkDirectory` only for local development with an unpacked
XCFramework:

```json
{
  "runtime": "core",
  "runtimeXcframeworkDirectory": "Artifacts/SQLiteNowCoreRuntime.xcframework"
}
```

`localZip` and `runtimeXcframeworkDirectory` are useful for local experiments.
The normal released setup omits both fields.

## Clean And Regenerate

The generated package directory is owned by SQLiteNow. Change SQL files or
`SQLiteNow.json`, then rerun:

```shell
swift package plugin --allow-writing-to-package-directory sqlitenow-generate
```

Generated output under `SQLiteNowGenerated/` can be deleted and regenerated. Do
not hand-edit generated `Package.swift`, Swift source, copied local runtime
artifacts, or `.sqlitenow/package-manifest.json`.

The plugin writes compiler request files under `.build/sqlitenow/requests/`.
Those files are build intermediates and can be removed with normal SwiftPM
`.build` cleanup. When `debug` is `true`, the plugin also uses
`.build/sqlitenow/schema/` for the compiler's file-backed inspection database.
`SQLiteNow.json` does not accept a custom `schemaDatabaseFile`; this keeps Xcode
and SwiftPM plugin runs inside tool-owned build output.

## Troubleshooting

If Java fails, make sure Java 17 or newer is on `PATH`:

```shell
java -version
```

If SwiftPM cannot resolve SQLiteNow, confirm the package dependency uses the
released package URL and a matching `X.Y.Z` version:

```swift
.package(url: "https://github.com/mobiletoly/sqlitenow-kmp.git", from: "X.Y.Z")
```

If generation fails with a package product, checksum, or binary target error,
confirm that the SQLiteNow package dependency version and generated package were
created from the same release. If you are using a custom `runtimeArtifact`,
confirm its URL, `sqliteNowVersion`, and `checksum` all come from the same
artifact.

If Xcode cannot add the generated local package, run generation first so the
output directory contains `Package.swift`.

If Xcode runs the plugin from a nested `.xcodeproj`, pass `--package-root` and
`--config` so SQLiteNow resolves the intended package root and config file.

If SwiftPM or Xcode keeps using stale package metadata, clear the app package's
`.build` directory or reset Xcode package caches, then rerun generation.
