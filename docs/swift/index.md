---
layout: page
title: Swift
permalink: /swift/
---

# Swift

SQLiteNow supports SwiftPM and Xcode apps by generating a local Swift package
from SQL files in your app repository. Your app depends on the released
SQLiteNow SwiftPM package for the generator, runs the `sqlitenow-generate`
command plugin, then imports the generated package product from Swift code.

The generated package contains Swift source plus a SwiftPM binary target for the
SQLiteNow runtime XCFramework. Core databases use `SQLiteNowCoreRuntime`; sync
databases use `SQLiteNowSyncRuntime`. App source imports the generated product,
not the runtime module.

## Start Here

Use the getting-started guide to add SQLiteNow to a Swift package or Xcode app,
create SQL files, configure `SQLiteNow.json`, generate the local package, and
open/query the database:

[Getting started]({{ site.baseurl }}/swift/getting-started/)

Use the configuration reference for `SQLiteNow.json` fields, default paths,
runtime artifact options, regeneration behavior, and troubleshooting:

[Configuration]({{ site.baseurl }}/swift/configuration/)

Use the sync guide when your database participates in Oversqlite sync:

[Swift sync]({{ site.baseurl }}/swift/sync/)

## How It Fits Together

Replace `X.Y.Z` with the latest SQLiteNow release version.

```swift
dependencies: [
    .package(url: "https://github.com/mobiletoly/sqlitenow-kmp.git", from: "X.Y.Z"),
    .package(path: "SQLiteNowGenerated/AppDatabaseSQLiteNow"),
]
```

By default, authored SQL lives under:

```text
SQLiteNow/databases/<databaseName>/
  schema/
  init/
  migration/
  queries/
```

The default generated package output path is:

```text
SQLiteNowGenerated/<swiftPackageName>
```

App code imports the generated product:

```swift
import AppDatabaseSQLiteNow
```

## Requirements

- macOS with Xcode command line tools
- SwiftPM with `swift-tools-version: 6.0` support
- Java 17 or newer on `PATH`
- A released SQLiteNow SwiftPM package version

The generator is distributed as a SwiftPM command plugin, but the compiler tool
inside that plugin still runs on Java. A Java-free Swift generator is not part of
the current Swift workflow.
