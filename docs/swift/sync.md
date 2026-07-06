---
layout: page
title: Swift Sync
permalink: /swift/sync/
---

# Swift Sync

SQLiteNow can generate a Swift package for an Oversqlite-enabled database. The
Swift app imports the generated package product, opens the local database,
creates a sync client, attaches an authenticated user, runs sync, and observes
sync progress from Swift async code.

Replace `X.Y.Z` with the latest SQLiteNow release version.

## Configure A Sync Database

Use `"runtime": "sync"`:

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

The released SQLiteNow SwiftPM package supplies the matching sync runtime
artifact URL and checksum for its version.

With `sqlDirectory` omitted, the generator reads SQL from
`SQLiteNow/databases/AppSyncDatabase` under the Swift package root. With
`outputDirectory` omitted, it writes the generated package to
`SQLiteNowGenerated/AppSyncDatabaseSQLiteNow`.

## Mark Sync-Managed Tables

Use `enableSync=true` on each table that should participate in Oversqlite sync.
Sync-managed tables must expose exactly one visible primary-key column, and that
key must hold UUID data.

```sql
-- SQLiteNow/databases/AppSyncDatabase/schema/person.sql
-- @@{ enableSync=true }
CREATE TABLE person (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);
```

Then add normal query files under `queries/`:

```sql
-- SQLiteNow/databases/AppSyncDatabase/queries/person/insert.sql
INSERT INTO person (id, name)
VALUES (:id, :name);
```

```sql
-- SQLiteNow/databases/AppSyncDatabase/queries/person/selectAll.sql
SELECT *
FROM person
ORDER BY name;
```

The generated package includes both the typed database API and the sync client
factory for the configured sync tables.

## Generate And Import

Run generation from the Swift package root:

```shell
swift package plugin --allow-writing-to-package-directory sqlitenow-generate
```

Add the generated package to your app and import the generated product:

```swift
import AppSyncDatabaseSQLiteNow
```

App code should import the generated package product, not the runtime module.

## Create A Sync Client

The generated database owns the local SQLite runtime. The sync client owns the
HTTP bridge and exposes Swift-owned configuration and result types.

```swift
let db = AppSyncDatabase(path: databaseURL)
try await db.open()

let sync = try db.makeSyncClient(
    baseURL: URL(string: "https://sync.example.com")!,
    auth: .bearer(accessToken: { tokenStore.currentAccessToken() }),
    schema: "business",
    verboseLogs: false
)
```

The Swift app does not import Ktor, Kotlin `StateFlow`, Kotlin `Result`, or the
internal bridge framework module.

## Open, Attach, And Sync

Call `open()` before connected sync operations. Your app owns authentication;
pass the authenticated user id returned by your server when attaching.

```swift
try await sync.open()

let source = try await sync.sourceInfo()
let signIn = try await requestSignInToken(sourceId: source.currentSourceId)

switch try await sync.attach(userId: signIn.user) {
case let .connected(_, status, _):
    print("Last seen bundle:", status.lastBundleSeqSeen)
case let .retryLater(retryAfterSeconds):
    print("Retry attach in \(retryAfterSeconds)s")
case let .unknown(raw):
    print("Unknown attach result:", raw)
}

let report = try await sync.sync()
print("Pending rows:", report.status.pending.pendingRowCount)
```

If `sourceInfo()` reports that rebuild or source recovery is required, call
`rebuild()` before continuing normal sync.

## Progress And Automatic Downloads

Progress is exposed as a Swift async sequence:

```swift
Task {
    for try await progress in sync.progress() {
        print(progress)
    }
}
```

Automatic downloads are default-off and return a Swift-owned cancellable handle:

```swift
let downloads = sync.startAutomaticDownloads(
    SQLiteNowAutomaticDownloadConfig(
        automaticDownloadIntervalMillis: 60_000,
        bundleChangeWatchMode: .auto
    ),
    onError: { error in
        print(error)
    }
)

downloads.cancel()
```

## Sign Out And Close

Use `syncThenDetach()` when signing out so local pending work has a chance to
upload before the source detaches.

```swift
let result = try await sync.syncThenDetach()
if result.success {
    sync.close()
}
```

Close the database when the owning app object shuts down:

```swift
try await db.close()
```

## Server Setup

SQLiteNow sync needs a compatible Oversqlite server. See the
[Sync server setup guide]({{ site.baseurl }}/sync/server-setup/) for server
requirements and deployment guidance.

For broader sync lifecycle concepts, see the
[sync core concepts guide]({{ site.baseurl }}/sync/core-concepts/).
