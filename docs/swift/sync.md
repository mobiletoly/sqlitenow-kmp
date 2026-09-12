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
Sync-managed tables must expose exactly one visible primary-key column. A discovered
`text` key uses ordinary SQLite `TEXT`; a discovered `uuid` key uses canonical UUID
strings in `TEXT` or 16 UUID bytes in `BLOB`.

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

## Coordinate Opening And Retrieve The Sync Client

The generated database owns the local SQLite runtime. The sync client owns the
HTTP transport and exposes Swift-owned configuration and result types.

```swift
let db = AppSyncDatabase(path: databaseURL)
let opening = try await db.openForSync(
    baseURL: URL(string: "https://sync.example.com")!,
    auth: .bearer(accessToken: { tokenStore.currentAccessToken() }),
    config: SQLiteNowSyncConfig(
        generationId: 1,
        databaseId: "AppSyncDatabase",
        serviceId: "myapp-sync",
        transferTargets: SQLiteNowTransferTargets(
            maxPushRows: 200,
            maxPushDecodedBytes: 1_048_576,
            maxPullBundles: 1_000,
            maxSnapshotChunkRows: 500,
            maxSnapshotChunkDecodedBytes: 4_194_304
        ),
        snapshotApplyBatchRows: 128,
        verboseLogs: false
    )
)
let openingStatus = try await opening.advance()
guard openingStatus.outcome == .ready,
      let sync = opening.readyOrNil()?.client else {
    throw SQLiteNowError.sync(message: "Coordinated opening did not publish the database.")
}
```

`generationId` selects the server contract. Keep `databaseId` and `serviceId` stable across
launches. The client discovers server schemas and conversions automatically; Swift delegates
validation and sync to the shared KMP runtime. `SQLiteNowSyncConfig` exposes local transfer targets,
snapshot apply budgets, and retry policies; admitted server limits remain hard ceilings.

Application database reads and writes are unavailable while coordinated opening is pending. Use only the database/client pair published after the opening reaches ready.

Pass a `SQLiteNowSyncUpgradePlan` when an installed database advances to another generation. The [synchronized schema evolution guide]({{ site.baseurl }}/sync/schema-evolution/) covers supported additive table/column/FK changes, migration ownership, failure recovery, and returning devices.

Validated remote contract errors surface as `SQLiteNowError.remote`, preserving the server code,
HTTP status, operation, remediation, retryability, and optional discovery generation ID. This
structured error is available through both generated support and the reusable runtime.

The Swift app does not import Ktor, Kotlin `StateFlow`, Kotlin `Result`, or an
app-specific Kotlin framework.

## Open, Attach, And Sync

Retain the opening while the database is in use. `advance()` opens and publishes both the
generated database and its exact sync client; do not call ordinary `db.open()` for a sync-managed
database or construct a client before ready publication. Your app owns authentication; pass the
authenticated user id returned by your server when attaching.

```swift
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

One `sync()` call drains successive bounded push bundles before pulling. If the shared 64-attempt
allowance is exhausted or durable work cannot progress, it throws `SQLiteNowError.syncIncomplete`
with a typed reason, pending counts, confirmed bundle count, and attempts used. Local intent and
confirmed effects remain durable for a later call. The facade cancels and joins the KMP operation
when the caller cancels its Swift task, including during a later batch.

Uploads and downloads can be controlled independently:

```swift
try await sync.pauseUploads()
try await sync.resumeUploads()
try await sync.pauseDownloads()
try await sync.resumeDownloads()
```

These flags pause background activity. Explicit Swift sync, push, pull, and recovery continue.

If `sourceInfo()` reports checkpoint rebuild is required, normal `sync()` resumes it automatically;
`rebuild()` remains available as an explicit control. Source recovery still requires explicit
`rebuild()`.

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

Blocked detach results retain the attachment and expose `blockedReason` and pending rows.
`lastSync` is optional because an incomplete first round has no completed sync report. The three
rounds share one 64-attempt allowance.

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
## Explicit conflict resolution

A nil resolver now means `.deferConflict`. When a push reports
`SQLiteNowError.conflictNeedsResolution`, inspect `pendingConflict()` and pass the returned handle
to `resolveConflict(_:result:)` or `retryConflict(_:)`. Stale handles report
`SQLiteNowError.staleConflictResolution`. `.serverWins` and `.clientWins` remain explicit opt-ins.

Swift sync clients do not follow redirects; 3xx responses surface as bounded HTTP failures without
replaying authorization, source, or generation-proof headers.
