---
layout: page
title: Kotlin/JS Integration
permalink: /documentation/kotlin-js/
---

SQLiteNow targets Android, iOS, JVM, and Kotlin/JS. The multiplatform API is identical, but the JS runtime ships with a few important differences because it uses [sql.js](https://github.com/sql-js/sql.js) under the hood instead of the bundled native driver used on the other platforms.

## Storage model

- **Native targets (Android/iOS/JVM)** persist data via the underlying filesystem immediately. A successful transaction is safely on disk—even if the app crashes.
- **Kotlin/JS** keeps the database in memory inside the sql.js WebAssembly module. SQLiteNow snapshots that state into IndexedDB so the database survives page reloads.

## Automatic persistence

When you construct a generated database on JS, SQLiteNow wires in an `IndexedDbSqlitePersistence` instance automatically. Every successful statement (outside a transaction) and each committed transaction exports the sql.js database and stores it in IndexedDB.

That behavior is controlled by `SqliteConnectionConfig.autoFlushPersistence`, which defaults to `true`. Native targets ignore this flag because their drivers persist to disk directly.

## Manual snapshot flush

If your app performs large batches or wants to control when snapshots are written, disable auto flush and persist explicitly:

```kotlin
val db = SampleDatabase()

// Turn off automatic snapshots before opening the connection.
db.connectionConfig = db.connectionConfig.copy(autoFlushPersistence = false)

// Open the database (runs migrations and loads the last snapshot if present).
db.open()

// ... perform work ...

// Persist manually when it makes sense (e.g., before navigating away).
db.persistSnapshotNow()
```

Calling `persistSnapshotNow()` is a no-op on native targets but forces a snapshot export on JS. The method throws if you call it while a transaction is active to avoid flushing an inconsistent state.

## Snapshot location

By default, the JS runtime writes to an IndexedDB database named `SqliteNow` with an object store `sqlite-databases`. Each SQLiteNow database is stored as a single entry whose key is the database filename (for example `test.db`).

You can provide your own `SqlitePersistence` implementation if you want to persist snapshots somewhere else (such as uploading them to remote storage).

## When to flush manually

Consider disabling auto flush and calling `persistSnapshotNow()` yourself when:

- You are importing large datasets and want to avoid exporting after every INSERT.
- You only need to guarantee durability at specific checkpoints (for example after a user taps “Save”).
- You want to throttle snapshot writes to reduce IndexedDB churn.

For most apps the default `autoFlushPersistence = true` is the easiest option—changes are stored after each statement and transaction, so refreshing the page shows the latest state.
