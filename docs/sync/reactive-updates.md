---
layout: doc
title: Reactive Updates
permalink: /sync/reactive-updates/
parent: Sync
---

# Reactive Updates

Oversqlite integrates with SQLiteNow invalidation so reactive queries stay current when sync-managed
tables change.

## What Produces Notifications

Reactive queries are invalidated when oversqlite changes managed tables through:

- local app writes captured by sync triggers
- push replay after successful local upload
- pulled incremental remote bundles
- snapshot rebuild apply
- successful `detach()` when managed local tables are cleared

## What Does Not Produce Notifications

`open()` is local runtime preparation. It does not attach an account and should not change managed
rows in the normal path.

If `detach()` returns `BLOCKED_UNSYNCED_DATA`, no destructive cleanup runs, so reactive tables are
not cleared.

Successful destructive detach also rotates oversqlite's internal source metadata, but the visible
reactive invalidation still comes from the managed-table wipe rather than from source bookkeeping.

## Rebuild And Recovery

`rebuild()` can replace large portions of managed local state. Reactive consumers should treat it
the same way they treat any other authoritative remote refresh: the data may change substantially in
one operation.

The internally managed source rotation that can happen during rebuild recovery or successful
destructive detach changes sync control state, but the user-visible invalidation still comes from
data changes rather than from source identity bookkeeping by itself.

## External Writers

If something outside SQLiteNow writes directly to the database, use:

```kotlin
database.reportExternalTableChanges(setOf("users", "posts"))
```

Oversqlite only auto-notifies for changes it performs through the managed runtime.
