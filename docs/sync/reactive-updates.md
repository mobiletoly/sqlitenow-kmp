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

Reactive queries are invalidated when Oversqlite changes managed tables through:

- local app writes captured by sync triggers
- push replay after successful local upload
- pulled incremental remote bundles
- snapshot rebuild apply
- successful `detach()` when managed local tables are cleared

## What Does Not Produce Notifications

`open(sourceId)` is local runtime preparation. It does not attach an account and should not change
managed rows in the normal path.

If `detach()` returns `BLOCKED_UNSYNCED_DATA`, no destructive cleanup runs, so reactive tables are
not cleared.

## Rebuild And Recovery

`rebuild(...)` can replace large portions of managed local state. Reactive consumers should treat it
the same way they treat any other authoritative remote refresh: the data may change substantially in
one operation.

`rotateSource(newSourceId)` changes sync control state only. By itself it should not invalidate
queries on managed business tables unless it is paired with a data-changing recovery flow.

## External Writers

If something outside SQLiteNow writes directly to the database, use:

```kotlin
database.reportExternalTableChanges(setOf("users", "posts"))
```

Oversqlite only auto-notifies for changes it performs through the managed runtime.

## Practical Guidance

- Keep one client instance per local database.
- Prefer ordinary reactive queries over manual refresh wiring.
- Expect `detach()` and rebuild flows to invalidate managed-table queries because they can clear or
  replace attached data.
