---
layout: doc
title: Sync Operations
permalink: /sync/sync-operations/
parent: Sync
---

# Sync Operations

The current KMP `oversqlite` client exposes a small bundle-era API:

- `bootstrap(userId, sourceId)`
- `pushPending()`
- `pullToStable()`
- `sync()`
- `hydrate()`
- `recover()`
- `lastBundleSeqSeen()`

All sync operations require a successful `bootstrap(...)` first.

## Bootstrap

`bootstrap(userId, sourceId)` prepares the local database for sync. It validates the managed
tables, creates the sync metadata tables, binds the current user/device identity, and installs the
change-capture triggers.

```kotlin
client.bootstrap(userId = userId, sourceId = deviceId).getOrThrow()
```

Call bootstrap after authentication and before any sync operation. Client construction alone does
not create sync metadata or install triggers.

## Push Pending

`pushPending()` freezes all currently dirty rows into one logical outbound bundle, uploads that
bundle through the server's chunked push-session transport, fetches the committed authoritative
rows back, and replays them locally. If the push is accepted and replay completes, the matching
frozen dirty rows are cleared.

```kotlin
val result = client.pushPending()
result.onFailure { error ->
    logger.e(error) { "pushPending failed" }
}
```

Use `pushPending()` when:

- you want to flush local edits before the app backgrounds
- the user taps a manual sync button
- you are running a periodic upload job

Notes:

- `uploadLimit` is a per-chunk limit, not a total-dirty-row ceiling
- the client keeps new local writes in `_sync_dirty_rows` while an earlier frozen outbound snapshot
  is in flight
- create only one `OversqliteClient` per local database at a time; sync-operation serialization is
  enforced per client instance, not across multiple client objects

## Pull To Stable

`pullToStable()` downloads remote bundles until the server's current stable bundle sequence is fully
applied locally. It fails if the local database still has dirty rows, because pull applies
authoritative remote history and must not mix with unpushed local edits.

```kotlin
val result = client.pullToStable()
result.onFailure { error ->
    logger.e(error) { "pullToStable failed" }
}
```

If the server has already pruned the requested history, `pullToStable()` falls back to snapshot
hydration automatically.

## Sync

`sync()` is the default interactive operation. It runs push first and then pull.

```kotlin
client.sync().getOrThrow()
```

Use `sync()` when you want the standard "send local changes, then catch up on remote changes"
behavior.

## Hydrate

`hydrate()` rebuilds the local managed tables from a full server snapshot. Snapshot rows are staged
in `_sync_snapshot_stage` and become visible to application queries only after the final apply step
commits.

```kotlin
client.hydrate().getOrThrow()
```

Use `hydrate()` for:

- first-time device setup
- local database replacement from server state
- automatic prune fallback after pull can no longer replay history incrementally

Hydration is chunked internally. The public API no longer exposes paging or window parameters.

## Recover

`recover()` also rebuilds from snapshot, but it rotates the local `sourceId` and resets local
bundle state as part of recovery.

```kotlin
client.recover().getOrThrow()
```

Use `recover()` when the device should be treated as a fresh sync source after severe local state
loss.

## Last Bundle Seq Seen

`lastBundleSeqSeen()` returns the last remote bundle sequence durably applied locally.

```kotlin
val lastSeen = client.lastBundleSeqSeen().getOrThrow()
```

This is mainly useful for diagnostics, status surfaces, or advanced telemetry.

## Pause / Resume Controls

The client can temporarily suppress uploads or downloads:

```kotlin
client.pauseUploads()
client.resumeUploads()

client.pauseDownloads()
client.resumeDownloads()
```

This is useful around bulk local imports or temporary UI flows where remote apply should be delayed.

## Recommended Patterns

### First Device Restore

```kotlin
client.bootstrap(userId = userId, sourceId = deviceId).getOrThrow()
client.hydrate().getOrThrow()
```

### Returning Device Catch-Up

```kotlin
client.bootstrap(userId = userId, sourceId = deviceId).getOrThrow()
client.pullToStable().getOrThrow()
```

### Manual Sync Button

```kotlin
syncButton.onClick {
    coroutineScope.launch {
        client.sync().getOrThrow()
    }
}
```

### Event-Driven Sync

```kotlin
class SyncManager(
    private val client: OversqliteClient,
) {
    suspend fun onAppForegrounded() {
        client.sync().getOrThrow()
    }

    suspend fun onAppBackgrounded() {
        client.pushPending().getOrThrow()
    }
}
```

## Error Handling

- `bootstrap(...)` failures usually mean unsupported local schema/configuration and should be fixed
  before sync is retried.
- `pushPending()` failures leave dirty rows intact unless an accepted bundle was fully replayed.
- `SyncOperationInProgressException` means another sync operation already owns that client-local
  lock; treat it as expected contention rather than a user-facing sync failure.
- `pullToStable()` failures leave the durable checkpoint at the last successfully applied bundle.
- `hydrate()` and `recover()` use staged snapshot rows so partial downloads do not become partially
  visible local state.

## Conflict Resolution

Conflicts are resolved by the configured resolver when remote authoritative state and local changes
disagree. For most applications, `ServerWinsResolver` is the simplest default:

```kotlin
val client = database.newOversqliteClient(
    schema = "myapp",
    httpClient = httpClient,
    resolver = ServerWinsResolver,
)
```

Custom resolvers can keep local payloads or merge fields as needed.

---

**Next Steps**: Review [Bootstrap & Hydration]({{ site.baseurl }}/sync/bootstrap-hydration/) for
device setup flows or [Reactive Sync Updates]({{ site.baseurl }}/sync/reactive-updates/) for UI
integration patterns.
