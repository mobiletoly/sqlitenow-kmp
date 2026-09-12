---
layout: doc
title: Open, Attach, and Rebuild
permalink: /kmp/sync/open-connect-rebuild/
parent: KMP Sync
parent_url: /kmp/sync/
---

# Open, Attach, and Rebuild

These three concepts do different jobs:

- `openForSync(...).advance()`: inspect, migrate, and publish the coordinated database/client pair
- `attach(userId)`: attach or resume an authenticated account scope
- `rebuild()`: explicitly recover local managed state from the authoritative remote snapshot

## Coordinated opening

Fresh and unchanged coordinated opening is local-only. It:

- validates sync-managed table metadata
- installs local triggers and guards
- repairs interrupted lifecycle metadata
- restores or creates the current internal source identity
- captures pre-existing managed rows once when bootstrap policy allows it

Generation-change advancement can use the server for old-intent drain, discovery, replacement,
and rebuild. Inspection before the first `advance()` has no target-storage effects.

Generation changes require an explicit directed upgrade plan. The [synchronized schema evolution guide]({{ site.baseurl }}/sync/schema-evolution/) defines the supported additive table/column/FK profile and the application-owned migration work.

```kotlin
val opening = db.openForSync(config, httpClient, resolver)
opening.advance()
val ready = checkNotNull(opening.readyOrNull())
val database = ready.database
val client = ready.client
```

The current source identity is oversqlite-managed. App code does not pass it to coordinated opening.

## `attach(userId)`

`attach(userId)` is the authenticated lifecycle step.

```kotlin
when (val result = client.attach(userId).getOrThrow()) {
    is AttachResult.Connected -> {
        when (result.outcome) {
            AttachOutcome.RESUMED_ATTACHED_STATE -> Unit
            AttachOutcome.USED_REMOTE_STATE -> Unit
            AttachOutcome.SEEDED_FROM_LOCAL -> Unit
            AttachOutcome.STARTED_EMPTY -> Unit
        }
    }
    is AttachResult.RetryLater -> Unit
}
```

Important points:

- call it whenever an authenticated session exists
- coordinated ready publication is not a substitute for `attach(userId)`
- `RetryLater` is a normal lifecycle response, not an auth error
- successful destructive logout later rotates to a fresh internal source before the next attach

## `rebuild()`

`rebuild()` is an optional explicit recovery entry point. Ordinary checkpoint recovery is resumed
automatically by `sync()` and `pullToStable()` after `history_pruned`, `checkpoint_ahead`, or an
interrupted snapshot. Source-identity recovery remains explicit.

```kotlin
val report = client.rebuild().getOrThrow()
```

Important points:

- it remains an attached/authenticated operation
- it rebuilds local managed state from the authoritative snapshot
- oversqlite chooses the internal mode
- app code does not provide a replacement source id

If the current source is still valid, rebuild keeps it.

If the current source has become stale or out-of-order, rebuild preserves frozen unsynced intent,
rebuilds from snapshot, rotates to a fresh internal source, and restores the frozen intent under
that fresh source stream.

Successful destructive `detach()` is the other internal source-rotation point. Unlike rebuild, it
retires the current local sync incarnation because managed local account data was wiped.

## Typical App Flow

```kotlin
val opening = db.openForSync(config, httpClient, resolver)
opening.advance()
val ready = checkNotNull(opening.readyOrNull())
val database = ready.database
val client = ready.client

if (currentUserId != null) {
    when (val attach = client.attach(currentUserId).getOrThrow()) {
        is AttachResult.Connected -> client.sync().getOrThrow()
        is AttachResult.RetryLater -> Unit
    }
}
```

## Typical Logout Flow

```kotlin
val result = client.syncThenDetach().getOrThrow()
if (!result.isSuccess()) {
    // Ask the UI to stop producing new writes or keep the user attached.
}
```

If `syncThenDetach()` succeeds, the next signed-in session attaches through a fresh oversqlite
source stream.
