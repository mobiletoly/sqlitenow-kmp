---
layout: doc
title: Sync Operations Reference
permalink: /sync/sync-operations/
parent: Sync
---

# Sync Operations Reference

This page describes the current Oversqlite operations.

## Lifecycle Operations

### `open(sourceId)`

Prepares the local runtime and binds or validates the app-owned install `sourceId`. Local-only.
Call on every launch.

### `attach(userId)`

Attaches or resumes the authenticated account scope. Required before connected sync operations.

### `syncStatus()`

Returns the current authority and pending-sync status for the attached scope.

Requires successful `open(sourceId)` and `attach(userId)`.

### `detach()`

Safely detaches the currently attached account.

Returns:

- `DetachOutcome.DETACHED`
- `DetachOutcome.BLOCKED_UNSYNCED_DATA`

If blocked, local attached state is preserved.

### `syncThenDetach()`

Runs bounded best-effort `sync()` rounds and then attempts `detach()`.

```kotlin
val result = client.syncThenDetach().getOrThrow()
if (result.isSuccess()) {
    // Detached successfully.
} else {
    // result.detach == BLOCKED_UNSYNCED_DATA
    // result.syncRounds and result.remainingPendingRowCount explain what happened.
}
```

This is convenience sugar over `sync()` plus `detach()`. It is not a separate lifecycle model.

## Connected Sync Operations

These operations require successful `open(sourceId)` and `attach(userId)`.

### `pushPending()`

Freezes the current dirty snapshot into one logical outbound bundle and uploads it.

Returns `PushReport` with:

- `PushOutcome.NO_CHANGE`
- `PushOutcome.COMMITTED`

### `pullToStable()`

Pulls and applies remote bundles until the local client is at the authoritative stable bundle
sequence.

Returns `RemoteSyncReport`.

### `sync()`

Runs the standard interactive flow:

1. `pushPending()`
2. `pullToStable()`

Returns `SyncReport`.

## Recovery Operations

### `rebuild(RebuildMode.KEEP_SOURCE)`

Replaces local managed state from the authoritative snapshot while preserving the current internal
source identity.

### `rebuild(RebuildMode.ROTATE_SOURCE, newSourceId)`

Replaces local managed state from the authoritative snapshot and rotates to a fresh caller-provided
source identity.

### `rotateSource(newSourceId)`

Rotates the current source identity without discarding pending local edits.

Use this only for advanced recovery.

## Result Types

### `OpenState`

- `ReadyAnonymous`
- `ReadyAttached(scope)`
- `AttachRecoveryRequired(targetScope)`

### `AttachResult`

- `Connected(outcome, status, restore)`
- `RetryLater(retryAfterSeconds)`

### `AttachOutcome`

- `RESUMED_ATTACHED_STATE`
- `USED_REMOTE_STATE`
- `SEEDED_FROM_LOCAL`
- `STARTED_EMPTY`

### `DetachOutcome`

- `DETACHED`
- `BLOCKED_UNSYNCED_DATA`

### `RebuildMode`

- `KEEP_SOURCE`
- `ROTATE_SOURCE`

## Exceptions You Should Recognize

### `SourceSequenceMismatchException`

The current source stream no longer matches what the server has already committed for that stream.
Treat this as an explicit recovery condition, not as a transient transport failure.

### `SourceBindingMismatchException`

`open(sourceId)` was called with a different value than the one already durably bound to the local
database. Default handling is explicit app-owned reset/recovery of the local Oversqlite database or
local sync state, not retry-with-random-id behavior.

### `SourceRotationBlockedException`

Source rotation was requested while another non-rotatable durable lifecycle operation was still
active.
