---
layout: doc
title: Sync Operations Reference
permalink: /sync/sync-operations/
parent: Sync
---

# Sync Operations Reference

This page describes the current oversqlite operations.

## Lifecycle Operations

### `open()`

Prepares the local runtime and restores or creates the current internal source identity. Local-only.
Call on every launch.

### `attach(userId)`

Attaches or resumes the authenticated account scope. Required before connected sync operations.

### `sourceInfo()`

Returns read-only diagnostic information about the current internal source state.

### `syncStatus()`

Returns the current authority and pending-sync status for the attached scope.

Requires successful `open()` and `attach(userId)`.

### `detach()`

Safely detaches the currently attached account.

Returns:

- `DetachOutcome.DETACHED`
- `DetachOutcome.BLOCKED_UNSYNCED_DATA`

If blocked, local attached state is preserved.

### `syncThenDetach()`

Runs bounded best-effort `sync()` rounds and then attempts `detach()`.

## Connected Sync Operations

These operations require successful `open()` and `attach(userId)`.

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

## Recovery Operation

### `rebuild()`

Replaces local managed state from the authoritative snapshot.

Oversqlite chooses the internal recovery mode:

- ordinary rebuild-required and pull-side pruning use keep-source rebuild
- source-recovery-required cases use rebuild-plus-rotate internally

## Result Types

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

### `SourceInfo`

- `currentSourceId`
- `rebuildRequired`
- `sourceRecoveryRequired`
- `sourceRecoveryReason`

## Exceptions You Should Recognize

### `RebuildRequiredException`

The client is in a durable rebuild-required state. Ordinary sync is blocked until explicit
`rebuild()` succeeds.

### `SourceRecoveryRequiredException`

The current source stream is stale or out-of-order. Ordinary sync is blocked until explicit
`rebuild()` succeeds and oversqlite performs managed rotated recovery internally.
