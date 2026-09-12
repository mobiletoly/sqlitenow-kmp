---
layout: doc
title: Sync Operations Reference
permalink: /kmp/sync/sync-operations/
parent: KMP Sync
parent_url: /kmp/sync/
---

# Sync Operations Reference

This page describes the current oversqlite operations.

## Lifecycle Operations

### `openForSync(...).advance()`

Coordinates inspection, migration, and publication of the exact ready database/client pair. Call
on every launch and retain the pair published by that opening:

```kotlin
val opening = database.openForSync(config, httpClient, resolver)
opening.advance()
val ready = checkNotNull(opening.readyOrNull())
val database = ready.database
val client = ready.client
```

Fresh and unchanged advancement is local-only. Generation-change advancement can require the
server. Do not call ordinary `open()` for a sync-managed database.

### `attach(userId)`

Attaches or resumes the authenticated account scope. Required before connected sync operations.

Every opened client admits the server's discovery contract before publishing an attached result.
A new attachment validates discovery before `/sync/connect`; a durable same-user resume refreshes
discovery for the new client lifetime. Ordinary sync operations reuse the immutable admitted
contract and do not perform contract preflights.

### `sourceInfo()`

Returns read-only diagnostic information about the current internal source state.

### `syncStatus()`

Returns the current authority and pending-sync status for the attached scope.

Requires a ready result from `openForSync(...).advance()` and successful `attach(userId)`.

### `detach()`

Safely detaches the currently attached account.

Returns:

- `DetachOutcome.DETACHED`
- `DetachOutcome.BLOCKED_UNSYNCED_DATA`

If blocked, local attached state and source identity are preserved.

If successful through the destructive cleanup path, managed local state is cleared and anonymous
state is rebound to a fresh internal source identity.

### `syncThenDetach()`

Runs at most three `sync()`/`detach()` rounds with one shared allowance of 64 push attempts.
`SyncThenDetachResult.lastSync` is absent when no round completed sync. A blocked result includes
`blockedReason` and the remaining pending row count; it leaves the attachment intact.

If the final detach succeeds destructively, `syncThenDetach()` rotates to the same fresh-source
anonymous state as plain `detach()`. If it returns `BLOCKED_UNSYNCED_DATA`, the existing source is
preserved.

## Connected Sync Operations

These operations require a ready result from `openForSync(...).advance()` and successful
`attach(userId)`.

Attachment obtains the contract once from `/sync/generations/{id}` and validates the full managed
SQLite shape, pending values, limits, and watch availability before admission. A changed
fingerprint fails closed. Server schemas and wire types are discovered automatically; applications
keep ordinary SQL and table selection.

### `pushPending()`

Freezes a dependency-ordered batch within both the local transfer targets and admitted server row
and decoded-byte limits, then sends exactly one compact `POST /sync/push`. Remaining rows stay
pending for later calls. One row may exceed a local byte target to guarantee progress, but a row
over the server hard limit fails before the outbox changes or a source bundle ID is consumed.
Retries reuse the exact frozen request.

Returns `PushReport` with:

- `PushOutcome.NO_CHANGE`
- `PushOutcome.COMMITTED`

### `pullToStable()`

Pulls and applies remote bundles until the local client is at the authoritative stable bundle
sequence.

Returns `RemoteSyncReport`.

### `sync()`

Drains successive bounded pushes, then pulls to a stable remote checkpoint. A local write arriving
before a fetched bundle applies returns the flow to upload drainage. One invocation admits at most
64 push attempts, including conflict reentry and checkpoint recovery. `SyncIncompleteException`
reports `PUSH_ATTEMPT_LIMIT` or `NO_PROGRESS`, pending dirty/outbound counts, push attempts, and
uniquely confirmed bundles. Already committed work stays durable and the next `sync()` may resume.
One-bundle `pushPending()` and fail-closed `pullToStable()` keep their separate meanings.

Upload and download pause flags control background work; explicit operations continue while paused.
An automatic watch retains its pending high-water target across a temporary blocker or pause.

Returns `SyncReport` only after the final local pending-state check succeeds.

## Numeric Wire Contract

Oversqlite's breaking v1 contract uses canonical strings for signed integers, finite binary64
values and exact decimals on every wire surface. Negative floating zero becomes `"0"`. Boolean
columns use JSON Booleans; UUID, byte and JSON columns use the representation declared by the
server generation. SQLite affinity determines whether local storage is compatible.

Pending changes retain their SQLite storage types before discovery. Invalid values remain pending
and fail admission or upload. Applications do not specify wire types or codecs in SQL.
Incompatible development databases must be explicitly recreated; legacy state conversion is not
part of this contract.

## Automatic Downloads And Watch

Automatic downloads are optional and default-off. Starting the worker is explicit;
`openForSync(...).advance()` and `attach(userId)` never start background network work.

The worker downloads only. It never uploads local changes, clears dirty state, rebuilds
automatically, or bypasses sync recovery gates.

Polling and bundle-change watch both wake the same authoritative download path:

| Mode | What Triggers A Download | What Carries Row Data | Failure Behavior |
| --- | --- | --- | --- |
| Polling | The configured interval expires. | `pullToStable()` responses. | The next interval tries again. |
| Bundle-change watch | The server emits a `/sync/watch` bundle event. | `pullToStable()` responses. | The worker falls back to polling and later retries watch. |

When bundle-change watch is enabled and the admitted discovery contract sets `watch_enabled`, the
worker opens `/sync/watch` as a metadata-only wake-up stream. Watch events do not contain
authoritative row data for the client to apply. Every remote mutation still reaches SQLite through
the ordinary `pullToStable()` path.

If watch is unavailable, disabled, disconnected, or malformed, the worker falls back to polling
`pullToStable()` on its configured interval.

`pauseDownloads()` suppresses automatic polling, watch requests, watch-event pulls, and fallback
pulls. Explicit `pullToStable()` still runs while automatic downloads are paused.

## Recovery Operation

### `rebuild()`

Replaces local managed state from the authoritative snapshot.

Oversqlite chooses the internal recovery mode:

- ordinary rebuild-required and pull-side pruning use keep-source rebuild
- a checkpoint ahead of current server history also uses keep-source rebuild
- source-recovery-required cases use rebuild-plus-rotate internally

Checkpoint recovery is restart-safe: the rebuild gate is durable before snapshot work begins, and
normal sync/pull resumes it automatically. Oversqlite first reconciles pending local work. If that
work cannot be drained safely, `CheckpointRecoveryBlockedException` preserves it and reports an
actionable reason; snapshot replacement does not begin.

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

The client is in a durable rebuild-required state. Ordinary sync and pull automatically resume
checkpoint recovery; explicit `rebuild()` remains available.

### `CheckpointRecoveryBlockedException`

Checkpoint recovery found pending offline work it could not safely upload. The exception reports a
normalized reason and counts; local rows, outbox state, checkpoint, and recovery gate remain
durable for retry or operator action.

### `SourceRecoveryRequiredException`

The current source stream is stale or out-of-order. Ordinary sync is blocked until explicit
`rebuild()` succeeds and oversqlite performs managed rotated recovery internally.

### `SyncTableContractMismatchException`

The server and client disagree about synchronized tables or their sync keys. Correct the generated
client configuration or server registration before retrying. Automatic downloads treat this as a
terminal compatibility failure instead of polling indefinitely.

### `OversqliteRemoteContractException`

A validated server contract error retains its `code`, `httpStatus`, `operation`, `remediation`,
`retryable` flag, and optional discovery `generationId`. Non-retryable errors stop automatic
downloads; retryable capacity errors retain transport retry behavior. Malformed error envelopes
fail protocol validation.

The SDK owns source and generation headers. A default header may match the exact ordinary request
identity once; conflicting or duplicate values fail before dispatch. Discovery rejects generation
proof defaults because it discovers that proof.
## Explicit conflict resolution

The default resolver defers conflicts and preserves the frozen outbound request. A failed
`pushPending()` or `sync()` reports `ConflictNeedsResolutionException`; inspect the durable handle
with `pendingConflict()`, then call `resolveConflict(handle, result)` or
`retryConflict(handle)`. Server-wins and client-wins behavior remain available only when selected
explicitly. A handle becomes stale after a newer local edit and is rejected without mutation.

Oversqlite does not follow HTTP redirects. Redirect responses are returned as bounded HTTP errors,
so authenticated source and generation-proof headers are never replayed to a redirect target.
