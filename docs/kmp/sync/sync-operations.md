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

### `open()`

Prepares the local runtime and restores or creates the current internal source identity. Local-only.
Call on every launch.

### `attach(userId)`

Attaches or resumes the authenticated account scope. Required before connected sync operations.

An initial or remote attach validates the server's advertised sync-table contract before
`/sync/connect` or snapshot creation. Resuming the same durable attachment remains local-only and
network-free; the next operation that already contacts the server performs the validation.

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

If blocked, local attached state and source identity are preserved.

If successful through the destructive cleanup path, managed local state is cleared and anonymous
state is rebound to a fresh internal source identity.

### `syncThenDetach()`

Runs bounded best-effort `sync()` rounds and then attempts `detach()`.

If the final detach succeeds destructively, `syncThenDetach()` rotates to the same fresh-source
anonymous state as plain `detach()`. If it returns `BLOCKED_UNSYNCED_DATA`, the existing source is
preserved.

## Connected Sync Operations

These operations require successful `open()` and `attach(userId)`.

Before remote sync work, SQLiteNow compares the validated local schema, managed tables, and ordered
sync-key columns with `registered_table_specs` from `/sync/capabilities`. Table ordering is ignored,
but identifiers and key order must match exactly. A difference throws
`SyncTableContractMismatchException` before connect, outbox freeze, pull, or snapshot work. Its
sorted `serverOnlyTables`, `clientOnlyTables`, and `syncKeyMismatches` fields are safe for
compatibility diagnostics.

This check establishes table/key compatibility only. It is not wire-profile, projection, or
per-column payload negotiation.

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

## Numeric Wire Contract

Oversqlite uses RFC 8785 JCS plus `jcs_uniform_numeric_strings_v1`. Every synchronized SQLite
`INTEGER` and finite `REAL` business value is a canonical JSON string on push, pull, conflict,
committed replay, and snapshot surfaces. Exact decimal values remain SQLite `TEXT` and JSON
strings. Floating values use the shortest finite binary64 spelling, and negative zero becomes
`"0"`.

SQLite Boolean affinity uploads only the strict strings `"0"` and `"1"`; authoritative server
responses use JSON Booleans. Invalid signed-64, non-finite, or non-canonical numeric values fail
before row or retry-state mutation. Numeric behavior is affinity-driven and has no per-column
configuration surface.

This is a fresh-database contract. In-place migration, hash fallback, mixed-version operation, and
wire-profile negotiation are not supported.

## Automatic Downloads And Watch

Automatic downloads are optional and default-off. Starting the worker is explicit; `open()` and
`attach(userId)` never start background network work.

The worker downloads only. It never uploads local changes, clears dirty state, rebuilds
automatically, or bypasses sync recovery gates.

Polling and bundle-change watch both wake the same authoritative download path:

| Mode | What Triggers A Download | What Carries Row Data | Failure Behavior |
| --- | --- | --- | --- |
| Polling | The configured interval expires. | `pullToStable()` responses. | The next interval tries again. |
| Bundle-change watch | The server emits a `/sync/watch` bundle event. | `pullToStable()` responses. | The worker falls back to polling and later retries watch. |

When bundle-change watch is enabled and the server advertises `features.bundle_change_watch`, the
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
