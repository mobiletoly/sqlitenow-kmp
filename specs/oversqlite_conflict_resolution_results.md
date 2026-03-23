# Oversqlite Conflict Resolution Results

Status: proposed

Implementation readiness note:

- this spec is ready to drive implementation only together with Phase 1
- specifically, `docs-engineering/sqlitenow_flow.md` must be updated before or alongside code
  changes so the repo does not carry conflicting engineering guidance

## Summary

This document defines the next-step conflict resolution contract for Oversqlite now that the server
returns structured push-conflict data and `ServerWinsResolver` is supported automatically.

The main decisions are:

- keep `MergeResult.AcceptServer`
- replace overloaded `MergeResult.KeepLocal(mergedPayload)` with two explicit outcomes:
  - `MergeResult.KeepLocal`
  - `MergeResult.KeepMerged(mergedPayload)`
- add a built-in `ClientWinsResolver` that returns `KeepLocal`
- replace the positional resolver callback arguments with a structured `ConflictContext`
- bound automatic conflict-resolution retries so `pushPending()` always terminates deterministically

This separates three distinct meanings:

- accept the authoritative server row
- retry the original local intent unchanged
- retry a new merged row payload produced by custom resolver logic

## Problem

The current `MergeResult.KeepLocal(mergedPayload)` shape mixes two different concepts:

- "retry my original local change"
- "I computed a new merged row; retry that instead"

That is ambiguous for both API users and runtime implementation.

Examples:

- for a stale `UPDATE`, "keep local" could mean retry the original local row unchanged
- for a domain merge, the resolver may want to combine server and local fields and retry the merged
  result
- for a local `DELETE`, there is no row payload to pass at all, so a payload-based `KeepLocal(...)`
  is awkward

The current positional resolver callback is also too weak for a bullet-proof design because it does
not expose enough context to make correct decisions about deletes, tombstones, row recreation, or
operation-specific validity.

## Goals

- make conflict outcomes explicit and unsurprising
- support automatic `ServerWinsResolver`
- support automatic client-wins retry behavior
- support custom merge resolvers that return a full merged row payload
- keep runtime behavior deterministic for `INSERT`, `UPDATE`, and `DELETE`
- make tests cover both mocked and real-server conflict paths
- ensure `pushPending()` terminates predictably under repeated conflicts

## Non-goals

- design app/UI-level conflict prompts
- add field-level merge policy DSL in this step
- support partial row patches or diffs as merge results
- support custom merged delete/tombstone payloads

## Proposed API

### Merge results

```kotlin
sealed class MergeResult {
    data object AcceptServer : MergeResult()
    data object KeepLocal : MergeResult()
    data class KeepMerged(val mergedPayload: JsonElement) : MergeResult()
}
```

### Conflict context

```kotlin
data class ConflictContext(
    val schema: String,
    val table: String,
    val key: SyncKey,
    val localOp: String,
    val localPayload: JsonElement?,
    val baseRowVersion: Long,
    val serverRowVersion: Long,
    val serverRowDeleted: Boolean,
    val serverRow: JsonElement?,
)
```

### Resolver

```kotlin
fun interface Resolver {
    fun resolve(conflict: ConflictContext): MergeResult
}
```

### Built-in resolvers

```kotlin
object ServerWinsResolver : Resolver {
    override fun resolve(conflict: ConflictContext): MergeResult =
        MergeResult.AcceptServer
}

object ClientWinsResolver : Resolver {
    override fun resolve(conflict: ConflictContext): MergeResult =
        MergeResult.KeepLocal
}
```

### Compatibility note

The existing positional callback shape may remain temporarily as adapter sugar during migration, but
`ConflictContext` is the canonical API going forward.

## Result Semantics

### 1. `AcceptServer`

Meaning:

- discard the conflicting local intent
- apply the authoritative server conflict payload locally
- clear the conflicting outbound snapshot row
- requeue any other rows from the same rejected outbound bundle

This is the current `ServerWinsResolver` behavior and remains the default policy.

### 2. `KeepLocal`

Meaning:

- preserve the original conflicting local intent from the frozen outbound snapshot
- rewrite that intent against the latest authoritative server row version
- requeue it automatically
- retry sync automatically in the same `pushPending()` flow

Important:

- `KeepLocal` uses the original local intent already captured in `_sync_push_outbound`
- it does not require a new payload from the resolver
- `KeepLocal` is the semantic primitive for automatic client-wins behavior
- `ClientWinsResolver` is only a built-in convenience that always returns `KeepLocal`
- `ClientWinsResolver` succeeds automatically only for conflict shapes where `KeepLocal` is valid

### 3. `KeepMerged(mergedPayload)`

Meaning:

- the resolver produced a complete merged row payload
- Oversqlite should write that merged payload locally
- requeue the conflicting row against the authoritative server row version
- retry sync automatically in the same `pushPending()` flow

Requirements:

- `mergedPayload` must be a complete valid row payload, not a patch
- it must include all table columns needed by local authoritative write/upsert logic
- it is only valid for row-preserving outcomes, not delete semantics

## Operation Semantics

### Local `INSERT`

#### `AcceptServer`

- apply current server row or server delete state from conflict payload
- do not retry local insert

#### `KeepLocal`

- local insert means "this row should exist with my local contents"
- if the authoritative server row currently exists, requeue as `UPDATE` using:
  - payload from the outbound snapshot
  - `base_row_version = server_row_version`
- if the authoritative server row is deleted/tombstoned, requeue as `INSERT` using:
  - payload from the outbound snapshot
  - `base_row_version = server_row_version`

Reason:

- live authoritative row -> update-on-latest-state
- tombstoned authoritative row -> recreate row explicitly as insert-on-latest-tombstone

#### `KeepMerged(mergedPayload)`

- if the authoritative server row currently exists:
  - write `mergedPayload` locally
  - requeue as `UPDATE` with `base_row_version = server_row_version`
- if the authoritative server row is deleted/tombstoned:
  - write `mergedPayload` locally
  - requeue as `INSERT` with `base_row_version = server_row_version`

### Local `UPDATE`

#### `AcceptServer`

- apply current server row or server delete state from conflict payload
- do not retry local update

#### `KeepLocal`

- only valid when the authoritative server row still exists
- keep the original local row payload from the rejected snapshot
- requeue as `UPDATE` with `base_row_version = server_row_version`

If the authoritative server row is deleted or missing, `KeepLocal` is invalid for local `UPDATE`.

Reason:

- local `UPDATE` means "modify an existing row"
- silently turning that into row recreation/resurrection is too surprising for a bullet-proof sync
  contract

#### `KeepMerged(mergedPayload)`

- only valid when the authoritative server row still exists
- write `mergedPayload` locally
- requeue as `UPDATE` with `base_row_version = server_row_version`

If the authoritative server row is deleted or missing, `KeepMerged(...)` is invalid for local
`UPDATE`.

### Local `DELETE`

#### `AcceptServer`

- accept authoritative server state
- if server says row is already deleted or missing, local delete objective is satisfied
- if server says row still exists, apply that authoritative row locally

#### `KeepLocal`

Meaning:

- local delete should win

Behavior:

- if server already reports the row as deleted or missing, consider the delete objective satisfied
  and do not requeue the conflicting row
- otherwise requeue a `DELETE` with `base_row_version = server_row_version`

#### `KeepMerged(mergedPayload)`

Invalid.

Reason:

- merged row payload is a row-preserving result
- delete conflicts should be represented by `AcceptServer` or `KeepLocal`

If a resolver returns `KeepMerged(...)` for a conflicting local `DELETE`, Oversqlite should throw an
explicit typed runtime error.

## Runtime State Machine

When `pushPending()` hits a typed `push_conflict`:

1. identify the conflicting row inside `_sync_push_outbound`
2. build `ConflictContext`
3. call `resolver.resolve(conflictContext)`
4. apply one of the three result semantics
5. delete the current outbound snapshot
6. requeue any surviving intents into `_sync_dirty_rows`
7. if dirty rows remain, create a fresh outbound snapshot and retry push automatically

Important invariants:

- `_sync_push_outbound` remains the source of truth for the rejected bundle
- conflict resolution must not partially preserve the old frozen snapshot
- after resolution, the next upload uses a fresh snapshot rebuilt from `_sync_dirty_rows`
- local business rows and `_sync_row_state` must be updated before retry
- when one row in a frozen outbound bundle conflicts, non-conflicting sibling rows must survive the
  resolution path and still eventually sync

## Retry Behavior

Automatic retry happens inside the same `pushPending()` invocation.

If the retried push conflicts again:

- Oversqlite may repeat the same resolution flow if the resolver again returns an auto-applicable
  result
- if the resolver returns an invalid result for the operation, throw immediately

Hard rule:

- Oversqlite must allow at most 2 automatic conflict-resolution retries inside one `pushPending()`
  invocation
- if a third push attempt would be required, throw a typed retry-exhausted exception

State rule on retry exhaustion:

- `_sync_push_outbound` must be cleared
- unresolved intents must remain replayable in `_sync_dirty_rows`
- local business rows and `_sync_row_state` must remain consistent with the last successfully
  applied resolution step
- the client must remain capable of a future explicit retry or explicit app-level recovery without
  requiring manual repair of internal sync tables

Reason:

- this guarantees termination
- it prevents recursive or oscillating resolver behavior from creating infinite loops
- it keeps `pushPending()` behavior deterministic
- it preserves a fail-closed but recoverable local sync state when automatic recovery gives up

## Structured Conflict Payload Usage

The resolver receives full `ConflictContext`, including:

- `schema`
- `table`
- `key`
- `localOp`
- `localPayload`
- `baseRowVersion`
- `serverRowVersion`
- `serverRowDeleted`
- `serverRow`

The runtime must not parse human-readable error messages to resolve conflicts.

## Default Policy

Default remains:

- `ServerWinsResolver`

Reason:

- it is the safest automatic policy
- it guarantees convergence without silently overwriting authoritative server state

`ClientWinsResolver` should be opt-in.

## Developer Configuration

Conflict policy is configured when constructing the Oversqlite client, whether through a generated
helper such as `newOversqliteClient(...)` or directly via `DefaultOversqliteClient`.

Examples:

```kotlin
val client = DefaultOversqliteClient(
    db = db,
    config = config,
    http = http,
    resolver = ServerWinsResolver,
    tablesUpdateListener = { ... },
)
```

```kotlin
val client = DefaultOversqliteClient(
    db = db,
    config = config,
    http = http,
    resolver = ClientWinsResolver,
    tablesUpdateListener = { ... },
)
```

Custom resolvers may return different results per table or per conflict:

```kotlin
val resolver = Resolver { conflict ->
    when (conflict.table) {
        "notes" -> MergeResult.KeepLocal
        "profiles" -> MergeResult.KeepMerged(
            mergeProfile(conflict.serverRow, conflict.localPayload)
        )
        else -> MergeResult.AcceptServer
    }
}
```

Important:

- developers choose a `Resolver` at client setup time
- `KeepLocal` and `KeepMerged(...)` are conflict-time outcomes returned by that resolver
- `KeepMerged(...)` is not a standalone top-level policy; it is an advanced custom-resolution result

## Test Matrix

### Unit / contract tests

- typed push conflict deserialization
- `AcceptServer` for insert/update/delete conflicts
- `KeepLocal` for insert/update/delete conflicts
- `KeepMerged(mergedPayload)` for update conflicts
- invalid `KeepLocal` for local update against deleted/missing authoritative row throws explicit error
- invalid `KeepMerged(...)` on delete throws explicit error
- invalid `KeepMerged(...)` for local update against deleted/missing authoritative row throws explicit error
- retry rebuilds a fresh outbound snapshot instead of reusing stale snapshot rows
- one conflicting row in a rejected bundle preserves and requeues the other rows correctly

### Cross-target integration tests

- stale writer with `ServerWinsResolver` converges to server state
- stale writer with a resolver that returns `KeepLocal` eventually pushes local state successfully
- stale writer with `ClientWinsResolver` eventually pushes local state successfully for conflict
  shapes where `KeepLocal` is valid
- custom merge resolver returns merged payload and final server row matches merged payload
- follow-up `pullToStable()` is unblocked after conflict auto-recovery
- a multi-row bundle with one conflicting row still eventually syncs the non-conflicting sibling
  rows
- retry exhaustion leaves the client in a replayable fail-closed state

### Real-server Android e2e tests

- stale writer with `ServerWinsResolver` auto-recovers to server state
- stale writer with a resolver that returns `KeepLocal` auto-retries and wins with latest local
  intent
- stale writer with `ClientWinsResolver` auto-retries and wins with latest local intent for
  conflict shapes where `KeepLocal` is valid
- custom merge resolver with `KeepMerged(...)` produces merged final row
- a multi-row rejected bundle preserves non-conflicting sibling intents through automatic recovery

## Consequences

Benefits:

- clearer resolver API
- proper built-in client-wins policy
- custom merge behavior remains possible without abusing "keep local"
- delete/update invalid cases become explicitly defined
- automatic retry semantics become bounded and deterministic

Tradeoffs:

- resolver/result API changes are source-breaking
- automatic client-wins can overwrite already-accepted remote changes
- merge payload validation becomes a runtime concern
- the compatibility path for the old resolver callback adds migration complexity

## Actionable Checklist

### Phase 1. Align specs and public API

- [x] Rewrite stale sections of `docs-engineering/sqlitenow_flow.md` so it reflects the current oversqlite architecture and this conflict model before implementation starts.
- [x] Introduce `ConflictContext` as the canonical resolver input.
- [x] Keep the old positional resolver callback only as temporary compatibility sugar, or remove it explicitly if doing a breaking API change now.
- [x] Replace `MergeResult.KeepLocal(mergedPayload)` with `KeepLocal` and `KeepMerged(mergedPayload)`.
- [x] Add built-in `ClientWinsResolver`.
- [x] Update resolver KDoc to document the new semantics, including that `ClientWinsResolver` only auto-succeeds where `KeepLocal` is valid.

### Phase 2. Implement runtime semantics

- [x] Implement automatic `KeepLocal` retry behavior in `DefaultOversqliteClient`.
- [x] Implement automatic `KeepMerged(...)` retry behavior in `DefaultOversqliteClient`.
- [x] For local `INSERT`, choose retry op based on authoritative state: use `UPDATE` when the server row exists, use `INSERT` when the server row is tombstoned.
- [x] Reject `KeepLocal` for local `UPDATE` when the authoritative server row is deleted or missing.
- [x] Reject `KeepMerged(...)` for local delete conflicts with a typed runtime error.
- [x] Reject `KeepMerged(...)` for local `UPDATE` when the authoritative server row is deleted or missing.
- [x] Add a typed retry-exhausted exception and enforce the max-2 auto-retry rule inside `pushPending()`.
- [x] On retry exhaustion, clear `_sync_push_outbound`, preserve unresolved intents in `_sync_dirty_rows`, and leave local business rows / `_sync_row_state` in a replayable fail-closed state.

### Phase 3. Documentation and developer-facing setup

- [x] Update generated/new-client API examples so developers can select `ServerWinsResolver`, `ClientWinsResolver`, or a custom `Resolver` at client construction time.
- [x] Update sync docs to explain resolver setup and the difference between top-level resolver policy and per-conflict merge results.

### Phase 4. Unit and mocked integration coverage

- [x] Add unit/contract coverage for invalid update-against-deleted-row `KeepLocal` and `KeepMerged(...)` decisions.
- [x] Add unit/contract coverage for whole-bundle preservation when exactly one row conflicts.
- [x] Add mocked cross-target integration coverage for a resolver that returns `KeepLocal`.
- [x] Add mocked cross-target integration coverage for `ClientWinsResolver`.
- [x] Add mocked cross-target integration coverage for custom merged payload resolution.
- [x] Add mocked cross-target integration coverage for whole-bundle preservation after one-row conflict.
- [x] Add mocked cross-target integration coverage for retry exhaustion leaving a replayable fail-closed state.

### Phase 5. Real-server Android e2e coverage

- [x] Add real-server Android e2e coverage for a resolver that returns `KeepLocal`.
- [x] Add real-server Android e2e coverage for `ClientWinsResolver`.
- [x] Add real-server Android e2e coverage for merged-payload resolution.
- [x] Add real-server Android e2e coverage for whole-bundle preservation after one-row conflict.
