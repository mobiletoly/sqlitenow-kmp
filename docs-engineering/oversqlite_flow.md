# SQLiteNow KMP Library – Oversqlite Runtime Flow

This document is the engineering reference for the current oversqlite client architecture in the
SQLiteNow KMP runtime.

It focuses on the managed-source client model, persistent local state, and the current push/pull
and rebuild flows.

## 1. Core Model

Oversqlite is a single-schema, bundle-based sync client.

Key ideas:

- the server is authoritative for committed history
- local writes are captured into `_sync_dirty_rows` by triggers
- upload does not stream dirty rows directly; it first freezes them into `_sync_outbox_*`
- accepted uploads are replayed locally from the committed bundle returned by the server
- pull applies committed bundles forward from `last_bundle_seq_seen`
- rebuild recovers local state from a full server snapshot when authoritative history can no longer
  be applied incrementally
- the current `sourceId` is internally managed and persisted in local sync metadata

## 2. Local Metadata Tables

The current runtime uses these private tables.

### 2.1 `_sync_attachment_state`

Tracks the current local lifecycle binding.

Important fields:

- `current_source_id`: current internally managed sync writer identity
- `binding_state`: anonymous vs attached
- `attached_user_id`: current attached account scope
- `last_bundle_seq_seen`: highest authoritative remote bundle durably applied
- `rebuild_required`: generic keep-source rebuild gate
- `pending_initialization_id`: active initialization lease when first local seed is in progress

### 2.2 `_sync_operation_state`

Tracks durable multi-step lifecycle or recovery operations.

Current kinds:

- `none`
- `remote_replace`
- `source_recovery`

For `source_recovery`, the table also preserves:

- the recovery reason
- the reserved replacement source id
- the staged replacement snapshot, once the server has accepted the replacement

Pending intent itself stays with its ordinary owners while recovery is required: `_sync_dirty_rows`,
`_sync_outbox_*` and `_sync_upload_journal`.

### 2.3 `_sync_source_state`

Tracks per-source sequencing and lineage.

Important fields:

- `source_id`
- `next_source_bundle_id`
- `replaced_by_source_id`

### 2.4 `_sync_row_state`

Tracks authoritative row metadata for managed rows already known locally.

### 2.5 `_sync_dirty_rows`

Captures local user-originated intent via triggers.

### 2.6 `_sync_outbox_bundle` and `_sync_outbox_rows`

Frozen outbound snapshot for one logical local source bundle.

These tables are also the durable preservation form for unsynced local intent during
source-recovery-required rebuild-plus-rotate flows.

### 2.7 `_sync_snapshot_stage`

Temporary storage for full snapshot rows during rebuild.

## 3. Trigger Capture Model

Bootstrap installs one trigger set per managed table:

- `AFTER INSERT`
- `AFTER UPDATE`
- `AFTER DELETE`

Trigger rules:

- guarded by `apply_mode = 0`
- write directly into `_sync_dirty_rows`
- coalesce repeated changes for the same row key
- preserve the original `base_row_version`
- maintain `dirty_ordinal` so upload order is stable
- represent row identity as canonical `key_json`

## 4. Startup And Identity Model

`open()` performs:

1. create or repair metadata tables
2. validate sync table configuration against the live schema
3. validate FK closure and compute table ordering
4. restore or create the current internal `sourceId`
5. install triggers for managed tables

`open()` also sets `PRAGMA synchronous = FULL` on the client connection and fails if the driver
does not honor it. Freeze, settlement, acknowledgement and checkpoint commits are protocol state
the server records too; a commit lost to power failure after the server accepted the matching
request cannot be replayed exactly, so every commit must reach storage.

`attach(userId)` against a scope that is already authoritative refuses with
`AttachResult.BlockedLocalPendingData` while the anonymous database holds dirty rows, a frozen
outbox or an upload journal. Anonymous data is seeded only into an uninitialized scope; it is never
merged into existing server data and never discarded implicitly. `discardAnonymousLocalData()` is
the explicit destructive exit: it clears managed tables, `_sync_row_state`, `_sync_dirty_rows` and
the outbox of an anonymous database and keeps the current `sourceId`.

Important identity rules:

- app code does not provide `sourceId`
- the current `sourceId` is opaque and persisted internally
- the current `sourceId` may rotate during explicit recovery on the same install
- the current `sourceId` also rotates after successful destructive detach
- the current `sourceId` is not the same concept as a product/backend `deviceId`

## 5. High-Level Client Operations

### 5.1 `pushPending()`

Purpose:

- freeze current dirty rows into one outbound source bundle
- encode and send that bundle as one compact `POST /sync/push`
- validate and expand the compact authoritative response
- apply that response atomically before clearing the prepared outbox

Key rule:

- if durable rebuild or source-recovery state is active, ordinary push fails closed

### 5.2 `pullToStable()`

Purpose:

- pull authoritative committed bundles until the current stable bundle sequence is fully applied
  locally

Key rules:

- if frozen outbox or dirty rows block incremental pull, fail closed
- if pull-side retained history is pruned, fall back to keep-source rebuild
- if source-recovery-required state is active, ordinary pull fails closed

### 5.3 `sync()`

The interactive happy path:

1. `pushPending()`
2. `pullToStable()`

### 5.4 Automatic Downloads

Automatic downloads are explicit and default-off. They never start from `open()` or
`attach(userId)`.

When started, the worker performs download work only:

- polling mode calls `pullToStable()` on the configured interval
- watch mode uses `watch_enabled` from the immutable admitted discovery contract
- watch mode opens `GET /sync/watch?after_bundle_seq=<last_bundle_seq_seen>` with the current
  `Oversync-Source-ID`
- bundle events are metadata-only wake-up hints; the event payload is not applied directly
- every remote data change still flows through `pullToStable()`
- active watch streams also keep a fallback polling interval, so watch failures do not permanently
  stall downloads

Pause behavior is split deliberately:

- `pauseDownloads()` suppresses automatic polling, watch opens, watch-event pulls, and fallback
  pulls
- explicit `pullToStable()` still runs while automatic downloads are paused

Cancellation behavior:

- KMP callers own the coroutine running `runAutomaticDownloads(...)`
- cancellation/stop must release open watch streams and reconnect sleeps promptly

### 5.5 `rebuild()`

`rebuild()` is the explicit recovery entry point.

Oversqlite chooses the internal mode:

- generic rebuild-required and pull-side pruning use keep-source rebuild
- direct-push replay pruning below retained floor uses keep-source rebuild plus local source
  floor advance
- create-time stale/out-of-order source failures and push-time source-sequence changes use
  rebuild-plus-rotate recovery

Only the rebuild-plus-rotate mode may start with pending intent. It never strands or drops that
intent; section 8 describes how it is carried. Every other mode keeps the strict clean gate.

### 5.6 `detach()` / `syncThenDetach()`

Successful destructive detach:

1. clears managed local account state
2. creates a fresh internal `sourceId`
3. records source lineage locally
4. persists anonymous state bound to that fresh source

Blocked detach, rolled-back detach, and pending `remote_replace` cancellation preserve the current
source.

## 6. Server Protocol

The current client talks to these endpoints:

### Discovery and connect

- `GET /sync/generations/{generationId}`
- `POST /sync/connect`

### Push

- `POST /sync/push`

### Pull

- `GET /sync/pull`

### Watch

- `GET /sync/watch`

Watch is optional server support. It is a latency optimization for waking the automatic download
worker; `/sync/pull` remains the authoritative data path.

### Snapshot rebuild

- `POST /sync/snapshot-sessions`
- `GET /sync/snapshot-sessions/{snapshotId}`
- `DELETE /sync/snapshot-sessions/{snapshotId}`

The current source identity is sent as `Oversync-Source-ID` on authenticated sync requests. It is
managed internal sync transport metadata rather than request-body identity input.

## 7. Authoritative Apply Rules

Authoritative rows from pull, committed bundle replay, or snapshot rebuild are applied with
`apply_mode = 1`.

Row application rules:

- `INSERT` / `UPDATE`: upsert full payload into the business table, then update `_sync_row_state`
- `DELETE`: delete the business row by PK, then update `_sync_row_state` as deleted
- stale incoming rows are skipped when local `_sync_row_state.row_version >= incoming.row_version`

Push settlement has a metadata-only acknowledgement path when every committed
INSERT/UPDATE has an `AcceptAuthoritative` decision and matches its accepted live
row under the typed payload codec, all committed effects are covered, and affected
tables have no application or TEMP triggers.
It records authoritative baselines and advances source/checkpoint/outbox state
without rewriting business rows or building the database-wide survivor edge index.
Root dependency checks, workspace/work admission, final FK validation and atomic
rollback remain in effect. Other pending rows retain their payloads, historical
baselines, provenance, desired relationships and physical exceptions through the
existing bounded preservation passes. These passes still cost work proportional
to pending intent; final FK validation can also grow with database size.
Edits to an uploaded row, normalized values, deletes, additional unplanned effects
and application triggers use ordinary survivor preparation.
The preparation unit binds this strategy so eligibility changes invalidate an
incompatible saved plan. Pull, conflict and snapshot behavior is unchanged.

Push settlement, incremental pull, conflict resolution and snapshot installation
borrow their writer's statement cache for survivor cleanup:
the two paged DELETE statements, their immediate `SELECT changes()` probes and
the final plan DELETE reuse handles within that writer. Cleanup retains 128-row
pages, terminal probes and cancellation checks. The enclosing writer releases
and closes the cache before commit. Cleanup borrows the cache and never closes it
or retains a handle across writers; the helper's uncached default remains available.

Pull and conflict survivor preparation also borrow that cache for workspace checks
and replay traversal, including version-skipped pull roots and newer conflict intent.
Pull restoration and final survivor verification use the same writer cache.
Snapshot traversal and conflict restoration already use it. Each resumed writer
creates fresh handles and repeats the existing context guards. Wire sizing and
dependency-validation internals retain their existing defaults. This reduces
preparations while preserving every scan, SQL execution and work charge; global
relationship discovery still grows with the managed row and relationship population.

Within a consecutive dependency-indexing phase, a writer may also retain the
non-apply workspace accounting total. Every admission still combines that total
with the current apply rows, edges, cursor and temporary allocation. This optional
holder costs two entries and256bytes; insufficient initial or later capacity
disables retention for the rest of the run and uses fresh accounting instead.
Main or TEMP triggers on the internal edge/plan tables also disable retention.
The holder ends before walking or other phases and never crosses a writer,
continuation or reopen. Other phases and raw callers retain fresh accounting.
This removes repeated metadata reads, while relationship scans and work charges
remain unchanged.

Pull indexes only the relationships its dependency walk can read. The walk looks a
relationship up by a parent row that is already in the plan, and a plan row is either
a pulled row or a child found through such a lookup. The pulled rows' tables, plus
every table that holds a foreign key to a reached table, therefore bound the parents
that matter. Relationships whose parent table is outside that set are not visited:
a pull of rows that nothing references indexes nothing, and its preparation does not
grow with unrelated tables. A pull that touches a referenced table still indexes
every row of the tables below it. The pulled tables are known only to the writer
that seeded every root; a plan resumed by a later writer indexes every relationship,
as before. Settlement, conflict resolution and snapshot installation always index
every relationship.

The unique-cycle working set (the transfer arcs, their reachability, the vacated
component and its foreign-key closure) is admitted before business mutation only
when planning recorded at least one unique transfer edge. Without one, no row waits
on another, no component is vacated and snapshot preservation has no unresolved rows
to close over, so every plan kind, snapshot installation included, skips that charge.
A snapshot still runs its unique-ordered install loop, whose work is reserved at
admission; its per-row captures and final verification check their own charges.
With unique transfer edges the existing charge applies unchanged.

## 8. Recovery And Intent Preservation

The most important recovery invariant is:

- frozen `_sync_outbox_*` is durable unsynced intent

For source-recovery-required cases the server refuses the current source, so nothing can settle
under it. Pending intent is every dirty row plus every frozen or rejected outbox row that no newer
dirty row replaced. Explicit `rebuild()` is the exit:

1. ordinary sync fails closed and the guard triggers refuse application writes
2. `rebuild()` first gives the old source its ordinary chance: a frozen upload runs resolve, apply
   and ack exactly as `pushPending()` would, and a pending acknowledgment is sent
3. a terminal refusal of the source leaves the frozen request in place; the pinned snapshot guard
   fingerprints the dirty count and outbox so nothing can change them while the snapshot is staged
4. the snapshot is requested with `source_replacement`; the server admits it only while the old
   source holds no unacknowledged outcome, which proves the frozen request was never applied
5. the final writer installs and verifies the snapshot unchanged, then requeues each pending edit
   into `_sync_dirty_rows`, writes the local value back to the business row, and deletes the
   retired source's `_sync_upload_journal` and `_sync_outbox_*` rows. A `DELETE` of a row the
   snapshot lacks is already satisfied. An edit made from the server state the snapshot still holds
   is rebased onto the installed row version (`UPDATE`/`DELETE` when the snapshot holds the row,
   `INSERT` when it does not). An edit whose row another source updated or deleted, or whose key
   another source created, keeps its original operation and base version instead
6. oversqlite rotates to the replacement `sourceId` in that same transaction
7. later `pushPending()` freezes the carried intent as `source_bundle_id = 1` under the new source

An edit that kept its original base is refused by the server as an ordinary `push_conflict`, so
the configured resolver decides. Rebasing it would upload it over the other source's change with no
decision. The frozen request is not rebound in place, because its encoded body and canonical hash
would change. `OversqliteSourceRecoveryIntent` owns steps 5 and the explicit discard.

If a requeued edit violates a foreign key or uniqueness constraint against the snapshot, or a stale
edit has no valid original base left to keep, the final transaction rolls back and `rebuild()` fails with `PendingIntentUnsatisfiableException`. Intent, the
staged snapshot and the recovery requirement stay intact. `discardPendingIntentForRecovery()` is
the only discard: it runs on explicit request, returns the dropped rows, and while an upload journal
exists requires the staged replacement snapshot from step 4.

For committed-remote replay pruning below the retained floor:

- keep the current source
- rebuild from snapshot
- advance local sequencing past the already-committed tuple
- clear committed-remote outbox state only after recovery succeeds

## 9. Engineering Invariants

- one oversqlite client instance should own a local database at a time
- sync serialization is enforced per client instance
- `pushPending()` freezes transport state before upload
- `pullToStable()` must not run while outbound replay is pending
- `rebuild()` is the only explicit recovery entry point
- business-table triggers should never fire while authoritative rows are being applied
- `last_bundle_seq_seen` advances only after authoritative replay succeeds locally
- pull rejects a non-contiguous bundle sequence before applying anything, so the checkpoint can
  never skip a committed bundle even if the server misbehaves
- a kept local UPDATE that conflicts with a remote DELETE retries as an INSERT whose base is the
  tombstone version; `KeepLocal` and `KeepMerged` never have to drop an edit because of a remote
  delete
- source-recovery-required state survives restart until explicit rebuild succeeds
- source recovery never strands or silently drops pending intent: `rebuild()` carries it onto the
  installed snapshot, and only `discardPendingIntentForRecovery()` may drop it
- a carried edit never overwrites a change another source made while this source was refused: it
  keeps its original base, and the server answers its upload with an ordinary conflict
- the retired source's frozen request and acknowledgment obligation are released only inside the
  final writer, after the server accepted the replacement source

## 10. Related Documents

- conflict evolution and resolver outcomes:
  [`docs/sync/core-concepts.md#conflict-resolution`](/Users/pochkin/Projects/my/sqlitenow-kmp/docs/sync/core-concepts.md#conflict-resolution)
- source pruning and recovery behavior:
  [`docs/sync/core-concepts.md#rebuild-and-recovery`](/Users/pochkin/Projects/my/sqlitenow-kmp/docs/sync/core-concepts.md#rebuild-and-recovery)
- public user-facing sync docs live under [`docs/sync/`](/Users/pochkin/Projects/my/sqlitenow-kmp/docs/sync)
