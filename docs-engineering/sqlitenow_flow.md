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
- the rejected source id
- the rejected source bundle id
- whether frozen unsynced intent still lives in `_sync_outbox_*`

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

Important identity rules:

- app code does not provide `sourceId`
- the current `sourceId` is opaque and persisted internally
- the current `sourceId` may rotate during explicit recovery on the same install
- the current `sourceId` is not the same concept as a product/backend `deviceId`

## 5. High-Level Client Operations

### 5.1 `pushPending()`

Purpose:

- freeze current dirty rows into one outbound source bundle
- upload that bundle through push sessions
- fetch the committed authoritative bundle back from the server
- replay that authoritative bundle locally

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

### 5.4 `rebuild()`

`rebuild()` is the explicit recovery entry point.

Oversqlite chooses the internal mode:

- generic rebuild-required and pull-side pruning use keep-source rebuild
- committed-remote replay pruning below retained floor uses keep-source rebuild plus local source
  floor advance
- create-time stale/out-of-order source failures and commit-time source-sequence changes use
  rebuild-plus-rotate recovery

## 6. Server Protocol

The current client talks to these endpoints:

### Push

- `POST /sync/push-sessions`
- `POST /sync/push-sessions/{pushId}/chunks`
- `POST /sync/push-sessions/{pushId}/commit`
- `DELETE /sync/push-sessions/{pushId}`

### Committed bundle fetch

- `GET /sync/committed-bundles/{bundleSeq}/rows`

### Pull

- `GET /sync/pull`

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

## 8. Recovery And Intent Preservation

The most important recovery invariant is:

- frozen `_sync_outbox_*` is durable unsynced intent

For source-recovery-required cases:

1. oversqlite freezes local intent into `_sync_outbox_*`
2. ordinary sync fails closed
3. explicit `rebuild()` stages and applies the authoritative snapshot
4. oversqlite rotates to a fresh internal `sourceId`
5. oversqlite rebinds `_sync_outbox_*` to that fresh source with `source_bundle_id = 1`
6. later `pushPending()` resumes upload under the new source stream

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
- source-recovery-required state survives restart until explicit rebuild succeeds

## 10. Related Documents

- conflict evolution and resolver model:
  [`specs/oversqlite_conflict_resolution_results.md`](/Users/pochkin/Projects/my/sqlitenow-kmp/specs/oversqlite_conflict_resolution_results.md)
- source pruning and recovery contract:
  [`specs/oversqlite-v1-pruning-and-source-recovery.md`](/Users/pochkin/Projects/my/sqlitenow-kmp/specs/oversqlite-v1-pruning-and-source-recovery.md)
- public user-facing sync docs live under [`docs/sync/`](/Users/pochkin/Projects/my/sqlitenow-kmp/docs/sync)
