# SQLiteNow KMP Library – Bundle-Era Oversqlite Flow

This document describes the bundle-era oversqlite client architecture used by the SQLiteNow KMP
runtime. It is intended as the long-lived engineering reference for how the client currently
bootstraps, tracks local state, talks to the real server, and rebuilds from authoritative history.

The detailed next-step conflict-resolution evolution is specified in
[`specs/oversqlite_conflict_resolution_results.md`](/Users/pochkin/Projects/my/sqlitenow-kmp/specs/oversqlite_conflict_resolution_results.md).
This document assumes that direction when describing conflict handling, but focuses primarily on the
core runtime flow and persistent local state.

## 1. Core Model

Oversqlite is a single-schema, bundle-based sync client.

Key ideas:

- The server is authoritative for committed history.
- Local writes are captured into `_sync_dirty_rows` by triggers.
- Upload does not stream dirty rows directly. It first freezes them into `_sync_push_outbound`.
- Accepted uploads are replayed locally from the committed bundle returned by the server.
- Pull applies committed bundles forward from `last_bundle_seq_seen`.
- Hydrate/recover rebuild local state from a full server snapshot when needed.

The runtime assumes:

- one logical schema per local database (`OversqliteConfig.schema`)
- exactly one sync key column per managed table in the current client runtime
- managed tables are FK-closed and validated during bootstrap

## 2. Local Metadata Tables

The current runtime uses these private tables.

### 2.1 `_sync_client_state`

Tracks client identity and high-level sync cursor state per user.

```sql
CREATE TABLE IF NOT EXISTS _sync_client_state (
  user_id TEXT NOT NULL PRIMARY KEY,
  source_id TEXT NOT NULL,
  schema_name TEXT NOT NULL DEFAULT '',
  next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
  last_bundle_seq_seen INTEGER NOT NULL DEFAULT 0,
  apply_mode INTEGER NOT NULL DEFAULT 0,
  rebuild_required INTEGER NOT NULL DEFAULT 0
)
```

Important fields:

- `source_id`: device/app-install identity
- `next_source_bundle_id`: next local outbound bundle ordinal
- `last_bundle_seq_seen`: highest authoritative remote bundle durably applied
- `apply_mode`: disables triggers while authoritative rows are being applied
- `rebuild_required`: set when local identity is rotated or local state must be rebuilt

### 2.2 `_sync_row_state`

Tracks authoritative row metadata for managed rows already known locally.

```sql
CREATE TABLE IF NOT EXISTS _sync_row_state (
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  key_json TEXT NOT NULL,
  row_version INTEGER NOT NULL DEFAULT 0,
  deleted INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (schema_name, table_name, key_json)
)
```

This is the current local view of server row version / tombstone state, not a queue.

### 2.3 `_sync_dirty_rows`

Captures local user-originated intent via triggers.

```sql
CREATE TABLE IF NOT EXISTS _sync_dirty_rows (
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  key_json TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
  base_row_version INTEGER NOT NULL DEFAULT 0,
  payload TEXT,
  dirty_ordinal INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
  PRIMARY KEY (schema_name, table_name, key_json)
)
```

This replaces the older `_sync_pending` model described in previous docs.

### 2.4 `_sync_push_outbound`

Frozen outbound snapshot for one logical local source bundle.

```sql
CREATE TABLE IF NOT EXISTS _sync_push_outbound (
  source_bundle_id INTEGER NOT NULL,
  row_ordinal INTEGER NOT NULL,
  schema_name TEXT NOT NULL,
  table_name TEXT NOT NULL,
  key_json TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
  base_row_version INTEGER NOT NULL DEFAULT 0,
  payload TEXT,
  PRIMARY KEY (source_bundle_id, row_ordinal)
)
```

If upload conflicts after freezing, this table is the source of truth for replay / conflict
resolution.

### 2.5 `_sync_push_stage`

Temporary storage for authoritative committed bundle rows fetched back from the server after upload.

### 2.6 `_sync_snapshot_stage`

Temporary storage for full snapshot rows during hydrate/recover.

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

Important consequences:

- there is no separate “assign IDs later” queue anymore
- local writes are stored as row intent, not transport records
- PK-changing updates are modeled as delete-old-key plus insert/update-new-key

## 4. Bootstrap and Identity Binding

`bootstrap(userId, sourceId)` performs:

1. create metadata tables if needed
2. validate sync table configuration against the live schema
3. validate FK closure and compute table ordering
4. persist or reconcile client identity in `_sync_client_state`
5. install triggers for managed tables

Identity behavior matters:

- same user + same source id: continue normally
- same user + different source id: managed tables are cleared, `rebuild_required = 1`, and the new
  identity is persisted
- different user on same database: managed state is cleared and client identity is rebound

This is why `recover()` may rotate source identity as part of rebuilding.

## 5. High-Level Client Operations

### 5.1 `pushPending()`

Purpose:

- freeze current dirty rows into one outbound source bundle
- upload that bundle through push sessions
- fetch the committed authoritative bundle back from the server
- replay that authoritative bundle locally

Flow:

1. reject if `rebuild_required = 1`
2. if `_sync_push_outbound` already exists, reuse it
3. otherwise collect `_sync_dirty_rows`, freeze them into `_sync_push_outbound`, and delete the
   corresponding dirty rows
4. create push session
5. upload rows in chunks
6. commit push session
7. fetch committed bundle rows from `/sync/committed-bundles/{bundleSeq}/rows`
8. stage them in `_sync_push_stage`
9. apply staged authoritative rows locally
10. advance `last_bundle_seq_seen`
11. delete `_sync_push_outbound` and `_sync_push_stage`

### 5.2 `pullToStable()`

Purpose:

- pull authoritative committed bundles until the current server stable bundle sequence is fully
  applied locally

Flow:

1. reject if `_sync_push_outbound` is non-empty
2. reject if `_sync_dirty_rows` is non-empty
3. request `/sync/pull?after_bundle_seq=...&max_bundles=...`
4. apply returned bundles in order
5. stop only when `last_bundle_seq_seen >= stable_bundle_seq`
6. if server history is pruned, fall back to snapshot rebuild

### 5.3 `sync()`

The interactive happy path:

1. `pushPending()`
2. `pullToStable()`

### 5.4 `hydrate()`

Purpose:

- rebuild local managed tables from a full server snapshot without rotating source identity

Used when:

- provisioning a new follower
- local state is empty and a full rebuild is desired

### 5.5 `recover()`

Purpose:

- rebuild local managed tables from a full server snapshot and rotate local source identity

Used when:

- the current local source identity must no longer continue replaying previous outbound state
- the client was rebound or local state was invalidated

## 6. Server Protocol

The bundle-era client talks to these endpoints:

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

The client no longer uses the older `/sync/upload` and `/sync/download` transport model.

## 7. Authoritative Apply Rules

Authoritative rows from pull / committed bundle replay / snapshot rebuild are applied with
`apply_mode = 1`.

Row application rules:

- `INSERT` / `UPDATE`: upsert full payload into the business table, then update `_sync_row_state`
- `DELETE`: delete the business row by PK, then update `_sync_row_state` as deleted
- stale incoming rows are skipped when local `_sync_row_state.row_version >= incoming.row_version`

When authoritative rows are applied:

- matching dirty rows may be deleted
- table update listeners are fired after the operation completes

## 8. Conflict Handling

Current runtime:

- push commit `409 push_conflict` is decoded into a typed conflict
- `ServerWinsResolver` is auto-applied
- valid `KeepLocal` / `KeepMerged(...)` results are auto-applied and retried in the same
  `pushPending()` invocation
- invalid `KeepLocal` / `KeepMerged(...)` results fail closed with typed runtime errors after
  restoring replayable dirty state
- automatic conflict retries are bounded; retry exhaustion clears `_sync_push_outbound` and leaves
  `_sync_dirty_rows` replayable

Canonical design direction:

- use the detailed contract in
  [`specs/oversqlite_conflict_resolution_results.md`](/Users/pochkin/Projects/my/sqlitenow-kmp/specs/oversqlite_conflict_resolution_results.md)

Important current invariant:

- conflicts are bundle-scoped failures
- the client must preserve non-conflicting sibling rows from the rejected outbound bundle
- the client must remain in a replayable state after conflict resolution or retry exhaustion

## 9. Snapshot Rebuild Rules

Snapshot rebuild is authoritative and clears managed local state before replay.

Behavior:

- `hydrate()` keeps current `source_id`
- `recover()` rotates `source_id` and resets `next_source_bundle_id = 1`
- both clear `_sync_dirty_rows`, `_sync_push_outbound`, `_sync_push_stage`, and managed business
  rows before authoritative snapshot application

## 10. Ordering and FK Closure

Managed table order is derived from FK dependencies.

Why it matters:

- insert/update replay uses parent-before-child order
- delete replay uses child-before-parent order
- the client validates FK closure during bootstrap so pull/push replay can stay deterministic

Composite foreign keys are currently rejected by validation.

## 11. Current Engineering Invariants

- one oversqlite client instance should own a local database at a time
- sync serialization is enforced per client instance
- `pushPending()` freezes transport state before upload
- `pullToStable()` must not run while outbound replay is pending
- `hydrate()` and `recover()` are full rebuild operations, not incremental pulls
- business-table triggers should never fire while authoritative rows are being applied
- `last_bundle_seq_seen` advances only after authoritative replay succeeds locally

## 12. Related Documents

- Conflict evolution and next-step resolver model:
  [`specs/oversqlite_conflict_resolution_results.md`](/Users/pochkin/Projects/my/sqlitenow-kmp/specs/oversqlite_conflict_resolution_results.md)
- Public user-facing sync docs live under [`docs/sync/`](/Users/pochkin/Projects/my/sqlitenow-kmp/docs/sync)
