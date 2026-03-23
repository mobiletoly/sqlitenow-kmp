# Oversqlite KMP Client Performance Port

Status: proposed

Implementation readiness note:

- target code is not released yet, so this work does not require migration code or compatibility shims
- correctness still matters more than micro-optimizing every call site; this spec only covers changes that
  preserve current sync semantics

## Summary

This document defines the next client-side performance pass for the KMP Oversqlite runtime. It ports the
highest-value, lowest-risk learnings from the Go client to the KMP implementation.

The work is intentionally limited to three changes:

1. stop unconditional trigger drop/recreate and extend `TableInfoCache` to carry reusable foreign-key metadata
2. add an oversqlite-local, operation-scoped statement-cache helper for hot transaction paths
3. port the `collectDirtyRowsForPush` join-plus-dirty-payload refactor

This spec does **not** change push replay semantics, wire protocol, or persisted schema.

## Problem

The current KMP client still has the pre-optimization shape in two important places:

- bootstrap performs repeated schema introspection and always recreates triggers, even when the desired
  trigger SQL is unchanged
- push snapshot construction rereads `_sync_row_state` and the live business row for every dirty row

The current runtime also prepares and closes SQLite statements repeatedly inside row loops, for example:

- staging committed push bundle rows
- staging snapshot rows
- freezing outbound push rows
- applying pulled bundles
- applying staged snapshots

This creates unnecessary prepare/finalize overhead on all targets because `SafeSQLiteConnection.prepare()`
is a thin wrapper and does not provide statement caching.

## Goals

- reduce bootstrap cost on repeated `bootstrap(userId, sourceId)` for an unchanged local schema
- remove redundant `PRAGMA foreign_key_list(...)` calls once metadata is already cached
- reduce repeated SQLite statement preparation in hot transaction loops
- reduce per-row SQL work in push snapshot construction
- preserve existing push replay, conflict, and crash-recovery behavior
- avoid any schema migration or sync protocol changes

## Non-goals

- rewrite `applyStagedPushBundle()` replay semantics
- add a process-wide or client-lifetime statement cache
- change `SafeSQLiteConnection.prepare()` semantics or add implicit prepare caching in core
- change trigger names, trigger-visible behavior, or dirty-row table layout
- redesign payload encoding, BLOB handling, or sync-key semantics
- optimize server code or transport behavior in this step

## Current Hot Spots

### 1. Bootstrap

Current bootstrap already uses `TableInfoCache` for table metadata lookup, but then issues separate FK
introspection queries again during config validation:

- `validateManagedForeignKeyClosure(...)`
- `computeManagedTableOrder(...)`

It also unconditionally drops and recreates triggers during trigger install.

Current files:

- `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/TableInfo.kt`
- `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/SyncBootstrapper.kt`

### 2. Statement preparation churn

Current hot loops repeatedly call `db.prepare(...).use { ... }` inside transactions, including:

- `stageCommittedBundleChunk(...)`
- `stageSnapshotChunk(...)`
- `ensurePushOutboundSnapshot(...)`
- `applyPulledBundle(...)`
- `applyStagedPushBundle(...)`
- `applyStagedSnapshot(...)`

Current file:

- `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/DefaultOversqliteClient.kt`

### 3. Push dirty-row collection

`collectDirtyRowsForPush(...)` currently:

- reads `_sync_dirty_rows`
- loads `_sync_row_state` separately for each dirty row
- serializes the live business row separately for each dirty row

This is the same shape that previously dominated client-side push cost in the Go implementation.

## Proposed Design

## 1. Extend `TableInfoCache` and make trigger installation idempotent

### 1.1 `TableInfo` changes

Extend `TableInfo` to carry reusable foreign-key metadata instead of only the set of FK column names.

Add:

```kotlin
data class ForeignKeyInfo(
    val seq: Int,
    val refTable: String,
    val fromCol: String,
    val toCol: String,
)
```

Update `TableInfo` to include:

```kotlin
val foreignKeys: List<ForeignKeyInfo>
```

and keep:

```kotlin
val foreignKeyColumnsLower: Set<String>
```

`TableInfoCache.get(...)` should populate both from the same `PRAGMA foreign_key_list(...)` pass.

### 1.2 Validation changes

Refactor:

- `validateManagedForeignKeyClosure(...)`
- `computeManagedTableOrder(...)`

to consume cached `TableInfo.foreignKeys` instead of issuing fresh `PRAGMA foreign_key_list(...)` calls.

Acceptable shapes:

- pass a `Map<String, TableInfo>` into both helpers
- or store a `tableInfoByName` map in `ValidatedConfig`

The chosen shape should avoid duplicate FK introspection for the same bootstrap.

### 1.3 Trigger installation changes

Current behavior:

- render desired trigger SQL
- always `DROP TRIGGER IF EXISTS ...`
- always `CREATE TRIGGER ...`

New behavior:

1. load existing trigger SQL for the table from `sqlite_master`
2. render desired SQL for each trigger
3. normalize both existing and desired SQL
4. skip recreation when normalized SQL matches
5. only `DROP + CREATE` when SQL actually differs or the trigger is missing

### 1.4 Trigger SQL normalization

Normalization must:

- trim leading/trailing whitespace
- collapse internal whitespace
- treat `CREATE TRIGGER IF NOT EXISTS ...` and `CREATE TRIGGER ...` as equivalent

Normalization must **not** rely on blanket lowercasing of the full SQL text unless the implementation is
explicitly limited to a generated trigger subset where literals and identifier case are known to be irrelevant.

The preferred implementation is:

- compare the SQL case-sensitively after whitespace normalization
- strip or normalize only the `IF NOT EXISTS` clause difference

An acceptable alternative is:

- keep `IF NOT EXISTS` in the rendered SQL and strip it during normalization
- or render canonical trigger SQL without `IF NOT EXISTS`

Either is acceptable as long as equality is stable against how SQLite stores trigger SQL in
`sqlite_master`.

### 1.5 Constraints

- trigger names must remain unchanged
- trigger bodies must remain semantically identical
- bootstrap must still repair missing or outdated triggers
- no client-state tables or business-table schema changes are allowed

## 2. Add an oversqlite-local operation-scoped statement cache

### 2.1 Scope

Add a lightweight statement-cache helper that is created for a single operation or transaction and closed
at the end of that scope.

This helper lives in the oversqlite runtime layer, not in `SafeSQLiteConnection` and not in the shared
core SQLite abstraction.

This helper must **not** be:

- global
- process-wide
- shared across unrelated sync operations
- shared across client instances
- implemented by making `SafeSQLiteConnection.prepare()` implicitly cached

The goal is only to avoid repeated `prepare()` calls within one hot path.

### 2.2 Expected shape

An acceptable API is:

```kotlin
internal class StatementCache(
    private val db: SafeSQLiteConnection,
) : AutoCloseable {
    suspend fun get(sql: String): SqliteStatement
    override fun close()
}
```

or a small helper with equivalent semantics under
`library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/`.

Required properties:

- cache key is the exact SQL string
- repeated lookups return the same prepared statement object for the life of the cache
- `get(sql)` returns a statement that is ready for a fresh bind/execute cycle
- `close()` closes every cached statement
- callers remain responsible for rebinding parameters before every execution

Statement lifecycle contract:

- `StatementCache` owns statement reuse safety
- before returning a cached statement, the cache must ensure the statement has been reset to the start state and
  prior bindings have been cleared
- this contract is implementable with the current KMP bindings because `SqliteStatement` already exposes
  `reset()` and `clearBindings()` on all targets
- call sites must not call `.use {}` on cached statements
- call sites must not close cached statements directly
- cached statements may only be reused sequentially within one operation scope; the cache must not hand out the
  same statement to overlapping consumers before the previous use is finished
- if a statement execution fails, the cache must still leave that statement in a reusable state before the next
  `get(sql)` or close it and replace it on the next lookup

### 2.3 Initial call sites

Apply the cache to the hottest transaction loops first:

- `stageCommittedBundleChunk(...)`
- `stageSnapshotChunk(...)`
- `ensurePushOutboundSnapshot(...)` / freeze insert-delete loop
- `applyPulledBundle(...)`
- `applyStagedPushBundle(...)`
- `applyStagedSnapshot(...)`

If needed for cleaner code, the cache may also be threaded through helper methods such as:

- dirty-row deletion
- structured-row-state updates
- requeue helpers
- authoritative apply helpers

### 2.4 Constraints

- do not change transaction boundaries
- do not change statement SQL text or bind order
- ensure cached statements are always closed on both success and failure
- do not use Kotlin `.use {}` with cached statements; cache lifetime and statement finalization are owned by the
  cache scope
- keep statement-cache lifetime to one operation or transaction on one `SafeSQLiteConnection`
- avoid caching dynamically generated SQL across tables unless the exact SQL string is reused safely

## 3. Port the `collectDirtyRowsForPush` refactor

### 3.1 Current behavior

`collectDirtyRowsForPush(...)` currently:

1. loads `_sync_dirty_rows`
2. decodes the key for each row
3. loads `_sync_row_state` for each row
4. serializes the live business row for each row
5. derives the outbound op from the live row and row-state

### 3.2 New behavior

Replace the initial dirty-row query with a single joined scan:

```sql
SELECT
  d.schema_name,
  d.table_name,
  d.key_json,
  d.base_row_version,
  d.payload,
  d.dirty_ordinal,
  CASE WHEN rs.key_json IS NULL THEN 0 ELSE 1 END AS state_exists,
  COALESCE(rs.deleted, 0) AS state_deleted
FROM _sync_dirty_rows AS d
LEFT JOIN _sync_row_state AS rs
  ON rs.schema_name = d.schema_name
 AND rs.table_name = d.table_name
 AND rs.key_json = d.key_json
ORDER BY d.dirty_ordinal, d.table_name, d.key_json
```

Then derive outbound rows from `_sync_dirty_rows.payload` plus the joined row-state:

- `payload != null && state_exists && !state_deleted` -> `UPDATE`
- `payload != null && (!state_exists || state_deleted)` -> `INSERT`
- `payload == null && state_exists && !state_deleted` -> `DELETE`
- `payload == null && (!state_exists || state_deleted)` -> no-op dirty key

### 3.3 Important semantic boundary

This change is limited to outbound snapshot construction.

It must **not** change:

- `applyStagedPushBundle(...)`
- replay/requeue decision logic
- conflict resolution logic
- authoritative bundle apply behavior

### 3.4 No-op handling

No-op dirty rows must continue to be deleted when freezing the outbound snapshot.

The freeze path should still:

- clear no-op dirty rows
- insert outbound rows into `_sync_push_outbound`
- delete transferred dirty rows from `_sync_dirty_rows`
- preserve the existing row ordering rules

### 3.5 Constraints

- do not change payload serialization format
- do not change `processPayloadForUpload(...)`
- do not change `DirtyRowCapture` ordering behavior
- preserve current upload chunk ordering across inserts/updates/deletes

## Implementation Order

Implement in this order:

1. `TableInfo` foreign-key metadata + bootstrap validation refactor
2. trigger SQL comparison + idempotent trigger installation
3. statement cache helper + highest-value loop call sites
4. `collectDirtyRowsForPush(...)` joined scan and dirty-payload op derivation

Rationale:

- bootstrap and trigger idempotency are low-risk and can stand alone
- statement caching is mostly mechanical and should be validated before semantic-adjacent push work
- dirty-row collection is the highest-value push change, but should come after the supporting cleanup

## Actionable Checklist

### Spec alignment

- [ ] Keep the statement cache inside the oversqlite package. Do not add implicit caching to
      `SafeSQLiteConnection.prepare()`.
- [ ] Keep replay semantics out of scope. Do not modify `applyStagedPushBundle(...)` decision rules as part of
      this spec.

### 1. Table metadata and trigger idempotency

- [ ] Update `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/TableInfo.kt`:
      add `ForeignKeyInfo`, add `TableInfo.foreignKeys`, and populate it from the existing
      `PRAGMA foreign_key_list(...)` pass.
- [ ] Update `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/SyncBootstrapper.kt`:
      carry a `tableInfoByName` map or equivalent through config validation so FK closure and table ordering can
      reuse cached metadata.
- [ ] Refactor `validateManagedForeignKeyClosure(...)` to consume cached FK metadata instead of calling
      `PRAGMA foreign_key_list(...)` again.
- [ ] Refactor `computeManagedTableOrder(...)` to consume cached FK metadata instead of calling
      `PRAGMA foreign_key_list(...)` again.
- [ ] Add helper(s) in `SyncBootstrapper.kt` to read existing trigger SQL from `sqlite_master`.
- [ ] Add helper(s) in `SyncBootstrapper.kt` to normalize trigger SQL for comparison.
- [ ] Change trigger installation to:
      load existing SQL, compare normalized SQL, skip identical triggers, and only `DROP + CREATE` when needed.
- [ ] Preserve trigger names and trigger body behavior exactly.

### 2. Oversqlite-local statement cache

- [ ] Add a new helper file such as
      `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/StatementCache.kt`.
- [ ] Implement exact-SQL lookup and per-cache statement reuse.
- [ ] Make `StatementCache.get(sql)` responsible for returning a statement that has already been reset and had prior
      bindings cleared.
- [ ] Do not use `.use {}` at cached call sites; switch those paths to explicit cache-owned statement lifetime.
- [ ] Ensure one cached statement instance is never reused concurrently or re-entrantly before its current
      caller finishes stepping/binding.
- [ ] Ensure every cached statement is closed when the operation ends, including failure paths.
- [ ] Wire the cache into `stageCommittedBundleChunk(...)`.
- [ ] Wire the cache into `stageSnapshotChunk(...)`.
- [ ] Wire the cache into outbound snapshot freeze in `ensurePushOutboundSnapshot(...)`.
- [ ] Wire the cache into `applyPulledBundle(...)`.
- [ ] Wire the cache into `applyStagedPushBundle(...)` for statement reuse only. Do not change replay branches,
      decision rules, or requeue behavior.
- [ ] Wire the cache into `applyStagedSnapshot(...)`.
- [ ] Only thread the cache into helpers such as dirty-row deletion and row-state updates when it reduces repeated
      prepares without changing SQL or transaction boundaries.

### 3. Push dirty-row collection

- [ ] Update `collectDirtyRowsForPush(...)` in
      `library/src/commonMain/kotlin/dev/goquick/sqlitenow/oversqlite/DefaultOversqliteClient.kt`
      to query `_sync_dirty_rows` with a `LEFT JOIN` to `_sync_row_state`.
- [ ] Include `payload`, `state_exists`, and `state_deleted` in the initial scan result.
- [ ] Remove per-row `loadStructuredRowState(...)` calls from `collectDirtyRowsForPush(...)`.
- [ ] Remove per-row `serializeExistingRow(...)` calls from `collectDirtyRowsForPush(...)`.
- [ ] Derive outbound op from dirty payload plus row-state:
      payload+live-state -> `UPDATE`, payload+no-live-state -> `INSERT`, null-payload+live-state -> `DELETE`,
      null-payload+no-live-state -> no-op.
- [ ] Preserve current outbound sort order across upserts and deletes.
- [ ] Preserve no-op deletion behavior when freezing outbound rows into `_sync_push_outbound`.
- [ ] Do not change `processPayloadForUpload(...)`.
- [ ] Do not change replay or conflict code in this step.

### 4. Test coverage

- [ ] Extend `library/src/jvmTest/kotlin/dev/goquick/sqlitenow/oversqlite/BundleBootstrapContractTest.kt`
      with repeated-bootstrap trigger-idempotency assertions.
- [ ] Add assertions that repeated bootstrap preserves trigger count and normalized trigger SQL.
- [ ] Add statement-cache regression coverage in an existing oversqlite contract test or a focused new JVM test:
      prove rebinding does not leak prior bindings, failed execution does not poison later reuse, and cached
      statements are finalized by the cache scope rather than by call sites.
- [ ] Add focused dirty-row classification tests to
      `library/src/jvmTest/kotlin/dev/goquick/sqlitenow/oversqlite/BundlePushContractTest.kt`:
      local insert, synced update, unsynced insert-then-delete no-op, synced delete.
- [ ] Re-run existing same-key replay/rebase tests and keep them unchanged:
      update->update, update->delete, delete->recreate, insert->update, insert->delete.
- [ ] Run `./gradlew :library:jvmTest`.
- [ ] Recommended before merge: run `./gradlew :library:jvmTest :library:jsTest :library:wasmJsBrowserTest`.

## Testing Plan

### Bootstrap tests

Extend `BundleBootstrapContractTest` to cover:

- repeated bootstrap on unchanged schema keeps trigger count stable
- repeated bootstrap on unchanged schema preserves normalized trigger SQL
- FK closure and composite-FK rejection still work after metadata reuse

Recommended assertion shape:

- query `sqlite_master` for trigger names and SQL before and after a second bootstrap
- assert the same trigger set and same normalized SQL

### Push dirty-row tests

Add focused JVM contract tests for outbound classification:

- local insert -> outbound `INSERT` using dirty payload
- synced update -> outbound `UPDATE` preserving `base_row_version`
- unsynced insert then delete -> no outbound row / dirty queue cleared as no-op
- synced delete -> outbound `DELETE` with null payload

These should live in `BundlePushContractTest` or its support layer.

### Replay regression tests

Existing same-key replay/rebase tests must stay green. In particular:

- update then later update
- update then later delete
- delete then later recreate
- insert then later update
- insert then later delete

The dirty-row refactor must not change replay outcomes.

### Statement-cache regression coverage

No dedicated micro-test is required for the cache itself if existing contract tests pass, but the following
paths must be exercised after the cache is wired in:

- pull apply
- snapshot stage/apply
- push stage/freeze/replay

The cache implementation should also be validated against these invariants:

- a cached statement can be rebound and stepped multiple times within one operation without leaking prior bindings
- a failed execution does not poison later `get(sql)` reuse within the same operation
- cached statements are not closed by call sites and are finalized once by the cache scope

### Suggested verification commands

At minimum:

```bash
./gradlew :library:jvmTest
```

Recommended before merge:

```bash
./gradlew :library:jvmTest :library:jsTest :library:wasmJsBrowserTest
```

If Android/device coverage is requested:

```bash
./gradlew :library-oversqlite-test:composeApp:connectedAndroidDeviceTest
```

## Risks

### Trigger equality bugs

Risk:

- a broken normalization function can leave stale triggers installed or recreate triggers unnecessarily

Mitigation:

- compare actual `sqlite_master.sql` before and after repeated bootstrap in tests
- keep trigger names and bodies unchanged except for normalization handling

### Statement lifetime bugs

Risk:

- cached statements may leak or be reused after the transaction scope that created them

Mitigation:

- keep cache lifetime lexical and short
- close all statements in `finally`
- do not store caches on the client instance

### Dirty-row misclassification

Risk:

- outbound snapshot could emit the wrong op or payload for insert/delete edge cases

Mitigation:

- add the focused outbound classification tests above
- keep replay logic unchanged in this spec

## Acceptance Criteria

- repeated bootstrap on an unchanged schema does not unconditionally recreate identical triggers
- bootstrap validation no longer issues separate FK introspection passes once `TableInfo` is loaded
- hot transaction loops stop preparing the same SQL text repeatedly within a single operation
- `collectDirtyRowsForPush(...)` no longer rereads live business rows for outbound snapshot construction
- all existing oversqlite contract tests still pass
- no schema migration is introduced
