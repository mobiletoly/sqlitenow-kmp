/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteException
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject

/**
 * Counts of pending intent a rotated rebuild moved onto the installed snapshot. [conflicting] is the
 * part of [requeued] that kept its original base because the server row changed after the edit.
 */
internal data class CarriedRecoveryIntent(val requeued: Long = 0, val alreadySatisfied: Long = 0, val conflicting: Long = 0)

private data class PendingRecoveryIntent(
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val op: String,
    val localPayload: String?,
    val baseRowVersion: Long,
    val base: HistoricalBase,
) {
    /**
     * Whether the installed snapshot still holds the server state this edit was made from. A row that is
     * absent on both sides has no server change to lose, whatever its tombstone version is.
     */
    fun madeFromInstalledState(live: Boolean, installedVersion: Long): Boolean =
        if (live) base.kind == "present" && baseRowVersion == installedVersion
        else base.kind == "absent" || base.kind == "deleted"
}

/**
 * Owns pending local intent across a rotated source-recovery rebuild.
 *
 * Pending intent is every dirty row plus every frozen or rejected outbox row that no newer dirty row
 * replaced. The server refuses the retired source, so that intent can never settle under it. The
 * rebuild therefore keeps it in place while the authoritative snapshot is staged, and this owner moves
 * it onto the installed snapshot inside the final writer, where the server has already accepted the
 * replacement source and so certified that the retired source holds no unacknowledged outcome.
 *
 * Nothing here discards intent on its own. [discard] is the explicit, consented exit for intent that
 * cannot be applied to the authoritative state.
 */
internal class OversqliteSourceRecoveryIntent(
    private val db: SafeSQLiteConnection,
    config: OversqliteConfig,
    private val localStore: OversqliteLocalStore,
    private val syncStateStore: OversqliteSyncStateStore,
    private val outboxStateStore: OversqliteOutboxStateStore,
    private val conflictStateStore: OversqliteConflictStateStore,
    private val operationStateStore: OversqliteOperationStateStore,
    private val stageStore: OversqliteStageStore,
    private val json: Json,
) {
    private val workspace = OversqliteWorkspace(db, config)

    suspend fun hasPending(): Boolean = db.scalarLong(
        """SELECT EXISTS(SELECT 1 FROM _sync_dirty_rows) OR EXISTS(SELECT 1 FROM _sync_outbox_rows)
            OR EXISTS(SELECT 1 FROM _sync_upload_journal)
            OR EXISTS(SELECT 1 FROM _sync_outbox_bundle WHERE singleton_key=1 AND state<>'none')""",
    ) != 0L

    /**
     * Requeues every pending edit on top of the snapshot the caller just installed and verified, then
     * releases the retired source's frozen request. Runs only inside that final apply-mode writer.
     *
     * An edit of a row the snapshot holds becomes an UPDATE or DELETE based on the installed version. An
     * edit of a row the snapshot lacks becomes an INSERT, and a DELETE of such a row is already satisfied.
     * The local value is written back to the business row so the application keeps seeing its edit, and
     * the next ordinary push uploads it as the replacement source's first bundle.
     *
     * That rebase is sound only while the snapshot still holds the server state the edit was made from.
     * When another device changed or removed the row in the meantime, or created the key the edit
     * inserts, the edit keeps its original base instead. The server then refuses its upload as an
     * ordinary conflict and the configured resolver decides, exactly as for any other stale edit.
     * Rebasing it here would overwrite that change without asking.
     */
    suspend fun carryOntoInstalledSnapshot(state: RuntimeState, cache: StatementCache): CarriedRecoveryIntent {
        var schema = ""
        var table = ""
        var key = ""
        var requeued = 0L
        var satisfied = 0L
        var conflicting = 0L
        while (true) {
            currentCoroutineContext().ensureActive()
            val intent = nextPending(schema, table, key, carryUnit, cache) ?: break
            schema = intent.schemaName
            table = intent.tableName
            key = intent.keyJson
            val baseline = db.withPreparedStatement(
                "SELECT row_version,server_kind FROM _sync_row_state WHERE schema_name=? AND table_name=? AND key_json=?",
                cache,
            ) { st ->
                st.bindText(1, schema); st.bindText(2, table); st.bindText(3, key)
                if (st.step()) st.getLong(0) to st.getText(1) else null
            } ?: error("installed snapshot left no authoritative observation for pending intent in $table")
            val live = baseline.second == "live"
            val current = intent.madeFromInstalledState(live, baseline.first)
            val localPk = localStore.decodeDirtyKeyForPush(state, table, key).first
            // Business rows are written through one-shot statements. A constraint failure is an expected,
            // recoverable blocker here, and a reusable statement would report it again on reset, which
            // the connection treats as a fatal cleanup failure.
            try {
                if (intent.op == "DELETE") {
                    if (!live) {
                        // The authoritative state already lacks the row the application deleted.
                        syncStateStore.deleteDirtyRow(schema, table, key, cache)
                        satisfied++
                        continue
                    }
                    if (current) syncStateStore.rebaseDirtyIntent(schema, table, key, "DELETE", baseline.first, null, cache)
                    else syncStateStore.requeueOriginalIntent(schema, table, key, "DELETE", intent.baseRowVersion, null, intent.base, cache)
                    localStore.deleteLocalRow(state, table, localPk, statementCache = null, skipUniqueReplay = true)
                } else {
                    val payload = checkNotNull(intent.localPayload) { "pending ${intent.op} intent has no local payload" }
                    if (current) syncStateStore.rebaseDirtyIntent(schema, table, key, if (live) "UPDATE" else "INSERT", baseline.first, payload, cache)
                    else syncStateStore.requeueOriginalIntent(schema, table, key, intent.op, intent.baseRowVersion, payload, intent.base, cache)
                    localStore.upsertRow(table, json.parseToJsonElement(payload) as JsonObject,
                        PayloadSource.LOCAL_STATE, statementCache = null, skipUniqueReplay = true)
                }
            } catch (failure: ResolutionBaseUnavailableException) {
                if (current) throw failure
                // Without its original base the edit could only be uploaded over the changed row.
                throw PendingIntentUnsatisfiableException(setOf(table), failure)
            } catch (failure: RuntimeException) {
                if (!isConstraintFailure(failure)) throw failure
                throw PendingIntentUnsatisfiableException(setOf(table), failure)
            }
            requeued++
            if (!current) conflicting++
        }
        releaseRetiredSourceOwnership(cache)
        if (requeued != 0L) requireForeignKeysSatisfied(state)
        return CarriedRecoveryIntent(requeued, satisfied, conflicting)
    }

    /**
     * Drops every pending edit after explicit caller consent and reports exactly what was dropped.
     *
     * A frozen request or acknowledgment obligation may only be released once the server has accepted the
     * replacement source, which the staged snapshot proves. Before that the server could still hold an
     * outcome for it, and losing the request would leave that outcome unresolvable.
     */
    suspend fun discard(): DiscardedPendingIntent {
        requireDiscardable()
        stageStore.clearPushPlanForLifecycle()
        return db.transaction(TransactionMode.IMMEDIATE) {
            requireDiscardable()
            db.withStatementCache { cache ->
                val rows = mutableListOf<DiscardedIntentRow>()
                var schema = ""
                var table = ""
                var key = ""
                var held = WorkspaceCharge()
                while (true) {
                    currentCoroutineContext().ensureActive()
                    val intent = nextPending(schema, table, key, discardUnit, cache, held) ?: break
                    schema = intent.schemaName
                    table = intent.tableName
                    key = intent.keyJson
                    val row = DiscardedIntentRow(schema, table, key, intent.op)
                    held = workspace.add(discardUnit, held,
                        WorkspaceCharge(4, 64 + 2 * (utf8Size(schema) + utf8Size(table) + utf8Size(key))))
                    rows += row
                }
                syncStateStore.clearDirtyRows()
                outboxStateStore.releaseRetiredSourceUpload(cache)
                conflictStateStore.clear()
                DiscardedPendingIntent(rows)
            }
        }
    }

    private suspend fun requireDiscardable() {
        val operation = operationStateStore.loadState()
        check(operation.isSourceRecoveryRequired()) {
            "discardPendingIntentForRecovery() is available only while source recovery is required"
        }
        check(operation.stagedSnapshotId.isNotEmpty() || loadUploadJournal(db) == null) {
            "run rebuild() first: an unsettled upload may be released only after the server accepts the replacement source"
        }
    }

    private suspend fun releaseRetiredSourceOwnership(cache: StatementCache) {
        outboxStateStore.releaseRetiredSourceUpload(cache)
        // A retained application rejection described the released request; its intent was requeued above.
        // A deferred row conflict never reaches this writer: rebuild refuses while one is pending.
        val rejected = db.withPreparedStatement(
            "SELECT decision_state='rejected' FROM _sync_conflict_state WHERE singleton_key=1", cache,
        ) { st -> check(st.step()); st.getLong(0) != 0L }
        if (rejected) conflictStateStore.clear()
        stageStore.invalidateFrozenPushPlan()
    }

    // Drivers report a violated constraint through either exception type, always with SQLite's own
    // "constraint failed" text. Anything else is a real failure and propagates unchanged.
    private fun isConstraintFailure(failure: RuntimeException): Boolean {
        if (failure !is SQLiteException && failure !is SqliteException) return false
        var current: Throwable? = failure
        repeat(8) {
            val message = current?.message ?: return false
            if (message.contains("constraint failed", ignoreCase = true)) return true
            current = current?.cause
        }
        return false
    }

    /** Deferred enforcement would only fail the commit; name the tables so the caller can act. */
    private suspend fun requireForeignKeysSatisfied(state: RuntimeState) {
        val violating = linkedSetOf<String>()
        for (managed in state.validated.tables) {
            val violations = db.scalarLong(
                "SELECT count(*) FROM pragma_foreign_key_check(${snapshotSqlText(managed.tableName)})",
            )
            if (violations != 0L) violating += managed.tableName
        }
        if (violating.isNotEmpty()) throw PendingIntentUnsatisfiableException(violating)
    }

    // One keyset reference at a time: measure before reading TEXT, as the settlement owner does.
    private suspend fun nextPending(
        schema: String,
        table: String,
        key: String,
        unit: String,
        cache: StatementCache,
        held: WorkspaceCharge = WorkspaceCharge(),
    ): PendingRecoveryIntent? {
        val size = db.withPreparedStatement(
            "SELECT 64+6*(length(CAST(schema_name AS BLOB))+length(CAST(table_name AS BLOB))+length(CAST(key_json AS BLOB)))" +
                "+length(CAST(COALESCE(payload,'') AS BLOB))+length(CAST(COALESCE(base_payload,'') AS BLOB))" +
                "+length(CAST(base_provenance AS BLOB))$pendingIntentTail",
            cache,
        ) { st ->
            st.bindText(1, schema); st.bindText(2, table); st.bindText(3, key)
            if (st.step()) st.getLong(0) else null
        } ?: return null
        workspace.check(unit, workspace.add(unit, held, WorkspaceCharge(8, workspace.scale(unit, size, 8))), statementCache = cache)
        return db.withPreparedStatement(
            "SELECT schema_name,table_name,key_json,op,payload,base_row_version,base_kind,base_payload,base_provenance$pendingIntentTail",
            cache,
        ) { st ->
            st.bindText(1, schema); st.bindText(2, table); st.bindText(3, key)
            check(st.step()) { "pending intent changed under its writer" }
            PendingRecoveryIntent(st.getText(0), st.getText(1), st.getText(2), st.getText(3),
                if (st.isNull(4)) null else st.getText(4), st.getLong(5),
                HistoricalBase(st.getText(6), if (st.isNull(7)) null else st.getText(7), st.getText(8)))
        }
    }

    private companion object {
        const val carryUnit = "source-recovery-intent"
        const val discardUnit = "source-recovery-intent-discard"

        // A dirty row is the newer application write for its key, so it replaces the frozen capture.
        const val pendingIntentTail = """ FROM (
 SELECT schema_name,table_name,key_json,op,payload,base_row_version,base_kind,base_payload,base_provenance FROM _sync_dirty_rows
 UNION ALL
 SELECT o.schema_name,o.table_name,o.key_json,o.op,o.local_payload,o.base_row_version,o.base_kind,o.base_payload,o.base_provenance FROM _sync_outbox_rows o
 WHERE NOT EXISTS(SELECT 1 FROM _sync_dirty_rows d
   WHERE (d.schema_name,d.table_name,d.key_json)=(o.schema_name,o.table_name,o.key_json))
) WHERE (schema_name,table_name,key_json)>(?,?,?) ORDER BY schema_name,table_name,key_json LIMIT 1"""
    }
}
