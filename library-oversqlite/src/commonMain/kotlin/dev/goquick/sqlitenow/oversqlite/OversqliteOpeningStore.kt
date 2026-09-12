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

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection

internal const val oversqliteOpeningTable = "_sync_opening_state"
internal const val oversqliteOpeningFormatVersion = 3L
internal const val ordinaryReadyDigestDomain = "sqlitenow-ordinary-ready-v1"

internal val oversqliteOpeningColumnNames = listOf(
    "singleton_key", "format_version", "mode", "phase", "outcome", "revision",
    "plan_digest", "migration_id", "database_id", "service_id",
    "attached_user_id", "old_source_id", "new_source_id", "replacement_reason",
    "from_generation_id", "to_generation_id", "from_local_version", "to_local_version",
    "preserved_tables", "old_fingerprint", "target_fingerprint", "preservation_digest",
    "migration_complete", "contract_confirmed",
    "pending_dirty_count", "pending_frozen_count", "pending_conflict_count", "drain_complete",
    "replacement_state", "snapshot_id", "snapshot_bundle_seq", "snapshot_row_count",
    "snapshot_byte_count", "staged_row_count", "staged_byte_count", "target_reads_available",
    "writes_available", "next_action", "pending_request_hash",
    "pending_intent_revision", "failure_code", "failure_message", "initial_old_source_id",
    "old_source_recovery_receipt", "integrity_hash",
)

internal val createOversqliteOpeningTableSql = """
    CREATE TABLE IF NOT EXISTS _sync_opening_state (
        singleton_key INTEGER PRIMARY KEY CHECK(singleton_key=1),
        format_version INTEGER NOT NULL CHECK(format_version=3),
        mode TEXT NOT NULL CHECK(mode IN ('fresh','unchanged','local_only','generation_change')),
        phase TEXT NOT NULL,
        outcome TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK(revision>=0),
        plan_digest TEXT NOT NULL,
        migration_id TEXT NOT NULL,
        database_id TEXT NOT NULL,
        service_id TEXT NOT NULL,
        attached_user_id TEXT NOT NULL,
        old_source_id TEXT NOT NULL,
        new_source_id TEXT NOT NULL,
        replacement_reason TEXT NOT NULL,
        from_generation_id INTEGER NOT NULL CHECK(from_generation_id>=0),
        to_generation_id INTEGER NOT NULL CHECK(to_generation_id BETWEEN 1 AND 2147483647),
        from_local_version INTEGER NOT NULL CHECK(from_local_version>=-1),
        to_local_version INTEGER NOT NULL CHECK(to_local_version>=0),
        preserved_tables TEXT NOT NULL,
        old_fingerprint TEXT NOT NULL,
        target_fingerprint TEXT NOT NULL,
        preservation_digest TEXT NOT NULL,
        migration_complete INTEGER NOT NULL CHECK(migration_complete IN (0,1)),
        contract_confirmed INTEGER NOT NULL CHECK(contract_confirmed IN (0,1)),
        pending_dirty_count INTEGER NOT NULL CHECK(pending_dirty_count>=0),
        pending_frozen_count INTEGER NOT NULL CHECK(pending_frozen_count>=0),
        pending_conflict_count INTEGER NOT NULL CHECK(pending_conflict_count>=0),
        drain_complete INTEGER NOT NULL CHECK(drain_complete IN (0,1)),
        replacement_state TEXT NOT NULL CHECK(replacement_state IN ('none','reserved','confirmed')),
        snapshot_id TEXT NOT NULL,
        snapshot_bundle_seq INTEGER NOT NULL CHECK(snapshot_bundle_seq>=0),
        snapshot_row_count INTEGER NOT NULL CHECK(snapshot_row_count>=0),
        snapshot_byte_count INTEGER NOT NULL CHECK(snapshot_byte_count>=0),
        staged_row_count INTEGER,
        staged_byte_count INTEGER,
        target_reads_available INTEGER NOT NULL CHECK(target_reads_available IN (0,1)),
        writes_available INTEGER NOT NULL CHECK(writes_available IN (0,1)),
        next_action TEXT NOT NULL,
        pending_request_hash TEXT NOT NULL,
        pending_intent_revision INTEGER NOT NULL CHECK(pending_intent_revision>=0),
        failure_code TEXT NOT NULL,
        failure_message TEXT NOT NULL,
        initial_old_source_id TEXT NOT NULL,
        old_source_recovery_receipt TEXT NOT NULL,
        integrity_hash TEXT NOT NULL
    )
""".trimIndent()

private val unsupportedOversqliteOpeningFormat2Sql = createOversqliteOpeningTableSql
    .replace("CHECK(format_version=3)", "CHECK(format_version=2)")
    .replace(
        "    target_reads_available INTEGER NOT NULL CHECK(target_reads_available IN (0,1)),",
        "    old_" + "reads_available INTEGER NOT NULL CHECK(old_" +
            "reads_available IN (0,1)),\n" +
            "    target_reads_available INTEGER NOT NULL CHECK(target_reads_available IN (0,1)),",
    )

internal data class OversqliteOpeningRecord(
    val mode: String,
    val phase: String,
    val outcome: String,
    val revision: Long,
    val planDigest: String,
    val migrationId: String,
    val databaseId: String,
    val serviceId: String,
    val attachedUserId: String,
    val oldSourceId: String,
    val newSourceId: String,
    val replacementReason: String,
    val fromGenerationId: Long,
    val toGenerationId: Long,
    val fromLocalVersion: Long,
    val toLocalVersion: Long,
    val preservedTables: String,
    val oldFingerprint: String,
    val targetFingerprint: String,
    val preservationDigest: String,
    val migrationComplete: Boolean,
    val contractConfirmed: Boolean,
    val pendingDirtyCount: Long,
    val pendingFrozenCount: Long,
    val pendingConflictCount: Long,
    val drainComplete: Boolean,
    val replacementState: String,
    val snapshotId: String,
    val snapshotBundleSeq: Long,
    val snapshotRowCount: Long,
    val snapshotByteCount: Long,
    val stagedRowCount: Long?,
    val stagedByteCount: Long?,
    val targetReadsAvailable: Boolean,
    val writesAvailable: Boolean,
    val nextAction: String,
    val pendingRequestHash: String = "",
    val pendingIntentRevision: Long = 0,
    val failureCode: String = "",
    val failureMessage: String = "",
    val initialOldSourceId: String = oldSourceId,
    val oldSourceRecoveryReceipt: String = "",
) {
    fun integrityMaterial(): String = listOf(
        oversqliteOpeningFormatVersion, mode, phase, outcome, revision, planDigest, migrationId,
        databaseId, serviceId, attachedUserId, oldSourceId, newSourceId, replacementReason,
        fromGenerationId, toGenerationId, fromLocalVersion, toLocalVersion,
        preservedTables, oldFingerprint, targetFingerprint, preservationDigest, migrationComplete,
        contractConfirmed, pendingDirtyCount,
        pendingFrozenCount, pendingConflictCount, drainComplete, replacementState,
        snapshotId, snapshotBundleSeq, snapshotRowCount, snapshotByteCount,
        stagedRowCount ?: "null", stagedByteCount ?: "null", targetReadsAvailable,
        writesAvailable, nextAction, pendingRequestHash, pendingIntentRevision,
        failureCode, failureMessage, initialOldSourceId, oldSourceRecoveryReceipt,
    ).joinToString("\u001f")

    fun integrityHash(): String = sha256Hex(integrityMaterial().encodeToByteArray())
}

internal class OversqliteOpeningStore(private val db: SafeSQLiteConnection) {
    suspend fun create(record: OversqliteOpeningRecord) {
        if (exists()) {
            requireSupportedSchema()
            require(db.scalarLong("SELECT count(*) FROM _sync_opening_state") == 0L) {
                "coordinated opening journal already exists"
            }
        }
        db.execSQL(createOversqliteOpeningTableSql)
        db.withPreparedStatement(
            """
            INSERT INTO _sync_opening_state(
                singleton_key,format_version,mode,phase,outcome,revision,plan_digest,migration_id,
                database_id,service_id,attached_user_id,old_source_id,new_source_id,replacement_reason,
                from_generation_id,to_generation_id,from_local_version,to_local_version,
                preserved_tables,old_fingerprint,target_fingerprint,preservation_digest,
                migration_complete,contract_confirmed,pending_dirty_count,pending_frozen_count,
                pending_conflict_count,drain_complete,replacement_state,snapshot_id,snapshot_bundle_seq,
                snapshot_row_count,snapshot_byte_count,staged_row_count,staged_byte_count,
                target_reads_available,writes_available,next_action,
                pending_request_hash,pending_intent_revision,failure_code,failure_message,
                initial_old_source_id,old_source_recovery_receipt,integrity_hash
            ) VALUES(1,3,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """.trimIndent(),
        ) { statement ->
            bindRecord(statement, record)
            statement.step()
        }
        require(db.scalarLong("SELECT changes()") == 1L) { "coordinated opening journal was not created" }
    }

    suspend fun readOrNull(): OversqliteOpeningRecord? {
        if (!exists()) return null
        requireSupportedSchema()
        require(db.scalarLong("SELECT count(*) FROM _sync_opening_state") == 1L) {
            "coordinated opening journal singleton is missing or duplicated"
        }
        val records = db.queryList(
            """
            SELECT mode,phase,outcome,revision,plan_digest,migration_id,database_id,service_id,
                attached_user_id,old_source_id,new_source_id,replacement_reason,
                from_generation_id,to_generation_id,from_local_version,to_local_version,
                preserved_tables,old_fingerprint,target_fingerprint,preservation_digest,
                migration_complete,contract_confirmed,pending_dirty_count,pending_frozen_count,
                pending_conflict_count,drain_complete,replacement_state,snapshot_id,
                snapshot_bundle_seq,snapshot_row_count,snapshot_byte_count,staged_row_count,
                staged_byte_count,target_reads_available,writes_available,
                next_action,pending_request_hash,pending_intent_revision,failure_code,failure_message,
                initial_old_source_id,old_source_recovery_receipt,integrity_hash,format_version
            FROM _sync_opening_state WHERE singleton_key=1
            """.trimIndent(),
        ) { row ->
            val record = OversqliteOpeningRecord(
                mode = row.getText(0), phase = row.getText(1), outcome = row.getText(2),
                revision = row.getLong(3), planDigest = row.getText(4), migrationId = row.getText(5),
                databaseId = row.getText(6), serviceId = row.getText(7),
                attachedUserId = row.getText(8), oldSourceId = row.getText(9),
                newSourceId = row.getText(10), replacementReason = row.getText(11),
                fromGenerationId = row.getLong(12), toGenerationId = row.getLong(13),
                fromLocalVersion = row.getLong(14), toLocalVersion = row.getLong(15),
                preservedTables = row.getText(16), oldFingerprint = row.getText(17),
                targetFingerprint = row.getText(18), preservationDigest = row.getText(19),
                migrationComplete = row.getLong(20) == 1L, contractConfirmed = row.getLong(21) == 1L,
                pendingDirtyCount = row.getLong(22), pendingFrozenCount = row.getLong(23),
                pendingConflictCount = row.getLong(24), drainComplete = row.getLong(25) == 1L,
                replacementState = row.getText(26), snapshotId = row.getText(27),
                snapshotBundleSeq = row.getLong(28), snapshotRowCount = row.getLong(29),
                snapshotByteCount = row.getLong(30),
                stagedRowCount = if (row.isNull(31)) null else row.getLong(31),
                stagedByteCount = if (row.isNull(32)) null else row.getLong(32),
                targetReadsAvailable = row.getLong(33) == 1L,
                writesAvailable = row.getLong(34) == 1L, nextAction = row.getText(35),
                pendingRequestHash = row.getText(36), pendingIntentRevision = row.getLong(37),
                failureCode = row.getText(38), failureMessage = row.getText(39),
                initialOldSourceId = row.getText(40), oldSourceRecoveryReceipt = row.getText(41),
            )
            Triple(record, row.getText(42), row.getLong(43))
        }
        require(records.size == 1) { "coordinated opening journal singleton is missing or duplicated" }
        val (record, integrityHash, formatVersion) = records.single()
        require(formatVersion == oversqliteOpeningFormatVersion) {
            "unsupported coordinated opening journal format; a fresh database is required"
        }
        require(integrityHash == record.integrityHash()) { "coordinated opening journal integrity mismatch" }
        return record
    }

    suspend fun replaceReady(record: OversqliteOpeningRecord, expected: OversqliteOpeningRecord) {
        require(expected.outcome == "ready") { "only a completed opening journal can start a new local edge" }
        db.withPreparedStatement(
            """
            UPDATE _sync_opening_state SET
                format_version=3,mode=?,phase=?,outcome=?,revision=?,plan_digest=?,migration_id=?,
                database_id=?,service_id=?,attached_user_id=?,old_source_id=?,new_source_id=?,
                replacement_reason=?,from_generation_id=?,to_generation_id=?,from_local_version=?,
                to_local_version=?,preserved_tables=?,old_fingerprint=?,target_fingerprint=?,
                preservation_digest=?,migration_complete=?,contract_confirmed=?,pending_dirty_count=?,
                pending_frozen_count=?,pending_conflict_count=?,drain_complete=?,replacement_state=?,
                snapshot_id=?,snapshot_bundle_seq=?,snapshot_row_count=?,snapshot_byte_count=?,
                staged_row_count=?,staged_byte_count=?,target_reads_available=?,writes_available=?,
                next_action=?,pending_request_hash=?,pending_intent_revision=?,failure_code=?,failure_message=?,
                initial_old_source_id=?,old_source_recovery_receipt=?,
                integrity_hash=?
            WHERE singleton_key=1 AND integrity_hash=? AND outcome='ready'
            """.trimIndent(),
        ) { statement ->
            bindRecord(statement, record)
            statement.bindText(44, expected.integrityHash())
            statement.step()
        }
        require(db.scalarLong("SELECT changes()") == 1L) { "coordinated opening journal changed concurrently" }
    }

    suspend fun update(record: OversqliteOpeningRecord, expected: OversqliteOpeningRecord) {
        db.withPreparedStatement(
            """
            UPDATE _sync_opening_state SET
                format_version=3,mode=?,phase=?,outcome=?,revision=?,plan_digest=?,migration_id=?,
                database_id=?,service_id=?,attached_user_id=?,old_source_id=?,new_source_id=?,
                replacement_reason=?,from_generation_id=?,to_generation_id=?,from_local_version=?,
                to_local_version=?,preserved_tables=?,old_fingerprint=?,target_fingerprint=?,
                preservation_digest=?,migration_complete=?,contract_confirmed=?,pending_dirty_count=?,
                pending_frozen_count=?,pending_conflict_count=?,drain_complete=?,replacement_state=?,
                snapshot_id=?,snapshot_bundle_seq=?,snapshot_row_count=?,snapshot_byte_count=?,
                staged_row_count=?,staged_byte_count=?,target_reads_available=?,writes_available=?,
                next_action=?,pending_request_hash=?,pending_intent_revision=?,failure_code=?,failure_message=?,
                initial_old_source_id=?,old_source_recovery_receipt=?,
                integrity_hash=?
            WHERE singleton_key=1 AND integrity_hash=? AND revision=?
            """.trimIndent(),
        ) { statement ->
            bindRecord(statement, record)
            statement.bindText(44, expected.integrityHash())
            statement.bindLong(45, expected.revision)
            statement.step()
        }
        require(db.scalarLong("SELECT changes()") == 1L) { "coordinated opening journal changed concurrently" }
    }

    private suspend fun exists(): Boolean = db.scalarLong(
        "SELECT count(*) FROM sqlite_schema WHERE lower(type)='table' AND lower(name)='$oversqliteOpeningTable'",
    ) != 0L

    private suspend fun requireSupportedSchema() {
        val schema = db.queryList(
            "SELECT sql FROM sqlite_schema WHERE type='table' AND lower(name)='$oversqliteOpeningTable'",
        ) { row -> row.getText(0) }
        require(
            schema.size == 1 &&
                normalizeOpeningDdl(schema.single()) == normalizeOpeningDdl(createOversqliteOpeningTableSql),
        ) {
            "unsupported coordinated opening journal schema or format; a fresh database is required"
        }
    }

    private fun bindRecord(statement: androidx.sqlite.SQLiteStatement, record: OversqliteOpeningRecord) {
        var index = 1
        fun text(value: String) = statement.bindText(index++, value)
        fun long(value: Long) = statement.bindLong(index++, value)
        text(record.mode); text(record.phase); text(record.outcome); long(record.revision)
        text(record.planDigest); text(record.migrationId); text(record.databaseId); text(record.serviceId)
        text(record.attachedUserId); text(record.oldSourceId); text(record.newSourceId)
        text(record.replacementReason)
        long(record.fromGenerationId); long(record.toGenerationId); long(record.fromLocalVersion)
        long(record.toLocalVersion); text(record.preservedTables); text(record.oldFingerprint)
        text(record.targetFingerprint); text(record.preservationDigest)
        long(if (record.migrationComplete) 1 else 0); long(if (record.contractConfirmed) 1 else 0)
        long(record.pendingDirtyCount); long(record.pendingFrozenCount); long(record.pendingConflictCount)
        long(if (record.drainComplete) 1 else 0); text(record.replacementState)
        text(record.snapshotId); long(record.snapshotBundleSeq); long(record.snapshotRowCount)
        long(record.snapshotByteCount)
        if (record.stagedRowCount == null) statement.bindNull(index++) else long(record.stagedRowCount)
        if (record.stagedByteCount == null) statement.bindNull(index++) else long(record.stagedByteCount)
        long(if (record.targetReadsAvailable) 1 else 0); long(if (record.writesAvailable) 1 else 0)
        text(record.nextAction); text(record.pendingRequestHash)
        long(record.pendingIntentRevision)
        text(record.failureCode); text(record.failureMessage); text(record.initialOldSourceId)
        text(record.oldSourceRecoveryReceipt); text(record.integrityHash())
    }
}

/**
 * Installs the ready singleton needed by the fail-closed managed-table guards when an ordinary
 * client creates or upgrades the local runtime outside a coordinated opening. A coordinated
 * opening has already written its non-ready journal before it opens the client, so that record is
 * validated and retained unchanged here.
 */
internal suspend fun ensureOrdinaryReadyOpeningRecord(
    db: SafeSQLiteConnection,
    identity: LocalContractIdentity,
    fingerprint: String,
) {
    db.execSQL(createOversqliteOpeningTableSql)
    val rowCount = db.scalarLong("SELECT count(*) FROM $oversqliteOpeningTable")
    if (rowCount != 0L) {
        require(rowCount == 1L) { "coordinated opening journal singleton is missing or duplicated" }
        OversqliteOpeningStore(db).readOrNull()
        return
    }

    val attachment = OversqliteAttachmentStateStore(db).loadState()
    val sourceId = attachment.currentSourceId
    val record = OversqliteOpeningRecord(
        mode = "unchanged",
        phase = "ready",
        outcome = "ready",
        revision = 1L,
        planDigest = sha256Hex(
            listOf(
                ordinaryReadyDigestDomain,
                identity.databaseId,
                identity.serviceId,
                identity.generationId,
                identity.localVersion,
                identity.physicalShape,
                fingerprint,
            ).joinToString("\u001f").encodeToByteArray(),
        ),
        migrationId = "",
        databaseId = identity.databaseId,
        serviceId = identity.serviceId,
        attachedUserId = attachment.attachedUserId,
        oldSourceId = sourceId,
        newSourceId = "",
        replacementReason = "",
        fromGenerationId = identity.generationId,
        toGenerationId = identity.generationId,
        fromLocalVersion = identity.localVersion,
        toLocalVersion = identity.localVersion,
        preservedTables = "",
        oldFingerprint = fingerprint,
        targetFingerprint = fingerprint,
        preservationDigest = "",
        migrationComplete = true,
        contractConfirmed = fingerprint.isNotEmpty(),
        pendingDirtyCount = db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"),
        pendingFrozenCount = db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"),
        pendingConflictCount = db.scalarLong(
            "SELECT count(*) FROM _sync_conflict_state " +
                "WHERE singleton_key=1 AND decision_state!='none'",
        ),
        drainComplete = true,
        replacementState = "none",
        snapshotId = "",
        snapshotBundleSeq = 0L,
        snapshotRowCount = 0L,
        snapshotByteCount = 0L,
        stagedRowCount = null,
        stagedByteCount = null,
        targetReadsAvailable = true,
        writesAvailable = true,
        nextAction = "use_ready",
        initialOldSourceId = sourceId,
    )
    OversqliteOpeningStore(db).create(record)
}

internal suspend fun inspectOversqliteOpeningExtension(db: SafeSQLiteConnection, tableExists: Boolean) {
    if (!tableExists) return
    val schema = db.queryList(
        "SELECT sql FROM sqlite_schema WHERE type='table' AND lower(name)='$oversqliteOpeningTable'",
    ) { row -> row.getText(0) }
    if (schema.size == 1 &&
        normalizeOpeningDdl(schema.single()) == normalizeOpeningDdl(unsupportedOversqliteOpeningFormat2Sql)
    ) {
        throw IllegalArgumentException(
            "unsupported coordinated opening journal format; a fresh database is required",
        )
    }
    val columns = db.queryList(
        "SELECT lower(name) FROM pragma_table_xinfo('$oversqliteOpeningTable') ORDER BY cid",
    ) { row -> row.getText(0) }
    require(columns == oversqliteOpeningColumnNames) {
        "malformed coordinated opening journal; preserve the database for inspected recovery"
    }
    require(schema.size == 1 && normalizeOpeningDdl(schema.single()) == normalizeOpeningDdl(createOversqliteOpeningTableSql)) {
        "coordinated opening journal schema differs from format 3; a fresh database is required"
    }
    require(OversqliteOpeningStore(db).readOrNull() != null) {
        "orphan coordinated opening journal collision; preserve the database for inspected recovery"
    }
}

private fun normalizeOpeningDdl(sql: String): String = sql.trim()
    .replace(
        Regex("CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?", RegexOption.IGNORE_CASE),
        "CREATE TABLE ",
    )
    .replace(Regex("\\s+"), " ")
