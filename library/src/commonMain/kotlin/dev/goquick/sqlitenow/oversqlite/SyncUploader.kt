package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.common.logger
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.serialization.json.*


/**
 * Handles the upload process for sync operations.
 * Separated from DefaultOversqliteClient to improve maintainability.
 */
internal class SyncUploader(
    private val http: HttpClient,
    private val baseUrl: String,
    private val config: OversqliteConfig,
    private val tokenProvider: suspend () -> String,
    private val resolver: Resolver,
    private val upsertBusinessFromPayload: suspend (SafeSQLiteConnection, String, String, JsonElement?) -> Unit,
    private val updateRowMeta: suspend (SafeSQLiteConnection, String, String, Long, Boolean) -> Unit
) {
    /**
     * Represents a pending change to be uploaded
     */
    private data class PendingChange(
        val table: String,
        val pk: String,
        val op: String,
        val baseVersion: Long,
        val payload: String?,
        var changeId: Long?
    )

    data class UploadResult(
        val summary: UploadSummary,
        val updatedTables: Set<String>
    )

    suspend fun uploadPendingChanges(
        db: SafeSQLiteConnection,
        nextChangeId: Long
    ): UploadResult {
        val updatedTables = mutableSetOf<String>()

        // 1. Load pending changes
        val pending = loadPendingChanges(db)
        if (pending.isEmpty()) {
            logger.d { "uploadOnce: no pending" }
            return UploadResult(UploadSummary(0, 0, 0, 0, 0), emptySet())
        }

        // Remove excessive debug logging

        // 2. Assign change IDs where missing
        assignChangeIds(db, pending, nextChangeId)

        // 3. Build and send upload request
        val changes = buildChangeUploads(pending)
        val response = sendUploadRequest(changes, db)

        // 4. Process response
        val summary = processUploadResponse(db, changes, response, updatedTables)

        return UploadResult(summary, updatedTables)
    }

    private suspend fun loadPendingChanges(db: SafeSQLiteConnection): MutableList<PendingChange> {
        return db.prepare("SELECT table_name, pk_uuid, op, base_version, payload, change_id FROM _sync_pending ORDER BY queued_at ASC")
            .use { st ->
                val out = mutableListOf<PendingChange>()
                while (st.step()) {
                    val cid = st.getLong(5)
                    out += PendingChange(
                        table = st.getText(0),
                        pk = st.getText(1),
                        op = st.getText(2),
                        baseVersion = st.getLong(3),
                        payload = st.getText(4),
                        changeId = if (cid < 0) null else cid
                    )
                }
                out
            }
    }

    private suspend fun assignChangeIds(
        db: SafeSQLiteConnection,
        pending: List<PendingChange>,
        startingId: Long
    ) {
        var nextId = startingId
        pending.forEach { p ->
            if (p.changeId == null) {
                db.prepare("UPDATE _sync_pending SET change_id=? WHERE table_name=? AND pk_uuid=?").use { st ->
                    st.bindLong(1, nextId)
                    st.bindText(2, p.table)
                    st.bindText(3, p.pk)
                    st.step()
                }
                p.changeId = nextId
                nextId += 1
            }
        }
    }

    private fun buildChangeUploads(pending: List<PendingChange>): List<ChangeUpload> {
        return pending.map { p ->
            val payloadElement = when (p.op) {
                "DELETE" -> null
                else -> p.payload?.takeIf { it.isNotBlank() }
                    ?.let { Json.parseToJsonElement(it) }
            }
            ChangeUpload(
                sourceChangeId = p.changeId!!,
                schema = config.schema.lowercase(),
                table = p.table.lowercase(),
                op = p.op,
                pk = p.pk,
                serverVersion = p.baseVersion,
                payload = payloadElement
            )
        }
    }

    private suspend fun sendUploadRequest(
        changes: List<ChangeUpload>,
        db: SafeSQLiteConnection
    ): UploadResponse {
        val lastServerSeq = db.prepare("SELECT last_server_seq_seen FROM _sync_client_info LIMIT 1")
            .use { st -> if (st.step()) st.getLong(0) else 0L }

        val req = UploadRequest(lastServerSeqSeen = lastServerSeq, changes = changes)
        val token = tokenProvider()

        val response: UploadResponse = http.post("${baseUrl.trimEnd('/')}/sync/upload") {
            header("Authorization", "Bearer $token")
            contentType(ContentType.Application.Json)
            setBody(req)
        }.body()

        logger.d { "Upload response: ${response.statuses.size} statuses" }

        response.statuses.forEach { status ->
            val matchingChange = changes.find { it.sourceChangeId == status.sourceChangeId }
            val changeInfo = matchingChange?.let { "${it.op} ${it.table}:${it.pk.take(8)}" } ?: "unknown"

            when (status.status.uppercase()) {
                "APPLIED" -> {
                    logger.d { "Applied: $changeInfo -> server_v${status.newServerVersion}" }
                }
                "CONFLICT" -> {
                    logger.w { "Sync conflict: $changeInfo - ${status.message}" }
                    logger.d { "Conflict details: serverRow=${status.serverRow}, localOp=${matchingChange?.op}" }
                }
                "INVALID", "MATERIALIZE_ERROR" -> {
                    logger.e { "Upload failed: $changeInfo - ${status.status}: ${status.message}" }
                }
                else -> {
                    logger.d { "Upload status: ${status.status} for $changeInfo" }
                }
            }
        }
        logger.d { "uploadOnce: sent=${changes.size} statuses=${response.statuses.size} highestSeq=${response.highestServerSeq}" }
        return response
    }

    private suspend fun processUploadResponse(
        db: SafeSQLiteConnection,
        changes: List<ChangeUpload>,
        response: UploadResponse,
        updatedTables: MutableSet<String>
    ): UploadSummary {
        var appliedCount = 0
        var conflictCount = 0
        var invalidCount = 0
        var materializeErrorCount = 0
        var firstErrorMessage: String? = null
        val invalidReasons = mutableMapOf<String, Int>()

        db.execSQL("BEGIN")
        try {
            db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
                st.bindLong(1, response.highestServerSeq)
                st.step()
            }

            response.statuses.forEachIndexed { idx, status ->
                val change = changes[idx]
                when (status.status) {
                    "applied" -> {
                        appliedCount += processAppliedStatus(db, change, status, updatedTables)
                    }

                    "conflict" -> {
                        conflictCount += processConflictStatus(db, change, status, updatedTables)
                    }

                    "invalid" -> {
                        invalidCount += processInvalidStatus(db, change, status, invalidReasons)
                        if (firstErrorMessage == null) {
                            firstErrorMessage =
                                status.message ?: getInvalidReason(status)?.let { "invalid: $it" }
                        }
                    }

                    "materialize_error" -> {
                        materializeErrorCount += 1
                        if (firstErrorMessage == null) firstErrorMessage = status.message
                    }
                }
            }
            db.execSQL("COMMIT")
        } finally {
            try {
                db.execSQL("ROLLBACK")
            } catch (_: Throwable) {
            }
        }

        logger.d { "uploadOnce: done" }
        return UploadSummary(
            total = changes.size,
            applied = appliedCount,
            conflict = conflictCount,
            invalid = invalidCount,
            materializeError = materializeErrorCount,
            invalidReasons = invalidReasons,
            firstErrorMessage = firstErrorMessage
        )
    }

    private suspend fun processAppliedStatus(
        db: SafeSQLiteConnection,
        change: ChangeUpload,
        status: ChangeUploadStatus,
        updatedTables: MutableSet<String>
    ): Int {
        // For INSERT/UPDATE operations, ensure local changes are preserved in business table
        if ((change.op == "INSERT" || change.op == "UPDATE") && change.payload != null) {
            logger.d { "Applying local payload for ${change.op} ${change.table}:${change.pk}" }
            upsertBusinessFromPayload(db, change.table, change.pk, change.payload)
            updatedTables += change.table
        }

        db.prepare("DELETE FROM _sync_pending WHERE table_name=? AND pk_uuid=?").use { st ->
            st.bindText(1, change.table)
            st.bindText(2, change.pk)
            st.step()
        }

        val inferredSv = status.newServerVersion ?: (change.serverVersion + 1)
        updateRowMeta(db, change.table, change.pk, inferredSv, deleted = (change.op == "DELETE"))

        return 1
    }

    private suspend fun processConflictStatus(
        db: SafeSQLiteConnection,
        change: ChangeUpload,
        status: ChangeUploadStatus,
        updatedTables: MutableSet<String>
    ): Int {
        val decision = resolveConflictWithFallbacks(
            table = change.table,
            pk = change.pk,
            localOp = change.op,
            serverRow = status.serverRow,
            localPayload = change.payload
        )

        when (decision) {
            is MergeResult.AcceptServer -> {
                processAcceptServerDecision(db, change, status, updatedTables)
            }

            is MergeResult.KeepLocal -> {
                processKeepLocalDecision(db, change, status, decision, updatedTables)
            }
        }

        return 1
    }

    /**
     * Resolve conflicts with automatic handling of infrastructure edge cases.
     * Only calls the user resolver for legitimate business conflicts.
     */
    private fun resolveConflictWithFallbacks(
        table: String,
        pk: String,
        localOp: String,
        serverRow: JsonElement?,
        localPayload: JsonElement?
    ): MergeResult {
        // CRITICAL FIX: Handle DELETE operations specially to prevent record resurrection
        if (localOp == "DELETE") {
            // For DELETE operations, we create a special KeepLocal result with null payload
            // This will be handled by processKeepLocalDecision to maintain the DELETE
            return MergeResult.KeepLocal(localPayload ?: kotlinx.serialization.json.JsonNull)
        }

        // Handle infrastructure edge cases automatically for non-DELETE operations
        if (localPayload == null) {
            logger.w { "Local payload is null for $table:$pk, accepting server version" }
            return MergeResult.AcceptServer
        }

        if (serverRow == null) {
            logger.w { "Server row is null for $table:$pk, keeping local version" }
            return MergeResult.KeepLocal(localPayload)
        }

        // Only call user resolver for legitimate business conflicts
        return resolver.merge(table, pk, serverRow, localPayload)
    }

    private suspend fun processAcceptServerDecision(
        db: SafeSQLiteConnection,
        change: ChangeUpload,
        status: ChangeUploadStatus,
        updatedTables: MutableSet<String>
    ) {
        status.serverRow?.let { server ->
            val obj =
                server as? JsonObject ?: Json.parseToJsonElement(server.toString()) as JsonObject
            val sv = obj["server_version"]!!.jsonPrimitive.content.toLong()
            val deleted = obj["deleted"]?.jsonPrimitive?.content == "true"

            if (!deleted) {
                upsertBusinessFromPayload(db, change.table, change.pk, obj["payload"])
            } else {
                db.prepare("DELETE FROM ${change.table.lowercase()} WHERE id=?").use { del ->
                    del.bindText(1, change.pk)
                    del.step()
                }
            }
            updatedTables += change.table
            updateRowMeta(db, change.table, change.pk, sv, deleted)
        }

        db.prepare("DELETE FROM _sync_pending WHERE table_name=? AND pk_uuid=?").use { st ->
            st.bindText(1, change.table)
            st.bindText(2, change.pk)
            st.step()
        }
    }

    private suspend fun processKeepLocalDecision(
        db: SafeSQLiteConnection,
        change: ChangeUpload,
        status: ChangeUploadStatus,
        decision: MergeResult.KeepLocal,
        updatedTables: MutableSet<String>
    ) {
        val serverObj = status.serverRow as? JsonObject
            ?: Json.parseToJsonElement(status.serverRow.toString()) as JsonObject
        val sv = serverObj["server_version"]!!.jsonPrimitive.content.toLong()

        logger.d { "Processing KeepLocal decision for ${change.table}:${change.pk}, op=${change.op}, server_v=$sv" }

        // CRITICAL FIX: Handle DELETE operations specially
        if (change.op == "DELETE") {
            logger.d { "DELETE conflict: maintaining local DELETE, updating server version to $sv" }

            // Ensure the record is deleted from business table
            db.prepare("DELETE FROM ${change.table.lowercase()} WHERE id=?").use { del ->
                del.bindText(1, change.pk)
                del.step()
            }

            // Update metadata to track server version but keep deleted=true
            updateRowMeta(db, change.table, change.pk, sv, true)

            // CRITICAL FIX: Re-enqueue DELETE with updated server version to propagate to server
            // This ensures other devices will receive the DELETE
            db.prepare(
                "UPDATE _sync_pending SET base_version=?, queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') " +
                "WHERE table_name=? AND pk_uuid=?"
            ).use { st ->
                st.bindLong(1, sv)
                st.bindText(2, change.table)
                st.bindText(3, change.pk)
                st.step()
            }

        } else {
            // Handle INSERT/UPDATE operations normally
            upsertBusinessFromPayload(db, change.table, change.pk, decision.mergedPayload)
            logger.d { "Applied merged payload for ${change.table}:${change.pk}" }

            // Update _sync_row_meta to reflect current server version
            updateRowMeta(db, change.table, change.pk, sv, false)

            // Update the pending change to retry with new base version
            db.prepare(
                "UPDATE _sync_pending SET op='UPDATE', base_version=?, payload=?, queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') " +
                "WHERE table_name=? AND pk_uuid=?"
            ).use { st ->
                st.bindLong(1, sv)
                st.bindText(2, decision.mergedPayload.toString())
                st.bindText(3, change.table)
                st.bindText(4, change.pk)
                st.step()
            }
        }

        updatedTables += change.table
    }

    private suspend fun processInvalidStatus(
        db: SafeSQLiteConnection,
        change: ChangeUpload,
        status: ChangeUploadStatus,
        invalidReasons: MutableMap<String, Int>
    ): Int {
        val reason = getInvalidReason(status)
        if (!reason.isNullOrEmpty()) {
            invalidReasons[reason] = (invalidReasons[reason] ?: 0) + 1
        }

        val keep = status.invalid?.toString()?.contains("fk_missing") == true
        if (!keep) {
            db.prepare("DELETE FROM _sync_pending WHERE table_name=? AND pk_uuid=?").use { st ->
                st.bindText(1, change.table)
                st.bindText(2, change.pk)
                st.step()
            }
        }

        return 1
    }

    private fun getInvalidReason(status: ChangeUploadStatus): String? {
        return try {
            (status.invalid as? JsonObject)?.get("reason")?.jsonPrimitive?.content
        } catch (_: Throwable) {
            null
        }
    }

    // Utility methods delegated to the client
    private suspend fun upsertBusinessFromPayload(
        db: SafeSQLiteConnection, table: String, pk: String, payload: JsonElement?
    ) = upsertBusinessFromPayload.invoke(db, table, pk, payload)

    private suspend fun updateRowMeta(
        db: SafeSQLiteConnection, table: String, pk: String, serverVersion: Long, deleted: Boolean
    ) = updateRowMeta.invoke(db, table, pk, serverVersion, deleted)


}
