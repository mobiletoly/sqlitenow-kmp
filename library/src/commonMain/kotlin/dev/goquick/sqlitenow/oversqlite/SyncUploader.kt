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
 *
 * The HttpClient should be pre-configured with authentication headers and base URL.
 */
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext

import io.ktor.client.statement.*
import io.ktor.http.isSuccess
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

internal class SyncUploader(
    private val http: HttpClient,
    private val config: OversqliteConfig,
    private val resolver: Resolver,
    private val upsertBusinessFromPayload: suspend (SafeSQLiteConnection, String, String, JsonElement?) -> Unit,
    private val updateRowMeta: suspend (SafeSQLiteConnection, String, String, Long, Boolean) -> Unit,
    private val ioDispatcher: CoroutineDispatcher,
) {
    private val uuidRegex = Regex("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
    data class PreparedUpload(
        val request: UploadRequest,
        val changes: List<ChangeUpload>,
        val pkOverride: Map<Long, String>,
    )

    /** Represents a pending change to be uploaded */
    private data class PendingChange(
        val table: String,
        val pk: String,
        val op: String,
        val baseVersion: Long,
        val payload: String?,
        var changeId: Long?
    )

    // New split-phase API: prepare (DB), perform (network), finalize (DB)
    suspend fun prepareUpload(
        db: SafeSQLiteConnection,
        nextChangeId: Long
    ): PreparedUpload {
        val pending = loadPendingChanges(db)

        if (config.verboseLogs) {
            logger.i { "[VERBOSE] SyncUploader: preparing upload with ${pending.size} pending changes" }
            pending.forEachIndexed { index, change ->
                logger.i { "[VERBOSE] SyncUploader: pending[$index] table=${change.table} op=${change.op} pk=${change.pk} baseVersion=${change.baseVersion}" }
                if (change.payload != null) {
                    logger.d { "[VERBOSE] SyncUploader: pending[$index] payload=${change.payload}" }
                }
            }
        }

        if (pending.isEmpty()) {
            // Return an empty request; callers can check changes.isEmpty() to skip network
            val lastServerSeq =
                db.prepare("SELECT last_server_seq_seen FROM _sync_client_info LIMIT 1")
                    .use { st -> if (st.step()) st.getLong(0) else 0L }

            if (config.verboseLogs) {
                logger.i { "[VERBOSE] SyncUploader: no pending changes, returning empty request with lastServerSeq=$lastServerSeq" }
            }

            return PreparedUpload(
                request = UploadRequest(lastServerSeqSeen = lastServerSeq, changes = emptyList()),
                changes = emptyList(),
                pkOverride = emptyMap(),
            )
        }

        assignChangeIds(db, pending, nextChangeId)
        val changes = buildChangeUploads(db, pending)
        val lastServerSeq = db.prepare("SELECT last_server_seq_seen FROM _sync_client_info LIMIT 1")
            .use { st -> if (st.step()) st.getLong(0) else 0L }
        val pkOverride = buildPkOverrides(db, changes)
        return PreparedUpload(
            request = UploadRequest(lastServerSeqSeen = lastServerSeq, changes = changes),
            changes = changes,
            pkOverride = pkOverride,
        )
    }

    suspend fun performUpload(prepared: PreparedUpload): Result<UploadResponse> {
        return withContext(ioDispatcher) {
            val wireRequest = prepared.request.copy(
                changes = prepared.request.changes.map { ch ->
                    val pk = prepared.pkOverride[ch.sourceChangeId] ?: ch.pk
                    ch.copy(pk = pk)
                }
            )

            if (config.verboseLogs) {
                logger.i { "[VERBOSE] SyncUploader: uploading ${wireRequest.changes.size} changes" }
                logger.i { "[VERBOSE] SyncUploader: lastServerSeqSeen=${wireRequest.lastServerSeqSeen}" }
                wireRequest.changes.forEachIndexed { index, change ->
                    logger.i { "[VERBOSE] SyncUploader: change[$index] table=${change.table} op=${change.op} pk=${change.pk} serverVersion=${change.serverVersion}" }
                    if (change.payload != null) {
                        logger.d { "[VERBOSE] SyncUploader: change[$index] payload=${change.payload}" }
                    }
                }
                logger.d { "[VERBOSE] SyncUploader: full request payload: $wireRequest" }
            }

            val call = http.post(config.uploadPath) {
                contentType(ContentType.Application.Json)
                setBody(wireRequest)
            }

            if (!call.status.isSuccess()) {
                val text = runCatching { call.bodyAsText() }.getOrElse { "" }
                if (config.verboseLogs) {
                    logger.e { "[VERBOSE] SyncUploader: upload failed with status=${call.status}" }
                    logger.e { "[VERBOSE] SyncUploader: error response body: $text" }
                }
                return@withContext Result.failure(UploadHttpException(call.status, text))
            }

            val response = runCatching { call.body<UploadResponse>() }
            if (config.verboseLogs && response.isSuccess) {
                val uploadResponse = response.getOrNull()
                logger.i { "[VERBOSE] SyncUploader: upload successful, accepted=${uploadResponse?.accepted}" }
                logger.i { "[VERBOSE] SyncUploader: highestServerSeq=${uploadResponse?.highestServerSeq}" }
                uploadResponse?.statuses?.forEachIndexed { index, status ->
                    logger.i { "[VERBOSE] SyncUploader: status[$index] sourceChangeId=${status.sourceChangeId} status=${status.status} newServerVersion=${status.newServerVersion}" }
                    if (status.message != null) {
                        logger.i { "[VERBOSE] SyncUploader: status[$index] message=${status.message}" }
                    }
                    if (status.invalid != null) {
                        logger.w { "[VERBOSE] SyncUploader: status[$index] invalid=${status.invalid}" }
                    }
                }
                logger.d { "[VERBOSE] SyncUploader: full response: $uploadResponse" }
            }

            response
        }
    }

    private suspend fun buildPkOverrides(
        db: SafeSQLiteConnection,
        changes: List<ChangeUpload>
    ): Map<Long, String> {
        val out = mutableMapOf<Long, String>()
        for (ch in changes) {
            val ti = TableInfoProvider.get(db, ch.table)
            if (ti.primaryKeyIsBlob) {
                // Convert BLOB UUID (stored as hex string) to UUID string for wire
                val uuidString = hexToUuidString(ch.pk)
                out[ch.sourceChangeId] = uuidString
            }
        }
        return out
    }

    suspend fun finalizeUpload(
        db: SafeSQLiteConnection,
        changes: List<ChangeUpload>,
        response: UploadResponse,
        updatedTables: MutableSet<String>
    ): UploadSummary {
        return processUploadResponse(db, changes, response, updatedTables)
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
                        payload = if (st.isNull(4)) null else st.getText(4),  // FIX: Handle NULL payload for DELETE operations
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
                db.prepare("UPDATE _sync_pending SET change_id=? WHERE table_name=? AND pk_uuid=?")
                    .use { st ->
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

    private suspend fun buildChangeUploads(
        db: SafeSQLiteConnection,
        pending: List<PendingChange>
    ): List<ChangeUpload> = pending.map { p ->
        val payloadElement = when (p.op) {
            "DELETE" -> null
            else -> resolvePayload(db, p)
        }
        // Important: keep local pk (as stored in _sync_pending.pk_uuid) in the ChangeUpload
        // so that DB-side finalization (deleting from _sync_pending, updating _sync_row_meta)
        // uses the correct identifier. We will override to wire format (UUID string for BLOB PKs)
        // only at network time via performUpload() using pkOverride.
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

    private suspend fun resolvePkWire(db: SafeSQLiteConnection, p: PendingChange): String {
        val tableLc = p.table.lowercase()
        val ti = TableInfoProvider.get(db, tableLc)
        val pkIsBlob = ti.primaryKeyIsBlob
        return if (pkIsBlob) {
            // Convert BLOB UUID (stored as hex string) to UUID string for wire
            hexToUuidString(p.pk)
        } else p.pk
    }

    private suspend fun resolvePayload(db: SafeSQLiteConnection, p: PendingChange): JsonElement? {
        val tableLc = p.table.lowercase()
        val ti = TableInfoProvider.get(db, tableLc)
        val hasBlob = ti.columns.any { it.declaredType.lowercase().contains("blob") }
        val raw = p.payload?.takeIf { it.isNotBlank() }?.let { runCatching { Json.parseToJsonElement(it) }.getOrNull() }
        return if (raw != null) {
            if (!hasBlob || raw !is JsonObject) raw else {
                buildJsonObject {
                    val pkLower = ti.primaryKey?.name?.lowercase() ?: "id"
                    raw.keys.forEach { k ->
                        val keyLc = k.lowercase()
                        val v = raw[k]
                        val isBlobCol = ti.typesByNameLower[keyLc]?.lowercase()?.contains("blob") == true
                        if (isBlobCol && v != null && v !is JsonNull) {
                            val content = v.jsonPrimitive.content
                            if (keyLc == pkLower) {
                                // Primary key: convert BLOB UUID to UUID string for wire
                                val uuid = if (uuidRegex.matches(content)) content.lowercase() else hexToUuidString(content)
                                put(keyLc, JsonPrimitive(uuid))
                            } else {
                                // Other BLOB columns: convert hex to Base64 for wire protocol
                                val bytes = hexToBytes(content)
                                put(keyLc, JsonPrimitive(Base64.encode(bytes)))
                            }
                        } else {
                            put(keyLc, v ?: JsonNull)
                        }
                    }
                }
            }
        } else {
            buildRowPayload(db, p.table, p.pk)
        }
    }

    // Column metadata access via shared provider
    private suspend fun getTableColumns(db: SafeSQLiteConnection, table: String): List<String> =
        TableInfoProvider.get(db, table).columnNamesLower

    private suspend fun getTableColumnTypes(db: SafeSQLiteConnection, table: String): Map<String, String> =
        TableInfoProvider.get(db, table).typesByNameLower

    @OptIn(ExperimentalEncodingApi::class)
    private suspend fun buildRowPayload(
        db: SafeSQLiteConnection,
        table: String,
        pk: String
    ): JsonElement {
        val tableLc = table.lowercase()
        val colsAll = getTableColumns(db, tableLc)
        val typeMap = getTableColumnTypes(db, tableLc)
        if (colsAll.isEmpty()) return JsonNull
        val (pkCol, pkDecl) = getPrimaryKeyInfo(db, tableLc)
        val pkIsBlob = pkDecl.lowercase().contains("blob")
        val cols = colsAll.filter { it != pkCol.lowercase() }
        val sql = "SELECT ${cols.joinToString(", ")} FROM $tableLc WHERE $pkCol=?"
        return db.prepare(sql).use { st ->
            if (pkIsBlob) st.bindBlob(1, uuidHexToBytes(pk)) else st.bindText(1, pk)
            if (!st.step()) return@use JsonNull
            buildJsonObject {
                // Include PK in payload as UUID string for wire (convert BLOB UUID to string)
                if (pkIsBlob) put(pkCol.lowercase(), JsonPrimitive(hexToUuidString(pk))) else put(pkCol.lowercase(), JsonPrimitive(pk))
                cols.forEachIndexed { idx, name ->
                    if (st.isNull(idx)) {
                        put(name, JsonNull)
                    } else {
                        val t = (typeMap[name] ?: "").lowercase()
                        when {
                            t.contains("blob") -> {
                                val bytes = st.getBlob(idx)
                                // Convert BLOB to Base64 for non-UUID columns
                                put(name, JsonPrimitive(Base64.encode(bytes)))
                            }
                            t.contains("int") -> put(name, JsonPrimitive(st.getLong(idx)))
                            t.contains("real") || t.contains("float") || t.contains("double") ->
                                put(name, JsonPrimitive(st.getDouble(idx)))
                            else -> put(name, JsonPrimitive(st.getText(idx)))
                        }
                    }
                }
            }
        }
    }

    private suspend fun getPrimaryKeyInfo(db: SafeSQLiteConnection, table: String): Pair<String, String> {
        val ti = TableInfoProvider.get(db, table)
        val pk = ti.primaryKey
        return (pk?.name ?: "id") to (pk?.declaredType ?: "")
    }

    private suspend fun deleteBusinessRow(
        db: SafeSQLiteConnection,
        table: String,
        pkLocal: String
    ) {
        val tableLc = table.lowercase()
        val (pkCol, pkDecl) = getPrimaryKeyInfo(db, tableLc)
        val isBlob = pkDecl.lowercase().contains("blob")
        val sql = "DELETE FROM $tableLc WHERE $pkCol=?"
        db.prepare(sql).use { st ->
            if (isBlob) {
                st.bindBlob(1, uuidHexToBytes(pkLocal))
            } else {
                st.bindText(1, pkLocal)
            }
            st.step()
        }
    }

    /**
     * Convert hex string to ByteArray for BLOB binding
     */
    private fun hexToBytes(hex: String): ByteArray {
        val clean = hex.trim().removePrefix("0x")
        require(clean.length % 2 == 0) { "Hex string must have even length" }
        val out = ByteArray(clean.length / 2)
        var i = 0
        while (i < clean.length) {
            out[i/2] = clean.substring(i, i+2).toInt(16).toByte()
            i += 2
        }
        return out
    }

    /**
     * Convert UUID hex string to ByteArray for UUID BLOB binding using kotlin.uuid.Uuid
     */
    @OptIn(ExperimentalUuidApi::class)
    private fun uuidHexToBytes(hex: String): ByteArray {
        val clean = hex.trim().removePrefix("0x")
        require(clean.length == 32) { "Expected 16-byte hex for UUID, got ${clean.length} chars" }
        return Uuid.parseHex(clean).toByteArray()
    }

    /**
     * Convert ByteArray to hex string using kotlin.uuid.Uuid
     */
    @OptIn(ExperimentalUuidApi::class)
    private fun bytesToHex(bytes: ByteArray): String {
        require(bytes.size == 16) { "Expected 16-byte array for UUID, got ${bytes.size} bytes" }
        return Uuid.fromByteArray(bytes).toHexString()
    }

    /**
     * Convert hex string (from BLOB UUID) to UUID string for wire protocol using kotlin.uuid.Uuid
     */
    @OptIn(ExperimentalUuidApi::class)
    private fun hexToUuidString(hex: String): String {
        val clean = hex.trim().removePrefix("0x")

        // If it's already a UUID string (36 chars with dashes), return it directly
        if (clean.length == 36 && uuidRegex.matches(clean)) {
            return clean.lowercase()
        }

        // Handle case where hex might be a hex-encoded UUID string (72 chars = UUID string as hex)
        if (clean.length == 72) {
            // This might be a hex-encoded UUID string, try to decode it
            try {
                val decoded = hexToBytes(clean).decodeToString()
                if (uuidRegex.matches(decoded)) {
                    return decoded.lowercase()
                }
            } catch (e: Exception) {
                // Fall through to treat as raw hex
            }
        }

        // Handle normal case: 32 hex chars = 16 UUID bytes
        require(clean.length == 32) { "Expected 16-byte hex for UUID, got ${clean.length} chars. Input: '$hex'" }
        return Uuid.parseHex(clean).toString()
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
        // Handle DELETE operations specially to prevent record resurrection
        if (localOp == "DELETE") {
            // For DELETE operations, we create a special KeepLocal result with null payload
            // This will be handled by processKeepLocalDecision to maintain the DELETE
            return MergeResult.KeepLocal(localPayload ?: JsonNull)
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
                deleteBusinessRow(db, change.table, change.pk)
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

        // Handle DELETE operations specially
        if (change.op == "DELETE") {
            logger.d { "DELETE conflict: maintaining local DELETE, updating server version to $sv" }

            // Ensure the record is deleted from business table
            deleteBusinessRow(db, change.table, change.pk)

            // Update metadata to track server version but keep deleted=true
            updateRowMeta(db, change.table, change.pk, sv, true)

            // Re-enqueue DELETE with updated server version to propagate to server
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

    private suspend fun upsertBusinessFromPayload(
        db: SafeSQLiteConnection, table: String, pk: String, payload: JsonElement?
    ) = upsertBusinessFromPayload.invoke(db, table, pk, payload)

    private suspend fun updateRowMeta(
        db: SafeSQLiteConnection, table: String, pk: String, serverVersion: Long, deleted: Boolean
    ) = updateRowMeta.invoke(db, table, pk, serverVersion, deleted)
}
