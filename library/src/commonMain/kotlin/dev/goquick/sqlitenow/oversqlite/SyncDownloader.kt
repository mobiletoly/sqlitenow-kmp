package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.common.logger
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import kotlinx.serialization.json.*

/**
 * Handles the download process for sync operations.
 * Separated from DefaultOversqliteClient to improve maintainability.
 */
internal class SyncDownloader(
    private val http: HttpClient,
    private val baseUrl: String,
    private val config: OversqliteConfig,
    private val tokenProvider: suspend () -> String,
    private val resolver: Resolver,
    private val upsertBusinessFromPayload: suspend (SafeSQLiteConnection, String, String, JsonElement?) -> Unit,
    private val updateRowMeta: suspend (SafeSQLiteConnection, String, String, Long, Boolean) -> Unit
) {

    data class DownloadResult(
        val applied: Int,
        val nextAfter: Long,
        val updatedTables: Set<String>
    )

    suspend fun downloadOnce(
        db: SafeSQLiteConnection,
        limit: Int,
        includeSelf: Boolean,
        until: Long = 0L,
        isPostUploadLookback: Boolean = false
    ): DownloadResult {
        logger.d { "downloadOnce: start limit=$limit includeSelf=$includeSelf until=$until isPostUploadLookback=$isPostUploadLookback" }

        // 1. Get client info
        val clientInfo = getClientInfo(db)

        // 2. Fetch changes from server
        val response =
            fetchChanges(clientInfo.sourceId, clientInfo.lastServerSeq, limit, includeSelf, until)
        logger.d { "downloadOnce: changes=${response.changes.size} nextAfter=${response.nextAfter} windowUntil=${response.windowUntil}" }

        // 3. Handle empty response
        if (response.changes.isEmpty()) {
            if (response.nextAfter > clientInfo.lastServerSeq) {
                db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
                    st.bindLong(1, response.nextAfter)
                    st.step()
                }
            }
            return DownloadResult(0, response.nextAfter, emptySet())
        }

        // 4. Apply changes
        val updatedTables = mutableSetOf<String>()
        val (applied, nextAfter) = applyDownloadPage(
            db = db,
            resp = response,
            includeSelf = includeSelf,
            sourceId = clientInfo.sourceId,
            previousAfter = clientInfo.lastServerSeq,
            onTableTouched = { updatedTables += it },
            isPostUploadLookback = isPostUploadLookback
        )

        logger.d { "downloadOnce: applied=$applied nextAfter=$nextAfter" }
        return DownloadResult(applied, nextAfter, updatedTables)
    }

    suspend fun hydrate(
        db: SafeSQLiteConnection,
        includeSelf: Boolean,
        limit: Int,
        windowed: Boolean
    ): Set<String> {
        logger.i { "hydrate: start includeSelf=$includeSelf limit=$limit windowed=$windowed" }

        val clientInfo = getClientInfo(db)
        var after = clientInfo.lastServerSeq
        var frozenUntil = 0L
        var first = true
        val allUpdatedTables = mutableSetOf<String>()

        while (true) {
            val untilParam = if (windowed) frozenUntil else 0L
            val response = fetchChanges(clientInfo.sourceId, after, limit, includeSelf, untilParam)

            if (first) {
                frozenUntil = if (windowed) response.windowUntil else 0L
                if (frozenUntil > 0) db.execSQL("UPDATE _sync_client_info SET current_window_until=$frozenUntil")
                logger.d { "hydrate: windowUntil=$frozenUntil" }
                first = false
            }

            if (response.changes.isEmpty()) {
                if (response.nextAfter > after) {
                    db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
                        st.bindLong(1, response.nextAfter)
                        st.step()
                    }
                    after = response.nextAfter
                }
            } else {
                val updatedTables = mutableSetOf<String>()
                val (pageApplied, nextAfter) = applyDownloadPage(
                    db = db,
                    resp = response,
                    includeSelf = includeSelf,
                    sourceId = clientInfo.sourceId,
                    previousAfter = after,
                    onTableTouched = { updatedTables += it }
                )
                allUpdatedTables += updatedTables
                after = nextAfter
                logger.d { "hydrate: page applied=$pageApplied nextAfter=$after hasMore=${response.hasMore}" }
            }

            if (!response.hasMore) break
        }

        if (frozenUntil > 0L) db.execSQL("UPDATE _sync_client_info SET current_window_until=0")
        logger.i { "hydrate: done" }

        return allUpdatedTables
    }

    private data class ClientInfo(
        val userId: String,
        val sourceId: String,
        val lastServerSeq: Long
    )

    private suspend fun getClientInfo(db: SafeSQLiteConnection): ClientInfo {
        return db.prepare("SELECT user_id, source_id, last_server_seq_seen FROM _sync_client_info LIMIT 1")
            .use { st ->
                if (st.step()) {
                    ClientInfo(st.getText(0), st.getText(1), st.getLong(2))
                } else {
                    error("client info missing")
                }
            }
    }

    private suspend fun fetchChanges(
        sourceId: String,
        lastServerSeq: Long,
        limit: Int,
        includeSelf: Boolean,
        until: Long
    ): DownloadResponse {
        val token = tokenProvider()
        val url = buildDownloadUrl(baseUrl, lastServerSeq, limit, includeSelf, until, config.schema)
        logger.d { "Download request: after=$lastServerSeq, limit=$limit, includeSelf=$includeSelf, until=$until" }

        val response = http.get(url) { header("Authorization", "Bearer $token") }.body<DownloadResponse>()
        logger.d { "Download response: ${response.changes.size} changes, nextAfter=${response.nextAfter}" }

        return response
    }

    // Apply a single download page atomically and advance cursor
    private suspend fun applyDownloadPage(
        db: SafeSQLiteConnection,
        resp: DownloadResponse,
        includeSelf: Boolean,
        sourceId: String,
        previousAfter: Long,
        onTableTouched: (String) -> Unit = {},
        isPostUploadLookback: Boolean = false
    ): Pair<Int, Long> = inApplyTx(db) {
        var applied = 0

        // DELETE operations that are superseded by later INSERT/UPDATE operations in the same batch
        val changesToApply = if (isPostUploadLookback) {
            optimizeChangeSequenceForLookback(resp.changes)
        } else {
            resp.changes
        }

        changesToApply.forEach { ch ->
            onTableTouched(ch.tableName.lowercase())
            if (!includeSelf && ch.sourceId == sourceId) {
                logger.d { "applyDownloadPage: skipping self change ${ch.op} ${ch.tableName}:${ch.pk} from sourceId=$sourceId" }
                return@forEach
            }
            logger.d { "applyDownloadPage: applying ${ch.op} ${ch.tableName}:${ch.pk} from sourceId=${ch.sourceId}" }

            when (ch.op) {
                "DELETE" -> {
                    applyDeleteChange(db, ch)
                }

                "INSERT", "UPDATE" -> {
                    applyInsertUpdateChange(db, ch, isPostUploadLookback)
                }

                else -> error("unknown op: ${ch.op}")
            }
            applied++
        }
        if (resp.nextAfter > previousAfter) {
            db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
                st.bindLong(1, resp.nextAfter)
                st.step()
            }
        }
        applied to resp.nextAfter
    }

    private suspend fun applyDeleteChange(db: SafeSQLiteConnection, ch: ChangeDownloadResponse) {
        logger.d { "Applying DELETE: ${ch.tableName}:${ch.pk.take(8)} v${ch.serverVersion}" }

        db.prepare("DELETE FROM ${ch.tableName.lowercase()} WHERE id=?").use { del ->
            del.bindText(1, ch.pk)
            del.step()
        }

        updateRowMeta(db, ch.tableName, ch.pk, ch.serverVersion, true)
    }

    private suspend fun applyInsertUpdateChange(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse,
        isPostUploadLookback: Boolean
    ) {
        // Check if there's a local DELETE pending OR if we recently uploaded a DELETE
        val hasLocalDelete = checkLocalDelete(db, ch)
        val recentlyDeleted = if (!hasLocalDelete) checkRecentlyDeleted(db, ch) else false
        val localPendingChange = if (!hasLocalDelete && !recentlyDeleted) {
            checkLocalPendingChange(db, ch)
        } else null

        when {
            hasLocalDelete || recentlyDeleted -> {
                handleLocalDeleteWins(db, ch, hasLocalDelete, recentlyDeleted)
            }

            localPendingChange != null -> {
                handleConflict(db, ch, localPendingChange)
            }

            else -> {
                handleNormalApply(db, ch, isPostUploadLookback)
            }
        }
    }

    private suspend fun checkLocalDelete(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse
    ): Boolean {
        return db.prepare("SELECT EXISTS(SELECT 1 FROM _sync_pending WHERE table_name=? AND pk_uuid=? AND op='DELETE')")
            .use { st ->
                st.bindText(1, ch.tableName.lowercase())
                st.bindText(2, ch.pk)
                st.step() && st.getLong(0) == 1L
            }
    }

    private suspend fun checkRecentlyDeleted(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse
    ): Boolean {
        return db.prepare("SELECT deleted, server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?")
            .use { st ->
                st.bindText(1, ch.tableName.lowercase())
                st.bindText(2, ch.pk)
                if (st.step()) {
                    val deleted = st.getLong(0) == 1L
                    val localServerVersion = st.getLong(1)
                    // CRITICAL FIX: Only consider recently deleted if server version is not newer
                    // If server version is newer, we should accept the server's reinsertion
                    deleted && ch.serverVersion <= localServerVersion
                } else {
                    false
                }
            }
    }

    private suspend fun checkLocalPendingChange(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse
    ): Pair<String, String>? {
        return db.prepare("SELECT op, payload FROM _sync_pending WHERE table_name=? AND pk_uuid=? AND op IN ('INSERT', 'UPDATE')")
            .use { st ->
                st.bindText(1, ch.tableName.lowercase())
                st.bindText(2, ch.pk)
                if (st.step()) {
                    st.getText(0) to (if (st.isNull(1)) "" else st.getText(1))  // Defensive: handle potential NULL payload
                } else null
            }
    }

    private suspend fun handleLocalDeleteWins(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse,
        hasLocalDelete: Boolean,
        recentlyDeleted: Boolean
    ) {
        // Local DELETE wins - don't apply server INSERT/UPDATE
        logger.d { "Skipping server ${ch.op} v${ch.serverVersion} for ${ch.tableName}:${ch.pk} - local DELETE wins (pending=$hasLocalDelete, recent=$recentlyDeleted)" }
        // Update meta to track that we've seen this server version but didn't apply it
        updateRowMeta(db, ch.tableName, ch.pk, ch.serverVersion, true)
    }

    private suspend fun handleConflict(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse,
        localPendingChange: Pair<String, String>
    ) {
        // There's a local pending INSERT/UPDATE - this is a conflict that needs resolution
        logger.d { "Conflict detected: server ${ch.op} v${ch.serverVersion} vs local ${localPendingChange.first} for ${ch.tableName}:${ch.pk}" }

        // Create a server row object for conflict resolution
        val serverRowJson = buildJsonObject {
            put("server_version", JsonPrimitive(ch.serverVersion))
            put("deleted", JsonPrimitive(false))
            put("payload", ch.payload ?: JsonNull)
        }

        val localPayloadJson = try {
            Json.parseToJsonElement(localPendingChange.second)
        } catch (e: Exception) {
            logger.w { "Failed to parse local payload for conflict resolution: ${localPendingChange.second}" }
            null
        }

        val decision = resolveConflictWithFallbacks(
            table = ch.tableName,
            pk = ch.pk,
            serverRow = serverRowJson,
            localPayload = localPayloadJson
        )

        when (decision) {
            is MergeResult.AcceptServer -> {
                logger.d { "Conflict resolution: accepting server version for ${ch.tableName}:${ch.pk}" }
                upsertBusinessFromPayload(db, ch.tableName, ch.pk, ch.payload)
                updateRowMeta(db, ch.tableName, ch.pk, ch.serverVersion, false)
                // Remove the local pending change since we're accepting server
                db.prepare("DELETE FROM _sync_pending WHERE table_name=? AND pk_uuid=?").use { st ->
                    st.bindText(1, ch.tableName.lowercase())
                    st.bindText(2, ch.pk)
                    st.step()
                }
            }

            is MergeResult.KeepLocal -> {
                logger.d { "Conflict resolution: keeping local version for ${ch.tableName}:${ch.pk}" }
                upsertBusinessFromPayload(db, ch.tableName, ch.pk, decision.mergedPayload)
                // Update the pending change to use the server version as base
                db.prepare(
                    "UPDATE _sync_pending SET op='UPDATE', base_version=?, payload=?, queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') " +
                    "WHERE table_name=? AND pk_uuid=?"
                ).use { st ->
                    st.bindLong(1, ch.serverVersion)
                    st.bindText(2, decision.mergedPayload.toString())
                    st.bindText(3, ch.tableName.lowercase())
                    st.bindText(4, ch.pk)
                    st.step()
                }
                // CRITICAL FIX: Update row meta to reflect server version and that record exists
                // This ensures _sync_row_meta is consistent with the server state
                updateRowMeta(db, ch.tableName, ch.pk, ch.serverVersion, false)
            }
        }
    }

    /**
     * Resolve conflicts with automatic handling of infrastructure edge cases.
     * Only calls the user resolver for legitimate business conflicts.
     */
    private fun resolveConflictWithFallbacks(
        table: String,
        pk: String,
        serverRow: JsonElement?,
        localPayload: JsonElement?
    ): MergeResult {
        // Handle infrastructure edge cases automatically
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

    private suspend fun handleNormalApply(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse,
        isPostUploadLookback: Boolean
    ) {
        // Only apply version checking during post-upload lookback to prevent older changes from overwriting newer local changes
        if (isPostUploadLookback) {
            // Check if incoming server version is newer than current local version
            val currentServerVersion =
                db.prepare("SELECT server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?")
                    .use { st ->
                        st.bindText(1, ch.tableName)
                        st.bindText(2, ch.pk)
                        if (st.step()) st.getLong(0) else 0L
                    }

            if (ch.serverVersion > currentServerVersion) {
                logger.d { "Applying server ${ch.op} v${ch.serverVersion} for ${ch.tableName}:${ch.pk} (current v$currentServerVersion)" }
                upsertBusinessFromPayload(db, ch.tableName, ch.pk, ch.payload)
                updateRowMeta(db, ch.tableName, ch.pk, ch.serverVersion, false)
            } else {
                logger.d { "Skipping server ${ch.op} v${ch.serverVersion} for ${ch.tableName}:${ch.pk} - not newer than current v$currentServerVersion" }
                // Still update meta to track that we've seen this server version
                if (ch.serverVersion > currentServerVersion) {
                    updateRowMeta(db, ch.tableName, ch.pk, ch.serverVersion, false)
                }
            }
        } else {
            // Normal download - apply without version checking
            upsertBusinessFromPayload(db, ch.tableName, ch.pk, ch.payload)
            updateRowMeta(db, ch.tableName, ch.pk, ch.serverVersion, false)
        }
    }

    // Utility methods
    private suspend fun inApplyTx(
        db: SafeSQLiteConnection,
        block: suspend () -> Pair<Int, Long>
    ): Pair<Int, Long> {
        db.execSQL("BEGIN")
        try {
            db.execSQL("PRAGMA defer_foreign_keys = ON")
            db.execSQL("UPDATE _sync_client_info SET apply_mode=1")
            val result = block()
            db.execSQL("UPDATE _sync_client_info SET apply_mode=0")
            db.execSQL("COMMIT")
            return result
        } catch (t: Throwable) {
            try {
                db.execSQL("ROLLBACK")
            } catch (_: Throwable) {
            }
            db.execSQL("UPDATE _sync_client_info SET apply_mode=0")
            throw t
        }
    }

    /**
     * Optimize change sequence during post-upload lookback to prevent DELETE operations
     * from overriding conflict resolution results when there are later INSERT/UPDATE operations
     * for the same record in the same batch.
     */
    private fun optimizeChangeSequenceForLookback(changes: List<ChangeDownloadResponse>): List<ChangeDownloadResponse> {
        if (changes.isEmpty()) return changes

        // Group changes by table and primary key
        val changesByRecord = changes.groupBy { "${it.tableName}:${it.pk}" }

        val optimizedChanges = mutableListOf<ChangeDownloadResponse>()

        changesByRecord.forEach { (recordKey, recordChanges) ->
            if (recordChanges.size == 1) {
                // Single change for this record - include it
                optimizedChanges.addAll(recordChanges)
            } else {
                // Multiple changes for the same record - optimize the sequence
                // Sort by original order in server response to maintain chronological sequence
                val sortedChanges = recordChanges.sortedBy { changes.indexOf(it) }

                sortedChanges.forEachIndexed { index, change ->
                    if (change.op == "DELETE") {
                        // Check if there's a later INSERT/UPDATE after this DELETE in the chronological sequence
                        val hasLaterInsertOrUpdate = sortedChanges.drop(index + 1).any {
                            it.op in listOf("INSERT", "UPDATE") && it.serverVersion > change.serverVersion
                        }

                        if (hasLaterInsertOrUpdate) {
                            // Skip DELETE if there's a later INSERT/UPDATE with higher server version for the same record
                            logger.d { "Skipping DELETE during lookback: ${change.tableName}:${change.pk.take(8)} v${change.serverVersion} (superseded by later INSERT/UPDATE)" }
                        } else {
                            optimizedChanges.add(change)
                        }
                    } else {
                        optimizedChanges.add(change)
                    }
                }
            }
        }

        // Sort by serverVersion to maintain chronological order (serverId is not chronological)
        return optimizedChanges.sortedBy { it.serverVersion }
    }

    /**
     * Build the download URL with mandatory schema filter and optional include_self/until parameters.
     * The server requires schema names to match ^[a-z0-9_]+$.
     */
    private fun buildDownloadUrl(
        baseUrl: String,
        after: Long,
        limit: Int,
        includeSelf: Boolean,
        until: Long,
        schema: String
    ): String {
        val sb = StringBuilder()
        sb.append(baseUrl.trimEnd('/'))
            .append("/sync/download?after=$after&limit=$limit&schema=$schema")
        if (includeSelf) sb.append("&include_self=true")
        if (until > 0L) sb.append("&until=").append(until)
        return sb.toString()
    }
}
