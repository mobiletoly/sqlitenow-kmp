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

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.common.sqliteNowLogger
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import kotlinx.serialization.json.*

/**
 * Handles the download process for sync operations.
 * Separated from DefaultOversqliteClient to improve maintainability.
 *
 * The HttpClient should be pre-configured with authentication headers and base URL.
 */
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import io.ktor.client.statement.bodyAsText
import io.ktor.http.isSuccess
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid


internal class SyncDownloader(
    private val http: HttpClient,
    private val config: OversqliteConfig,
    private val resolver: Resolver,
    private val upsertBusinessFromPayload: suspend (SafeSQLiteConnection, String, String, JsonElement?) -> Unit,
    private val updateRowMeta: suspend (SafeSQLiteConnection, String, String, Long, Boolean) -> Unit,
    private val ioDispatcher: CoroutineDispatcher,
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
        if (config.verboseLogs) {
            sqliteNowLogger.i { "SyncDownloader: starting download limit=$limit includeSelf=$includeSelf until=$until isPostUploadLookback=$isPostUploadLookback" }
        } else {
            sqliteNowLogger.d { "downloadOnce: start limit=$limit includeSelf=$includeSelf until=$until isPostUploadLookback=$isPostUploadLookback" }
        }

        // 1. Get client info
        val clientInfo = getClientInfo(db)

        if (config.verboseLogs) {
            sqliteNowLogger.i { "SyncDownloader: client info sourceId=${clientInfo.sourceId} lastServerSeq=${clientInfo.lastServerSeq}" }
        }

        // 2. Fetch changes from server
        val response = fetchChanges(clientInfo.sourceId, clientInfo.lastServerSeq, limit, includeSelf, until)
            .getOrElse { throw it }

        if (config.verboseLogs) {
            sqliteNowLogger.i { "SyncDownloader: received ${response.changes.size} changes, nextAfter=${response.nextAfter} windowUntil=${response.windowUntil}" }
            response.changes.forEachIndexed { index, change ->
                sqliteNowLogger.i { "SyncDownloader: change[$index] serverId=${change.serverId} table=${change.tableName} op=${change.op} pk=${change.pk} serverVersion=${change.serverVersion} deleted=${change.deleted}" }
                if (change.payload != null) {
                    sqliteNowLogger.d { "SyncDownloader: change[$index] payload=${change.payload}" }
                }
            }
        } else {
            sqliteNowLogger.d { "downloadOnce: changes=${response.changes.size} nextAfter=${response.nextAfter} windowUntil=${response.windowUntil}" }
        }

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

        sqliteNowLogger.d { "downloadOnce: applied=$applied nextAfter=$nextAfter" }
        return DownloadResult(applied, nextAfter, updatedTables)
    }

    suspend fun hydrate(
        db: SafeSQLiteConnection,
        includeSelf: Boolean,
        limit: Int,
        windowed: Boolean
    ): Set<String> {
        sqliteNowLogger.i { "hydrate: start includeSelf=$includeSelf limit=$limit windowed=$windowed" }

        val clientInfo = getClientInfo(db)
        var after = clientInfo.lastServerSeq
        var frozenUntil = 0L
        var first = true
        val allUpdatedTables = mutableSetOf<String>()

        while (true) {
            val untilParam = if (windowed) frozenUntil else 0L
            val response = fetchChanges(clientInfo.sourceId, after, limit, includeSelf, untilParam)
                .getOrElse { throw it }

            if (first) {
                frozenUntil = if (windowed) response.windowUntil else 0L
                if (frozenUntil > 0) db.execSQL("UPDATE _sync_client_info SET current_window_until=$frozenUntil")
                sqliteNowLogger.d { "hydrate: windowUntil=$frozenUntil" }
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
                sqliteNowLogger.d { "hydrate: page applied=$pageApplied nextAfter=$after hasMore=${response.hasMore}" }
            }

            if (!response.hasMore) break
        }

        if (frozenUntil > 0L) db.execSQL("UPDATE _sync_client_info SET current_window_until=0")
        sqliteNowLogger.i { "hydrate: done" }

        return allUpdatedTables
    }

    internal data class ClientInfo(
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

    // Split-phase API helpers
    internal suspend fun getClientInfoNow(db: SafeSQLiteConnection): ClientInfo = getClientInfo(db)

    internal suspend fun fetchChangesNow(
        sourceId: String,
        lastServerSeq: Long,
        limit: Int,
        includeSelf: Boolean,
        until: Long
    ): Result<DownloadResponse> = fetchChanges(sourceId, lastServerSeq, limit, includeSelf, until)

    internal suspend fun applyDownloadedPage(
        db: SafeSQLiteConnection,
        response: DownloadResponse,
        includeSelf: Boolean,
        sourceId: String,
        previousAfter: Long,
        isPostUploadLookback: Boolean = false
    ): DownloadResult {
        val updatedTables = mutableSetOf<String>()
        val (applied, nextAfter) = applyDownloadPage(
            db = db,
            resp = response,
            includeSelf = includeSelf,
            sourceId = sourceId,
            previousAfter = previousAfter,
            onTableTouched = { updatedTables += it },
            isPostUploadLookback = isPostUploadLookback
        )
        return DownloadResult(applied, nextAfter, updatedTables)
    }

    private suspend fun fetchChanges(
        sourceId: String,
        lastServerSeq: Long,
        limit: Int,
        includeSelf: Boolean,
        until: Long
    ): Result<DownloadResponse> {
        val url = buildDownloadUrl(lastServerSeq, limit, includeSelf, until, config.schema)

        if (config.verboseLogs) {
            sqliteNowLogger.i { "SyncDownloader: fetching changes from server" }
            sqliteNowLogger.i { "SyncDownloader: request URL: $url" }
            sqliteNowLogger.i { "SyncDownloader: parameters - after=$lastServerSeq, limit=$limit, includeSelf=$includeSelf, until=$until, schema=${config.schema}" }
        } else {
            sqliteNowLogger.d { "Download request: after=$lastServerSeq, limit=$limit, includeSelf=$includeSelf, until=$until" }
        }

        // Perform network I/O on injected IO dispatcher to avoid blocking db.dispatcher
        return withContext(ioDispatcher) {
            val call = http.get(url)
            if (!call.status.isSuccess()) {
                val text = runCatching { call.bodyAsText() }.getOrElse { "" }
                if (config.verboseLogs) {
                    sqliteNowLogger.e { "SyncDownloader: download failed with status=${call.status}" }
                    sqliteNowLogger.e { "SyncDownloader: error response body: $text" }
                }
                return@withContext Result.failure(DownloadHttpException(call.status, text))
            }

            val response = runCatching { call.body<DownloadResponse>() }
            if (config.verboseLogs && response.isSuccess) {
                val downloadResponse = response.getOrNull()
                sqliteNowLogger.i { "SyncDownloader: download successful, received ${downloadResponse?.changes?.size} changes" }
                sqliteNowLogger.d { "SyncDownloader: full response: $downloadResponse" }
            }

            response
        }
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

        if (config.verboseLogs) {
            sqliteNowLogger.i { "SyncDownloader: applying ${changesToApply.size} changes (optimized from ${resp.changes.size})" }
            sqliteNowLogger.i { "SyncDownloader: includeSelf=$includeSelf, sourceId=$sourceId, isPostUploadLookback=$isPostUploadLookback" }
        }

        changesToApply.forEach { ch ->
            onTableTouched(ch.tableName.lowercase())
            if (!includeSelf && ch.sourceId == sourceId) {
                if (config.verboseLogs) {
                    sqliteNowLogger.d { "SyncDownloader: skipping self change serverId=${ch.serverId} table=${ch.tableName} op=${ch.op}" }
                }
                return@forEach
            }

            if (config.verboseLogs) {
                sqliteNowLogger.i { "SyncDownloader: applying change serverId=${ch.serverId} table=${ch.tableName} op=${ch.op} pk=${ch.pk} serverVersion=${ch.serverVersion}" }
                if (ch.payload != null) {
                    sqliteNowLogger.d { "SyncDownloader: change payload=${ch.payload}" }
                }
            }

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
        val tableLc = ch.tableName.lowercase()
        val (pkCol, pkDecl) = getPrimaryKeyInfo(db, tableLc)
        val pkIsBlob = pkDecl.lowercase().contains("blob")
        val pkUuidForMeta: String
        if (pkIsBlob) {
            // Server sends UUID string, convert to bytes for BLOB storage and hex for meta
            val bytes = uuidStringToBytes(ch.pk)
            pkUuidForMeta = bytesToHexLower(bytes)
            db.prepare("DELETE FROM $tableLc WHERE $pkCol=?").use { del ->
                del.bindBlob(1, bytes)
                del.step()
            }
        } else {
            pkUuidForMeta = ch.pk
            db.prepare("DELETE FROM $tableLc WHERE $pkCol=?").use { del ->
                del.bindText(1, ch.pk)
                del.step()
            }
        }
        updateRowMeta(db, ch.tableName, pkUuidForMeta, ch.serverVersion, true)
    }

    // Helpers for PK handling
    private suspend fun getPrimaryKeyInfo(db: SafeSQLiteConnection, table: String): Pair<String, String> {
        val ti = TableInfoProvider.get(db, table)
        val pk = ti.primaryKey
        return (pk?.name ?: "id") to (pk?.declaredType ?: "")
    }

    /**
     * Convert UUID string from wire protocol to ByteArray for BLOB storage using kotlin.uuid.Uuid
     * Server always sends UUID strings, we convert to bytes for BLOB columns
     */
    @OptIn(ExperimentalUuidApi::class)
    private fun uuidStringToBytes(uuidString: String): ByteArray {
        return Uuid.parse(uuidString).toByteArray()
    }

    /**
     * Convert ByteArray to hex string for internal storage using kotlin.uuid.Uuid
     */
    @OptIn(ExperimentalUuidApi::class)
    private fun bytesToHexLower(bytes: ByteArray): String {
        require(bytes.size == 16) { "Expected 16-byte array for UUID, got ${bytes.size} bytes" }
        return Uuid.fromByteArray(bytes).toHexString().lowercase()
    }

    /**
     * Convert UUID string from wire to hex string for internal storage using kotlin.uuid.Uuid
     */
    @OptIn(ExperimentalUuidApi::class)
    private fun uuidStringToHex(uuidString: String): String {
        return Uuid.parse(uuidString).toHexString().lowercase()
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
        val tableLc = ch.tableName.lowercase()
        val (pkCol, pkDecl) = getPrimaryKeyInfo(db, tableLc)
        val pkIsBlob = pkDecl.lowercase().contains("blob")
        val pkUuid = if (pkIsBlob) uuidStringToHex(ch.pk) else ch.pk
        return db.prepare("SELECT EXISTS(SELECT 1 FROM _sync_pending WHERE table_name=? AND pk_uuid=? AND op='DELETE')")
            .use { st ->
                st.bindText(1, tableLc)
                st.bindText(2, pkUuid)
                st.step() && st.getLong(0) == 1L
            }
    }

    private suspend fun checkRecentlyDeleted(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse
    ): Boolean {
        val tableLc = ch.tableName.lowercase()
        val (pkCol, pkDecl) = getPrimaryKeyInfo(db, tableLc)
        val pkIsBlob = pkDecl.lowercase().contains("blob")
        val pkUuid = if (pkIsBlob) uuidStringToHex(ch.pk) else ch.pk
        return db.prepare("SELECT deleted, server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?")
            .use { st ->
                st.bindText(1, tableLc)
                st.bindText(2, pkUuid)
                if (st.step()) {
                    val deleted = st.getLong(0) == 1L
                    val localServerVersion = st.getLong(1)
                    // Only consider recently deleted if server version is not newer
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
        val tableLc = ch.tableName.lowercase()
        val (pkCol, pkDecl) = getPrimaryKeyInfo(db, tableLc)
        val pkIsBlob = pkDecl.lowercase().contains("blob")
        val pkUuid = if (pkIsBlob) uuidStringToHex(ch.pk) else ch.pk
        return db.prepare("SELECT op, payload FROM _sync_pending WHERE table_name=? AND pk_uuid=? AND op IN ('INSERT', 'UPDATE')")
            .use { st ->
                st.bindText(1, tableLc)
                st.bindText(2, pkUuid)
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
        // Update meta to track that we've seen this server version but didn't apply it
        // Convert UUID string to hex format for BLOB primary keys when updating metadata
        val tableLc = ch.tableName.lowercase()
        val (_, pkDecl) = getPrimaryKeyInfo(db, tableLc)
        val pkIsBlob = pkDecl.lowercase().contains("blob")
        val pkForMeta = if (pkIsBlob) uuidStringToHex(ch.pk) else ch.pk
        updateRowMeta(db, ch.tableName, pkForMeta, ch.serverVersion, true)
    }

    private suspend fun handleConflict(
        db: SafeSQLiteConnection,
        ch: ChangeDownloadResponse,
        localPendingChange: Pair<String, String>
    ) {
        // There's a local pending INSERT/UPDATE - this is a conflict that needs resolution
        sqliteNowLogger.d { "Conflict detected: server ${ch.op} v${ch.serverVersion} vs local ${localPendingChange.first} for ${ch.tableName}:${ch.pk}" }

        // Create a server row object for conflict resolution
        val serverRowJson = buildJsonObject {
            put("server_version", JsonPrimitive(ch.serverVersion))
            put("deleted", JsonPrimitive(false))
            put("payload", ch.payload ?: JsonNull)
        }

        val localPayloadJson = try {
            Json.parseToJsonElement(localPendingChange.second)
        } catch (e: Exception) {
            sqliteNowLogger.w { "Failed to parse local payload for conflict resolution: ${localPendingChange.second}" }
            null
        }

        val decision = resolveConflictWithFallbacks(
            table = ch.tableName,
            pk = ch.pk,
            serverRow = serverRowJson,
            localPayload = localPayloadJson
        )

        // Convert UUID string to hex format for BLOB primary keys when updating metadata
        val tableLc = ch.tableName.lowercase()
        val (_, pkDecl) = getPrimaryKeyInfo(db, tableLc)
        val pkIsBlob = pkDecl.lowercase().contains("blob")
        val pkForMeta = if (pkIsBlob) uuidStringToHex(ch.pk) else ch.pk

        when (decision) {
            is MergeResult.AcceptServer -> {
                sqliteNowLogger.d { "Conflict resolution: accepting server version for ${ch.tableName}:${ch.pk}" }
                upsertBusinessFromPayload(db, ch.tableName, ch.pk, ch.payload)
                updateRowMeta(db, ch.tableName, pkForMeta, ch.serverVersion, false)
                // Remove the local pending change since we're accepting server
                db.prepare("DELETE FROM _sync_pending WHERE table_name=? AND pk_uuid=?").use { st ->
                    st.bindText(1, tableLc)
                    st.bindText(2, pkForMeta)
                    st.step()
                }
            }

            is MergeResult.KeepLocal -> {
                sqliteNowLogger.d { "Conflict resolution: keeping local version for ${ch.tableName}:${ch.pk}" }
                upsertBusinessFromPayload(db, ch.tableName, ch.pk, decision.mergedPayload)
                // Update the pending change to use the server version as base
                db.prepare(
                    "UPDATE _sync_pending SET op='UPDATE', base_version=?, payload=?, queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') " +
                    "WHERE table_name=? AND pk_uuid=?"
                ).use { st ->
                    st.bindLong(1, ch.serverVersion)
                    st.bindText(2, decision.mergedPayload.toString())
                    st.bindText(3, tableLc)
                    st.bindText(4, pkForMeta)
                    st.step()
                }
                // Update row meta to reflect server version and that record exists
                // This ensures _sync_row_meta is consistent with the server state
                updateRowMeta(db, ch.tableName, pkForMeta, ch.serverVersion, false)
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
            sqliteNowLogger.w { "Local payload is null for $table:$pk, accepting server version" }
            return MergeResult.AcceptServer
        }

        if (serverRow == null) {
            sqliteNowLogger.w { "Server row is null for $table:$pk, keeping local version" }
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
            // Convert UUID string to hex format for BLOB primary keys BEFORE checking version
            val tableLc = ch.tableName.lowercase()
            val (_, pkDecl) = getPrimaryKeyInfo(db, tableLc)
            val pkIsBlob = pkDecl.lowercase().contains("blob")
            val pkForMeta = if (pkIsBlob) uuidStringToHex(ch.pk) else ch.pk

            // Check if incoming server version is newer than current local version
            // FIXED: Use correct primary key format for BLOB columns in version check
            val currentServerVersion =
                db.prepare("SELECT server_version FROM _sync_row_meta WHERE table_name=? AND pk_uuid=?")
                    .use { st ->
                        st.bindText(1, ch.tableName)
                        st.bindText(2, pkForMeta)  // FIXED: Use pkForMeta instead of ch.pk
                        if (st.step()) st.getLong(0) else 0L
                    }

            if (ch.serverVersion > currentServerVersion) {
                sqliteNowLogger.d { "Applying server ${ch.op} v${ch.serverVersion} for ${ch.tableName}:${ch.pk} (current v$currentServerVersion)" }
                upsertBusinessFromPayload(db, ch.tableName, ch.pk, ch.payload)
                updateRowMeta(db, ch.tableName, pkForMeta, ch.serverVersion, false)
            } else {
                // Still update meta to track that we've seen this server version
                if (ch.serverVersion > currentServerVersion) {
                    updateRowMeta(db, ch.tableName, pkForMeta, ch.serverVersion, false)
                }
            }
        } else {
            // Normal download - apply without version checking
            upsertBusinessFromPayload(db, ch.tableName, ch.pk, ch.payload)
            // Convert UUID string to hex format for BLOB primary keys when updating metadata
            val tableLc = ch.tableName.lowercase()
            val (_, pkDecl) = getPrimaryKeyInfo(db, tableLc)
            val pkIsBlob = pkDecl.lowercase().contains("blob")
            val pkForMeta = if (pkIsBlob) uuidStringToHex(ch.pk) else ch.pk
            updateRowMeta(db, ch.tableName, pkForMeta, ch.serverVersion, false)
        }
    }

    // Utility methods
    private suspend fun inApplyTx(
        db: SafeSQLiteConnection,
        block: suspend () -> Pair<Int, Long>
    ): Pair<Int, Long> {
        val alreadyInTx = db.inTransaction()
        return if (alreadyInTx) {
            try {
                db.execSQL("PRAGMA defer_foreign_keys = ON")
                db.execSQL("UPDATE _sync_client_info SET apply_mode=1")
                val result = block()
                result
            } finally {
                // Always clear apply_mode in the same (outer) transaction context
                try { db.execSQL("UPDATE _sync_client_info SET apply_mode=0") } catch (_: Throwable) {}
            }
        } else {
            try {
                db.transaction {
                    db.execSQL("PRAGMA defer_foreign_keys = ON")
                    db.execSQL("UPDATE _sync_client_info SET apply_mode=1")
                    val result = block()
                    db.execSQL("UPDATE _sync_client_info SET apply_mode=0")
                    result
                }
            } catch (t: Throwable) {
                // Ensure apply_mode is cleared if transaction rolled back
                try { db.execSQL("UPDATE _sync_client_info SET apply_mode=0") } catch (_: Throwable) {}
                throw t
            }
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
                            sqliteNowLogger.d { "Skipping DELETE during lookback: ${change.tableName}:${change.pk.take(8)} v${change.serverVersion} (superseded by later INSERT/UPDATE)" }
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
        after: Long,
        limit: Int,
        includeSelf: Boolean,
        until: Long,
        schema: String
    ): String {
        val sb = StringBuilder()
        sb.append("${config.downloadPath}?after=$after&limit=$limit&schema=$schema")
        if (includeSelf) sb.append("&include_self=true")
        if (until > 0L) sb.append("&until=").append(until)
        return sb.toString()
    }
}
