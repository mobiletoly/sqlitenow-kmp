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
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.sync.Mutex
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.concurrent.Volatile
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

class DefaultOversqliteClient(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val http: HttpClient,
    @Suppress("UNUSED_PARAMETER") private val resolver: Resolver = ServerWinsResolver,
    private val tablesUpdateListener: (table: Set<String>) -> Unit,
) : OversqliteClient {
    private val syncGate = Mutex()
    private val tableInfoCache = TableInfoCache()
    private val bootstrapper = SyncBootstrapper(config, tableInfoCache)
    private val ioDispatcher: CoroutineDispatcher = PlatformDispatchers().io
    private val json = Json { ignoreUnknownKeys = true }

    @Volatile
    private var uploadsPaused = false

    @Volatile
    private var downloadsPaused = false

    @Volatile
    private var validatedConfig: ValidatedConfig? = null

    @Volatile
    private var currentUserId: String? = null

    @Volatile
    private var currentSourceId: String? = null

    override suspend fun pauseUploads() {
        uploadsPaused = true
    }

    override suspend fun resumeUploads() {
        uploadsPaused = false
    }

    override suspend fun pauseDownloads() {
        downloadsPaused = true
    }

    override suspend fun resumeDownloads() {
        downloadsPaused = false
    }

    override suspend fun bootstrap(userId: String, sourceId: String): Result<Unit> {
        val result = bootstrapper.bootstrap(db, userId, sourceId)
        return result.map { validated ->
            validatedConfig = validated
            currentUserId = userId.trim()
            currentSourceId = sourceId.trim()
        }
    }

    override suspend fun pushPending(): Result<Unit> = runSyncOperation {
        if (uploadsPaused) return@runSyncOperation

        val state = requireBootstrapped()
        pushPendingLocked(state)
    }

    override suspend fun pullToStable(): Result<Unit> = runSyncOperation {
        if (downloadsPaused) return@runSyncOperation

        val state = requireBootstrapped()
        pullToStableLocked(state)
    }

    override suspend fun sync(): Result<Unit> = runSyncOperation {
        val state = requireBootstrapped()
        if (isRebuildRequired(state.userId)) {
            throw RebuildRequiredException()
        }
        if (!uploadsPaused) {
            pushPendingLocked(state)
        }
        if (!downloadsPaused) {
            pullToStableLocked(state)
        }
    }

    override suspend fun hydrate(): Result<Unit> = runSyncOperation {
        if (downloadsPaused) return@runSyncOperation
        val state = requireBootstrapped()
        val updatedTables = rebuildFromSnapshot(state, rotateSource = false)
        if (updatedTables.isNotEmpty()) {
            tablesUpdateListener(updatedTables)
        }
    }

    override suspend fun recover(): Result<Unit> = runSyncOperation {
        if (downloadsPaused) return@runSyncOperation
        val state = requireBootstrapped()
        val updatedTables = rebuildFromSnapshot(state, rotateSource = true)
        if (updatedTables.isNotEmpty()) {
            tablesUpdateListener(updatedTables)
        }
    }

    override suspend fun lastBundleSeqSeen(): Result<Long> = runCatching {
        val state = requireBootstrapped()
        loadLastBundleSeqSeen(state.userId)
    }

    override fun close() {
    }

    private suspend fun pushPendingLocked(state: RuntimeState) {
        if (isRebuildRequired(state.userId)) {
            throw RebuildRequiredException()
        }

        val snapshot = ensurePushOutboundSnapshot(state)
        if (snapshot.rows.isEmpty()) {
            return
        }

        clearPushStage()
        val committed = commitPushOutboundSnapshot(state, snapshot)
        fetchCommittedPushBundle(state, committed)
        val updatedTables = applyStagedPushBundle(state, snapshot.rows, committed)
        if (updatedTables.isNotEmpty()) {
            tablesUpdateListener(updatedTables)
        }
    }

    private suspend fun pullToStableLocked(state: RuntimeState) {
        if (isRebuildRequired(state.userId)) {
            throw RebuildRequiredException()
        }
        try {
            pullToStableInternal(state)
        } catch (e: HistoryPrunedException) {
            val updatedTables = rebuildFromSnapshot(state, rotateSource = false)
            if (updatedTables.isNotEmpty()) {
                tablesUpdateListener(updatedTables)
            }
        }
    }

    private suspend fun pullToStableInternal(state: RuntimeState) {
        val pendingOutboundCount = pendingPushOutboundCount()
        if (pendingOutboundCount > 0) {
            throw PendingPushReplayException(pendingOutboundCount)
        }

        val pendingCount = pendingDirtyCount()
        if (pendingCount > 0) {
            throw DirtyStateRejectedException(pendingCount)
        }

        var afterBundleSeq = loadLastBundleSeqSeen(state.userId)
        val maxBundles = config.downloadLimit.takeIf { it > 0 } ?: 1000
        var targetBundleSeq = 0L

        while (true) {
            val response = sendPullRequest(afterBundleSeq, maxBundles, targetBundleSeq)
            validatePullResponse(response, afterBundleSeq)

            if (targetBundleSeq == 0L) {
                targetBundleSeq = response.stableBundleSeq
            } else if (response.stableBundleSeq != targetBundleSeq) {
                error("pull response stable bundle seq changed from $targetBundleSeq to ${response.stableBundleSeq}")
            }

            for (bundle in response.bundles) {
                applyPulledBundle(state, bundle)
                afterBundleSeq = bundle.bundleSeq
            }

            if (afterBundleSeq >= response.stableBundleSeq) {
                return
            }
            if (!response.hasMore && response.bundles.isEmpty()) {
                error("pull ended before reaching stable bundle seq ${response.stableBundleSeq}")
            }
            if (!response.hasMore && afterBundleSeq < response.stableBundleSeq) {
                error("pull ended early at bundle seq $afterBundleSeq before stable bundle seq ${response.stableBundleSeq}")
            }
        }
    }

    private suspend fun rebuildFromSnapshot(
        state: RuntimeState,
        rotateSource: Boolean,
    ): Set<String> {
        val pendingOutboundCount = pendingPushOutboundCount()
        if (pendingOutboundCount > 0) {
            throw PendingPushReplayException(pendingOutboundCount)
        }

        val pendingCount = pendingDirtyCount()
        if (pendingCount > 0) {
            throw DirtyStateRejectedException(pendingCount)
        }

        clearSnapshotStage()
        val session = createSnapshotSession()
        try {
            var afterRowOrdinal = 0L
            while (true) {
                val chunk = fetchSnapshotChunk(
                    snapshotId = session.snapshotId,
                    snapshotBundleSeq = session.snapshotBundleSeq,
                    afterRowOrdinal = afterRowOrdinal,
                    maxRows = snapshotChunkRows(),
                )
                stageSnapshotChunk(state, session, chunk, afterRowOrdinal)
                if (!chunk.hasMore) {
                    break
                }
                afterRowOrdinal = chunk.nextRowOrdinal
            }

            return applyStagedSnapshot(state, session, rotateSource)
        } finally {
            deleteSnapshotSessionBestEffort(session.snapshotId)
        }
    }

    private fun requireBootstrapped(): RuntimeState {
        val validated = validatedConfig ?: error("oversqlite bootstrap(userId, sourceId) must complete successfully before sync operations run")
        val userId = currentUserId ?: error("oversqlite bootstrap(userId, sourceId) must complete successfully before sync operations run")
        val sourceId = currentSourceId ?: error("oversqlite bootstrap(userId, sourceId) must complete successfully before sync operations run")
        return RuntimeState(validated = validated, userId = userId, sourceId = sourceId)
    }

    private suspend fun loadLastBundleSeqSeen(userId: String): Long {
        return db.prepare(
            "SELECT last_bundle_seq_seen FROM _sync_client_state WHERE user_id = ?"
        ).use { st ->
            st.bindText(1, userId)
            if (!st.step()) error("_sync_client_state missing for user $userId")
            st.getLong(0)
        }
    }

    private suspend fun pendingDirtyCount(): Int {
        return db.prepare("SELECT COUNT(*) FROM _sync_dirty_rows").use { st ->
            check(st.step())
            st.getLong(0).toInt()
        }
    }

    private suspend fun pendingPushOutboundCount(): Int {
        return db.prepare("SELECT COUNT(*) FROM _sync_push_outbound").use { st ->
            check(st.step())
            st.getLong(0).toInt()
        }
    }

    private suspend fun isRebuildRequired(userId: String): Boolean {
        return db.prepare("SELECT rebuild_required FROM _sync_client_state WHERE user_id = ?").use { st ->
            st.bindText(1, userId)
            if (!st.step()) error("_sync_client_state missing for user $userId")
            st.getLong(0) == 1L
        }
    }

    private suspend fun clearSnapshotStage() {
        db.execSQL("DELETE FROM _sync_snapshot_stage")
    }

    private suspend fun clearPushStage() {
        db.execSQL("DELETE FROM _sync_push_stage")
    }

    private suspend fun collectDirtyRowsForPush(state: RuntimeState): PreparedPush {
        val clientState = loadClientBundleState(state.userId)
        val snapshotRows = mutableListOf<DirtySnapshotRow>()

        db.prepare(
            """
            SELECT schema_name, table_name, key_json, base_row_version, dirty_ordinal
            FROM _sync_dirty_rows
            ORDER BY dirty_ordinal, table_name, key_json
            """.trimIndent()
        ).use { st ->
            while (st.step()) {
                snapshotRows += DirtySnapshotRow(
                    schemaName = st.getText(0),
                    tableName = st.getText(1),
                    keyJson = st.getText(2),
                    baseRowVersion = st.getLong(3),
                    dirtyOrdinal = st.getLong(4),
                )
            }
        }

        val dirtyRows = mutableListOf<DirtyRowCapture>()
        val noOps = mutableListOf<Triple<String, String, String>>()

        for (snapshot in snapshotRows) {
            val (localPk, wireKey) = decodeDirtyKeyForPush(state, snapshot.tableName, snapshot.keyJson)
            val rowState = loadStructuredRowState(snapshot.schemaName, snapshot.tableName, snapshot.keyJson)
            val livePayload = serializeExistingRow(snapshot.tableName, localPk)

            if (livePayload == null && (!rowState.exists || rowState.deleted)) {
                noOps += Triple(snapshot.schemaName, snapshot.tableName, snapshot.keyJson)
                continue
            }

            val op = when {
                livePayload == null -> "DELETE"
                rowState.exists && !rowState.deleted -> "UPDATE"
                else -> "INSERT"
            }
            dirtyRows += DirtyRowCapture(
                schemaName = snapshot.schemaName,
                tableName = snapshot.tableName,
                keyJson = snapshot.keyJson,
                localPk = localPk,
                wireKey = wireKey,
                op = op,
                baseRowVersion = snapshot.baseRowVersion,
                localPayload = livePayload,
                dirtyOrdinal = snapshot.dirtyOrdinal,
            )
        }

        for ((schemaName, tableName, keyJson) in noOps) {
            db.prepare(
                "DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?"
            ).use { st ->
                st.bindText(1, schemaName)
                st.bindText(2, tableName)
                st.bindText(3, keyJson)
                st.step()
            }
        }

        val tableOrder = state.validated.tableOrder
        dirtyRows.sortWith(
            compareBy<DirtyRowCapture> { if (it.op == "DELETE") 1 else 0 }
                .thenComparator { left, right ->
                    val leftOrder = tableOrder[left.tableName] ?: Int.MAX_VALUE
                    val rightOrder = tableOrder[right.tableName] ?: Int.MAX_VALUE
                    when {
                        left.op == "DELETE" && leftOrder != rightOrder -> rightOrder - leftOrder
                        left.op != "DELETE" && leftOrder != rightOrder -> leftOrder - rightOrder
                        else -> left.dirtyOrdinal.compareTo(right.dirtyOrdinal)
                    }
                }
        )

        return PreparedPush(
            sourceBundleId = clientState.nextSourceBundleId,
            rows = dirtyRows,
        )
    }

    private suspend fun ensurePushOutboundSnapshot(state: RuntimeState): PushOutboundSnapshot {
        val existing = loadPushOutboundSnapshot(state)
        if (existing != null) {
            return existing
        }

        var frozen = PushOutboundSnapshot(sourceBundleId = 0, rows = emptyList())
        db.transaction(TransactionMode.IMMEDIATE) {
            val prepared = collectDirtyRowsForPush(state)
            if (prepared.rows.isEmpty()) {
                return@transaction
            }

            prepared.rows.forEachIndexed { index, row ->
                db.prepare(
                    """
                    INSERT INTO _sync_push_outbound (
                      source_bundle_id, row_ordinal, schema_name, table_name, key_json, op, base_row_version, payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """.trimIndent()
                ).use { st ->
                    st.bindLong(1, prepared.sourceBundleId)
                    st.bindLong(2, index.toLong())
                    st.bindText(3, row.schemaName)
                    st.bindText(4, row.tableName)
                    st.bindText(5, row.keyJson)
                    st.bindText(6, row.op)
                    st.bindLong(7, row.baseRowVersion)
                    if (row.localPayload == null) st.bindNull(8) else st.bindText(8, row.localPayload)
                    st.step()
                }
                deleteDirtyRow(row.schemaName, row.tableName, row.keyJson)
            }
            frozen = PushOutboundSnapshot(
                sourceBundleId = prepared.sourceBundleId,
                rows = prepared.rows,
            )
        }
        return frozen
    }

    private suspend fun loadPushOutboundSnapshot(state: RuntimeState): PushOutboundSnapshot? {
        var sourceBundleId: Long? = null
        val rows = mutableListOf<DirtyRowCapture>()
        db.prepare(
            """
            SELECT source_bundle_id, schema_name, table_name, key_json, op, base_row_version, payload, row_ordinal
            FROM _sync_push_outbound
            ORDER BY source_bundle_id, row_ordinal
            """.trimIndent()
        ).use { st ->
            while (st.step()) {
                val rowSourceBundleId = st.getLong(0)
                if (sourceBundleId == null) {
                    sourceBundleId = rowSourceBundleId
                } else {
                    require(sourceBundleId == rowSourceBundleId) {
                        "outbound push snapshot contains multiple source_bundle_id values ($sourceBundleId and $rowSourceBundleId)"
                    }
                }
                val tableName = st.getText(2)
                val keyJson = st.getText(3)
                val (localPk, wireKey) = decodeDirtyKeyForPush(state, tableName, keyJson)
                rows += DirtyRowCapture(
                    schemaName = st.getText(1),
                    tableName = tableName,
                    keyJson = keyJson,
                    localPk = localPk,
                    wireKey = wireKey,
                    op = st.getText(4),
                    baseRowVersion = st.getLong(5),
                    localPayload = if (st.isNull(6)) null else st.getText(6),
                    dirtyOrdinal = st.getLong(7),
                )
            }
        }
        val frozenSourceBundleId = sourceBundleId ?: return null
        return PushOutboundSnapshot(
            sourceBundleId = frozenSourceBundleId,
            rows = rows,
        )
    }

    private suspend fun commitPushOutboundSnapshot(
        state: RuntimeState,
        snapshot: PushOutboundSnapshot,
    ): CommittedPushBundle {
        require(snapshot.rows.isNotEmpty()) { "outbound push snapshot is empty" }

        val createResponse = createPushSession(snapshot.sourceBundleId, snapshot.rows.size.toLong(), state.sourceId)
        return when (createResponse.status) {
            "already_committed" -> committedPushBundleFromCreateResponse(createResponse, state.sourceId, snapshot.sourceBundleId)
            "staging" -> {
                val pushId = createResponse.pushId.trim()
                require(pushId.isNotEmpty()) { "push session response missing push_id" }
                try {
                    val chunkSize = pushChunkRows()
                    var start = 0
                    while (start < snapshot.rows.size) {
                        val end = minOf(start + chunkSize, snapshot.rows.size)
                        val chunkResponse = uploadPushChunk(
                            pushId = pushId,
                            request = PushSessionChunkRequest(
                                startRowOrdinal = start.toLong(),
                                rows = buildPushRequestRows(snapshot.rows.subList(start, end)),
                            ),
                        )
                        require(chunkResponse.pushId == pushId) {
                            "push chunk response push_id ${chunkResponse.pushId} does not match requested $pushId"
                        }
                        require(chunkResponse.nextExpectedRowOrdinal == end.toLong()) {
                            "push chunk response next_expected_row_ordinal ${chunkResponse.nextExpectedRowOrdinal} does not match expected $end"
                        }
                        start = end
                    }
                    committedPushBundleFromCommitResponse(commitPushSession(pushId), state.sourceId, snapshot.sourceBundleId)
                } finally {
                    deletePushSessionBestEffort(pushId)
                }
            }
            else -> error("unexpected push session status ${createResponse.status}")
        }
    }

    private suspend fun buildPushRequestRows(rows: List<DirtyRowCapture>): List<PushRequestRow> {
        return rows.map { dirty ->
            PushRequestRow(
                schema = dirty.schemaName,
                table = dirty.tableName,
                key = dirty.wireKey,
                op = dirty.op,
                baseRowVersion = dirty.baseRowVersion,
                payload = if (dirty.op == "DELETE") null else processPayloadForUpload(dirty.tableName, dirty.localPayload ?: ""),
            )
        }
    }

    private suspend fun fetchCommittedPushBundle(
        state: RuntimeState,
        committed: CommittedPushBundle,
    ) {
        clearPushStage()

        var afterRowOrdinal: Long? = null
        var stagedRowCount = 0L
        while (true) {
            val chunk = fetchCommittedBundleChunk(
                bundleSeq = committed.bundleSeq,
                afterRowOrdinal = afterRowOrdinal,
                maxRows = committedBundleChunkRows(),
            )
            validateCommittedBundleRowsResponse(chunk, committed, afterRowOrdinal)
            stageCommittedBundleChunk(state, chunk, afterRowOrdinal)
            stagedRowCount += chunk.rows.size
            if (!chunk.hasMore) {
                break
            }
            afterRowOrdinal = chunk.nextRowOrdinal
        }

        require(stagedRowCount == committed.rowCount) {
            "staged committed bundle row count $stagedRowCount does not match expected row_count ${committed.rowCount}"
        }
        val bundleHash = computeStagedPushBundleHash(state, committed.bundleSeq)
        require(bundleHash == committed.bundleHash) {
            "staged committed bundle hash $bundleHash does not match expected ${committed.bundleHash}"
        }
    }

    private suspend fun stageCommittedBundleChunk(
        state: RuntimeState,
        chunk: CommittedBundleRowsResponse,
        afterRowOrdinal: Long?,
    ) {
        var rowOrdinal = afterRowOrdinal ?: -1L
        db.transaction(TransactionMode.IMMEDIATE) {
            for (row in chunk.rows) {
                require(row.schema == state.validated.schema) {
                    "committed bundle row schema ${row.schema} does not match client schema ${state.validated.schema}"
                }
                validateBundleRow(row)
                val (keyJson, _) = bundleRowKeyToLocalKey(state, row.table, row.key)
                rowOrdinal++
                db.prepare(
                    """
                    INSERT INTO _sync_push_stage (
                      bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """.trimIndent()
                ).use { st ->
                    st.bindLong(1, chunk.bundleSeq)
                    st.bindLong(2, rowOrdinal)
                    st.bindText(3, row.schema)
                    st.bindText(4, row.table)
                    st.bindText(5, keyJson)
                    st.bindText(6, row.op)
                    st.bindLong(7, row.rowVersion)
                    if (row.op == "DELETE" || row.payload == null) st.bindNull(8) else st.bindText(8, row.payload.toString())
                    st.step()
                }
            }
        }
    }

    private suspend fun computeStagedPushBundleHash(
        state: RuntimeState,
        bundleSeq: Long,
    ): String {
        val rows = mutableListOf<JsonElement>()
        db.prepare(
            """
            SELECT row_ordinal, schema_name, table_name, key_json, op, row_version, payload
            FROM _sync_push_stage
            WHERE bundle_seq = ?
            ORDER BY row_ordinal
            """.trimIndent()
        ).use { st ->
            st.bindLong(1, bundleSeq)
            while (st.step()) {
                val rowOrdinal = st.getLong(0)
                val schemaName = st.getText(1)
                val tableName = st.getText(2)
                val keyJson = st.getText(3)
                val op = st.getText(4)
                val rowVersion = st.getLong(5)
                val payloadText = if (st.isNull(6)) null else st.getText(6)
                val (_, wireKey) = decodeDirtyKeyForPush(state, tableName, keyJson)
                rows += buildJsonObject {
                    put("row_ordinal", JsonPrimitive(rowOrdinal))
                    put("schema", JsonPrimitive(schemaName))
                    put("table", JsonPrimitive(tableName))
                    put("key", buildJsonObject {
                        for ((key, value) in wireKey.entries.sortedBy { it.key }) {
                            put(key, JsonPrimitive(value))
                        }
                    })
                    put("op", JsonPrimitive(op))
                    put("row_version", JsonPrimitive(rowVersion))
                    put("payload", payloadText?.let { json.parseToJsonElement(it) } ?: JsonNull)
                }
            }
        }
        return sha256Hex(canonicalizeJsonElement(JsonArray(rows)).encodeToByteArray())
    }

    private suspend fun loadClientBundleState(userId: String): ClientBundleState {
        return db.prepare(
            """
            SELECT next_source_bundle_id, last_bundle_seq_seen
            FROM _sync_client_state
            WHERE user_id = ?
            """.trimIndent()
        ).use { st ->
            st.bindText(1, userId)
            if (!st.step()) error("_sync_client_state missing for user $userId")
            ClientBundleState(
                nextSourceBundleId = st.getLong(0),
                lastBundleSeqSeen = st.getLong(1),
            )
        }
    }

    private suspend fun decodeDirtyKeyForPush(
        state: RuntimeState,
        tableName: String,
        keyJson: String,
    ): Pair<String, SyncKey> {
        val keyColumn = state.validated.keyByTable[tableName]?.singleOrNull()
            ?: error("table $tableName is not configured for sync")
        val raw = json.parseToJsonElement(keyJson) as? JsonObject
            ?: error("dirty key for $tableName must be a JSON object")
        val localValue = raw[keyColumn.lowercase()]?.jsonPrimitive?.content
            ?: raw[keyColumn]?.jsonPrimitive?.content
            ?: error("dirty key for $tableName is missing $keyColumn")
        val wireValue = normalizePkForServer(tableName, localValue)
        return localValue to mapOf(keyColumn.lowercase() to wireValue)
    }

    private suspend fun loadStructuredRowState(
        schemaName: String,
        tableName: String,
        keyJson: String,
    ): StructuredRowState {
        return db.prepare(
            """
            SELECT row_version, deleted
            FROM _sync_row_state
            WHERE schema_name = ? AND table_name = ? AND key_json = ?
            """.trimIndent()
        ).use { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            if (!st.step()) {
                StructuredRowState()
            } else {
                StructuredRowState(
                    exists = true,
                    rowVersion = st.getLong(0),
                    deleted = st.getLong(1) == 1L,
                )
            }
        }
    }

    private suspend fun createPushSession(
        sourceBundleId: Long,
        plannedRowCount: Long,
        sourceId: String,
    ): PushSessionCreateResponse {
        val call = http.post("/sync/push-sessions") {
            contentType(ContentType.Application.Json)
            setBody(
                PushSessionCreateRequest(
                    sourceId = sourceId,
                    sourceBundleId = sourceBundleId,
                    plannedRowCount = plannedRowCount,
                )
            )
        }
        if (call.status != HttpStatusCode.OK) {
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            throw RuntimeException("push session request failed: HTTP ${call.status} - $raw")
        }
        val response: PushSessionCreateResponse = call.body()
        validatePushSessionCreateResponse(response, sourceBundleId, plannedRowCount, sourceId)
        return response
    }

    private suspend fun uploadPushChunk(
        pushId: String,
        request: PushSessionChunkRequest,
    ): PushSessionChunkResponse {
        val call = http.post("/sync/push-sessions/$pushId/chunks") {
            contentType(ContentType.Application.Json)
            setBody(request)
        }
        if (call.status != HttpStatusCode.OK) {
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            throw RuntimeException("push chunk request failed: HTTP ${call.status} - $raw")
        }
        return call.body()
    }

    private suspend fun commitPushSession(pushId: String): PushSessionCommitResponse {
        val call = http.post("/sync/push-sessions/$pushId/commit")
        if (call.status != HttpStatusCode.OK) {
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            throw RuntimeException("push commit request failed: HTTP ${call.status} - $raw")
        }
        return call.body()
    }

    private suspend fun fetchCommittedBundleChunk(
        bundleSeq: Long,
        afterRowOrdinal: Long?,
        maxRows: Int,
    ): CommittedBundleRowsResponse {
        val call = http.get("/sync/committed-bundles/$bundleSeq/rows") {
            url {
                if (afterRowOrdinal != null) {
                    parameters.append("after_row_ordinal", afterRowOrdinal.toString())
                }
                parameters.append("max_rows", maxRows.toString())
            }
        }
        if (call.status != HttpStatusCode.OK) {
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            throw RuntimeException("committed bundle chunk request failed: HTTP ${call.status} - $raw")
        }
        return call.body()
    }

    private suspend fun deletePushSessionBestEffort(pushId: String) {
        if (pushId.isBlank()) return
        runCatching { http.delete("/sync/push-sessions/$pushId") }
    }

    private suspend fun applyStagedPushBundle(
        state: RuntimeState,
        uploadedRows: List<DirtyRowCapture>,
        committed: CommittedPushBundle,
    ): Set<String> {
        val updatedTables = linkedSetOf<String>()
        db.transaction(TransactionMode.IMMEDIATE) {
            db.execSQL("PRAGMA defer_foreign_keys = ON")
            setApplyMode(state.userId, true)

            val stagedRows = loadStagedPushBundleRows(state, committed.bundleSeq)
            require(stagedRows.size.toLong() == committed.rowCount) {
                "staged push replay row count ${stagedRows.size} does not match expected row_count ${committed.rowCount}"
            }

            val decisions = buildMap<String, ReplayDecision> {
                for (uploaded in uploadedRows) {
                    val currentDirty = loadDirtyUploadState(uploaded.schemaName, uploaded.tableName, uploaded.keyJson)
                    val livePayload = serializeExistingRow(uploaded.tableName, uploaded.localPk)
                    val dirtyMatches = currentDirty.exists && currentDirty.op == uploaded.op &&
                        canonicalPayloadEqual(currentDirty.payload, uploaded.localPayload)
                    val liveMatches = livePayloadMatchesUploadedIntent(uploaded, livePayload)
                    val decision = reconcileAppliedUploadState(
                        change = uploaded,
                        pendingExists = currentDirty.exists,
                        pendingMatches = dirtyMatches,
                        livePayload = livePayload,
                        liveExists = livePayload != null,
                        liveMatches = liveMatches,
                    )
                    put("${uploaded.schemaName}\u0000${uploaded.tableName}\u0000${uploaded.keyJson}", decision)
                }
            }

            for (row in stagedRows) {
                val bundleRow = BundleRow(
                    schema = row.schemaName,
                    table = row.tableName,
                    key = row.wireKey,
                    op = row.op,
                    rowVersion = row.rowVersion,
                    payload = row.payload?.let { json.parseToJsonElement(it) },
                )
                val decision = decisions["${row.schemaName}\u0000${row.tableName}\u0000${row.keyJson}"]
                if (decision == null || !decision.needsRequeue) {
                    applyBundleRowAuthoritatively(state, bundleRow, row.localPk)
                    if (decision != null) {
                        deleteDirtyRow(row.schemaName, row.tableName, row.keyJson)
                    }
                } else {
                    updateStructuredRowState(state.validated.schema, row.tableName, row.keyJson, row.rowVersion, row.op == "DELETE")
                    requeueDirtyIntent(
                        schemaName = row.schemaName,
                        tableName = row.tableName,
                        keyJson = row.keyJson,
                        op = decision.requeueOp,
                        baseRowVersion = row.rowVersion,
                        payload = decision.requeuePayload,
                    )
                }
                updatedTables += row.tableName.lowercase()
            }

            db.prepare(
                """
                UPDATE _sync_client_state
                SET last_bundle_seq_seen = CASE
                      WHEN last_bundle_seq_seen < ? THEN ?
                      ELSE last_bundle_seq_seen
                    END,
                    next_source_bundle_id = CASE
                      WHEN next_source_bundle_id <= ? THEN ?
                      ELSE next_source_bundle_id
                    END
                WHERE user_id = ?
                """.trimIndent()
            ).use { st ->
                st.bindLong(1, committed.bundleSeq)
                st.bindLong(2, committed.bundleSeq)
                st.bindLong(3, committed.sourceBundleId)
                st.bindLong(4, committed.sourceBundleId + 1)
                st.bindText(5, state.userId)
                st.step()
            }
            db.prepare("DELETE FROM _sync_push_outbound WHERE source_bundle_id = ?").use { st ->
                st.bindLong(1, committed.sourceBundleId)
                st.step()
            }
            db.prepare("DELETE FROM _sync_push_stage WHERE bundle_seq = ?").use { st ->
                st.bindLong(1, committed.bundleSeq)
                st.step()
            }
            setApplyMode(state.userId, false)
        }
        return updatedTables
    }

    private suspend fun loadStagedPushBundleRows(
        state: RuntimeState,
        bundleSeq: Long,
    ): List<StagedPushBundleRow> {
        val rows = mutableListOf<StagedPushBundleRow>()
        db.prepare(
            """
            SELECT schema_name, table_name, key_json, op, row_version, payload
            FROM _sync_push_stage
            WHERE bundle_seq = ?
            ORDER BY row_ordinal
            """.trimIndent()
        ).use { st ->
            st.bindLong(1, bundleSeq)
            while (st.step()) {
                val schemaName = st.getText(0)
                val tableName = st.getText(1)
                val keyJson = st.getText(2)
                val (localPk, wireKey) = decodeDirtyKeyForPush(state, tableName, keyJson)
                rows += StagedPushBundleRow(
                    schemaName = schemaName,
                    tableName = tableName,
                    keyJson = keyJson,
                    localPk = localPk,
                    wireKey = wireKey,
                    op = st.getText(3),
                    rowVersion = st.getLong(4),
                    payload = if (st.isNull(5)) null else st.getText(5),
                )
            }
        }
        return rows
    }

    private suspend fun applyPulledBundle(
        state: RuntimeState,
        bundle: Bundle,
    ): Set<String> {
        val updatedTables = linkedSetOf<String>()
        db.transaction(TransactionMode.IMMEDIATE) {
            setApplyMode(state.userId, true)
            db.execSQL("PRAGMA defer_foreign_keys = ON")

            for (row in bundle.rows) {
                val (keyJson, localPk) = bundleRowKeyToLocalKey(state, row.table, row.key)
                val current = loadStructuredRowState(row.schema, row.table, keyJson)
                if (current.exists && current.rowVersion >= row.rowVersion) {
                    continue
                }
                applyBundleRowAuthoritatively(state, row, localPk)
                deleteDirtyRow(row.schema, row.table, keyJson)
                updatedTables += row.table.lowercase()
            }

            db.prepare(
                """
                UPDATE _sync_client_state
                SET last_bundle_seq_seen = CASE
                      WHEN last_bundle_seq_seen < ? THEN ?
                      ELSE last_bundle_seq_seen
                    END
                WHERE user_id = ?
                """.trimIndent()
            ).use { st ->
                st.bindLong(1, bundle.bundleSeq)
                st.bindLong(2, bundle.bundleSeq)
                st.bindText(3, state.userId)
                st.step()
            }
            setApplyMode(state.userId, false)
        }
        return updatedTables
    }

    private suspend fun applyStagedSnapshot(
        state: RuntimeState,
        session: SnapshotSession,
        rotateSource: Boolean,
    ): Set<String> {
        val updatedTables = state.validated.tables.map { it.tableName.lowercase() }.toSet()
        db.transaction(TransactionMode.IMMEDIATE) {
            db.execSQL("PRAGMA defer_foreign_keys = ON")
            setApplyMode(state.userId, true)
            clearManagedTables(state)

            var stagedRowCount = 0L
            db.prepare(
                """
                SELECT row_ordinal, schema_name, table_name, key_json, row_version, payload
                FROM _sync_snapshot_stage
                WHERE snapshot_id = ?
                ORDER BY row_ordinal
                """.trimIndent()
            ).use { st ->
                st.bindText(1, session.snapshotId)
                while (st.step()) {
                    val schemaName = st.getText(1)
                    val tableName = st.getText(2)
                    val keyJson = st.getText(3)
                    val rowVersion = st.getLong(4)
                    val payloadText = st.getText(5)
                    val key = json.parseToJsonElement(keyJson) as? JsonObject
                        ?: error("staged snapshot key_json must be a JSON object")
                    val (_, localPk) = bundleRowKeyToLocalKey(state, tableName, key.entries.associate { it.key to it.value.jsonPrimitive.content })
                    val bundleRow = BundleRow(
                        schema = schemaName,
                        table = tableName,
                        key = key.entries.associate { it.key to it.value.jsonPrimitive.content },
                        op = "INSERT",
                        rowVersion = rowVersion,
                        payload = json.parseToJsonElement(payloadText),
                    )
                    applyBundleRowAuthoritatively(state, bundleRow, localPk)
                    stagedRowCount++
                }
            }

            require(stagedRowCount == session.rowCount) {
                "staged snapshot row count $stagedRowCount does not match expected row_count ${session.rowCount}"
            }

            if (rotateSource) {
                val newSourceId = randomSourceId()
                db.prepare(
                    """
                    UPDATE _sync_client_state
                    SET source_id = ?, next_source_bundle_id = 1, last_bundle_seq_seen = ?, rebuild_required = 0
                    WHERE user_id = ?
                    """.trimIndent()
                ).use { st ->
                    st.bindText(1, newSourceId)
                    st.bindLong(2, session.snapshotBundleSeq)
                    st.bindText(3, state.userId)
                    st.step()
                }
                currentSourceId = newSourceId
            } else {
                db.prepare(
                    """
                    UPDATE _sync_client_state
                    SET last_bundle_seq_seen = ?, rebuild_required = 0
                    WHERE user_id = ?
                    """.trimIndent()
                ).use { st ->
                    st.bindLong(1, session.snapshotBundleSeq)
                    st.bindText(2, state.userId)
                    st.step()
                }
            }

            db.prepare("DELETE FROM _sync_snapshot_stage WHERE snapshot_id = ?").use { st ->
                st.bindText(1, session.snapshotId)
                st.step()
            }
            setApplyMode(state.userId, false)
        }
        return updatedTables
    }

    private suspend fun clearManagedTables(state: RuntimeState) {
        for (table in state.validated.tables) {
            db.execSQL("DELETE FROM ${quoteIdent(table.tableName)}")
            db.prepare(
                "DELETE FROM _sync_row_state WHERE schema_name = ? AND table_name = ?"
            ).use { st ->
                st.bindText(1, state.validated.schema)
                st.bindText(2, table.tableName)
                st.step()
            }
        }
        db.execSQL("DELETE FROM _sync_dirty_rows")
        db.execSQL("DELETE FROM _sync_push_outbound")
        db.execSQL("DELETE FROM _sync_push_stage")
    }

    private suspend fun stageSnapshotChunk(
        state: RuntimeState,
        session: SnapshotSession,
        chunk: SnapshotChunkResponse,
        afterRowOrdinal: Long,
    ) {
        validateSnapshotChunkResponse(chunk, session.snapshotId, session.snapshotBundleSeq, afterRowOrdinal)
        db.transaction(TransactionMode.IMMEDIATE) {
            var rowOrdinal = afterRowOrdinal
            for (row in chunk.rows) {
                require(row.schema == state.validated.schema) {
                    "snapshot row schema ${row.schema} does not match client schema ${state.validated.schema}"
                }
                validateSnapshotRow(row)
                val (keyJson, _) = bundleRowKeyToLocalKey(state, row.table, row.key)
                rowOrdinal++
                db.prepare(
                    """
                    INSERT INTO _sync_snapshot_stage (
                      snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """.trimIndent()
                ).use { st ->
                    st.bindText(1, session.snapshotId)
                    st.bindLong(2, rowOrdinal)
                    st.bindText(3, row.schema)
                    st.bindText(4, row.table)
                    st.bindText(5, keyJson)
                    st.bindLong(6, row.rowVersion)
                    st.bindText(7, row.payload.toString())
                    st.step()
                }
            }
        }
    }

    private suspend fun createSnapshotSession(): SnapshotSession {
        val call = http.post("/sync/snapshot-sessions")
        if (call.status != HttpStatusCode.OK) {
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            throw RuntimeException("snapshot session request failed: HTTP ${call.status} - $raw")
        }
        val session: SnapshotSession = call.body()
        validateSnapshotSession(session)
        return session
    }

    private suspend fun fetchSnapshotChunk(
        snapshotId: String,
        snapshotBundleSeq: Long,
        afterRowOrdinal: Long,
        maxRows: Int,
    ): SnapshotChunkResponse {
        val call = http.get("/sync/snapshot-sessions/$snapshotId") {
            url {
                parameters.append("after_row_ordinal", afterRowOrdinal.toString())
                parameters.append("max_rows", maxRows.toString())
            }
        }
        if (call.status != HttpStatusCode.OK) {
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            throw RuntimeException("snapshot chunk request failed: HTTP ${call.status} - $raw")
        }
        val chunk: SnapshotChunkResponse = call.body()
        validateSnapshotChunkResponse(chunk, snapshotId, snapshotBundleSeq, afterRowOrdinal)
        return chunk
    }

    private suspend fun deleteSnapshotSessionBestEffort(snapshotId: String) {
        if (snapshotId.isBlank()) return
        runCatching { http.delete("/sync/snapshot-sessions/$snapshotId") }
    }

    private suspend fun sendPullRequest(
        afterBundleSeq: Long,
        maxBundles: Int,
        targetBundleSeq: Long,
    ): PullResponse {
        val call = http.get("/sync/pull") {
            url {
                parameters.append("after_bundle_seq", afterBundleSeq.toString())
                parameters.append("max_bundles", maxBundles.toString())
                if (targetBundleSeq > 0) {
                    parameters.append("target_bundle_seq", targetBundleSeq.toString())
                }
            }
        }
        if (call.status != HttpStatusCode.OK) {
            val raw = runCatching { call.bodyAsText() }.getOrDefault("")
            if (call.status == HttpStatusCode.Conflict) {
                val error = runCatching { json.decodeFromString(ErrorResponse.serializer(), raw) }.getOrNull()
                if (error?.error == "history_pruned") {
                    throw HistoryPrunedException(error.message)
                }
            }
            throw RuntimeException("pull failed: HTTP ${call.status} - $raw")
        }
        return call.body()
    }

    private suspend fun setApplyMode(userId: String, enabled: Boolean) {
        db.prepare("UPDATE _sync_client_state SET apply_mode = ? WHERE user_id = ?").use { st ->
            st.bindLong(1, if (enabled) 1 else 0)
            st.bindText(2, userId)
            st.step()
        }
    }

    private suspend fun deleteDirtyRow(
        schemaName: String,
        tableName: String,
        keyJson: String,
    ) {
        db.prepare(
            "DELETE FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?"
        ).use { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.step()
        }
    }

    private suspend fun loadDirtyUploadState(
        schemaName: String,
        tableName: String,
        keyJson: String,
    ): DirtyUploadState {
        return db.prepare(
            """
            SELECT op, payload, base_row_version, dirty_ordinal
            FROM _sync_dirty_rows
            WHERE schema_name = ? AND table_name = ? AND key_json = ?
            """.trimIndent()
        ).use { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            if (!st.step()) {
                DirtyUploadState()
            } else {
                DirtyUploadState(
                    exists = true,
                    op = st.getText(0),
                    payload = if (st.isNull(1)) null else st.getText(1),
                    baseRowVersion = st.getLong(2),
                    currentOrdinal = st.getLong(3),
                )
            }
        }
    }

    private suspend fun requeueDirtyIntent(
        schemaName: String,
        tableName: String,
        keyJson: String,
        op: String,
        baseRowVersion: Long,
        payload: String?,
    ) {
        db.prepare(
            """
            INSERT INTO _sync_dirty_rows(schema_name, table_name, key_json, op, base_row_version, payload, dirty_ordinal, updated_at)
            VALUES (
              ?, ?, ?, ?, ?, ?,
              COALESCE(
                (SELECT dirty_ordinal FROM _sync_dirty_rows WHERE schema_name = ? AND table_name = ? AND key_json = ?),
                (SELECT COALESCE(MAX(dirty_ordinal), 0) + 1 FROM _sync_dirty_rows)
              ),
              strftime('%Y-%m-%dT%H:%M:%fZ','now')
            )
            ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
              op = excluded.op,
              base_row_version = excluded.base_row_version,
              payload = excluded.payload,
              updated_at = excluded.updated_at
            """.trimIndent()
        ).use { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.bindText(4, op)
            st.bindLong(5, baseRowVersion)
            if (payload == null) st.bindNull(6) else st.bindText(6, payload)
            st.bindText(7, schemaName)
            st.bindText(8, tableName)
            st.bindText(9, keyJson)
            st.step()
        }
    }

    private suspend fun applyBundleRowAuthoritatively(
        state: RuntimeState,
        row: BundleRow,
        localPk: String,
    ) {
        when (row.op) {
            "INSERT", "UPDATE" -> {
                val payload = row.payload as? JsonObject
                    ?: error("bundle row payload must be a JSON object for ${row.schema}.${row.table}")
                upsertRow(row.table, payload)
            }
            "DELETE" -> {
                val pkColumn = state.validated.pkByTable[row.table]
                    ?: error("table ${row.table} is not configured for sync")
                db.prepare("DELETE FROM ${quoteIdent(row.table)} WHERE ${quoteIdent(pkColumn)} = ?").use { st ->
                    bindPrimaryKey(st, 1, row.table, localPk)
                    st.step()
                }
            }
            else -> error("unsupported bundle row op ${row.op}")
        }

        val (keyJson, _) = bundleRowKeyToLocalKey(state, row.table, row.key)
        updateStructuredRowState(state.validated.schema, row.table, keyJson, row.rowVersion, row.op == "DELETE")
    }

    private suspend fun updateStructuredRowState(
        schemaName: String,
        tableName: String,
        keyJson: String,
        rowVersion: Long,
        deleted: Boolean,
    ) {
        db.prepare(
            """
            INSERT INTO _sync_row_state(schema_name, table_name, key_json, row_version, deleted, updated_at)
            VALUES(?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            ON CONFLICT(schema_name, table_name, key_json) DO UPDATE SET
              row_version = excluded.row_version,
              deleted = excluded.deleted,
              updated_at = excluded.updated_at
            """.trimIndent()
        ).use { st ->
            st.bindText(1, schemaName)
            st.bindText(2, tableName)
            st.bindText(3, keyJson)
            st.bindLong(4, rowVersion)
            st.bindLong(5, if (deleted) 1 else 0)
            st.step()
        }
    }

    private suspend fun bundleRowKeyToLocalKey(
        state: RuntimeState,
        tableName: String,
        key: SyncKey,
    ): Pair<String, String> {
        val keyColumn = state.validated.keyByTable[tableName]?.singleOrNull()
            ?: error("table $tableName is not configured for sync")
        val wireValue = key[keyColumn.lowercase()] ?: key[keyColumn]
            ?: error("bundle row key for $tableName is missing $keyColumn")
        val localPk = if (isPrimaryKeyBlob(tableName)) {
            bytesToHexLower(decodeCanonicalWireUuidBytes(wireValue))
        } else {
            wireValue
        }
        return buildKeyJson(tableName, localPk) to localPk
    }

    private suspend fun buildKeyJson(tableName: String, localPk: String): String {
        val keyColumn = requireBootstrapped().validated.keyByTable[tableName]?.singleOrNull()
            ?: error("table $tableName is not configured for sync")
        return buildJsonObject {
            put(keyColumn.lowercase(), JsonPrimitive(localPk))
        }.toString()
    }

    private suspend fun serializeExistingRow(
        tableName: String,
        localPk: String,
    ): String? {
        val tableInfo = tableInfoCache.get(db, tableName)
        if (tableInfo.columns.isEmpty()) return null
        val pkColumn = requireBootstrapped().validated.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        val selectColumns = tableInfo.columns.joinToString(", ") { quoteIdent(it.name) }
        return db.prepare(
            "SELECT $selectColumns FROM ${quoteIdent(tableName)} WHERE ${quoteIdent(pkColumn)} = ?"
        ).use { st ->
            bindPrimaryKey(st, 1, tableName, localPk)
            if (!st.step()) return@use null
            buildJsonObject {
                tableInfo.columns.forEachIndexed { index, column ->
                    val key = column.name.lowercase()
                    if (st.isNull(index)) {
                        put(key, JsonNull)
                    } else {
                        val type = column.declaredType.lowercase()
                        when {
                            type.contains("blob") -> put(key, JsonPrimitive(bytesToHexLower(st.getBlob(index))))
                            type.contains("int") -> put(key, JsonPrimitive(st.getLong(index)))
                            type.contains("real") || type.contains("float") || type.contains("double") ->
                                put(key, JsonPrimitive(st.getDouble(index)))
                            else -> put(key, JsonPrimitive(st.getText(index)))
                        }
                    }
                }
            }.toString()
        }
    }

    @OptIn(ExperimentalEncodingApi::class)
    private suspend fun processPayloadForUpload(
        tableName: String,
        payloadText: String,
    ): JsonElement {
        val payload = json.parseToJsonElement(payloadText) as? JsonObject
            ?: error("dirty payload for $tableName must be a JSON object")
        val tableInfo = tableInfoCache.get(db, tableName)
        val pkColumn = requireBootstrapped().validated.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")

        return buildJsonObject {
            for (column in tableInfo.columns) {
                val key = column.name.lowercase()
                val value = payload[key] ?: payload[column.name] ?: JsonNull
                if (value is JsonNull) {
                    put(key, JsonNull)
                    continue
                }
                if (column.declaredType.lowercase().contains("blob")) {
                    val text = value.jsonPrimitive.content
                    if (column.name.equals(pkColumn, ignoreCase = true) || tableInfo.isBlobReferenceColumn(column.name)) {
                        put(key, JsonPrimitive(hexToUuidString(text)))
                    } else {
                        put(key, JsonPrimitive(Base64.encode(decodeBlobBytesFromString(text))))
                    }
                } else {
                    put(key, value)
                }
            }
        }
    }

    private suspend fun upsertRow(
        tableName: String,
        payload: JsonObject,
    ) {
        val tableInfo = tableInfoCache.get(db, tableName)
        val pkColumn = requireBootstrapped().validated.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        val normalized = payload.entries.associate { it.key.lowercase() to it.value }
        require(normalized.size == tableInfo.columns.size) {
            "payload for $tableName must contain every table column"
        }

        val columns = mutableListOf<String>()
        val values = mutableListOf<JsonElement>()
        val updates = mutableListOf<String>()
        for (column in tableInfo.columns) {
            val key = column.name.lowercase()
            val value = normalized[key] ?: error("payload for $tableName is missing column ${column.name}")
            columns += quoteIdent(column.name)
            values += value
            if (!column.name.equals(pkColumn, ignoreCase = true)) {
                updates += "${quoteIdent(column.name)} = excluded.${quoteIdent(column.name)}"
            }
        }

        val sql = buildString {
            append("INSERT INTO ${quoteIdent(tableName)} (${columns.joinToString(", ")}) VALUES (")
            append(columns.indices.joinToString(", ") { "?" })
            append(")")
            if (updates.isEmpty()) {
                append(" ON CONFLICT(${quoteIdent(pkColumn)}) DO NOTHING")
            } else {
                append(" ON CONFLICT(${quoteIdent(pkColumn)}) DO UPDATE SET ${updates.joinToString(", ")}")
            }
        }

        db.prepare(sql).use { st ->
            tableInfo.columns.forEachIndexed { index, column ->
                bindPayloadValue(st, index + 1, column, values[index], tableName)
            }
            st.step()
        }
    }

    private suspend fun bindPrimaryKey(
        statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement,
        index: Int,
        tableName: String,
        pkValue: String,
    ) {
        if (isPrimaryKeyBlob(tableName)) {
            statement.bindBlob(index, decodeUuidBytesFromString(pkValue))
        } else {
            statement.bindText(index, pkValue)
        }
    }

    private suspend fun isPrimaryKeyBlob(tableName: String): Boolean {
        val tableInfo = tableInfoCache.get(db, tableName)
        val pkColumn = requireBootstrapped().validated.pkByTable[tableName]
            ?: error("table $tableName is not configured for sync")
        return tableInfo.columns.any {
            it.name.equals(pkColumn, ignoreCase = true) && it.declaredType.lowercase().contains("blob")
        }
    }

    private suspend fun normalizePkForMeta(tableName: String, pkValue: String): String {
        return if (isPrimaryKeyBlob(tableName)) {
            bytesToHexLower(decodeUuidBytesFromString(pkValue))
        } else {
            pkValue
        }
    }

    private suspend fun normalizePkForServer(tableName: String, pkValue: String): String {
        return if (isPrimaryKeyBlob(tableName)) {
            hexToUuidString(pkValue)
        } else {
            pkValue
        }
    }

    private suspend fun bindPayloadValue(
        statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement,
        index: Int,
        column: ColumnInfo,
        value: JsonElement,
        tableName: String,
    ) {
        if (value is JsonNull) {
            statement.bindNull(index)
            return
        }
        val primitive = value.jsonPrimitive
        if (column.declaredType.lowercase().contains("blob")) {
            val tableInfo = tableInfoCache.get(db, tableName)
            val bytes = if ((column.isPrimaryKey && isPrimaryKeyBlob(tableName)) || tableInfo.isBlobReferenceColumn(column.name)) {
                decodeCanonicalWireUuidBytes(primitive.content)
            } else {
                decodeCanonicalWireBlobBytes(primitive.content)
            }
            statement.bindBlob(index, bytes)
            return
        }
        if (primitive.isString) {
            statement.bindText(index, primitive.content)
            return
        }

        val content = primitive.content
        val asLong = content.toLongOrNull()
        if (asLong != null) {
            statement.bindLong(index, asLong)
            return
        }
        val asDouble = content.toDoubleOrNull()
        if (asDouble != null) {
            statement.bindDouble(index, asDouble)
            return
        }
        statement.bindText(index, content)
    }

    private fun validatePullResponse(
        response: PullResponse,
        afterBundleSeq: Long,
    ) {
        require(response.stableBundleSeq >= 0) {
            "pull response stable_bundle_seq ${response.stableBundleSeq} must be non-negative"
        }
        require(response.stableBundleSeq >= afterBundleSeq) {
            "pull response stable_bundle_seq ${response.stableBundleSeq} is behind requested after_bundle_seq $afterBundleSeq"
        }
        require(response.bundles.isEmpty() || response.stableBundleSeq > 0) {
            "pull response missing stable_bundle_seq for non-empty bundle list"
        }

        var previous = afterBundleSeq
        response.bundles.forEachIndexed { index, bundle ->
            validateBundle(bundle)
            require(bundle.bundleSeq > previous) {
                "pull response bundle_seq ${bundle.bundleSeq} is not strictly greater than previous $previous"
            }
            require(bundle.bundleSeq <= response.stableBundleSeq) {
                "pull response bundle_seq ${bundle.bundleSeq} exceeds stable_bundle_seq ${response.stableBundleSeq}"
            }
            previous = bundle.bundleSeq
            if (index > 0) {
                require(response.bundles[index - 1].bundleSeq < bundle.bundleSeq) {
                    "pull response bundle order is not strictly increasing"
                }
            }
        }
    }

    private fun validateBundle(bundle: Bundle) {
        require(bundle.bundleSeq > 0) { "bundle_seq ${bundle.bundleSeq} must be positive" }
        require(bundle.sourceId.isNotBlank()) { "bundle source_id must be non-empty" }
        require(bundle.sourceBundleId > 0) { "bundle source_bundle_id ${bundle.sourceBundleId} must be positive" }
        bundle.rows.forEachIndexed { index, row ->
            try {
                validateBundleRow(row)
            } catch (e: Throwable) {
                throw IllegalArgumentException("invalid bundle row $index: ${e.message}", e)
            }
        }
    }

    private fun validateBundleRow(row: BundleRow) {
        require(row.schema.isNotBlank()) { "bundle row schema must be non-empty" }
        require(row.table.isNotBlank()) { "bundle row table must be non-empty" }
        require(row.key.isNotEmpty()) { "bundle row key must be non-empty" }
        require(row.key.values.all { it.isNotBlank() }) { "bundle row key values must be non-empty strings" }
        require(row.rowVersion > 0) { "bundle row row_version ${row.rowVersion} must be positive" }
        require(row.op in setOf("INSERT", "UPDATE", "DELETE")) { "bundle row op ${row.op} is unsupported" }
        if (row.op != "DELETE") {
            require(row.payload != null) { "bundle row payload must be present for ${row.op}" }
        }
    }

    private fun validateSnapshotRow(row: SnapshotRow) {
        require(row.schema.isNotBlank()) { "snapshot row schema must be non-empty" }
        require(row.table.isNotBlank()) { "snapshot row table must be non-empty" }
        require(row.key.isNotEmpty()) { "snapshot row key must be non-empty" }
        require(row.rowVersion > 0) { "snapshot row row_version ${row.rowVersion} must be positive" }
        require(row.payload !is JsonNull) { "snapshot row payload must be present" }
    }

    private fun validateSnapshotSession(session: SnapshotSession) {
        require(session.snapshotId.isNotBlank()) { "snapshot session response missing snapshot_id" }
        require(session.snapshotBundleSeq >= 0) {
            "snapshot session snapshot_bundle_seq ${session.snapshotBundleSeq} must be non-negative"
        }
        require(session.rowCount >= 0) { "snapshot session row_count ${session.rowCount} must be non-negative" }
        require(session.rowCount == 0L || session.snapshotBundleSeq > 0) {
            "snapshot session missing snapshot_bundle_seq for non-empty row set"
        }
        require(session.byteCount >= 0) { "snapshot session byte_count ${session.byteCount} must be non-negative" }
        require(session.expiresAt.isNotBlank()) { "snapshot session response missing expires_at" }
    }

    private fun validateSnapshotChunkResponse(
        chunk: SnapshotChunkResponse,
        snapshotId: String,
        snapshotBundleSeq: Long,
        afterRowOrdinal: Long,
    ) {
        require(chunk.snapshotId == snapshotId) {
            "snapshot chunk response snapshot_id ${chunk.snapshotId} does not match requested $snapshotId"
        }
        require(chunk.snapshotBundleSeq == snapshotBundleSeq) {
            "snapshot chunk response snapshot_bundle_seq ${chunk.snapshotBundleSeq} does not match session $snapshotBundleSeq"
        }
        require(chunk.rows.isEmpty() || chunk.snapshotBundleSeq > 0) {
            "snapshot chunk response missing snapshot_bundle_seq for non-empty row set"
        }
        require(chunk.nextRowOrdinal == afterRowOrdinal + chunk.rows.size) {
            "snapshot chunk response next_row_ordinal ${chunk.nextRowOrdinal} does not match expected ${afterRowOrdinal + chunk.rows.size}"
        }
        require(!chunk.hasMore || chunk.rows.isNotEmpty()) {
            "snapshot chunk response with has_more=true must include at least one row"
        }
        chunk.rows.forEachIndexed { index, row ->
            try {
                validateSnapshotRow(row)
            } catch (e: Throwable) {
                throw IllegalArgumentException("invalid snapshot row $index: ${e.message}", e)
            }
        }
    }

    private fun validatePushSessionCreateResponse(
        response: PushSessionCreateResponse,
        sourceBundleId: Long,
        plannedRowCount: Long,
        sourceId: String,
    ) {
        when (response.status) {
            "staging" -> {
                require(response.pushId.isNotBlank()) { "push session response missing push_id" }
                require(response.plannedRowCount == plannedRowCount) {
                    "push session response planned_row_count ${response.plannedRowCount} does not match requested $plannedRowCount"
                }
                require(response.nextExpectedRowOrdinal == 0L) {
                    "push session response next_expected_row_ordinal ${response.nextExpectedRowOrdinal} must be 0"
                }
            }

            "already_committed" -> {
                require(response.bundleSeq > 0) { "push session already_committed response missing bundle_seq" }
                require(response.sourceId == sourceId) {
                    "push session already_committed response source_id ${response.sourceId} does not match client $sourceId"
                }
                require(response.sourceBundleId == sourceBundleId) {
                    "push session already_committed response source_bundle_id ${response.sourceBundleId} does not match requested $sourceBundleId"
                }
                require(response.rowCount >= 0) {
                    "push session already_committed response missing row_count"
                }
                require(response.bundleHash.isNotBlank()) {
                    "push session already_committed response missing bundle_hash"
                }
            }

            else -> error("push session response returned unsupported status ${response.status}")
        }
    }

    private fun committedPushBundleFromCreateResponse(
        response: PushSessionCreateResponse,
        sourceId: String,
        sourceBundleId: Long,
    ): CommittedPushBundle {
        validatePushSessionCreateResponse(response, sourceBundleId, response.rowCount, sourceId)
        require(response.status == "already_committed") {
            "unexpected push session status ${response.status}"
        }
        return CommittedPushBundle(
            bundleSeq = response.bundleSeq,
            sourceId = response.sourceId,
            sourceBundleId = response.sourceBundleId,
            rowCount = response.rowCount,
            bundleHash = response.bundleHash,
        )
    }

    private fun committedPushBundleFromCommitResponse(
        response: PushSessionCommitResponse,
        sourceId: String,
        sourceBundleId: Long,
    ): CommittedPushBundle {
        require(response.bundleSeq > 0) { "push commit response bundle_seq must be positive" }
        require(response.sourceId == sourceId) {
            "push commit response source_id ${response.sourceId} does not match client $sourceId"
        }
        require(response.sourceBundleId == sourceBundleId) {
            "push commit response source_bundle_id ${response.sourceBundleId} does not match requested $sourceBundleId"
        }
        require(response.rowCount >= 0) { "push commit response row_count must be non-negative" }
        require(response.bundleHash.isNotBlank()) { "push commit response bundle_hash must be non-empty" }
        return CommittedPushBundle(
            bundleSeq = response.bundleSeq,
            sourceId = response.sourceId,
            sourceBundleId = response.sourceBundleId,
            rowCount = response.rowCount,
            bundleHash = response.bundleHash,
        )
    }

    private fun validateCommittedBundleRowsResponse(
        response: CommittedBundleRowsResponse,
        committed: CommittedPushBundle,
        afterRowOrdinal: Long?,
    ) {
        require(response.bundleSeq == committed.bundleSeq) {
            "committed bundle chunk response bundle_seq ${response.bundleSeq} does not match expected ${committed.bundleSeq}"
        }
        require(response.sourceId == committed.sourceId) {
            "committed bundle chunk response source_id ${response.sourceId} does not match expected ${committed.sourceId}"
        }
        require(response.sourceBundleId == committed.sourceBundleId) {
            "committed bundle chunk response source_bundle_id ${response.sourceBundleId} does not match expected ${committed.sourceBundleId}"
        }
        require(response.rowCount == committed.rowCount) {
            "committed bundle chunk response row_count ${response.rowCount} does not match expected ${committed.rowCount}"
        }
        require(response.bundleHash == committed.bundleHash) {
            "committed bundle chunk response bundle_hash ${response.bundleHash} does not match expected ${committed.bundleHash}"
        }

        val logicalAfter = afterRowOrdinal ?: -1L
        val expectedNext = if (response.rows.isEmpty()) logicalAfter else logicalAfter + response.rows.size
        require(response.nextRowOrdinal == expectedNext) {
            "committed bundle chunk response next_row_ordinal ${response.nextRowOrdinal} does not match expected $expectedNext"
        }
        require(!response.hasMore || response.rows.isNotEmpty()) {
            "committed bundle chunk response with has_more=true must include at least one row"
        }
        response.rows.forEachIndexed { index, row ->
            try {
                validateBundleRow(row)
            } catch (e: Throwable) {
                throw IllegalArgumentException("invalid committed bundle row $index: ${e.message}", e)
            }
        }
    }

    private fun snapshotChunkRows(): Int = config.snapshotChunkRows.takeIf { it > 0 } ?: 1000

    private fun pushChunkRows(): Int = config.uploadLimit.takeIf { it > 0 } ?: 1000

    private fun committedBundleChunkRows(): Int = config.downloadLimit.takeIf { it > 0 } ?: 1000

    private fun canonicalPayloadEqual(left: String?, right: String?): Boolean {
        if (left == null && right == null) return true
        if (left == null || right == null) return false
        val leftValue = json.parseToJsonElement(left)
        val rightValue = json.parseToJsonElement(right)
        return json.encodeToString(JsonElement.serializer(), leftValue) ==
            json.encodeToString(JsonElement.serializer(), rightValue)
    }

    private fun livePayloadMatchesUploadedIntent(
        uploaded: DirtyRowCapture,
        livePayload: String?,
    ): Boolean {
        return if (uploaded.op == "DELETE") {
            livePayload == null
        } else {
            livePayload != null && canonicalPayloadEqual(livePayload, uploaded.localPayload)
        }
    }

    private fun reconcileAppliedUploadState(
        change: DirtyRowCapture,
        pendingExists: Boolean,
        pendingMatches: Boolean,
        livePayload: String?,
        liveExists: Boolean,
        liveMatches: Boolean,
    ): ReplayDecision {
        return when (change.op) {
            "DELETE" -> {
                if (liveExists && livePayload != null) {
                    ReplayDecision(
                        requeueOp = "INSERT",
                        requeuePayload = livePayload,
                        needsRequeue = true,
                    )
                } else {
                    ReplayDecision()
                }
            }
            else -> {
                if (!liveExists) {
                    ReplayDecision(requeueOp = "DELETE", requeuePayload = null, needsRequeue = true)
                } else if (pendingExists && !pendingMatches) {
                    ReplayDecision(requeueOp = "UPDATE", requeuePayload = livePayload, needsRequeue = true)
                } else if (!liveMatches) {
                    ReplayDecision(requeueOp = "UPDATE", requeuePayload = livePayload, needsRequeue = true)
                } else {
                    ReplayDecision()
                }
            }
        }
    }

    private suspend fun <T> runSyncOperation(block: suspend () -> T): Result<T> = runCatching {
        if (!syncGate.tryLock()) {
            throw SyncOperationInProgressException()
        }
        try {
            block()
        } finally {
            syncGate.unlock()
        }
    }
}

private data class RuntimeState(
    val validated: ValidatedConfig,
    val userId: String,
    val sourceId: String,
)

private data class ClientBundleState(
    val nextSourceBundleId: Long,
    val lastBundleSeqSeen: Long,
)

private data class DirtySnapshotRow(
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val baseRowVersion: Long,
    val dirtyOrdinal: Long,
)

private data class DirtyRowCapture(
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val localPk: String,
    val wireKey: SyncKey,
    val op: String,
    val baseRowVersion: Long,
    val localPayload: String?,
    val dirtyOrdinal: Long,
)

private data class PreparedPush(
    val sourceBundleId: Long,
    val rows: List<DirtyRowCapture>,
)

private data class PushOutboundSnapshot(
    val sourceBundleId: Long,
    val rows: List<DirtyRowCapture>,
)

private data class CommittedPushBundle(
    val bundleSeq: Long,
    val sourceId: String,
    val sourceBundleId: Long,
    val rowCount: Long,
    val bundleHash: String,
)

private data class StagedPushBundleRow(
    val schemaName: String,
    val tableName: String,
    val keyJson: String,
    val localPk: String,
    val wireKey: SyncKey,
    val op: String,
    val rowVersion: Long,
    val payload: String?,
)

private data class StructuredRowState(
    val exists: Boolean = false,
    val rowVersion: Long = 0,
    val deleted: Boolean = false,
)

private data class DirtyUploadState(
    val exists: Boolean = false,
    val op: String = "",
    val payload: String? = null,
    val baseRowVersion: Long = 0,
    val currentOrdinal: Long = 0,
)

private data class ReplayDecision(
    val requeueOp: String = "",
    val requeuePayload: String? = null,
    val needsRequeue: Boolean = false,
)

private class DirtyStateRejectedException(
    private val dirtyCount: Int,
) : RuntimeException("cannot pull while $dirtyCount local dirty rows exist")

private fun bytesToHexLower(bytes: ByteArray): String {
    val out = StringBuilder(bytes.size * 2)
    for (byte in bytes) {
        val value = byte.toInt() and 0xff
        out.append("0123456789abcdef"[value ushr 4])
        out.append("0123456789abcdef"[value and 0x0f])
    }
    return out.toString()
}

private fun isHexStringValue(value: String): Boolean {
    return value.all { it.isDigit() || it.lowercaseChar() in 'a'..'f' }
}

private fun tryDecodeBase64Exact(value: String): ByteArray? {
    return runCatching { Base64.decode(value) }
        .getOrNull()
        ?.takeIf { Base64.encode(it) == value }
}

// Local/internal helper only. The supported wire contract must use the canonical
// decoders below instead of this tolerant local representation parser.
@OptIn(ExperimentalUuidApi::class, ExperimentalEncodingApi::class)
private fun decodeBlobBytesFromString(value: String): ByteArray {
    if (value.isEmpty()) return ByteArray(0)
    runCatching { return Uuid.parse(value).toByteArray() }
    tryDecodeBase64Exact(value)?.let { return it }
    val clean = value.trim().removePrefix("\\x").removePrefix("\\X")
    if (clean.length % 2 == 0 && isHexStringValue(clean)) {
        val out = ByteArray(clean.length / 2)
        var index = 0
        while (index < clean.length) {
            out[index / 2] = clean.substring(index, index + 2).toInt(16).toByte()
            index += 2
        }
        return out
    }
    error("invalid blob encoding")
}

// Canonical wire decoder for non-key binary payload fields.
@OptIn(ExperimentalEncodingApi::class)
private fun decodeCanonicalWireBlobBytes(value: String): ByteArray {
    if (value.isEmpty()) return ByteArray(0)
    return tryDecodeBase64Exact(value) ?: error("invalid canonical wire blob encoding")
}

// Local/internal helper only. The supported wire contract must use
// decodeCanonicalWireUuidBytes for UUID-valued keys and UUID payload key columns.
@OptIn(ExperimentalUuidApi::class, ExperimentalEncodingApi::class)
private fun decodeUuidBytesFromString(value: String): ByteArray {
    runCatching { return Uuid.parse(value).toByteArray() }
    tryDecodeBase64Exact(value)?.let {
        require(it.size == 16) { "base64 decoded length is ${it.size}, want 16" }
        return it
    }
    val clean = value.trim()
    if (clean.length % 2 == 0 && isHexStringValue(clean)) {
        val bytes = decodeBlobBytesFromString(clean)
        require(bytes.size == 16) { "hex decoded length is ${bytes.size}, want 16" }
        return bytes
    }
    error("not a UUID encoding")
}

// Canonical wire decoder for UUID-valued keys and UUID payload key columns.
@OptIn(ExperimentalUuidApi::class)
private fun decodeCanonicalWireUuidBytes(value: String): ByteArray {
    require(value.trim() == value) { "invalid canonical wire UUID encoding" }
    require(value.length == 36) { "invalid canonical wire UUID encoding" }
    val parsed = Uuid.parse(value)
    require(parsed.toString() == value.lowercase()) { "invalid canonical wire UUID encoding" }
    return parsed.toByteArray()
}

@OptIn(ExperimentalUuidApi::class)
private fun hexToUuidString(value: String): String {
    val clean = value.trim()
    if (clean.length == 36) {
        return Uuid.parse(clean).toString()
    }
    require(clean.length == 32) { "expected 32 hex chars for UUID, got ${clean.length}" }
    return Uuid.parseHex(clean).toString()
}
