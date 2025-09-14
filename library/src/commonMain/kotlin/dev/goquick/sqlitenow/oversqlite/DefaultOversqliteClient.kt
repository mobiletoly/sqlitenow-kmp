package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.logger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.concurrent.Volatile

/**
 * A multiplatform SQLite-backed implementation of OversqliteClient.
 * Relies on androidx.sqlite for the database and Ktor client for networking.
 *
 * The HttpClient should be configured with proper authentication (e.g., Bearer token),
 * token refresh logic, and base URL. This gives users full control over HTTP configuration.
 *
 * Example HttpClient configuration:
 * ```kotlin
 * HttpClient {
 *     install(Auth) {
 *         bearer {
 *             loadTokens { BearerTokens(savedToken, null) }
 *             refreshTokens { BearerTokens(fetchNewToken(), null) }
 *         }
 *     }
 *     defaultRequest {
 *         url("https://api.myapp.com")
 *     }
 * }
 * ```
 */
class DefaultOversqliteClient(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val http: HttpClient,
    private val resolver: Resolver = ServerWinsResolver,
    private val tablesUpdateListener: (table: Set<String>) -> Unit,
) : OversqliteClient {
    // Prevent concurrent uploadOnce/downloadOnce DB phases from overlapping
    private val opGate = Semaphore(1)

    // Cache PRAGMA table_info lookups per table (lowercased key)
    private val tableColumnsCache = mutableMapOf<String, List<String>>()

    private val bootstrapper = SyncBootstrapper(config = config)

    private val ioDispatcher: CoroutineDispatcher = PlatformDispatchers().io

    private val uploader = SyncUploader(
        http = http,
        config = config,
        resolver = resolver,
        upsertBusinessFromPayload = ::upsertBusinessFromPayload,
        updateRowMeta = ::updateRowMeta,
        ioDispatcher = ioDispatcher
    )

    private val downloader = SyncDownloader(
        http = http,
        config = config,
        resolver = resolver,
        upsertBusinessFromPayload = ::upsertBusinessFromPayload,
        updateRowMeta = ::updateRowMeta,
        ioDispatcher = ioDispatcher
    )

    @Volatile
    private var uploadsPaused = false

    @Volatile
    private var downloadsPaused = false

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

    override suspend fun bootstrap(userId: String, sourceId: String): Result<Unit> =
        withContext(db.dispatcher) {
            bootstrapper.bootstrap(db, userId, sourceId)
        }

    override suspend fun uploadOnce(): Result<UploadSummary> {
        if (uploadsPaused) return Result.success(UploadSummary(0, 0, 0, 0, 0))

        return runCatching {
            logger.d { "uploadOnce: start" }

            // Phase 1 (DB, gated): read nextChangeId and prepare changes
            opGate.acquire()
            val prepared = try {
                val nextChangeId = withContext(db.dispatcher) {
                    db.prepare("SELECT next_change_id FROM _sync_client_info LIMIT 1")
                        .use { st -> if (st.step()) st.getLong(0) else null } ?: error("_sync_client_info missing")
                }
                withContext(db.dispatcher) { uploader.prepareUpload(db, nextChangeId) }
            } finally {
                opGate.release()
            }
            if (prepared.changes.isEmpty()) {
                return@runCatching UploadSummary(0, 0, 0, 0, 0)
            }

            // Phase 2 (Network): perform upload
            val response = uploader.performUpload(prepared.request)

            // Phase 3 (DB, gated): finalize
            val updatedTables = mutableSetOf<String>()
            val summary = try {
                opGate.acquire()
                withContext(db.dispatcher) {
                    uploader.finalizeUpload(db, prepared.changes, response, updatedTables)
                }
            } finally {
                opGate.release()
            }

            // Post-upload lookback drain (network + db), not holding DB context
            if (summary.total > 0) {
                drainLookbackUntilInternal()
            }

            if (updatedTables.isNotEmpty()) tablesUpdateListener(updatedTables)

            summary
        }.onFailure {
            logger.e(it) { "uploadOnce: failed ${it.message}" }
        }
    }

    override suspend fun downloadOnce(
        limit: Int, includeSelf: Boolean, until: Long
    ): Result<Pair<Int, Long>> =
        downloadOnce(limit, includeSelf, until, isPostUploadLookback = false)

    private suspend fun downloadOnce(
        limit: Int, includeSelf: Boolean, until: Long = 0L, isPostUploadLookback: Boolean = false
    ): Result<Pair<Int, Long>> {
        if (downloadsPaused) return Result.success(0 to 0L)

        val result = runCatching {
            // Phase 1 (DB, gated): read client info
            opGate.acquire()
            val clientInfo = try {
                withContext(db.dispatcher) { downloader.getClientInfoNow(db) }
            } finally {
                opGate.release()
            }

            // Phase 2 (Network): fetch changes
            val response = downloader.fetchChangesNow(
                clientInfo.sourceId, clientInfo.lastServerSeq, limit, includeSelf, until
            )

            // Phase 3 (DB, gated): apply
            val downloadResult = try {
                opGate.acquire()
                withContext(db.dispatcher) {
                    downloader.applyDownloadedPage(
                        db = db,
                        response = response,
                        includeSelf = includeSelf,
                        sourceId = clientInfo.sourceId,
                        previousAfter = clientInfo.lastServerSeq,
                        isPostUploadLookback = isPostUploadLookback
                    )
                }
            } finally {
                opGate.release()
            }

            if (downloadResult.updatedTables.isNotEmpty()) tablesUpdateListener(downloadResult.updatedTables)

            downloadResult.applied to downloadResult.nextAfter
        }.onFailure {
            logger.e(it) { "downloadOnce: failed ${it.message}" }
        }

        return result
    }

    private suspend fun downloadOnceInternal(
        limit: Int, includeSelf: Boolean, until: Long = 0L, isPostUploadLookback: Boolean = false
    ): Result<Pair<Int, Long>> {
        if (downloadsPaused) return Result.success(0 to 0L)

        val result = runCatching {
            opGate.acquire()
            val clientInfo = try {
                withContext(db.dispatcher) { downloader.getClientInfoNow(db) }
            } finally {
                opGate.release()
            }
            val response = downloader.fetchChangesNow(
                clientInfo.sourceId, clientInfo.lastServerSeq, limit, includeSelf, until
            )
            val downloadResult = try {
                opGate.acquire()
                withContext(db.dispatcher) {
                    downloader.applyDownloadedPage(
                        db = db,
                        response = response,
                        includeSelf = includeSelf,
                        sourceId = clientInfo.sourceId,
                        previousAfter = clientInfo.lastServerSeq,
                        isPostUploadLookback = isPostUploadLookback
                    )
                }
            } finally {
                opGate.release()
            }
            downloadResult.applied to downloadResult.nextAfter
        }.onFailure {
            logger.e(it) { "downloadOnceInternal: failed ${it.message}" }
        }

        return result
    }

    override fun close() { /* host manages SQLiteConnection lifecycle */
    }

    override suspend fun hydrate(
        includeSelf: Boolean, limit: Int, windowed: Boolean
    ): Result<Unit> {
        val result = runCatching {
            // Phase 0 (DB, gated): get client info and initial cursor
            opGate.acquire()
            val clientInfo = try {
                withContext(db.dispatcher) { downloader.getClientInfoNow(db) }
            } finally {
                opGate.release()
            }

            var after = clientInfo.lastServerSeq
            var frozenUntil = 0L
            var first = true
            val allUpdatedTables = mutableSetOf<String>()

            while (true) {
                val untilParam = if (windowed) frozenUntil else 0L
                val response = downloader.fetchChangesNow(
                    clientInfo.sourceId, after, limit, includeSelf, untilParam
                )

                if (first) {
                    frozenUntil = if (windowed) response.windowUntil else 0L
                    if (frozenUntil > 0L) {
                        opGate.acquire()
                        try {
                            withContext(db.dispatcher) {
                                db.execSQL("UPDATE _sync_client_info SET current_window_until=$frozenUntil")
                            }
                        } finally { opGate.release() }
                    }
                    first = false
                }

                if (response.changes.isEmpty()) {
                    if (response.nextAfter > after) {
                        opGate.acquire()
                        try {
                            withContext(db.dispatcher) {
                                db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
                                    st.bindLong(1, response.nextAfter)
                                    st.step()
                                }
                            }
                        } finally { opGate.release() }
                        after = response.nextAfter
                    }
                } else {
                    val updatedTables = try {
                        opGate.acquire()
                        withContext(db.dispatcher) {
                            downloader.applyDownloadedPage(
                                db = db,
                                response = response,
                                includeSelf = includeSelf,
                                sourceId = clientInfo.sourceId,
                                previousAfter = after
                            )
                        }
                    } finally { opGate.release() }
                    allUpdatedTables += updatedTables.updatedTables
                    after = updatedTables.nextAfter
                }

                if (!response.hasMore) break
            }

            if (frozenUntil > 0L) {
                opGate.acquire()
                try {
                    withContext(db.dispatcher) {
                        db.execSQL("UPDATE _sync_client_info SET current_window_until=0")
                    }
                } finally { opGate.release() }
            }

            if (allUpdatedTables.isNotEmpty()) tablesUpdateListener(allUpdatedTables)
        }.onFailure { logger.e(it) { "hydrate: failed" } }

        return result.map { }
    }

    // --- Helpers ---


    private suspend fun updateRowMeta(
        db: SafeSQLiteConnection, table: String, pk: String, serverVersion: Long, deleted: Boolean
    ) {
        val del = if (deleted) 1 else 0

        db.prepare(
            "INSERT OR REPLACE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted, updated_at) " +
                    "VALUES(?,?,?,?,strftime('%Y-%m-%dT%H:%M:%fZ','now'))"
        ).use { st ->
            st.bindText(1, table)
            st.bindText(2, pk)
            st.bindLong(3, serverVersion)
            st.bindLong(4, del.toLong())
            st.step()
        }

        // Remove debug logging
    }

    private suspend fun upsertBusinessFromPayload(
        db: SafeSQLiteConnection, table: String, pk: String, payload: JsonElement?
    ) {
        if (payload == null || payload !is JsonObject) return
        val tableLc = table.lowercase()
        val tableCols = getTableColumns(db, tableLc).toSet()

        // Normalize payload keys to lowercase for case-insensitive matching
        val normalized: Map<String, JsonElement> =
            payload.keys.associate { it.lowercase() to (payload[it] ?: JsonNull) }
        // Never set id from payload; only from `pk`
        val cols = normalized.keys.filter { it != "id" && it in tableCols }

        if (cols.isEmpty()) {
            // Ensure row exists; if missing, insert stub row with id only
            val exists = db.prepare("SELECT EXISTS(SELECT 1 FROM $tableLc WHERE id=?)").use { st ->
                st.bindText(1, pk)
                st.step() && st.getLong(0) == 1L
            }
            if (!exists) {
                db.prepare("INSERT INTO $tableLc(id) VALUES(?)").use { st ->
                    st.bindText(1, pk)
                    st.step()
                }
            }
            return
        }

        // UPDATE first
        val setClause = cols.joinToString(", ") { "$it=?" }
        val updateSql = "UPDATE $tableLc SET $setClause WHERE id=?"
        var updateRowsAffected = 0
        db.prepare(updateSql).use { st ->
            cols.forEachIndexed { idx, key ->
                val prim = normalized[key]?.jsonPrimitive
                val i = idx + 1
                if (prim == null || prim is JsonNull) {
                    st.bindNull(i)
                } else if (prim.isString) {
                    st.bindText(i, prim.content)
                } else {
                    val c = prim.content
                    if (c.equals("true", true) || c.equals("false", true)) {
                        st.bindLong(i, if (c.equals("true", true)) 1 else 0)
                    } else {
                        val l = c.toLongOrNull()
                        if (l != null) st.bindLong(i, l) else {
                            val d = c.toDoubleOrNull()
                            if (d != null) st.bindDouble(i, d) else st.bindText(i, c)
                        }
                    }
                }
            }
            st.bindText(cols.size + 1, pk)
            st.step()
            // Note: SQLite changes() function not available in this API
        }

        // If UPDATE affected 0 rows, INSERT id + provided columns
        val changed =
            db.prepare("SELECT changes()").use { st -> if (st.step()) st.getLong(0) else 0L }
        if (changed == 0L) {
            val insertCols = listOf("id") + cols
            val placeholders = insertCols.indices.joinToString(", ") { "?" }
            val insertSql = "INSERT INTO $tableLc (${insertCols.joinToString(", ")}) VALUES ($placeholders)"
            db.prepare(insertSql).use { st ->
                st.bindText(1, pk)

                cols.forEachIndexed { idx, key ->
                    val prim = normalized[key]?.jsonPrimitive
                    val i = idx + 2
                    if (prim == null || prim is JsonNull) {
                        st.bindNull(i)
                    } else if (prim.isString) {
                        st.bindText(i, prim.content)
                    } else {
                        val c = prim.content
                        if (c.equals("true", true) || c.equals("false", true)) {
                            st.bindLong(i, if (c.equals("true", true)) 1 else 0)
                        } else {
                            val l = c.toLongOrNull()
                            if (l != null) st.bindLong(i, l) else {
                                val d = c.toDoubleOrNull()
                                if (d != null) st.bindDouble(i, d) else st.bindText(i, c)
                            }
                        }
                    }
                }
                st.step()
            }
        }
    }

    // Lookback drain helper used by uploadOnce to avoid resurrecting deletes

    private suspend fun drainLookbackUntilInternal() {
        val target: Long =
            db.prepare("SELECT last_server_seq_seen FROM _sync_client_info LIMIT 1").use { st ->
                if (st.step()) st.getLong(0) else 0L
            }
        val lb = maxOf(1000L, config.downloadLimit.toLong() * 2)
        val windowStart = (target - lb).coerceAtLeast(0L)
        logger.d { "drainLookbackUntilInternal: from=$windowStart to=$target" }
        db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
            st.bindLong(1, windowStart)
            st.step()
        }
        var nextAfter = windowStart
        var passes = 0
        val maxPasses = 50
        while (true) {
            val prev = nextAfter
            logger.d { "drainLookbackUntilInternal: pass $passes downloading from seq=$prev" }
            val (applied, na) = downloadOnceInternal(
                limit = config.downloadLimit,
                includeSelf = true,  // Include self during lookback to update local metadata
                isPostUploadLookback = true
            ).getOrElse { 0 to prev }
            logger.d { "drainLookbackUntilInternal: pass $passes downloaded $applied changes, nextAfter=$na" }
            nextAfter = na
            passes++
            val caughtUp = nextAfter >= target
            val stagnated = nextAfter == prev
            if (applied == 0 || caughtUp || stagnated || passes >= maxPasses) break
        }
        // Restore to target to avoid leaving cursor behind
        val current: Long = db.prepare("SELECT last_server_seq_seen FROM _sync_client_info LIMIT 1")
            .use { st -> if (st.step()) st.getLong(0) else 0L }
        if (current < target) {
            db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
                st.bindLong(1, target)
                st.step()
            }
        }
    }

    private suspend fun getTableColumns(db: SafeSQLiteConnection, table: String): List<String> {
        val key = table.lowercase()
        tableColumnsCache[key]?.let { return it }
        val cols = mutableListOf<String>()
        db.prepare("PRAGMA table_info($key)").use { st ->
            while (st.step()) cols += st.getText(1).lowercase()
        }
        tableColumnsCache[key] = cols

        return cols
    }
}
