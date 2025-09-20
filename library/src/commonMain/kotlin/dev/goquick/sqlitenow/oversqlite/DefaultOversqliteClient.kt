package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

import kotlin.concurrent.Volatile
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

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
 *
 * @param db The SQLite database connection
 * @param config The oversqlite configuration
 * @param http The authenticated HTTP client (must provide token refresh logic)
 * @param resolver The conflict resolution strategy
 * @param tablesUpdateListener Callback for table updates, useful for UI updates
 */
class DefaultOversqliteClient(
    private val db: SafeSQLiteConnection,
    private val config: OversqliteConfig,
    private val http: HttpClient,
    private val resolver: Resolver = ServerWinsResolver,
    private val tablesUpdateListener: (table: Set<String>) -> Unit,
) : OversqliteClient {
    // Prevent concurrent hydration/upload/download DB phases from overlapping
    private val opGate = Semaphore(1)

    private val bootstrapper = SyncBootstrapper(config = config)

    private val ioDispatcher: CoroutineDispatcher = PlatformDispatchers().io

    private val uploader = SyncUploader(
        http = http,
        config = config,
        resolver = resolver,
        upsertBusinessFromPayload = ::upsertBusinessFromPayload,
        updateRowMeta = ::updateRowMeta,
        ioDispatcher = ioDispatcher,
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
            sqliteNowLogger.d { "uploadOnce: start" }

            // Phase 1 (DB, gated): read nextChangeId and prepare changes
            val prepared = opGate.lockOnDatabaseDispatcher {
                val nextChangeId =
                    db.prepare("SELECT next_change_id FROM _sync_client_info LIMIT 1")
                        .use { st -> if (st.step()) st.getLong(0) else null }
                        ?: error("_sync_client_info missing")
                uploader.prepareUpload(db, nextChangeId)
            }
            if (prepared.changes.isEmpty()) {
                return@runCatching UploadSummary(
                    total = 0,
                    applied = 0,
                    conflict = 0,
                    invalid = 0,
                    materializeError = 0,
                )
            }

            // Phase 2 (Network): perform upload
            val response = uploader.performUpload(prepared).getOrElse { throw it }

            // Phase 3 (DB, gated): finalize
            val updatedTables = mutableSetOf<String>()
            val summary = opGate.lockOnDatabaseDispatcher {
                uploader.finalizeUpload(db, prepared.changes, response, updatedTables)
            }

            // Post-upload lookback drain (network + db), not holding DB context
            if (summary.total > 0) {
                drainLookbackUntilInternal()
            }

            if (updatedTables.isNotEmpty()) tablesUpdateListener(updatedTables)

            summary
        }.onFailure {
            sqliteNowLogger.e(it) { "uploadOnce: failed ${it.message}" }
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
            val clientInfo = opGate.lockOnDatabaseDispatcher {
                downloader.getClientInfoNow(db)
            }

            // Phase 2 (Network): fetch changes
            val response = downloader.fetchChangesNow(
                clientInfo.sourceId, clientInfo.lastServerSeq, limit, includeSelf, until
            ).getOrElse { throw it }

            // Phase 3 (DB, gated): apply
            val downloadResult = opGate.lockOnDatabaseDispatcher {
                downloader.applyDownloadedPage(
                    db = db,
                    response = response,
                    includeSelf = includeSelf,
                    sourceId = clientInfo.sourceId,
                    previousAfter = clientInfo.lastServerSeq,
                    isPostUploadLookback = isPostUploadLookback
                )
            }

            if (downloadResult.updatedTables.isNotEmpty()) tablesUpdateListener(downloadResult.updatedTables)

            downloadResult.applied to downloadResult.nextAfter
        }.onFailure {
            sqliteNowLogger.e(it) { "downloadOnce: failed ${it.message}" }
        }

        return result
    }

    private suspend fun downloadOnceInternal(
        limit: Int, includeSelf: Boolean, until: Long = 0L, isPostUploadLookback: Boolean = false
    ): Result<Pair<Int, Long>> {
        if (downloadsPaused) return Result.success(0 to 0L)

        val result = runCatching {
            val clientInfo = opGate.lockOnDatabaseDispatcher {
                downloader.getClientInfoNow(db)
            }
            val response = downloader.fetchChangesNow(
                clientInfo.sourceId, clientInfo.lastServerSeq, limit, includeSelf, until
            ).getOrElse { throw it }
            val downloadResult = opGate.lockOnDatabaseDispatcher {
                downloader.applyDownloadedPage(
                    db = db,
                    response = response,
                    includeSelf = includeSelf,
                    sourceId = clientInfo.sourceId,
                    previousAfter = clientInfo.lastServerSeq,
                    isPostUploadLookback = isPostUploadLookback
                )
            }
            downloadResult.applied to downloadResult.nextAfter
        }.onFailure {
            sqliteNowLogger.e(it) { "downloadOnceInternal: failed ${it.message}" }
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
            val clientInfo = opGate.lockOnDatabaseDispatcher {
                downloader.getClientInfoNow(db)
            }

            var after = clientInfo.lastServerSeq
            var frozenUntil = 0L
            var first = true
            val allUpdatedTables = mutableSetOf<String>()

            while (true) {
                val untilParam = if (windowed) frozenUntil else 0L
                val response = downloader.fetchChangesNow(
                    clientInfo.sourceId, after, limit, includeSelf, untilParam
                ).getOrElse { throw it }

                if (first) {
                    frozenUntil = if (windowed) response.windowUntil else 0L
                    if (frozenUntil > 0L) {
                        opGate.lockOnDatabaseDispatcher {
                            db.execSQL("UPDATE _sync_client_info SET current_window_until=$frozenUntil")
                        }
                    }
                    first = false
                }

                if (response.changes.isEmpty()) {
                    if (response.nextAfter > after) {
                        opGate.lockOnDatabaseDispatcher {
                            db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?")
                                .use { st ->
                                    st.bindLong(1, response.nextAfter)
                                    st.step()
                                }
                        }
                        after = response.nextAfter
                    }
                } else {
                    val updatedTables = opGate.lockOnDatabaseDispatcher {
                        downloader.applyDownloadedPage(
                            db = db,
                            response = response,
                            includeSelf = includeSelf,
                            sourceId = clientInfo.sourceId,
                            previousAfter = after
                        )
                    }
                    allUpdatedTables += updatedTables.updatedTables
                    after = updatedTables.nextAfter
                }

                if (!response.hasMore) break
            }

            if (frozenUntil > 0L) {
                opGate.lockOnDatabaseDispatcher {
                    db.execSQL("UPDATE _sync_client_info SET current_window_until=0")
                }
            }

            if (allUpdatedTables.isNotEmpty()) tablesUpdateListener(allUpdatedTables)
        }.onFailure { sqliteNowLogger.e(it) { "hydrate: failed" } }

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

    @OptIn(ExperimentalEncodingApi::class)
    private suspend fun upsertBusinessFromPayload(
        db: SafeSQLiteConnection, table: String, pk: String, payload: JsonElement?
    ) {
        if (config.verboseLogs) {
            sqliteNowLogger.i { "DefaultOversqliteClient: upserting record table=$table pk=$pk" }
            if (payload != null) {
                sqliteNowLogger.d { "DefaultOversqliteClient: payload=$payload" }
            }
        }

        if (payload == null || payload !is JsonObject) return
        val tableLc = table.lowercase()
        val ti = TableInfoProvider.get(db, tableLc)
        val tableCols = ti.columnNamesLower.toSet()
        val typeMap = ti.typesByNameLower.mapValues { it.value.lowercase() }
        val pkCol = ti.primaryKey?.name?.lowercase() ?: "id"
        val pkIsBlob = ti.primaryKeyIsBlob

        if (config.verboseLogs) {
            sqliteNowLogger.d { "DefaultOversqliteClient: table info - pkCol=$pkCol, pkIsBlob=$pkIsBlob, columns=${tableCols.joinToString(", ")}" }
        }

        // Normalize payload keys to lowercase for case-insensitive matching
        val normalized: Map<String, JsonElement> =
            payload.keys.associate { it.lowercase() to (payload[it] ?: JsonNull) }

        // Derive PK strictly from payload
        val pkElem = normalized[pkCol] ?: return
        val pkPrim = pkElem.jsonPrimitive
        val pkBind: (Int, androidx.sqlite.SQLiteStatement) -> Unit = { idx, st ->
            if (pkIsBlob) {
                // Server sends UUID string, convert to bytes for BLOB storage
                val bytes = uuidStringToBytes(pkPrim.content)
                st.bindBlob(idx, bytes)
            } else {
                st.bindText(idx, pkPrim.content)
            }
        }

        // Columns to write (exclude PK in SET)
        val cols = normalized.keys.filter { it != pkCol && it in tableCols }
        if (cols.isEmpty()) {
            val exists = db.prepare("SELECT EXISTS(SELECT 1 FROM $tableLc WHERE $pkCol=?)").use { st ->
                pkBind(1, st)
                st.step() && st.getLong(0) == 1L
            }
            if (!exists) {
                db.prepare("INSERT INTO $tableLc($pkCol) VALUES(?)").use { st ->
                    pkBind(1, st)
                    st.step()
                }
            }
            return
        }

        // UPDATE first
        val setClause = cols.joinToString(", ") { "$it=?" }
        val updateSql = "UPDATE $tableLc SET $setClause WHERE $pkCol=?"
        db.prepare(updateSql).use { st ->
            cols.forEachIndexed { idx, key ->
                val prim = normalized[key]?.jsonPrimitive
                val i = idx + 1
                if (prim == null || prim is JsonNull) {
                    st.bindNull(i)
                } else if (prim.isString) {
                    val colType = (typeMap[key] ?: "")
                    if (colType.contains("blob")) {
                        // For BLOB columns, expect Base64 encoding from server
                        val bytes = Base64.decode(prim.content)
                        st.bindBlob(i, bytes)
                    } else {
                        st.bindText(i, prim.content)
                    }
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
            pkBind(cols.size + 1, st)
            st.step()
        }

        // If UPDATE affected 0 rows, INSERT pk + provided columns
        val changed = db.prepare("SELECT changes()").use { st -> if (st.step()) st.getLong(0) else 0L }
        if (changed == 0L) {
            val insertCols = listOf(pkCol) + cols
            val placeholders = insertCols.indices.joinToString(", ") { "?" }
            val insertSql = "INSERT INTO $tableLc (${insertCols.joinToString(", ")}) VALUES ($placeholders)"
            db.prepare(insertSql).use { st ->
                pkBind(1, st)
                cols.forEachIndexed { idx, key ->
                    val prim = normalized[key]?.jsonPrimitive
                    val i = idx + 2
                    if (prim == null || prim is JsonNull) {
                        st.bindNull(i)
                    } else if (prim.isString) {
                        val colType = (typeMap[key] ?: "")
                        if (colType.contains("blob")) {
                            // For BLOB columns, expect Base64 encoding (except for UUID primary keys)
                            val bytes = Base64.decode(prim.content)
                            st.bindBlob(i, bytes)
                        } else {
                            st.bindText(i, prim.content)
                        }
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
        sqliteNowLogger.d { "drainLookbackUntilInternal: from=$windowStart to=$target" }
        db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
            st.bindLong(1, windowStart)
            st.step()
        }
        var nextAfter = windowStart
        var passes = 0
        val maxPasses = 50
        while (true) {
            val prev = nextAfter
            sqliteNowLogger.d { "drainLookbackUntilInternal: pass $passes downloading from seq=$prev" }
            val (applied, na) = downloadOnceInternal(
                limit = config.downloadLimit,
                includeSelf = true,  // Include self during lookback to update local metadata
                isPostUploadLookback = true
            ).getOrElse { 0 to prev }
            sqliteNowLogger.d { "drainLookbackUntilInternal: pass $passes downloaded $applied changes, nextAfter=$na" }
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

    @OptIn(ExperimentalContracts::class)
    private suspend inline fun <T> Semaphore.lockOnDatabaseDispatcher(crossinline action: suspend () -> T): T {
        contract {
            callsInPlace(action, InvocationKind.EXACTLY_ONCE)
        }
        acquire()
        return try {
            withContext(db.dispatcher) {
                action()
            }
        } finally {
            release()
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
     * Convert UUID string from wire protocol to ByteArray for BLOB storage using kotlin.uuid.Uuid
     */
    @OptIn(ExperimentalUuidApi::class)
    private fun uuidStringToBytes(uuidString: String): ByteArray {
        return Uuid.parse(uuidString).toByteArray()
    }
}
