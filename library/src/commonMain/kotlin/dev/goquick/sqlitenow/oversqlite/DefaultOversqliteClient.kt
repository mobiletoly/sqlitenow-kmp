package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.logger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.concurrent.Volatile

/**
 * A multiplatform SQLite-backed implementation of OversqliteClient.
 * Relies on androidx.sqlite for the database and Ktor client for networking.
 */
class DefaultOversqliteClient(
    private val db: SafeSQLiteConnection,
    private val baseUrl: String,
    private val config: OversqliteConfig,
    private val tokenProvider: suspend () -> String,
    private val resolver: Resolver = ServerWinsResolver,
    private val http: HttpClient = HttpClient {
        install(ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
    },
    private val networkDispatchers2: PlatformDispatchers = PlatformDispatchers(),
    private val tablesUpdateListener: (table: Set<String>) -> Unit,
) : OversqliteClient {

    private val writeMutex = Mutex()

    // Cache PRAGMA table_info lookups per table (lowercased key)
    private val tableColumnsCache = mutableMapOf<String, List<String>>()

    private val bootstrapper = SyncBootstrapper(config = config)

    private val uploader = SyncUploader(
        http = http,
        baseUrl = baseUrl,
        config = config,
        tokenProvider = tokenProvider,
        resolver = resolver,
        upsertBusinessFromPayload = ::upsertBusinessFromPayload,
        updateRowMeta = ::updateRowMeta
    )

    private val downloader = SyncDownloader(
        http = http,
        baseUrl = baseUrl,
        config = config,
        tokenProvider = tokenProvider,
        resolver = resolver,
        upsertBusinessFromPayload = ::upsertBusinessFromPayload,
        updateRowMeta = ::updateRowMeta
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
            writeMutex.withLock {
                bootstrapper.bootstrap(db, userId, sourceId)
            }
        }


    override suspend fun uploadOnce(): Result<UploadSummary> = withContext(db.dispatcher) {
        if (uploadsPaused) return@withContext Result.success(UploadSummary(0, 0, 0, 0, 0))

        val result = runCatching {
            writeMutex.withLock {
                logger.d { "uploadOnce: start" }
                val info: Triple<String, Long, Long>? =
                    db.prepare("SELECT source_id, next_change_id, last_server_seq_seen FROM _sync_client_info LIMIT 1")
                        .use { st ->
                            if (st.step()) Triple(
                                st.getText(0), st.getLong(1), st.getLong(2)
                            ) else null
                        }
                val (_, nextChangeId, _) = info ?: error("_sync_client_info missing")

                // Delegate to the uploader
                uploader.uploadPendingChanges(db, nextChangeId)
            }
        }
        result.onFailure {
            logger.e(it) { "uploadOnce: failed ${it.message}" }
        }

        // Post-upload lookback drain: ensure we didn't skip peer changes by jumping watermark
        if (result.isSuccess) {
            runCatching {
                drainLookbackUntil()
            }.onFailure { e -> logger.e(e) { "uploadOnce: post-lookback failed" } }
            val uploadResult = result.getOrNull()
            if (uploadResult != null && uploadResult.updatedTables.isNotEmpty()) {
                launch {
                    tablesUpdateListener(uploadResult.updatedTables)
                }
            }
        }
        result.map { it.summary }
    }

    override suspend fun downloadOnce(
        limit: Int, includeSelf: Boolean, until: Long
    ): Result<Pair<Int, Long>> =
        downloadOnce(limit, includeSelf, until, isPostUploadLookback = false)

    private suspend fun downloadOnce(
        limit: Int, includeSelf: Boolean, until: Long = 0L, isPostUploadLookback: Boolean = false
    ): Result<Pair<Int, Long>> = withContext(db.dispatcher) {
        if (downloadsPaused) return@withContext Result.success(0 to 0L)

        val result = runCatching {
            writeMutex.withLock {
                downloader.downloadOnce(db, limit, includeSelf, until, isPostUploadLookback)
            }
        }

        if (result.isSuccess) {
            val downloadResult = result.getOrNull()
            if (downloadResult != null && downloadResult.updatedTables.isNotEmpty()) {
                launch {
                    tablesUpdateListener(downloadResult.updatedTables)
                }
            }
        }

        result.map { it.applied to it.nextAfter }
    }

    override fun close() { /* host manages SQLiteConnection lifecycle */
    }

    override suspend fun hydrate(
        includeSelf: Boolean, limit: Int, windowed: Boolean
    ): Result<Unit> = withContext(db.dispatcher) {
        val result = writeMutex.withLock {
            runCatching {
                downloader.hydrate(db, includeSelf, limit, windowed)
            }
        }

        if (result.isSuccess) {
            val updatedTables = result.getOrNull()
            if (updatedTables != null && updatedTables.isNotEmpty()) {
                launch {
                    tablesUpdateListener(updatedTables)
                }
            }
        }

        result.map { }
    }

    // --- Helpers ---


    private suspend fun updateRowMeta(
        db: SafeSQLiteConnection, table: String, pk: String, serverVersion: Long, deleted: Boolean
    ) {
        val del = if (deleted) 1 else 0
        logger.d { "updateRowMeta: ${table}:${pk} -> server_v${serverVersion} (deleted=${deleted})" }

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
        logger.d { "upsertBusinessFromPayload: table=$tableLc pk=$pk" }

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
        logger.d { "upsertBusinessFromPayload: executing UPDATE with pk=$pk" }
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
        logger.d { "upsertBusinessFromPayload: UPDATE affected $updateRowsAffected rows" }

        // If UPDATE affected 0 rows, INSERT id + provided columns
        val changed =
            db.prepare("SELECT changes()").use { st -> if (st.step()) st.getLong(0) else 0L }
        logger.d { "upsertBusinessFromPayload: changes()=$changed, will INSERT=${changed == 0L}" }
        if (changed == 0L) {
            val insertCols = listOf("id") + cols
            val placeholders = insertCols.indices.joinToString(", ") { "?" }
            val insertSql =
                "INSERT INTO $tableLc (${insertCols.joinToString(", ")}) VALUES ($placeholders)"
            logger.d { "upsertBusinessFromPayload: executing INSERT with pk=$pk" }
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
            logger.d { "upsertBusinessFromPayload: INSERT completed for pk=$pk" }
        }
    }

    // Lookback drain helper used by uploadOnce to avoid resurrecting deletes
    private suspend fun drainLookbackUntil() {
        val target: Long =
            db.prepare("SELECT last_server_seq_seen FROM _sync_client_info LIMIT 1").use { st ->
                if (st.step()) st.getLong(0) else 0L
            }
        val lb = maxOf(1000L, config.downloadLimit.toLong() * 2)
        val windowStart = (target - lb).coerceAtLeast(0L)
        logger.d { "drainLookbackUntil: from=$windowStart to=$target" }
        db.prepare("UPDATE _sync_client_info SET last_server_seq_seen=?").use { st ->
            st.bindLong(1, windowStart)
            st.step()
        }
        var nextAfter = windowStart
        var passes = 0
        val maxPasses = 50
        while (true) {
            val prev = nextAfter
            logger.d { "drainLookbackUntil: pass $passes downloading from seq=$prev" }
            val (applied, na) = downloadOnce(
                limit = config.downloadLimit,
                includeSelf = true,  // Include self during lookback to update local metadata
                isPostUploadLookback = true
            ).getOrElse { 0 to prev }
            logger.d { "drainLookbackUntil: pass $passes downloaded $applied changes, nextAfter=$na" }
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
