package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import java.util.UUID

private fun computeCommittedBundleHashForTest(rows: List<BundleRow>): String {
    val logicalRows = rows.mapIndexed { index, row ->
        buildJsonObject {
            put("row_ordinal", JsonPrimitive(index.toLong()))
            put("schema", JsonPrimitive(row.schema))
            put("table", JsonPrimitive(row.table))
            put("key", buildJsonObject {
                for ((key, value) in row.key.entries.sortedBy { it.key }) {
                    put(key, JsonPrimitive(value))
                }
            })
            put("op", JsonPrimitive(row.op))
            put("row_version", JsonPrimitive(row.rowVersion))
            put("payload", row.payload ?: JsonNull)
        }
    }
    return sha256Hex(canonicalizeJsonElement(JsonArray(logicalRows)).encodeToByteArray())
}

open class BundleClientContractTestSupport {
    protected data class DirtyRowRecord(
        val op: String,
        val baseRowVersion: Long,
        val payload: String?,
    )

    protected val json = Json { ignoreUnknownKeys = true }

    protected fun newClient(
        db: SafeSQLiteConnection,
        http: HttpClient,
        syncTables: List<SyncTable>,
        uploadLimit: Int = 200,
        transientRetryPolicy: OversqliteTransientRetryPolicy = OversqliteTransientRetryPolicy(),
        resolver: Resolver = ServerWinsResolver,
    ): DefaultOversqliteClient {
        return DefaultOversqliteClient(
            db = db,
            config = OversqliteConfig(
                schema = "main",
                syncTables = syncTables,
                uploadLimit = uploadLimit,
                transientRetryPolicy = transientRetryPolicy,
            ),
            http = http,
            resolver = resolver,
        )
    }

    protected fun newServer(): HttpServer {
        val server = HttpServer.create(java.net.InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/sync/capabilities") { exchange ->
            if (exchange.requestMethod != "GET") {
                exchange.sendResponseHeaders(405, -1)
                exchange.close()
                return@createContext
            }
            respondJson(
                exchange,
                200,
                json.encodeToString(
                    CapabilitiesResponse.serializer(),
                    CapabilitiesResponse(
                        protocolVersion = "v1",
                        schemaVersion = 1,
                        features = mapOf("connect_lifecycle" to true),
                    ),
                ),
            )
        }
        server.createContext("/sync/connect") { exchange ->
            if (exchange.requestMethod != "POST") {
                exchange.sendResponseHeaders(405, -1)
                exchange.close()
                return@createContext
            }
            val request = json.decodeFromString(
                ConnectRequest.serializer(),
                exchange.requestBody.readBytes().decodeToString(),
            )
            val response = if (request.hasLocalPendingRows) {
                ConnectResponse(
                    resolution = "initialize_local",
                    initializationId = "init-connect",
                    leaseExpiresAt = "2099-01-01T00:00:00Z",
                )
            } else {
                ConnectResponse(resolution = "initialize_empty")
            }
            respondJson(
                exchange,
                200,
                json.encodeToString(ConnectResponse.serializer(), response),
            )
        }
        return server
    }

    protected fun newHttpClient(server: HttpServer): HttpClient =
        HttpClient(CIO) {
            install(ContentNegotiation) {
                json()
            }
            defaultRequest {
                url("http://127.0.0.1:${server.address.port}")
            }
        }

    protected suspend fun createUsersTable(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
    }

    protected suspend fun createBlobUsersTable(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id BLOB PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
    }

    protected suspend fun createIntegerUsersTable(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id INTEGER PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
    }

    protected suspend fun createBigIntUsersTable(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id BIGINT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
    }

    protected suspend fun createUsersTableWithReservedScopeColumn(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE users (
              id TEXT PRIMARY KEY NOT NULL,
              _sync_scope_id TEXT NOT NULL,
              name TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createScoredUsersTable(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL, score REAL NOT NULL)")
    }

    protected suspend fun createUsersAndPostsTables(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
        db.execSQL(
            """
            CREATE TABLE posts (
              id TEXT PRIMARY KEY NOT NULL,
              user_id TEXT NOT NULL REFERENCES users(id),
              title TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createUsersAndCascadePostsTables(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
        db.execSQL(
            """
            CREATE TABLE posts (
              id TEXT PRIMARY KEY NOT NULL,
              user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
              title TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createActivityProgramTables(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE activity (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL)")
        db.execSQL(
            """
            CREATE TABLE program_item (
              id TEXT PRIMARY KEY NOT NULL,
              activity_id TEXT NOT NULL REFERENCES activity(id),
              title TEXT NOT NULL
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE daily_log (
              id TEXT PRIMARY KEY NOT NULL,
              activity_id TEXT NOT NULL REFERENCES activity(id),
              program_item_id TEXT NOT NULL REFERENCES program_item(id),
              notes TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createUsersAndAuditTables(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
        db.execSQL(
            """
            CREATE TABLE audit_logs (
              id TEXT PRIMARY KEY NOT NULL,
              user_id TEXT NOT NULL,
              message TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createBlobDocsTable(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE blob_docs (
              id BLOB PRIMARY KEY NOT NULL,
              name TEXT NOT NULL,
              payload BLOB NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createTypedRowsTable(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE typed_rows (
              id TEXT PRIMARY KEY NOT NULL,
              name TEXT NOT NULL,
              note TEXT NULL,
              count_value INTEGER NULL,
              enabled_flag INTEGER NOT NULL,
              rating REAL NULL,
              data BLOB NULL,
              created_at TEXT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createBlobFilesAndReviewsTables(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE files (
              id BLOB PRIMARY KEY NOT NULL,
              name TEXT NOT NULL,
              data BLOB NOT NULL
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE file_reviews (
              id BLOB PRIMARY KEY NOT NULL,
              file_id BLOB NOT NULL REFERENCES files(id),
              review TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createEmployeesTable(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE employees (
              id TEXT PRIMARY KEY NOT NULL,
              manager_id TEXT REFERENCES employees(id),
              name TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createAuthorsAndProfilesCycleTables(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE authors (
              id TEXT PRIMARY KEY NOT NULL,
              profile_id TEXT NOT NULL REFERENCES profiles(id) DEFERRABLE INITIALLY DEFERRED,
              name TEXT NOT NULL
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE profiles (
              id TEXT PRIMARY KEY NOT NULL,
              author_id TEXT NOT NULL REFERENCES authors(id) DEFERRABLE INITIALLY DEFERRED,
              bio TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createImmediateAuthorsAndProfilesCycleTables(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE authors (
              id TEXT PRIMARY KEY NOT NULL,
              profile_id TEXT NOT NULL REFERENCES profiles(id),
              name TEXT NOT NULL
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE profiles (
              id TEXT PRIMARY KEY NOT NULL,
              author_id TEXT NOT NULL REFERENCES authors(id),
              bio TEXT NOT NULL
            )
            """.trimIndent()
        )
    }

    protected suspend fun createCompositePkTable(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE pairs (
              id_a TEXT NOT NULL,
              id_b TEXT NOT NULL,
              name TEXT NOT NULL,
              PRIMARY KEY (id_a, id_b)
            )
            """.trimIndent()
        )
    }

    protected suspend fun createCompositeForeignKeyTables(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE parent_pairs (
              id TEXT PRIMARY KEY NOT NULL,
              parent_a TEXT NOT NULL,
              parent_b TEXT NOT NULL,
              UNIQUE (parent_a, parent_b)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE child_pairs (
              id TEXT PRIMARY KEY NOT NULL,
              parent_a TEXT NOT NULL,
              parent_b TEXT NOT NULL,
              FOREIGN KEY (parent_a, parent_b) REFERENCES parent_pairs(parent_a, parent_b)
            )
            """.trimIndent()
        )
    }

    protected suspend fun createSyncMetadataTables(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_apply_state (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              apply_mode INTEGER NOT NULL DEFAULT 0
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            INSERT INTO _sync_apply_state(singleton_key, apply_mode)
            VALUES(1, 0)
            ON CONFLICT(singleton_key) DO NOTHING
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_row_state (
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              row_version INTEGER NOT NULL DEFAULT 0,
              deleted INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              PRIMARY KEY (schema_name, table_name, key_json)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_dirty_rows (
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
              base_row_version INTEGER NOT NULL DEFAULT 0,
              payload TEXT,
              dirty_ordinal INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              PRIMARY KEY (schema_name, table_name, key_json)
            )
            """.trimIndent()
        )
        db.execSQL("CREATE INDEX IF NOT EXISTS idx_sync_dirty_rows_dirty_ordinal ON _sync_dirty_rows(dirty_ordinal)")
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_snapshot_stage (
              snapshot_id TEXT NOT NULL,
              row_ordinal INTEGER NOT NULL,
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              row_version INTEGER NOT NULL,
              payload TEXT NOT NULL,
              PRIMARY KEY (snapshot_id, row_ordinal)
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_source_state (
              source_id TEXT NOT NULL PRIMARY KEY,
              next_source_bundle_id INTEGER NOT NULL DEFAULT 1,
              replaced_by_source_id TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_attachment_state (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              current_source_id TEXT NOT NULL DEFAULT '',
              binding_state TEXT NOT NULL DEFAULT 'anonymous' CHECK (binding_state IN ('anonymous', 'attached')),
              attached_user_id TEXT NOT NULL DEFAULT '',
              schema_name TEXT NOT NULL DEFAULT '',
              last_bundle_seq_seen INTEGER NOT NULL DEFAULT 0,
              rebuild_required INTEGER NOT NULL DEFAULT 0,
              pending_initialization_id TEXT NOT NULL DEFAULT ''
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            INSERT INTO _sync_attachment_state(
              singleton_key,
              current_source_id,
              binding_state,
              attached_user_id,
              schema_name,
              last_bundle_seq_seen,
              rebuild_required,
              pending_initialization_id
            )
            VALUES (1, '', 'anonymous', '', '', 0, 0, '')
            ON CONFLICT(singleton_key) DO NOTHING
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_operation_state (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              kind TEXT NOT NULL DEFAULT 'none' CHECK (kind IN ('none', 'remote_replace')),
              target_user_id TEXT NOT NULL DEFAULT '',
              staged_snapshot_id TEXT NOT NULL DEFAULT '',
              snapshot_bundle_seq INTEGER NOT NULL DEFAULT 0,
              snapshot_row_count INTEGER NOT NULL DEFAULT 0
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            INSERT INTO _sync_operation_state(
              singleton_key,
              kind,
              target_user_id,
              staged_snapshot_id,
              snapshot_bundle_seq,
              snapshot_row_count
            )
            VALUES (1, 'none', '', '', 0, 0)
            ON CONFLICT(singleton_key) DO NOTHING
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_outbox_bundle (
              singleton_key INTEGER NOT NULL PRIMARY KEY CHECK (singleton_key = 1),
              state TEXT NOT NULL DEFAULT 'none' CHECK (state IN ('none', 'prepared', 'committed_remote')),
              source_id TEXT NOT NULL DEFAULT '',
              source_bundle_id INTEGER NOT NULL DEFAULT 0,
              initialization_id TEXT NOT NULL DEFAULT '',
              canonical_request_hash TEXT NOT NULL DEFAULT '',
              row_count INTEGER NOT NULL DEFAULT 0,
              remote_bundle_hash TEXT NOT NULL DEFAULT '',
              remote_bundle_seq INTEGER NOT NULL DEFAULT 0
            )
            """.trimIndent()
        )
        db.execSQL(
            """
            INSERT INTO _sync_outbox_bundle(
              singleton_key,
              state,
              source_id,
              source_bundle_id,
              initialization_id,
              canonical_request_hash,
              row_count,
              remote_bundle_hash,
              remote_bundle_seq
            )
            VALUES (1, 'none', '', 0, '', '', 0, '', 0)
            ON CONFLICT(singleton_key) DO NOTHING
            """.trimIndent()
        )
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_outbox_rows (
              source_bundle_id INTEGER NOT NULL,
              row_ordinal INTEGER NOT NULL,
              schema_name TEXT NOT NULL,
              table_name TEXT NOT NULL,
              key_json TEXT NOT NULL,
              wire_key_json TEXT NOT NULL,
              op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
              base_row_version INTEGER NOT NULL DEFAULT 0,
              local_payload TEXT,
              wire_payload TEXT,
              PRIMARY KEY (source_bundle_id, row_ordinal)
            )
            """.trimIndent()
        )
    }

    protected suspend fun scalarLong(db: SafeSQLiteConnection, sql: String): Long {
        return db.prepare(sql).use { st ->
            check(st.step())
            st.getLong(0)
        }
    }

    protected suspend fun scalarText(db: SafeSQLiteConnection, sql: String): String {
        return db.prepare(sql).use { st ->
            check(st.step())
            st.getText(0)
        }
    }

    protected suspend fun dirtyOps(db: SafeSQLiteConnection): List<String> {
        val rows = mutableListOf<String>()
        db.prepare(
            """
            SELECT table_name, op
            FROM _sync_dirty_rows
            ORDER BY dirty_ordinal, table_name, key_json
            """.trimIndent()
        ).use { st ->
            while (st.step()) {
                rows += "${st.getText(0)}:${st.getText(1)}"
            }
        }
        return rows
    }

    protected suspend fun dirtyTables(db: SafeSQLiteConnection): Set<String> {
        val rows = linkedSetOf<String>()
        db.prepare("SELECT table_name FROM _sync_dirty_rows ORDER BY table_name").use { st ->
            while (st.step()) {
                rows += st.getText(0)
            }
        }
        return rows
    }

    protected suspend fun dirtyKeysAndOps(db: SafeSQLiteConnection): List<String> {
        val rows = mutableListOf<String>()
        db.prepare(
            """
            SELECT key_json, op
            FROM _sync_dirty_rows
            ORDER BY dirty_ordinal, key_json
            """.trimIndent()
        ).use { st ->
            while (st.step()) {
                val key = json.parseToJsonElement(st.getText(0)).toString()
                val id = key.substringAfter(":\"").substringBefore("\"")
                rows += "$id:${st.getText(1)}"
            }
        }
        return rows
    }

    protected suspend fun dirtyRow(
        db: SafeSQLiteConnection,
        tableName: String,
        keyJson: String,
    ): DirtyRowRecord {
        return db.prepare(
            """
            SELECT op, base_row_version, payload
            FROM _sync_dirty_rows
            WHERE table_name = ? AND key_json = ?
            """.trimIndent()
        ).use { st ->
            st.bindText(1, tableName)
            st.bindText(2, keyJson)
            check(st.step())
            DirtyRowRecord(
                op = st.getText(0),
                baseRowVersion = st.getLong(1),
                payload = if (st.isNull(2)) null else st.getText(2),
            )
        }
    }

    protected fun queryParam(exchange: HttpExchange, name: String): String {
        val raw = exchange.requestURI.rawQuery ?: return ""
        return raw.split("&")
            .mapNotNull {
                val parts = it.split("=", limit = 2)
                if (parts.firstOrNull() == name) parts.getOrElse(1) { "" } else null
            }
            .firstOrNull()
            ?: ""
    }

    protected fun respondJson(exchange: HttpExchange, status: Int, body: String) {
        val bytes = body.toByteArray()
        exchange.responseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    protected fun computeCommittedBundleHash(rows: List<BundleRow>): String {
        return computeCommittedBundleHashForTest(rows)
    }

    protected class FakeChunkedSyncServer(
        private val json: Json,
        private val queryParam: (HttpExchange, String) -> String,
        private val respondJson: (HttpExchange, Int, String) -> Unit,
    ) {
        data class RecordedChunk(
            val pushId: String,
            val startRowOrdinal: Long,
            val rows: List<PushRequestRow>,
        )

        data class StoredBundle(
            val bundleSeq: Long,
            val sourceId: String,
            val sourceBundleId: Long,
            val bundleHash: String,
            val rows: List<BundleRow>,
        )

        private data class SessionState(
            val pushId: String,
            val sourceId: String,
            val sourceBundleId: Long,
            val plannedRowCount: Long,
            val rows: MutableList<PushRequestRow> = mutableListOf(),
            var committedBundleSeq: Long? = null,
        )

        val createRequests = mutableListOf<PushSessionCreateRequest>()
        val uploadedChunks = mutableListOf<RecordedChunk>()
        val bundles = mutableListOf<StoredBundle>()

        var nextBundleSeq = 1L
        var commitError: ((Long, Int) -> Pair<Int, String>?)? = null
        var committedBundleChunkError: ((Long, Long?) -> Pair<Int, String>?)? = null
        var bundleChunkOverride: ((StoredBundle, Long?, Int) -> CommittedBundleRowsResponse)? = null
        var beforeCreateSessionResponse: (() -> Unit)? = null
        var beforeCommitResponse: ((Long, Int) -> Unit)? = null
        var beforeCommittedBundleChunkResponse: ((Long, Long?) -> Unit)? = null
        var beforePullResponse: ((Long, Long) -> Unit)? = null

        private val sessionsById = linkedMapOf<String, SessionState>()
        private val appliedBySourceBundleId = linkedMapOf<Pair<String, Long>, StoredBundle>()

        fun install(server: HttpServer) {
            server.createContext("/sync/push-sessions") { exchange ->
                if (exchange.requestMethod != "POST") {
                    exchange.sendResponseHeaders(405, -1)
                    exchange.close()
                    return@createContext
                }
                val request = json.decodeFromString(PushSessionCreateRequest.serializer(), exchange.requestBody.readBytes().decodeToString())
                createRequests += request
                val applied = appliedBySourceBundleId[request.sourceId to request.sourceBundleId]
                if (applied != null) {
                    respondJson(
                        exchange,
                        200,
                        json.encodeToString(
                            PushSessionCreateResponse.serializer(),
                            PushSessionCreateResponse(
                                status = "already_committed",
                                bundleSeq = applied.bundleSeq,
                                sourceId = applied.sourceId,
                                sourceBundleId = applied.sourceBundleId,
                                rowCount = applied.rows.size.toLong(),
                                bundleHash = applied.bundleHash,
                            )
                        )
                    )
                    return@createContext
                }

                val pushId = UUID.randomUUID().toString()
                sessionsById[pushId] = SessionState(
                    pushId = pushId,
                    sourceId = request.sourceId,
                    sourceBundleId = request.sourceBundleId,
                    plannedRowCount = request.plannedRowCount,
                )
                beforeCreateSessionResponse?.invoke()
                respondJson(
                    exchange,
                    200,
                    json.encodeToString(
                        PushSessionCreateResponse.serializer(),
                        PushSessionCreateResponse(
                            pushId = pushId,
                            status = "staging",
                            plannedRowCount = request.plannedRowCount,
                            nextExpectedRowOrdinal = 0,
                        )
                    )
                )
            }

            server.createContext("/sync/push-sessions/") { exchange ->
                val path = exchange.requestURI.path.removePrefix("/sync/push-sessions/")
                val segments = path.split("/").filter { it.isNotBlank() }
                val pushId = segments.firstOrNull().orEmpty()
                val session = sessionsById[pushId]
                if (session == null) {
                    respondJson(exchange, 404, """{"error":"push_session_not_found","message":"unknown push session"}""")
                    return@createContext
                }

                when {
                    exchange.requestMethod == "POST" && segments.getOrNull(1) == "chunks" -> {
                        val request = json.decodeFromString(PushSessionChunkRequest.serializer(), exchange.requestBody.readBytes().decodeToString())
                        if (request.startRowOrdinal != session.rows.size.toLong()) {
                            respondJson(exchange, 409, """{"error":"push_chunk_out_of_order","message":"out of order"}""")
                            return@createContext
                        }
                        session.rows += request.rows
                        uploadedChunks += RecordedChunk(pushId, request.startRowOrdinal, request.rows)
                        respondJson(
                            exchange,
                            200,
                            json.encodeToString(
                                PushSessionChunkResponse.serializer(),
                                PushSessionChunkResponse(
                                    pushId = pushId,
                                    nextExpectedRowOrdinal = session.rows.size.toLong(),
                                )
                            )
                        )
                    }

                    exchange.requestMethod == "POST" && segments.getOrNull(1) == "commit" -> {
                        commitError?.invoke(session.sourceBundleId, session.rows.size)?.let { (status, body) ->
                            respondJson(exchange, status, body)
                            return@createContext
                        }
                        beforeCommitResponse?.invoke(session.sourceBundleId, session.rows.size)

                        val existing = appliedBySourceBundleId[session.sourceId to session.sourceBundleId]
                        if (existing != null) {
                            respondJson(
                                exchange,
                                200,
                                json.encodeToString(
                                    PushSessionCommitResponse.serializer(),
                                    PushSessionCommitResponse(
                                        bundleSeq = existing.bundleSeq,
                                        sourceId = existing.sourceId,
                                        sourceBundleId = existing.sourceBundleId,
                                        rowCount = existing.rows.size.toLong(),
                                        bundleHash = existing.bundleHash,
                                    )
                                )
                            )
                            return@createContext
                        }

                        val bundleSeq = nextBundleSeq++
                        val rows = session.rows.map { row ->
                            BundleRow(
                                schema = row.schema,
                                table = row.table,
                                key = row.key,
                                op = row.op,
                                rowVersion = bundleSeq,
                                payload = row.payload,
                            )
                        }
                        val stored = StoredBundle(
                            bundleSeq = bundleSeq,
                            sourceId = session.sourceId,
                            sourceBundleId = session.sourceBundleId,
                            bundleHash = computeBundleHash(rows),
                            rows = rows,
                        )
                        session.committedBundleSeq = bundleSeq
                        bundles += stored
                        appliedBySourceBundleId[session.sourceId to session.sourceBundleId] = stored
                        respondJson(
                            exchange,
                            200,
                            json.encodeToString(
                                PushSessionCommitResponse.serializer(),
                                PushSessionCommitResponse(
                                    bundleSeq = stored.bundleSeq,
                                    sourceId = stored.sourceId,
                                    sourceBundleId = stored.sourceBundleId,
                                    rowCount = stored.rows.size.toLong(),
                                    bundleHash = stored.bundleHash,
                                )
                            )
                        )
                    }

                    exchange.requestMethod == "DELETE" -> {
                        sessionsById.remove(pushId)
                        exchange.sendResponseHeaders(204, -1)
                        exchange.close()
                    }

                    else -> {
                        exchange.sendResponseHeaders(405, -1)
                        exchange.close()
                    }
                }
            }

            server.createContext("/sync/committed-bundles/") { exchange ->
                val path = exchange.requestURI.path.removePrefix("/sync/committed-bundles/")
                val bundleSeq = path.substringBefore('/').toLong()
                val afterRowOrdinal = queryParam(exchange, "after_row_ordinal").toLongOrNull()
                val maxRows = queryParam(exchange, "max_rows").toIntOrNull() ?: 1000
                committedBundleChunkError?.invoke(bundleSeq, afterRowOrdinal)?.let { (status, body) ->
                    respondJson(exchange, status, body)
                    return@createContext
                }
                val bundle = bundles.first { it.bundleSeq == bundleSeq }
                beforeCommittedBundleChunkResponse?.invoke(bundleSeq, afterRowOrdinal)
                val response = bundleChunkOverride?.invoke(bundle, afterRowOrdinal, maxRows)
                    ?: buildCommittedBundleChunk(bundle, afterRowOrdinal, maxRows)
                respondJson(
                    exchange,
                    200,
                    json.encodeToString(CommittedBundleRowsResponse.serializer(), response)
                )
            }

            server.createContext("/sync/pull") { exchange ->
                val afterBundleSeq = queryParam(exchange, "after_bundle_seq").toLong()
                val targetBundleSeq = queryParam(exchange, "target_bundle_seq").toLongOrNull() ?: 0L
                val stableBundleSeq = bundles.maxOfOrNull { it.bundleSeq } ?: 0L
                val ceiling = if (targetBundleSeq > 0) targetBundleSeq else stableBundleSeq
                beforePullResponse?.invoke(afterBundleSeq, stableBundleSeq)
                val responseBundles = bundles
                    .filter { it.bundleSeq > afterBundleSeq && it.bundleSeq <= ceiling }
                    .map { stored ->
                        Bundle(
                            bundleSeq = stored.bundleSeq,
                            sourceId = stored.sourceId,
                            sourceBundleId = stored.sourceBundleId,
                            rows = stored.rows,
                        )
                    }

                respondJson(
                    exchange,
                    200,
                    json.encodeToString(
                        PullResponse.serializer(),
                        PullResponse(
                            stableBundleSeq = stableBundleSeq,
                            bundles = responseBundles,
                            hasMore = false,
                        )
                    )
                )
            }
        }

        fun buildCommittedBundleChunk(
            bundle: StoredBundle,
            afterRowOrdinal: Long?,
            maxRows: Int,
        ): CommittedBundleRowsResponse {
            val logicalAfter = afterRowOrdinal ?: -1L
            val startIndex = (logicalAfter + 1).toInt()
            val rows = bundle.rows.drop(startIndex)
            val page = rows.take(maxRows)
            val nextRowOrdinal = if (page.isEmpty()) logicalAfter else logicalAfter + page.size
            return CommittedBundleRowsResponse(
                bundleSeq = bundle.bundleSeq,
                sourceId = bundle.sourceId,
                sourceBundleId = bundle.sourceBundleId,
                rowCount = bundle.rows.size.toLong(),
                bundleHash = bundle.bundleHash,
                rows = page,
                nextRowOrdinal = nextRowOrdinal,
                hasMore = rows.size > page.size,
            )
        }

        private fun computeBundleHash(rows: List<BundleRow>): String = computeCommittedBundleHashForTest(rows)
    }
}
