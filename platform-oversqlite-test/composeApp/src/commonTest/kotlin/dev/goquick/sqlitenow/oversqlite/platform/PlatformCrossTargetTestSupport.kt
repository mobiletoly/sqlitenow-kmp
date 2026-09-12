package dev.goquick.sqlitenow.oversqlite.platform

import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.prepare
import androidx.sqlite.async.step
import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.oversqlite.platformsupport.canonicalizeJsonElement
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteConnectionProvider
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.HttpRequestData
import io.ktor.http.ContentType
import io.ktor.http.Headers
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import io.ktor.http.content.TextContent
import io.ktor.serialization.kotlinx.json.json
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.readRemaining
import io.ktor.utils.io.core.readText
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import kotlinx.coroutines.CancellationException

internal class PlatformTestDatabase internal constructor(
    private val dbName: String,
    private val connectionProvider: SqliteConnectionProvider,
    private val schemaStatements: MutableList<String>,
    private val connectionConfig: dev.goquick.sqlitenow.core.SqliteConnectionConfig?,
    private val targetVersion: Int = 1,
) {
    internal val databaseName: String get() = dbName
    internal var migrationFailureForTest: (() -> Unit)? = null

    private val preOpenDataStatements = mutableListOf<String>()
    private var database = createDatabase()
    private var opening: OversqliteOpening<SqliteNowDatabase>? = null

    private fun createDatabase(): SqliteNowDatabase = SqliteNowDatabase(
        dbName = dbName,
        migration = PlatformTestMigrations(
            targetVersion = targetVersion,
            schemaStatements = schemaStatements,
            preOpenDataStatements = preOpenDataStatements,
            beforeReturn = { migrationFailureForTest?.invoke() },
        ),
        debug = false,
        connectionProvider = connectionProvider,
        syncManaged = true,
    ).also { database ->
        connectionConfig?.let { database.connectionConfig = it }
    }

    fun connection(): SafeSQLiteConnection = database.connection()

    suspend fun prepare(sql: String): SQLiteStatement = database.connection().prepare(sql)

    suspend fun reopenForInspection(): SafeSQLiteConnection =
        connectionProvider.openConnection(
            dbName = dbName,
            debug = false,
            config = connectionConfig ?: dev.goquick.sqlitenow.core.SqliteConnectionConfig(),
        )

    suspend fun execSQL(sql: String) {
        if (database.isOpen()) {
            database.connection().execSQL(sql)
        } else if (isSchemaStatement(sql)) {
            schemaStatements += sql
        } else {
            preOpenDataStatements += sql
        }
    }

    suspend fun <T> withExclusiveAccess(block: suspend (SafeSQLiteConnection) -> T): T {
        val connection = database.connection()
        return connection.withExclusiveAccess { block(connection) }
    }

    suspend fun <T> transaction(mode: TransactionMode, block: suspend () -> T): T =
        database.connection().transaction(mode, block)

    suspend fun openClient(
        config: OversqliteConfig,
        http: HttpClient,
        resolver: Resolver,
        upgradePlan: OversqliteUpgradePlan? = null,
        onRefusalBeforeClose: (suspend (Throwable) -> Unit)? = null,
    ): DefaultOversqliteClient {
        opening?.close()
        opening = null
        if (database.isOpen()) database.close()
        database = createDatabase()
        val nextOpening = database.openForSync(
            config = config,
            httpClient = http,
            resolver = resolver,
            upgradePlan = upgradePlan,
        )
        opening = nextOpening
        return try {
            nextOpening.advance()
            preOpenDataStatements.clear()
            checkNotNull(nextOpening.readyOrNull()) {
                "platform test coordinated opening did not publish ready"
            }.client as DefaultOversqliteClient
        } catch (failure: Throwable) {
            // A same-lifetime test oracle must run after public advance refuses,
            // before close revokes access and transient provider state is lost.
            runCatching { onRefusalBeforeClose?.invoke(failure) }.onFailure(failure::addSuppressed)
            runCatching { nextOpening.close() }.onFailure(failure::addSuppressed)
            opening = null
            throw failure
        }
    }

    suspend fun close() {
        opening?.close()
        opening = null
        if (database.isOpen()) database.close()
    }

    private fun isSchemaStatement(sql: String): Boolean =
        sql.trimStart().takeWhile { !it.isWhitespace() }.uppercase() in setOf("ALTER", "CREATE", "DROP", "PRAGMA")
}

private class PlatformTestMigrations(
    override val targetVersion: Int,
    private val schemaStatements: MutableList<String>,
    private val preOpenDataStatements: MutableList<String>,
    private val beforeReturn: () -> Unit,
) : DatabaseMigrations {
    override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
        if (currentVersion > 0) return currentVersion
        check(currentVersion == -1 || currentVersion == 0) {
            "platform test database must start at the fresh migration boundary or an imported legacy version"
        }
        for (statement in schemaStatements) {
            conn.execSQL(statement)
        }
        if (currentVersion == -1) {
            for (statement in preOpenDataStatements) {
                conn.execSQL(statement)
            }
        }
        beforeReturn()
        return targetVersion
    }
}

internal open class PlatformCrossTargetTestSupport {
    private companion object {
        const val sourceIdHeaderName = "Oversync-Source-ID"
    }

    protected val json = Json { ignoreUnknownKeys = true }
    protected val workerOwnedStorage = OwnedWorkerStorage()

    private val fileBackedDbPaths = linkedSetOf<String>()

    protected suspend fun newDb(): PlatformTestDatabase = newCoordinatedDb()

    protected suspend fun newCoordinatedDb(
        connectionProvider: SqliteConnectionProvider = oversqliteTestConnectionProvider(),
    ): PlatformTestDatabase {
        val browserRuntime = webRuntimeKind() in setOf("js-browser", "wasm-browser", "js-node")
        val dbName = if (browserRuntime) {
            workerOwnedStorage.newDatabaseName()
        } else {
            createTempSqliteNowTestDbPath("sqlitenow-platform-parity").also { fileBackedDbPaths += it }
        }
        val schemaStatements = mutableListOf<String>()
        return PlatformTestDatabase(
            dbName = dbName,
            connectionProvider = connectionProvider,
            schemaStatements = schemaStatements,
            connectionConfig = null,
        )
    }

    protected suspend fun newCoordinatedFileBackedDb(path: String): PlatformTestDatabase {
        return PlatformTestDatabase(
            dbName = path,
            connectionProvider = oversqliteTestConnectionProvider(),
            schemaStatements = mutableListOf(),
            connectionConfig = createSqliteNowTestConnectionConfig(path),
        )
    }

    protected suspend fun cleanupOwnedWorkerStorage() {
        workerOwnedStorage.cleanup()
        fileBackedDbPaths.forEach { deleteTempSqliteNowTestDbArtifacts(it) }
        fileBackedDbPaths.clear()
    }

    protected suspend fun newFileBackedDb(path: String): SafeSQLiteConnection {
        return oversqliteTestConnectionProvider().openConnection(
            dbName = path,
            debug = true,
            config = createSqliteNowTestConnectionConfig(path),
        )
    }

    protected suspend fun newClient(
        db: PlatformTestDatabase,
        http: HttpClient,
        maxPushRows: Int = 200,
        maxPullBundles: Int = 1000,
        maxSnapshotChunkRows: Int = 1000,
        maxSnapshotChunkDecodedBytes: Long = 4L * 1024L * 1024L,
        snapshotApplyBatchRows: Int = 256,
        snapshotApplyBatchBytes: Long = 4L * 1024L * 1024L,
        resolver: Resolver = ServerWinsResolver,
        syncTables: List<SyncTable> = listOf(
            SyncTable("users", syncKeyColumnName = "id"),
            SyncTable("posts", syncKeyColumnName = "id"),
        ),
    ): DefaultOversqliteClient {
        return db.openClient(
            config = OversqliteConfig(
                generationId = 1L, databaseId = "platform-sync", serviceId = "platform-mock",
                syncTables = syncTables,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = maxPushRows,
                    maxPullBundles = maxPullBundles,
                    maxSnapshotChunkRows = maxSnapshotChunkRows,
                    maxSnapshotChunkDecodedBytes = maxSnapshotChunkDecodedBytes,
                ),
                snapshotApplyBatchRows = snapshotApplyBatchRows,
                snapshotApplyBatchBytes = snapshotApplyBatchBytes,
            ),
            http = http,
            resolver = resolver,
        )
    }

    protected suspend fun createUsersAndPostsTables(db: PlatformTestDatabase) {
        db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
        db.execSQL(
            """
            CREATE TABLE posts (
              id TEXT PRIMARY KEY NOT NULL,
              user_id TEXT NOT NULL REFERENCES users(id),
              title TEXT NOT NULL
            )
            """.trimIndent(),
        )
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
            """.trimIndent(),
        )
    }

    protected suspend fun createUsersWithUpdatedAtAndPostsTables(db: PlatformTestDatabase) {
        db.execSQL(
            """
            CREATE TABLE users (
              id TEXT PRIMARY KEY NOT NULL,
              name TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE posts (
              id TEXT PRIMARY KEY NOT NULL,
              user_id TEXT NOT NULL REFERENCES users(id),
              title TEXT NOT NULL
            )
            """.trimIndent(),
        )
    }

    protected suspend fun createUsersWithUpdatedAtAndPostsTables(db: SafeSQLiteConnection) {
        db.execSQL(
            """
            CREATE TABLE users (
              id TEXT PRIMARY KEY NOT NULL,
              name TEXT NOT NULL,
              updated_at TEXT NOT NULL
            )
            """.trimIndent(),
        )
        db.execSQL(
            """
            CREATE TABLE posts (
              id TEXT PRIMARY KEY NOT NULL,
              user_id TEXT NOT NULL REFERENCES users(id),
              title TEXT NOT NULL
            )
            """.trimIndent(),
        )
    }

    protected suspend fun scalarLong(db: PlatformTestDatabase, sql: String): Long {
        return db.withExclusiveAccess { connection ->
            connection.prepare(sql).use { st ->
                check(st.step())
                st.getLong(0)
            }
        }
    }

    protected suspend fun scalarLong(db: SafeSQLiteConnection, sql: String): Long {
        return db.withExclusiveAccess {
            db.prepare(sql).use { st ->
                check(st.step())
                st.getLong(0)
            }
        }
    }

    protected suspend fun scalarText(db: PlatformTestDatabase, sql: String): String {
        return db.withExclusiveAccess { connection ->
            connection.prepare(sql).use { st ->
                check(st.step())
                st.getText(0)
            }
        }
    }

    protected suspend fun scalarText(db: SafeSQLiteConnection, sql: String): String {
        return db.withExclusiveAccess {
            db.prepare(sql).use { st ->
                check(st.step())
                st.getText(0)
            }
        }
    }

    protected suspend fun scalarDouble(db: PlatformTestDatabase, sql: String): Double {
        return db.withExclusiveAccess { connection ->
            connection.prepare(sql).use { statement ->
                check(statement.step()) { "query returned no rows: $sql" }
                statement.getDouble(0)
            }
        }
    }

    protected suspend fun scalarDouble(db: SafeSQLiteConnection, sql: String): Double {
        return db.withExclusiveAccess {
            db.prepare(sql).use { statement ->
                check(statement.step()) { "query returned no rows: $sql" }
                statement.getDouble(0)
            }
        }
    }

    protected suspend fun insertUser(db: PlatformTestDatabase, id: String, name: String) {
        db.execSQL("INSERT INTO users(id, name) VALUES('$id', '$name')")
    }

    protected suspend fun insertUser(db: SafeSQLiteConnection, id: String, name: String) {
        db.execSQL("INSERT INTO users(id, name) VALUES('$id', '$name')")
    }

    protected suspend fun insertPost(db: PlatformTestDatabase, id: String, userId: String, title: String) {
        db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('$id', '$userId', '$title')")
    }

    protected suspend fun insertPost(db: SafeSQLiteConnection, id: String, userId: String, title: String) {
        db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('$id', '$userId', '$title')")
    }

    protected suspend fun updateUserName(db: PlatformTestDatabase, id: String, name: String) {
        db.execSQL("UPDATE users SET name = '$name' WHERE id = '$id'")
    }

    protected suspend fun updateUserName(db: SafeSQLiteConnection, id: String, name: String) {
        db.execSQL("UPDATE users SET name = '$name' WHERE id = '$id'")
    }

    protected class MockSyncServer(
        private val registeredTableSpecs: List<TestRegisteredTableSpec> =
            testRegisteredTableSpecs("users", "posts"),
        private val wireLimits: TestWireLimits = testWireLimits(),
        private val usersUpdatedAt: Boolean = false,
        private val generationResponses: Map<Long, JsonObject>? = null,
    ) {
        private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
        private val liveRows = linkedMapOf<String, LiveRow>()
        private val bundles = mutableListOf<StoredBundle>()
        private val committedBySourceBundle = linkedMapOf<String, StoredBundle>()
        private val snapshotSessions = linkedMapOf<String, SnapshotSessionState>()
        private var scopeInitialized = false
        private var initializingSourceId: String? = null
        private var initializationId: String? = null

        var retainedBundleFloor: Long = 0
        var pushRequestCount: Int = 0
        var maxRequestBodyBytes: Int = 0
        var maxResponseBodyBytes: Int = 0
        var snapshotChunkRequestCount: Int = 0
        var pullRequestCount: Int = 0
        var cancelNextRequestPathPrefix: String? = null
        val requestCounts = linkedMapOf<String, Int>()
        val requestBodies = mutableListOf<Pair<String, String>>()
        val requestSourceIds = mutableListOf<Pair<String, String>>()
        var authoritativePayloadTransform:
            ((PushRequestRow) -> kotlinx.serialization.json.JsonElement?)? = null
        var conflictOverride: ((PushRequestRow, Long, kotlinx.serialization.json.JsonElement?) -> PushConflictDetails?)? = null
        var rejectPush: Boolean = false
        var beforePushCommit: (suspend () -> Unit)? = null
        var beforePushResponse: (suspend () -> Unit)? = null
        // Behavior-defining test fixtures may return a normalized effect union
        // whose wire order differs from the final SQL application order.
        var normalizePushEffects: ((List<BundleRow>) -> List<BundleRow>)? = null
        var sourceRetiredOnPush: SourceRetiredResponse? = null
        var sourceRetiredOnSnapshotCreate: SourceRetiredResponse? = null
        var sourceReplacementInvalidOnSnapshotCreate: ErrorResponse? = null
        val requestedSnapshotSourceReplacements = mutableListOf<SnapshotSourceReplacement>()

        private var nextBundleSeq = 1L
        private var nextSnapshotId = 1L
        private var nextInitializationId = 1L

        fun newHttpClient(): HttpClient {
            return HttpClient(MockEngine { request ->
                handle(request)
            }) {
                install(ContentNegotiation) {
                    json(json)
                }
                defaultRequest {
                    url("https://sync.test")
                }
            }
        }

        fun addRemoteBundleForTest(rows: List<BundleRow>): Long {
            val sequence = nextBundleSeq++
            installCommittedRows(rows)
            bundles += StoredBundle(
                bundleSeq = sequence,
                sourceId = "remote-source",
                sourceBundleId = sequence,
                rows = rows,
                bundleHash = computeCommittedBundleHash(rows),
                canonicalRequestHash = "",
            )
            scopeInitialized = true
            return sequence
        }

        private fun installCommittedRows(rows: List<BundleRow>) {
            for (row in rows) {
                val key = liveKey(row.table, row.key)
                if (row.op == "DELETE") liveRows.remove(key)
                else liveRows[key] = LiveRow(row.table, row.key, row.rowVersion,
                    row.payload ?: error("payload required for ${row.op}"))
            }
        }

        private suspend fun MockRequestHandleScope.handle(
            request: HttpRequestData,
        ): io.ktor.client.request.HttpResponseData {
            val path = request.url.encodedPath
            requestCounts[path] = requestCounts.getOrElse(path) { 0 } + 1
            requestSourceIds += path to request.headers[sourceIdHeaderName].orEmpty()
            val cancelledPrefix = cancelNextRequestPathPrefix
            if (cancelledPrefix != null && path.startsWith(cancelledPrefix)) {
                cancelNextRequestPathPrefix = null
                throw CancellationException("worker migration cancelled $path")
            }
            return when {
            request.method.value == "GET" && request.url.encodedPath.startsWith("/sync/generations/") -> {
                val generationId = request.url.encodedPath.substringAfterLast('/').toLongOrNull()
                    ?: return errorResponse("invalid_request", "invalid generation id")
                val generation = generationResponse(generationId)
                    ?: return errorResponse("schema_generation_not_found", "generation $generationId is unavailable", HttpStatusCode.NotFound)
                jsonResponse(testDiscoveryResponse(generation, wireLimits).toString())
            }

            request.method.value == "POST" && request.url.encodedPath == "/sync/connect" ->
                handleConnect(request)

            request.method.value == "POST" && request.url.encodedPath == "/sync/push" ->
                handlePush(request)

            request.method.value == "POST" && request.url.encodedPath == "/sync/snapshot-sessions" ->
                handleCreateSnapshotSession(request)

            request.method.value == "GET" &&
                request.url.encodedPath.startsWith("/sync/snapshot-sessions/") ->
                handleSnapshotChunk(request)

            request.method.value == "DELETE" &&
                request.url.encodedPath.startsWith("/sync/snapshot-sessions/") -> {
                val snapshotId = request.url.encodedPath.removePrefix("/sync/snapshot-sessions/")
                snapshotSessions.remove(snapshotId)
                jsonResponse("""{}""")
            }

            request.method.value == "GET" && request.url.encodedPath == "/sync/pull" ->
                handlePull(request)

            else -> jsonResponse(
                body = json.encodeToString(ErrorResponse("not_found", request.url.encodedPath)),
                status = HttpStatusCode.NotFound,
            )
        }
        }

        private val connectJson = Json { encodeDefaults = false }

        private suspend fun MockRequestHandleScope.handleConnect(request: HttpRequestData) =
            try {
                val connect = json.decodeFromString(ConnectRequest.serializer(), request.recordedBody())
                val sourceId = request.headers[sourceIdHeaderName].orEmpty()
                when {
                    scopeInitialized || bundles.isNotEmpty() -> {
                        scopeInitialized = true
                        jsonResponse(
                            connectJson.encodeToString(
                                ConnectResponse.serializer(),
                                ConnectResponse(resolution = "remote_authoritative"),
                            ),
                        )
                    }

                    initializingSourceId != null -> {
                        if (initializingSourceId == sourceId) {
                            if (connect.hasLocalPendingRows) {
                                jsonResponse(
                                    connectJson.encodeToString(
                                        ConnectResponse.serializer(),
                                        ConnectResponse(
                                            resolution = "initialize_local",
                                            initializationId = initializationId.orEmpty(),
                                            leaseExpiresAt = "2099-01-01T00:00:00Z",
                                        ),
                                    ),
                                )
                            } else {
                                scopeInitialized = true
                                initializingSourceId = null
                                initializationId = null
                                jsonResponse(
                                    connectJson.encodeToString(
                                        ConnectResponse.serializer(),
                                        ConnectResponse(resolution = "initialize_empty"),
                                    ),
                                )
                            }
                        } else {
                            jsonResponse(
                                connectJson.encodeToString(
                                    ConnectResponse.serializer(),
                                    ConnectResponse(
                                        resolution = "retry_later",
                                        retryAfterSeconds = 1,
                                        leaseExpiresAt = "2099-01-01T00:00:00Z",
                                    ),
                                ),
                            )
                        }
                    }

                    connect.hasLocalPendingRows -> {
                        val nextId = "00000000-0000-4000-8001-${(nextInitializationId++).toString().padStart(12, '0')}"
                        initializingSourceId = sourceId
                        initializationId = nextId
                        jsonResponse(
                            connectJson.encodeToString(
                                ConnectResponse.serializer(),
                                ConnectResponse(
                                    resolution = "initialize_local",
                                    initializationId = nextId,
                                    leaseExpiresAt = "2099-01-01T00:00:00Z",
                                ),
                            ),
                        )
                    }

                    else -> {
                        scopeInitialized = true
                        jsonResponse(
                            connectJson.encodeToString(
                                ConnectResponse.serializer(),
                                ConnectResponse(resolution = "initialize_empty"),
                            ),
                        )
                    }
                }
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "connect failed")
            }

        private fun generationResponse(generationId: Long): JsonObject? =
            generationResponses?.get(generationId)
                ?: if (generationId == 1L) testGenerationResponse(registeredTableSpecs, usersUpdatedAt) else null

        private suspend fun MockRequestHandleScope.handlePush(
            request: HttpRequestData,
        ): io.ktor.client.request.HttpResponseData {
            if (rejectPush) {
                return errorResponse(
                    error = "worker_push_rejected",
                    message = "worker migration test rejected direct push.",
                    status = HttpStatusCode.BadRequest,
                )
            }
            return try {
                pushRequestCount++
                val body = request.recordedBody()
                val push = json.parseToJsonElement(body).jsonObject
                val sourceId = request.headers[sourceIdHeaderName].orEmpty()
                val sourceBundleId = push.getValue("source_bundle_id").jsonPrimitive.long
                val canonicalRequestHash = push.getValue("canonical_request_hash").jsonPrimitive.content
                val requestInitializationId = push["initialization_id"]?.jsonPrimitive?.content
                val generationId = request.headers["Oversync-Schema-Generation-ID"]?.toLongOrNull() ?: 1L
                val generation = generationResponse(generationId)
                    ?: return errorResponse("schema_generation_not_found", "generation is unavailable", HttpStatusCode.NotFound)
                val rows = decodeCompactPushRows(generation, push.getValue("rows"))
                if (computePushRequestHash(rows, generation) != canonicalRequestHash) {
                    return errorResponse("push_invalid", "canonical request hash mismatch")
                }
                sourceRetiredOnPush?.let { retired -> return sourceRetiredResponse(retired) }
                val replayKey = "$sourceId\u0000$sourceBundleId"
                committedBySourceBundle[replayKey]?.let { existing ->
                    if (existing.canonicalRequestHash != canonicalRequestHash) {
                        return errorResponse("push_replay_mismatch", "replay hash mismatch", HttpStatusCode.Conflict)
                    }
                    return completePushResponse(existing, generation)
                }
                beforePushCommit?.invoke()
                val expectedInitializationId = initializationId
                if (expectedInitializationId != null && requestInitializationId != expectedInitializationId) {
                    return errorResponse("initialization_stale", "initialization_id is required", HttpStatusCode.Conflict)
                }
                if (expectedInitializationId == null && requestInitializationId != null) {
                    return errorResponse("initialization_stale", "scope is no longer initializing", HttpStatusCode.Conflict)
                }
                for (row in rows) {
                    val key = liveKey(row.table, row.key)
                    val liveRow = liveRows[key]
                    val currentVersion = liveRow?.rowVersion ?: 0L
                    val forcedConflict = conflictOverride?.invoke(row, currentVersion, liveRow?.payload)
                    if (forcedConflict != null) {
                        if (forcedConflict.serverRowDeleted) {
                            liveRows.remove(key)
                        } else {
                            val forcedServerRow = forcedConflict.serverRow
                            if (forcedServerRow != null) {
                            liveRows[key] = LiveRow(
                                table = row.table,
                                key = row.key,
                                rowVersion = forcedConflict.serverRowVersion,
                                    payload = forcedServerRow,
                            )
                            }
                        }
                        return pushConflictResponse(forcedConflict)
                    }
                    if (currentVersion != row.baseRowVersion) {
                        return pushConflictResponse(
                            PushConflictDetails(
                                schema = row.schema,
                                table = row.table,
                                key = row.key,
                                op = row.op,
                                baseRowVersion = row.baseRowVersion,
                                serverRowVersion = currentVersion,
                                serverRowDeleted = false,
                                serverRow = liveRow?.payload,
                            ),
                        )
                    }
                }

                val bundleSeq = nextBundleSeq++
                val proposedRows = rows.map { row ->
                    BundleRow(
                        schema = row.schema,
                        table = row.table,
                        key = row.key,
                        op = row.op,
                        rowVersion = bundleSeq,
                        payload = authoritativePayloadTransform?.invoke(row) ?: row.payload,
                    )
                }
                val committedRows = normalizePushEffects?.invoke(proposedRows) ?: proposedRows
                installCommittedRows(committedRows)
                val stored = StoredBundle(
                    bundleSeq = bundleSeq,
                    sourceId = sourceId,
                    sourceBundleId = sourceBundleId,
                    rows = committedRows,
                    bundleHash = computeCommittedBundleHash(committedRows),
                    canonicalRequestHash = canonicalRequestHash,
                )
                bundles += stored
                committedBySourceBundle[replayKey] = stored
                if (initializingSourceId == sourceId) {
                    scopeInitialized = true
                    initializingSourceId = null
                    initializationId = null
                }
                completePushResponse(stored, generation)
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                errorResponse("invalid_request", t.message ?: "push failed")
            }
        }

        private suspend fun MockRequestHandleScope.completePushResponse(
            bundle: StoredBundle,
            generation: JsonObject,
        ): io.ktor.client.request.HttpResponseData {
            beforePushResponse?.invoke()
            return jsonResponse(buildJsonObject {
            put("bundle_seq", bundle.bundleSeq)
            put("bundle_hash", bundle.bundleHash)
            put("rows", encodeCompactRows(generation, bundle.rows))
            }.toString())
        }

        private suspend fun MockRequestHandleScope.handleCreateSnapshotSession(
            request: HttpRequestData,
        ): io.ktor.client.request.HttpResponseData = try {
            val rawBody = request.recordedBody().trim()
            val createRequest = rawBody.takeIf { it.isNotEmpty() }?.let {
                json.decodeFromString(SnapshotSessionCreateRequest.serializer(), it)
            }
            createRequest?.sourceReplacement?.let { requestedSnapshotSourceReplacements += it }
            sourceReplacementInvalidOnSnapshotCreate?.let { invalid ->
                return errorResponse(
                    error = invalid.error,
                    message = invalid.message,
                    status = HttpStatusCode.Conflict,
                )
            }
            sourceRetiredOnSnapshotCreate?.let { retired ->
                return sourceRetiredResponse(retired)
            }
            val generationId = request.headers["Oversync-Schema-Generation-ID"]?.toLongOrNull() ?: 1L
            val snapshotRows = currentSnapshotRows(generationId)
            val snapshotId = "snapshot-${nextSnapshotId++}"
            val stableBundleSeq = bundles.lastOrNull()?.bundleSeq ?: if (scopeInitialized) 0L else 0L
            snapshotSessions[snapshotId] = SnapshotSessionState(
                snapshotId = snapshotId,
                snapshotBundleSeq = stableBundleSeq,
                sourceId = createRequest?.sourceReplacement?.newSourceId
                    ?: request.headers[sourceIdHeaderName].orEmpty(),
                rows = snapshotRows,
            )
            val generation = generationResponse(generationId)
                ?: return errorResponse("schema_generation_not_found", "generation is unavailable", HttpStatusCode.NotFound)
            jsonResponse(
                json.encodeToString(
                    SnapshotSession.serializer(),
                    SnapshotSession(
                        snapshotId = snapshotId,
                        snapshotBundleSeq = stableBundleSeq,
                        rowCount = snapshotRows.size.toLong(),
                        logicalByteCount = snapshotLogicalByteCount(generation, snapshotRows),
                        expiresAt = "2099-01-01T00:00:00Z",
                    ),
                ),
            )
        } catch (t: Throwable) {
            errorResponse("invalid_request", t.message ?: "snapshot create failed")
        }

        private suspend fun MockRequestHandleScope.handleSnapshotChunk(request: HttpRequestData) : io.ktor.client.request.HttpResponseData {
            return try {
                snapshotChunkRequestCount++
                val snapshotId = request.url.encodedPath.removePrefix("/sync/snapshot-sessions/")
                val session = snapshotSessions[snapshotId]
                    ?: return errorResponse("snapshot_not_found", "unknown snapshot", HttpStatusCode.NotFound)
                require(session.sourceId == request.headers[sourceIdHeaderName]) {
                    "snapshot chunks require effective source"
                }
                val afterRowOrdinal = request.url.parameters["after_row_ordinal"]?.toLongOrNull() ?: 0L
                val maxRows = request.url.parameters["max_rows"]?.toIntOrNull() ?: 1000
                val startIndex = afterRowOrdinal.toInt()
                val rows = session.rows.drop(startIndex).take(maxRows)
                val generationId = request.headers["Oversync-Schema-Generation-ID"]?.toLongOrNull() ?: 1L
                val generation = generationResponse(generationId)
                    ?: return errorResponse("schema_generation_not_found", "generation is unavailable", HttpStatusCode.NotFound)
                val bundleRows = rows.map { row ->
                    BundleRow(row.schema, row.table, row.key, "INSERT", row.rowVersion, row.payload)
                }
                jsonResponse(buildJsonObject {
                    put("rows", encodeCompactRows(generation, bundleRows, afterRowOrdinal + 1L))
                    put("next_row_ordinal", afterRowOrdinal + rows.size)
                    put("has_more", startIndex + rows.size < session.rows.size)
                    put("logical_byte_count", snapshotLogicalByteCount(generation, rows))
                }.toString())
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "snapshot fetch failed")
            }
        }

        private suspend fun MockRequestHandleScope.handlePull(request: HttpRequestData) : io.ktor.client.request.HttpResponseData {
            return try {
                pullRequestCount++
                val afterBundleSeq = request.url.parameters["after_bundle_seq"]?.toLongOrNull() ?: 0L
                val maxBundles = request.url.parameters["max_bundles"]?.toIntOrNull() ?: 1000
                val targetBundleSeq = request.url.parameters["target_bundle_seq"]?.toLongOrNull() ?: 0L
                val stableBundleSeq = bundles.lastOrNull()?.bundleSeq ?: 0L
                if (afterBundleSeq > stableBundleSeq) {
                    return errorResponse(
                        "checkpoint_ahead",
                        "after_bundle_seq $afterBundleSeq is ahead of stable bundle $stableBundleSeq",
                        HttpStatusCode.Conflict,
                    )
                }
                if (stableBundleSeq > afterBundleSeq && afterBundleSeq < retainedBundleFloor) {
                    return errorResponse(
                        "history_pruned",
                        "after_bundle_seq $afterBundleSeq is below retained floor $retainedBundleFloor",
                        HttpStatusCode.Conflict,
                    )
                }
                val target = when {
                    targetBundleSeq > 0L -> minOf(targetBundleSeq, stableBundleSeq)
                    else -> stableBundleSeq
                }
                val selected = bundles
                    .filter { it.bundleSeq > afterBundleSeq && it.bundleSeq <= target }
                    .take(maxBundles)
                    .toList()
                val lastReturned = selected.lastOrNull()?.bundleSeq ?: afterBundleSeq
                val generationId = request.headers["Oversync-Schema-Generation-ID"]?.toLongOrNull() ?: 1L
                val generation = generationResponse(generationId)
                    ?: return errorResponse("schema_generation_not_found", "generation is unavailable", HttpStatusCode.NotFound)
                jsonResponse(buildJsonObject {
                    put("stable_bundle_seq", target)
                    put("bundles", JsonArray(selected.map { bundle -> buildJsonObject {
                        put("bundle_seq", bundle.bundleSeq)
                        put("source_id", bundle.sourceId)
                        put("source_bundle_id", bundle.sourceBundleId)
                        put("bundle_hash", bundle.bundleHash)
                        put("rows", encodeCompactRows(generation, bundle.rows))
                    }}))
                    put("has_more", lastReturned < target)
                }.toString())
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "pull failed")
            }
        }

        private fun currentSnapshotRows(generationId: Long): List<SnapshotRow> {
            return liveRows.values
                .sortedWith(compareBy<LiveRow> { it.table }.thenBy { canonicalKey(it.key) })
                .map { row ->
                    val payload = if (
                        generationResponses != null && generationId > 1L && row.table == "people"
                    ) {
                        JsonObject(row.payload.jsonObject + mapOf("details" to JsonNull, "project_id" to JsonNull))
                    } else {
                        row.payload
                    }
                    SnapshotRow(
                        schema = "main",
                        table = row.table,
                        key = row.key,
                        rowVersion = row.rowVersion,
                        payload = payload,
                    )
                }
        }

        private fun computeCommittedBundleHash(rows: List<BundleRow>): String = sha256Hex(committedBundleBytes(rows))

        private fun decodeCompactPushRows(
            discovery: JsonObject,
            encoded: JsonElement,
        ): List<PushRequestRow> {
            val generation = discovery.getValue("schema_generation").jsonObject
            val tables = generation.getValue("tables").jsonArray
            var nextOrdinal = 0L
            val result = mutableListOf<PushRequestRow>()
            encoded.jsonArray.forEach { groupElement ->
                val group = groupElement.jsonArray
                require(group.size == 3)
                val tableIndex = group[0].jsonPrimitive.int
                require(group[1].jsonPrimitive.long == nextOrdinal)
                val table = tables[tableIndex].jsonObject
                val schema = table.getValue("schema").jsonPrimitive.content
                val tableName = table.getValue("table").jsonPrimitive.content
                val keyColumns = table.getValue("sync_key_columns").jsonArray.map { it.jsonPrimitive.content }
                group[2].jsonArray.forEach { tupleElement ->
                    val tuple = tupleElement.jsonArray
                    val op = when (tuple[0].jsonPrimitive.content) {
                        "i" -> "INSERT"
                        "u" -> "UPDATE"
                        "d" -> "DELETE"
                        else -> error("invalid compact operation")
                    }
                    val keyValues = tuple[1].jsonArray
                    require(keyValues.size == keyColumns.size)
                    val key = keyColumns.mapIndexed { index, column ->
                        column to keyValues[index].jsonPrimitive.content
                    }.toMap()
                    result += PushRequestRow(
                        schema = schema,
                        table = tableName,
                        key = key,
                        op = op,
                        baseRowVersion = tuple[2].jsonPrimitive.long,
                        payload = tuple.getOrNull(3),
                    )
                    nextOrdinal++
                }
            }
            return result
        }

        private fun encodeCompactRows(
            discovery: JsonObject,
            rows: List<BundleRow>,
            firstOrdinal: Long = 0L,
        ): JsonArray {
            val generation = discovery.getValue("schema_generation").jsonObject
            val tables = generation.getValue("tables").jsonArray
            if (rows.isEmpty()) return JsonArray(emptyList())
            val groups = mutableListOf<JsonElement>()
            var activeTableIndex = -1
            var activeFirstOrdinal = firstOrdinal
            var activeTuples = mutableListOf<JsonElement>()
            rows.forEachIndexed { index, row ->
                val tableIndex = tables.indexOfFirst { element ->
                    val table = element.jsonObject
                    table.getValue("schema").jsonPrimitive.content == row.schema &&
                        table.getValue("table").jsonPrimitive.content == row.table
                }
                require(tableIndex >= 0)
                if (activeTableIndex != tableIndex) {
                    if (activeTableIndex >= 0) {
                        groups.add(JsonArray(listOf(
                            JsonPrimitive(activeTableIndex),
                            JsonPrimitive(activeFirstOrdinal),
                            JsonArray(activeTuples),
                        )))
                    }
                    activeTableIndex = tableIndex
                    activeFirstOrdinal = firstOrdinal + index
                    activeTuples = mutableListOf()
                }
                val keyColumns = tables[tableIndex].jsonObject
                    .getValue("sync_key_columns").jsonArray.map { it.jsonPrimitive.content }
                activeTuples.add(buildJsonArray {
                    add(when (row.op) { "INSERT" -> "i"; "UPDATE" -> "u"; "DELETE" -> "d"; else -> error("invalid op") })
                    add(JsonArray(keyColumns.map { JsonPrimitive(row.key.getValue(it)) }))
                    add(row.rowVersion)
                    if (row.op != "DELETE") add(row.payload ?: error("payload required"))
                })
            }
            groups.add(JsonArray(listOf(
                JsonPrimitive(activeTableIndex),
                JsonPrimitive(activeFirstOrdinal),
                JsonArray(activeTuples),
            )))
            return JsonArray(groups)
        }

        private fun computePushRequestHash(rows: List<PushRequestRow>, discovery: JsonObject): String {
            val generation = discovery.getValue("schema_generation").jsonObject
            return sha256Hex(canonicalizeJsonElement(buildJsonObject {
                put("generation_fingerprint", generation.getValue("fingerprint"))
                put("generation_id", generation.getValue("id"))
                put("rows", JsonArray(rows.mapIndexed { index, row -> buildJsonObject {
                    put("row_ordinal", index.toString())
                    put("schema", row.schema)
                    put("table", row.table)
                    put("key", buildJsonObject {
                        row.key.entries.sortedBy { it.key }.forEach { (name, value) -> put(name, value) }
                    })
                    put("op", row.op)
                    put("base_row_version", row.baseRowVersion.toString())
                    put("payload", row.payload ?: JsonNull)
                }}))
                put("type", "oversync.generation-pinned-push-request.v1")
            }).encodeToByteArray())
        }

        private fun snapshotLogicalByteCount(
            discovery: JsonObject,
            rows: List<SnapshotRow>,
        ): Long {
            val generation = discovery.getValue("schema_generation").jsonObject
            val tables = generation.getValue("tables").jsonArray
            return rows.sumOf { row ->
                val tableIndex = tables.indexOfFirst { element ->
                    val table = element.jsonObject
                    table.getValue("schema").jsonPrimitive.content == row.schema &&
                        table.getValue("table").jsonPrimitive.content == row.table
                }
                val keyColumns = tables[tableIndex].jsonObject
                    .getValue("sync_key_columns").jsonArray.map { it.jsonPrimitive.content }
                canonicalizeJsonElement(JsonArray(listOf(
                    JsonPrimitive("i"),
                    JsonPrimitive(tableIndex),
                    JsonArray(keyColumns.map { JsonPrimitive(row.key.getValue(it)) }),
                    JsonPrimitive(row.rowVersion),
                    row.payload,
                ))).encodeToByteArray().size.toLong()
            }
        }

        private fun committedBundleBytes(rows: List<BundleRow>): ByteArray {
            val logicalRows = rows.mapIndexed { index, row ->
                buildJsonObject {
					put("row_ordinal", JsonPrimitive(index.toString()))
                    put("schema", JsonPrimitive(row.schema))
                    put("table", JsonPrimitive(row.table))
                    put("key", buildJsonObject {
                        for ((key, value) in row.key.entries.sortedBy { it.key }) {
                            put(key, JsonPrimitive(value))
                        }
                    })
                    put("op", JsonPrimitive(row.op))
					put("row_version", JsonPrimitive(row.rowVersion.toString()))
                    put("payload", row.payload ?: JsonNull)
                }
            }
            return canonicalizeJsonElement(JsonArray(logicalRows)).encodeToByteArray()
        }

        private fun liveKey(table: String, key: SyncKey): String = "$table\u0000${canonicalKey(key)}"

        private fun canonicalKey(key: SyncKey): String =
            key.entries.sortedBy { it.key }.joinToString("&") { "${it.key}=${it.value}" }

        private fun MockRequestHandleScope.jsonResponse(
            body: String,
            status: HttpStatusCode = HttpStatusCode.OK,
        ): io.ktor.client.request.HttpResponseData {
            maxResponseBodyBytes = maxOf(maxResponseBodyBytes, body.encodeToByteArray().size)
            return respond(
                content = body,
                status = status,
                headers = Headers.build {
                    append(HttpHeaders.ContentType, ContentType.Application.Json.toString())
                },
            )
        }

        private fun MockRequestHandleScope.errorResponse(
            error: String,
            message: String,
            status: HttpStatusCode = HttpStatusCode.BadRequest,
        ) = jsonResponse(
            body = json.encodeToString(ErrorResponse.serializer(), ErrorResponse(error = error, message = message)),
            status = status,
        )

        private fun MockRequestHandleScope.pushConflictResponse(
            conflict: PushConflictDetails,
        ) = jsonResponse(
            body = json.encodeToString(
                PushConflictResponse.serializer(),
                PushConflictResponse(
                    error = "push_conflict",
                    message = "base_row_version ${conflict.baseRowVersion} does not match live ${conflict.serverRowVersion}",
                    conflict = conflict,
                ),
            ),
            status = HttpStatusCode.Conflict,
        )

        private fun MockRequestHandleScope.sourceRetiredResponse(
            retired: SourceRetiredResponse,
        ) = jsonResponse(
            body = json.encodeToString(SourceRetiredResponse.serializer(), retired),
            status = HttpStatusCode.Conflict,
        )

        private suspend fun HttpRequestData.bodyText(): String {
            return when (val content = body) {
                is TextContent -> content.text
                is OutgoingContent.ByteArrayContent -> content.bytes().decodeToString()
                is OutgoingContent.ReadChannelContent -> content.readFrom().readRemaining().readText()
                is OutgoingContent.WriteChannelContent -> {
                    val channel = ByteChannel(autoFlush = true)
                    content.writeTo(channel)
                    channel.close()
                    channel.readRemaining().readText()
                }
                is OutgoingContent.NoContent -> ""
                else -> error("unsupported request body type ${content::class.simpleName}")
            }
        }

        private suspend fun HttpRequestData.recordedBody(): String =
            bodyText().also { body ->
                requestBodies += url.encodedPath to body
                maxRequestBodyBytes = maxOf(maxRequestBodyBytes, body.encodeToByteArray().size)
            }

        private data class LiveRow(
            val table: String,
            val key: SyncKey,
            val rowVersion: Long,
            val payload: kotlinx.serialization.json.JsonElement,
        )

        private data class StoredBundle(
            val bundleSeq: Long,
            val sourceId: String,
            val sourceBundleId: Long,
            val rows: List<BundleRow>,
            val bundleHash: String,
            val canonicalRequestHash: String,
        )

        private data class SnapshotSessionState(
            val snapshotId: String,
            val snapshotBundleSeq: Long,
            val sourceId: String,
            val rows: List<SnapshotRow>,
        )
    }
}
