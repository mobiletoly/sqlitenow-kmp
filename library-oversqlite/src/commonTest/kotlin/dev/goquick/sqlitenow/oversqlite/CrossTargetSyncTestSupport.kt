package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.common.PlatformType
import dev.goquick.sqlitenow.common.initializePlatformTestContext
import dev.goquick.sqlitenow.common.platform
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
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
import io.ktor.serialization.kotlinx.json.json
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.random.Random

internal fun computeCommittedBundleHashForTest(rows: List<BundleRow>): String {
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
    return sha256Hex(canonicalizeJsonElement(JsonArray(logicalRows)).encodeToByteArray())
}

internal fun computePushRequestHashForTest(
    rows: List<PushRequestRow>,
    generation: SyncGeneration,
): String = sha256Hex(
    canonicalizeJsonElement(buildJsonObject {
        put("generation_fingerprint", JsonPrimitive(generation.fingerprint))
        put("generation_id", JsonPrimitive(generation.id))
        put("rows", JsonArray(rows.mapIndexed { index, row ->
            buildJsonObject {
                put("row_ordinal", JsonPrimitive(index.toString()))
                put("schema", JsonPrimitive(row.schema))
                put("table", JsonPrimitive(row.table))
                put("key", buildJsonObject {
                    row.key.entries.sortedBy { it.key }.forEach { (key, value) ->
                        put(key, JsonPrimitive(value))
                    }
                })
                put("op", JsonPrimitive(row.op))
                put("base_row_version", JsonPrimitive(row.baseRowVersion.toString()))
                put("payload", row.payload ?: JsonNull)
            }
        }))
        put("type", JsonPrimitive("oversync.generation-pinned-push-request.v1"))
    }).encodeToByteArray(),
)

internal open class CrossTargetSyncTestSupport {
    protected suspend fun requeueDefinitelyUncommittedBaselineOutbox(db: SafeSQLiteConnection) {
        val config = OversqliteConfig(generationId=1, databaseId="fixture", serviceId="main",
            syncTables=listOf(SyncTable("users",syncKeyColumnName="id"),SyncTable("posts",syncKeyColumnName="id")))
        val validated = SyncRuntimeInitializer(config,TableInfoCache()).validateConfig(db)
            .withGeneration(OversqliteContractStore(db).readGeneration(1))
        val local = OversqliteLocalStore(db,Json) { validated }
        val state = OversqliteSyncStateStore(db) { local }
        val outbox = OversqliteOutboxStateStore(db)
        val bundle = outbox.loadBundleState()
        val rows = outbox.loadRows()
        val captures = rows.map { row ->
            val key = Json.parseToJsonElement(row.wireKeyJson).jsonObject.mapValues { it.value.jsonPrimitive.content }
            DirtyRowCapture(row.schemaName,row.tableName,row.keyJson,key.getValue("id"),key,row.op,
                row.baseRowVersion,row.localPayload,row.rowOrdinal,row.base)
        }
        db.transaction(TransactionMode.IMMEDIATE) {
            state.requeueSnapshotRows(PushOutboundSnapshot(bundle.sourceBundleId,captures,rows,bundle.canonicalRequestHash))
            outbox.clearBundleAndRows()
        }
    }

    private companion object {
        const val sourceIdHeaderName = "Oversync-Source-ID"
        var webDatabaseSequence: Long = 0
    }

    private val ownedWebDatabaseNames = linkedSetOf<String>()

    protected val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }

    protected fun runOwnedWebDatabaseTest(timeout: kotlin.time.Duration = kotlin.time.Duration.parse("60s"), testBody: suspend TestScope.() -> Unit) = runTest(timeout=timeout) {
        var primaryFailure: Throwable? = null
        try {
            testBody()
        } catch (failure: Throwable) {
            primaryFailure = failure
        }
        try {
            withContext(NonCancellable) {
                cleanupOwnedWebDatabases()
            }
        } catch (cleanupFailure: Throwable) {
            val primary = primaryFailure
            if (primary == null) {
                primaryFailure = cleanupFailure
            } else {
                primary.addSuppressed(cleanupFailure)
            }
        }
        primaryFailure?.let { throw it }
    }

    private suspend fun cleanupOwnedWebDatabases() {
        var primaryFailure: Throwable? = null
        ownedWebDatabaseNames.forEach { dbName ->
            try {
                cleanupDirectWorkerTestDatabase(dbName)
            } catch (failure: Throwable) {
                val primary = primaryFailure
                if (primary == null) {
                    primaryFailure = failure
                } else {
                    primary.addSuppressed(failure)
                }
            }
        }
        ownedWebDatabaseNames.clear()
        primaryFailure?.let { throw it }
    }

    protected suspend fun newDb(): SafeSQLiteConnection {
        initializePlatformTestContext()
        val dbName = if (platform() == PlatformType.JS) {
            webDatabaseSequence += 1
            "sqlitenow-oversqlite-test-$webDatabaseSequence-${Random.nextLong()}.db".also {
                ownedWebDatabaseNames += it
            }
        } else {
            ":memory:"
        }
        return BundledSqliteConnectionProvider.openConnection(dbName, debug = true)
    }

    protected suspend fun newFileBackedDb(path: String): SafeSQLiteConnection {
        initializePlatformTestContext()
        if (platform() == PlatformType.JS) {
            ownedWebDatabaseNames += path
        }
        return BundledSqliteConnectionProvider.openConnection(
            dbName = path,
            debug = true,
            config = createSqliteNowTestConnectionConfig(path),
        )
    }

    protected fun newClient(
        db: SafeSQLiteConnection,
        http: HttpClient,
        maxPushRows: Int = 200,
        maxPushDecodedBytes: Long = 1L * 1024L * 1024L,
        maxPullBundles: Int = 1000,
        resolver: Resolver = ServerWinsResolver,
        schema: String = "main",
        syncTables: List<SyncTable> = listOf(
            SyncTable("users", syncKeyColumnName = "id"),
            SyncTable("posts", syncKeyColumnName = "id"),
        ),
        requestGzipEngine: OversqliteRequestGzipEngine = OversqliteRequestGzipEngine(::compressOversqliteRequestGzip),
        workspaceEntries: Long = 100000, workspaceBytes: Long = 256L * 1024 * 1024,
        workUnits: Long = 1000000,
        leaseGroup: Any = Any(),
    ): DefaultOversqliteClient {
        return DefaultOversqliteClient.forOpening(
            db = db,
            config = OversqliteConfig(
                generationId = 1, databaseId = "fixture", serviceId = schema,
                maxSyncWorkspaceEntries = workspaceEntries, maxSyncWorkspaceBytes = workspaceBytes,
                maxSyncWorkUnitsPerInvocation = workUnits,
                syncTables = syncTables,
                transferTargets = OversqliteTransferTargets(
                    maxPushRows = maxPushRows,
                    maxPushDecodedBytes = maxPushDecodedBytes,
                    maxPullBundles = maxPullBundles,
                ),
            ),
            http = http,
            resolver = resolver,
            requestGzipEngine = requestGzipEngine,
            leaseGroup = leaseGroup,
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

    protected suspend fun scalarLong(db: SafeSQLiteConnection, sql: String): Long {
        return db.withExclusiveAccess {
            db.prepare(sql).use { st ->
                check(st.step())
                st.getLong(0)
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

    protected suspend fun insertUser(db: SafeSQLiteConnection, id: String, name: String) {
        db.execSQL("INSERT INTO users(id, name) VALUES('$id', '$name')")
    }

    protected suspend fun insertPost(db: SafeSQLiteConnection, id: String, userId: String, title: String) {
        db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('$id', '$userId', '$title')")
    }

    protected suspend fun updateUserName(db: SafeSQLiteConnection, id: String, name: String) {
        db.execSQL("UPDATE users SET name = '$name' WHERE id = '$id'")
    }

    protected class MockSyncServer {
        internal var generationOverride: SyncGeneration? = null
        internal var wireLimits = testWireLimits()
        internal var beforePushResponse: (suspend () -> Unit)? = null
        internal var beforePushCommit: (suspend () -> Unit)? = null
        internal var beforePullResponse: (suspend () -> Unit)? = null
        internal var remainingTransientPullFailures: Int = 0
        internal var beforeSnapshotChunkResponse: (suspend () -> Unit)? = null
        internal var extraPushEffects: ((List<PushRequestRow>, Long) -> List<BundleRow>)? = null
        internal var normalizePushEffects: ((List<BundleRow>) -> List<BundleRow>)? = null
        private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
        private val connectJson = Json { ignoreUnknownKeys = true; encodeDefaults = false }
        private val liveRows = linkedMapOf<String, LiveRow>()
        private val bundles = mutableListOf<StoredBundle>()
        private val committedBySourceBundle = linkedMapOf<String, StoredBundle>()
        private val snapshotSessions = linkedMapOf<String, SnapshotSessionState>()
        private var scopeInitialized = false
        private var initializingSourceId: String? = null
        private var initializationId: String? = null

        var retainedBundleFloor: Long = 0
        var conflictOverride: ((PushRequestRow, Long, kotlinx.serialization.json.JsonElement?) -> PushConflictDetails?)? = null
        var sourceRetiredOnPush: SourceRetiredResponse? = null
        var sourceRetiredOnSnapshotCreate: SourceRetiredResponse? = null
        var sourceReplacementInvalidOnSnapshotCreate: ErrorResponse? = null
        var bundleChangeWatchSupported: Boolean = false
        var protocolVersion: String = "v1"
        val discoverySourceIds = mutableListOf<String>()
        val requestTrace = mutableListOf<String>()
        val pushBodies = mutableListOf<String>()

        fun remoteRowsForBaselineTest(): Map<String, String> =
            liveRows.values.associate { row -> "${row.table}/${canonicalKey(row.key)}" to row.payload.toString() }
        var registeredTableSpecs: List<RegisteredTableSpec> = testRegisteredTableSpecs("users", "posts")
        var connectRequestCount: Int = 0
        var pullRequestCount: Int = 0
        var pushRequestCount: Int = 0
        var snapshotSessionCreateRequestCount: Int = 0
        var cancelNextPush: Boolean = false
        var watchCloseCount: Int = 0
        val watchSourceIds = mutableListOf<String>()
        val watchAfterBundleSeqs = mutableListOf<Long>()
        val requestedSnapshotSourceReplacements = mutableListOf<SnapshotSourceReplacement>()
        private val watchResponseOverrides = mutableListOf<WatchResponseOverride>()
        private var holdNextWatchResponseOpen: Boolean = false
        private val heldWatchResponseChannels = mutableListOf<ByteChannel>()

        private var nextBundleSeq = 1L
        private var nextSnapshotId = 1L
        private var nextInitializationId = 1L

        fun newHttpClient(): HttpClient {
            return HttpClient(MockEngine { request ->
                requestTrace += "${request.method.value} ${request.url.encodedPath}"
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

        fun enqueueWatchResponse(
            status: HttpStatusCode = HttpStatusCode.OK,
            body: String,
        ) {
            watchResponseOverrides += WatchResponseOverride(status = status, body = body)
        }

        fun holdNextWatchResponseOpen() {
            holdNextWatchResponseOpen = true
        }

        suspend fun sendHeldWatchBundleEvent(bundleSeq: Long) {
            check(heldWatchResponseChannels.isNotEmpty()) { "no active held watch response" }
            heldWatchResponseChannels.last().writeStringUtf8(
                "event: bundle\ndata: {\"bundle_seq\":$bundleSeq}\n\n",
            )
        }

        fun closeHeldWatchResponses(cause: Throwable? = null) {
            heldWatchResponseChannels.forEach { channel ->
                if (cause == null) channel.close() else channel.cancel(cause)
            }
            watchCloseCount += heldWatchResponseChannels.size
            heldWatchResponseChannels.clear()
        }

        fun addRemoteBundleForTest(rows: List<BundleRow>): Long {
            val bundleSeq = nextBundleSeq++
            rows.forEach { row ->
                val key = liveKey(row.table, row.key)
                if (row.op == "DELETE") {
                    liveRows.remove(key)
                } else {
                    liveRows[key] = LiveRow(
                        table = row.table,
                        key = row.key,
                        rowVersion = row.rowVersion,
                        payload = row.payload ?: error("payload required for ${row.op}"),
                    )
                }
            }
            bundles += StoredBundle(
                bundleSeq = bundleSeq,
                sourceId = "remote-source",
                sourceBundleId = bundleSeq,
                rows = rows,
                bundleHash = computeCommittedBundleHash(rows),
            )
            scopeInitialized = true
            return bundleSeq
        }

        suspend fun addRemoteBundleAndNotifyForTest(rows: List<BundleRow>) {
            val bundleSeq = addRemoteBundleForTest(rows)
            val body =
                "event: bundle\n" +
                    "data: {\"bundle_seq\":$bundleSeq}\n\n"
            heldWatchResponseChannels
                .filterNot { it.isClosedForWrite }
                .forEach { channel ->
                    channel.writeStringUtf8(body)
                }
        }

        private suspend fun MockRequestHandleScope.handle(request: HttpRequestData) = when {
            request.method.value == "GET" && request.url.encodedPath == "/sync/generations/1" ->
                run {
                    discoverySourceIds += request.headers[sourceIdHeaderName].orEmpty()
                    jsonResponse(
                        json.encodeToString(
                            GenerationDiscovery.serializer(),
                            GenerationDiscovery(
                                protocolVersion = protocolVersion,
                                generation = generationOverride ?: testGeneration(registeredTableSpecs),
                                limits = wireLimits,
                                watchEnabled = bundleChangeWatchSupported,
                            ),
                        ),
                    )
                }

            request.method.value == "POST" && request.url.encodedPath == "/sync/connect" -> {
                connectRequestCount++
                handleConnect(request)
            }

            request.method.value == "POST" && request.url.encodedPath == "/sync/push" ->
                handlePush(request)

            request.method.value == "POST" && request.url.encodedPath == "/sync/snapshot-sessions" -> {
                snapshotSessionCreateRequestCount++
                handleCreateSnapshotSession(request)
            }

            request.method.value == "GET" &&
                request.url.encodedPath.startsWith("/sync/snapshot-sessions/") ->
                handleSnapshotChunk(request)

            request.method.value == "DELETE" &&
                request.url.encodedPath.startsWith("/sync/snapshot-sessions/") -> {
                val snapshotId = request.url.encodedPath.removePrefix("/sync/snapshot-sessions/")
                require(snapshotSessions[snapshotId]?.sourceId == request.headers[sourceIdHeaderName]) { "snapshot retirement requires effective source" }
                snapshotSessions.remove(snapshotId)
                jsonResponse("""{}""")
            }

            request.method.value == "GET" && request.url.encodedPath == "/sync/pull" ->
                handlePull(request)

            request.method.value == "GET" && request.url.encodedPath == "/sync/watch" ->
                handleWatch(request)

            else -> jsonResponse(
                body = json.encodeToString(ErrorResponse("not_found", request.url.encodedPath)),
                status = HttpStatusCode.NotFound,
            )
        }

        private suspend fun MockRequestHandleScope.handleConnect(request: HttpRequestData) =
            try {
                val connect = json.decodeFromString(ConnectRequest.serializer(), request.bodyText())
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

        private suspend fun MockRequestHandleScope.handlePush(
            request: HttpRequestData,
        ): io.ktor.client.request.HttpResponseData = try {
                pushRequestCount++
                if (cancelNextPush) {
                    cancelNextPush = false
                    throw CancellationException("cancelled push request")
                }
                val body = request.bodyText()
                pushBodies += body
                val push = json.decodeFromString(CompletePushRequest.serializer(), body)
                val sourceId = request.headers[sourceIdHeaderName].orEmpty()
                sourceRetiredOnPush?.let { return sourceRetiredResponse(it) }
                val selectedGeneration = generationOverride ?: testGeneration(registeredTableSpecs)
                val rows = OversqliteCompactCodec.decodeBundleRows(selectedGeneration, push.rows).map { row ->
                    PushRequestRow(row.schema, row.table, row.key, row.op, row.rowVersion, row.payload)
                }
                val computedHash = computePushRequestHashForTest(rows, selectedGeneration)
                if (computedHash != push.canonicalRequestHash) {
                    return errorResponse("push_invalid", "canonical request hash mismatch")
                }
                val replayKey = "$sourceId\u0000${push.sourceBundleId}"
                val existing = committedBySourceBundle[replayKey]
                if (existing != null) {
                    if (existing.canonicalRequestHash != push.canonicalRequestHash) {
                        return errorResponse("push_replay_mismatch", "replay hash mismatch", HttpStatusCode.Conflict)
                    }
                    return completePushResponse(existing, selectedGeneration)
                }
                val expectedInitializationId = initializationId
                if (expectedInitializationId != null && push.initializationId != expectedInitializationId) {
                    return errorResponse("initialization_stale", "initialization_id is required", HttpStatusCode.Conflict)
                }
                if (expectedInitializationId == null && push.initializationId != null) {
                    return errorResponse("initialization_stale", "scope is no longer initializing", HttpStatusCode.Conflict)
                }
                beforePushCommit?.let { hook -> beforePushCommit = null; hook() }
                for (row in rows) {
                    val key = liveKey(row.table, row.key)
                    val liveRow = liveRows[key]
                    val currentVersion = liveRow?.rowVersion ?: 0L
                    val forcedConflict = conflictOverride?.invoke(row, currentVersion, liveRow?.payload)
                    if (forcedConflict != null) {
                        // Conflict observations are authoritative history; subsequent commits must be newer.
                        nextBundleSeq = maxOf(nextBundleSeq, forcedConflict.serverRowVersion + 1)
                        if (forcedConflict.serverRowDeleted) {
                            liveRows.remove(key)
                        } else if (forcedConflict.serverRow != null) {
                            liveRows[key] = LiveRow(
                                table = row.table,
                                key = row.key,
                                rowVersion = forcedConflict.serverRowVersion,
                                payload = forcedConflict.serverRow,
                            )
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
                val committedRows = rows.map { row ->
                    BundleRow(
                        schema = row.schema,
                        table = row.table,
                        key = row.key,
                        op = row.op,
                        rowVersion = bundleSeq,
                        payload = row.payload,
                    )
                }.let { effects ->
                    val complete = effects + (extraPushEffects?.invoke(rows, bundleSeq) ?: emptyList())
                    normalizePushEffects?.invoke(complete) ?: complete
                }
                committedRows.forEach { row ->
                    val key = liveKey(row.table, row.key)
                    if (row.op == "DELETE") {
                        liveRows.remove(key)
                    } else {
                        liveRows[key] = LiveRow(
                            table = row.table,
                            key = row.key,
                            rowVersion = row.rowVersion,
                            payload = row.payload ?: error("payload required for ${row.op}"),
                        )
                    }
                }
                val stored = StoredBundle(
                    bundleSeq = bundleSeq,
                    sourceId = sourceId,
                    sourceBundleId = push.sourceBundleId,
                    rows = committedRows,
                    bundleHash = computeCommittedBundleHash(committedRows),
                    canonicalRequestHash = push.canonicalRequestHash,
                )
                bundles += stored
                committedBySourceBundle[replayKey] = stored
                if (initializingSourceId == sourceId) {
                    scopeInitialized = true
                    initializingSourceId = null
                    initializationId = null
                }
                beforePushResponse?.let { hook -> beforePushResponse = null; hook() }
                completePushResponse(stored, selectedGeneration)
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                errorResponse("invalid_request", t.message ?: "push failed")
            }

        private fun MockRequestHandleScope.completePushResponse(
            bundle: StoredBundle,
            generation: SyncGeneration,
        ) = jsonResponse(
            buildJsonObject {
                put("bundle_seq", JsonPrimitive(bundle.bundleSeq))
                put("bundle_hash", JsonPrimitive(bundle.bundleHash))
                put("rows", OversqliteCompactCodec.encodeBundleRows(generation, bundle.rows))
            }.toString(),
        )

        private suspend fun MockRequestHandleScope.handleCreateSnapshotSession(
            request: HttpRequestData,
        ): io.ktor.client.request.HttpResponseData = try {
            val rawBody = request.bodyText().trim()
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
            val snapshotRows = currentSnapshotRows()
            val snapshotId = "snapshot-${nextSnapshotId++}"
            val stableBundleSeq = bundles.lastOrNull()?.bundleSeq ?: if (scopeInitialized) 0L else 0L
            snapshotSessions[snapshotId] = SnapshotSessionState(
                snapshotId = snapshotId,
                snapshotBundleSeq = stableBundleSeq,
                sourceId = createRequest?.sourceReplacement?.newSourceId ?: request.headers[sourceIdHeaderName].orEmpty(),
                rows = snapshotRows,
            )
            jsonResponse(
                json.encodeToString(
                    SnapshotSession.serializer(),
                    SnapshotSession(
                        snapshotId = snapshotId,
                        snapshotBundleSeq = stableBundleSeq,
                        rowCount = snapshotRows.size.toLong(),
                        logicalByteCount = snapshotLogicalByteCount(snapshotRows),
                        expiresAt = "2099-01-01T00:00:00Z",
                    ),
                ),
            )
        } catch (t: Throwable) {
            errorResponse("invalid_request", t.message ?: "snapshot create failed")
        }

        private suspend fun MockRequestHandleScope.handleSnapshotChunk(request: HttpRequestData) : io.ktor.client.request.HttpResponseData {
            return try {
                val snapshotId = request.url.encodedPath.removePrefix("/sync/snapshot-sessions/")
                val session = snapshotSessions[snapshotId]
                    ?: return errorResponse("snapshot_not_found", "unknown snapshot", HttpStatusCode.NotFound)
                require(session.sourceId == request.headers[sourceIdHeaderName]) { "snapshot chunks require effective source" }
                val afterRowOrdinal = request.url.parameters["after_row_ordinal"]?.toLongOrNull() ?: 0L
                val maxRows = request.url.parameters["max_rows"]?.toIntOrNull() ?: 1000
                val startIndex = afterRowOrdinal.toInt()
                val rows = session.rows.drop(startIndex).take(maxRows)
                val selectedGeneration = generationOverride ?: testGeneration(registeredTableSpecs)
                val bundleRows = rows.map { row ->
                    BundleRow(row.schema, row.table, row.key, "INSERT", row.rowVersion, row.payload)
                }
                beforeSnapshotChunkResponse?.let { hook -> beforeSnapshotChunkResponse = null; hook() }
                jsonResponse(buildJsonObject {
                    put("rows", OversqliteCompactCodec.encodeBundleRows(
                        generation = selectedGeneration,
                        rows = bundleRows,
                        firstOrdinal = afterRowOrdinal + 1L,
                        snapshot = true,
                    ))
                    put("next_row_ordinal", JsonPrimitive(afterRowOrdinal + rows.size))
                    put("has_more", JsonPrimitive(startIndex + rows.size < session.rows.size))
                    put("logical_byte_count", JsonPrimitive(
                        OversqliteCompactCodec.snapshotLogicalByteCount(selectedGeneration, bundleRows),
                    ))
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
                val selectedGeneration = generationOverride ?: testGeneration(registeredTableSpecs)
                val selected = bundles
                    .filter { it.bundleSeq > afterBundleSeq && it.bundleSeq <= target }
                    .take(maxBundles)
                    .toList()
                val lastReturned = selected.lastOrNull()?.bundleSeq ?: afterBundleSeq
                beforePullResponse?.let { hook -> beforePullResponse = null; hook() }
                if (remainingTransientPullFailures > 0) {
                    remainingTransientPullFailures--
                    return errorResponse(
                        "server_unavailable", "injected transient pull failure", HttpStatusCode.ServiceUnavailable,
                    )
                }
                jsonResponse(buildJsonObject {
                    put("stable_bundle_seq", JsonPrimitive(target))
                    put("bundles", JsonArray(selected.map { bundle -> buildJsonObject {
                        put("bundle_seq", JsonPrimitive(bundle.bundleSeq))
                        put("source_id", JsonPrimitive(bundle.sourceId))
                        put("source_bundle_id", JsonPrimitive(bundle.sourceBundleId))
                        put("bundle_hash", JsonPrimitive(bundle.bundleHash))
                        put("rows", OversqliteCompactCodec.encodeBundleRows(selectedGeneration, bundle.rows))
                    }}))
                    put("has_more", JsonPrimitive(lastReturned < target))
                }.toString())
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "pull failed")
            }
        }

        private fun MockRequestHandleScope.handleWatch(
            request: HttpRequestData,
        ): io.ktor.client.request.HttpResponseData {
            if (!bundleChangeWatchSupported) {
                return errorResponse(
                    error = "bundle_change_watch_disabled",
                    message = "bundle change watch is disabled",
                    status = HttpStatusCode.ServiceUnavailable,
                )
            }
            val afterBundleSeq = request.url.parameters["after_bundle_seq"]?.toLongOrNull() ?: 0L
            watchSourceIds += request.headers[sourceIdHeaderName].orEmpty()
            watchAfterBundleSeqs += afterBundleSeq
            if (holdNextWatchResponseOpen) {
                holdNextWatchResponseOpen = false
                val channel = ByteChannel(autoFlush = true)
                heldWatchResponseChannels += channel
                return respond(
                    content = channel,
                    status = HttpStatusCode.OK,
                    headers = Headers.build {
                        append(HttpHeaders.ContentType, "text/event-stream")
                    },
                )
            }
            watchResponseOverrides.removeFirstOrNull()?.let { override ->
                watchCloseCount++
                return respond(
                    content = override.body,
                    status = override.status,
                    headers = Headers.build {
                        append(
                            HttpHeaders.ContentType,
                            if (override.status == HttpStatusCode.OK) {
                                "text/event-stream"
                            } else {
                                ContentType.Application.Json.toString()
                            },
                        )
                    },
                )
            }
            val changedBundles = bundles.filter { it.bundleSeq > afterBundleSeq }
            val body = if (changedBundles.isEmpty()) {
                ": heartbeat\n\n"
            } else {
                changedBundles.joinToString(separator = "\n\n", postfix = "\n\n") { bundle ->
                    """
                    event: bundle
                    data: {"bundle_seq":${bundle.bundleSeq}}
                    """.trimIndent()
                }
            }
            return respond(
                content = body,
                status = HttpStatusCode.OK,
                headers = Headers.build {
                    append(HttpHeaders.ContentType, "text/event-stream")
                },
            )
        }

        private fun currentSnapshotRows(): List<SnapshotRow> {
            return liveRows.values
                .sortedWith(compareBy<LiveRow> { it.table }.thenBy { canonicalKey(it.key) })
                .map { row ->
                    SnapshotRow(
                        schema = "main",
                        table = row.table,
                        key = row.key,
                        rowVersion = row.rowVersion,
                        payload = row.payload,
                    )
                }
        }

        private fun computeCommittedBundleHash(rows: List<BundleRow>): String = computeCommittedBundleHashForTest(rows)

        private fun snapshotLogicalByteCount(rows: List<SnapshotRow>): Long {
            val selectedGeneration = generationOverride ?: testGeneration(registeredTableSpecs)
            return OversqliteCompactCodec.snapshotLogicalByteCount(
                selectedGeneration,
                rows.map { BundleRow(it.schema, it.table, it.key, "INSERT", it.rowVersion, it.payload) },
            )
        }

        private fun liveKey(table: String, key: SyncKey): String = "$table\u0000${canonicalKey(key)}"

        private fun canonicalKey(key: SyncKey): String =
            key.entries.sortedBy { it.key }.joinToString("&") { "${it.key}=${it.value}" }

        private fun MockRequestHandleScope.jsonResponse(
            body: String,
            status: HttpStatusCode = HttpStatusCode.OK,
        ) = respond(
            content = body,
            status = status,
            headers = Headers.build {
                append(HttpHeaders.ContentType, ContentType.Application.Json.toString())
            },
        )

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
            val canonicalRequestHash: String = "",
        )

        private data class SnapshotSessionState(
            val snapshotId: String,
            val snapshotBundleSeq: Long,
            val sourceId: String,
            val rows: List<SnapshotRow>,
        )

        private data class WatchResponseOverride(
            val status: HttpStatusCode,
            val body: String,
        )
    }
}
