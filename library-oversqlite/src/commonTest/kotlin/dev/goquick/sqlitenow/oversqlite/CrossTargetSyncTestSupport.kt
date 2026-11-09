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
import kotlinx.serialization.json.encodeToJsonElement
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

    // Internal bounded kernels remain testable without making customers drive slices.
    protected suspend fun preparePushSlice(db: SafeSQLiteConnection, http: HttpClient, work: Long,
        tables: List<SyncTable> = listOf(SyncTable("users", "id"), SyncTable("posts", "id"))): Result<PushPlan?> {
        val config = OversqliteConfig(1, tables, "fixture", "main", maxSyncWorkUnitsPerInvocation = work)
        val generation = OversqliteContractStore(db).readGeneration(1)
        val validated = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db).withGeneration(generation)
        val state = RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false),
            "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
        val operations = OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}
        val local = OversqliteLocalStore(db, json) { validated }
        val stage = OversqliteStageStore(db, local, OversqliteSyncStateStore(db) { local }, json, config, operations)
        val step = 32L + validated.tables.sumOf { table ->
            val info=validated.tableInfoByName.getValue(table.tableName)
            4L+16L*info.columns.size+32L*info.foreignKeys.size
        }
        return operations.runExclusiveOperation { stage.preparePushPlanSlice(state) { step } }
    }

    protected suspend fun releasePushSlice(db: SafeSQLiteConnection, http: HttpClient, diagnostic: String): Result<Unit> {
        val config = OversqliteConfig(1, listOf(SyncTable("users", "id")), "fixture", "main", maxSyncWorkUnitsPerInvocation = 8)
        val operations = OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}
        val local = OversqliteLocalStore(db, json) { error("cleanup does not read payloads") }
        val stage = OversqliteStageStore(db, local, OversqliteSyncStateStore(db) { local }, json, config, operations)
        return operations.runExclusiveOperation { stage.clearPushPlanForLifecycleSlice(false, diagnostic, OversqliteStageStore.PushCleanupIdentity()) }
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
        maxSnapshotChunkRows: Int = 1000,
        maxSnapshotChunkDecodedBytes: Long = 4L * 1024L * 1024L,
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
                    maxSnapshotChunkRows = maxSnapshotChunkRows,
                    maxSnapshotChunkDecodedBytes = maxSnapshotChunkDecodedBytes,
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
        internal val generationDiscoveryOverrides=mutableMapOf<String,SyncGeneration>()
        internal var wireLimits = testWireLimits()
        internal var beforePushResponse: (suspend () -> Unit)? = null
        internal var beforePushCommit: (suspend () -> Unit)? = null
        internal var beforePullResponse: (suspend () -> Unit)? = null
        internal var remainingTransientPullFailures: Int = 0
        internal var beforeSnapshotChunkResponse: (suspend () -> Unit)? = null
        internal var snapshotChunkTooSmallOnceForTest: Boolean = false
        internal var beforeSnapshotChunkRequest: (suspend () -> Unit)? = null
        internal var beforeSnapshotCreateRequest: (suspend () -> Unit)? = null
        internal var beforeSnapshotCreateResponse: (suspend () -> Unit)? = null
        internal var extraPushEffects: ((List<PushRequestRow>, Long) -> List<BundleRow>)? = null
        internal var normalizePushEffects: ((List<BundleRow>) -> List<BundleRow>)? = null
        private val json = Json { ignoreUnknownKeys = true; encodeDefaults = true }
        private val connectJson = Json { ignoreUnknownKeys = true; encodeDefaults = false }
        private val liveRows = linkedMapOf<String, LiveRow>()
        // Authoritative delete observations: the real server keeps a tombstone version that a
        // later INSERT must name as its base; a stale UPDATE/DELETE against it conflicts as deleted.
        private val tombstones = linkedMapOf<String, Long>()
        private val bundles = mutableListOf<StoredBundle>()
        private val committedBySourceBundle = linkedMapOf<String, StoredBundle>()
        private val terminalOutcomes = linkedMapOf<String, TerminalPushOutcome>()
        private val terminalBytes = linkedMapOf<String, String>()
        private val originalGenerations = linkedMapOf<String, SyncGeneration>()
        private val latestAck = linkedMapOf<String, Pair<PushOperationBinding,PushOutcomeAckRequest>>()
        val terminalStatuses = linkedMapOf<String,String>()
        val resolveRequests = mutableListOf<PushOutcomeResolveRequest>()
        val retainedRequests = mutableListOf<PushOutcomeRetainedRequest>()
        val ackRequests = mutableListOf<PushOutcomeAckRequest>()
        internal var beforeResolve: (suspend () -> Unit)? = null
        internal var beforeAck: (suspend () -> Unit)? = null
        internal var afterAck: (suspend () -> Unit)? = null
        internal var outcomeResponseOverride: ((String) -> String)? = null
        internal var applicationRejectionCode: String? = null
        internal var rejectMutableAdmission = false
        internal var retireOldGeneration = false
        internal var unavailableSecondGeneration = false
        internal var resolveCapacityUnavailable = false

        private val snapshotSessions = linkedMapOf<String, SnapshotSessionState>()
        private var scopeInitialized = false
        private var initializingSourceId: String? = null
        private var initializationId: String? = null

        var retainedBundleFloor: Long = 0
        var conflictOverride: ((PushRequestRow, Long, kotlinx.serialization.json.JsonElement?) -> PushConflictDetails?)? = null
        var sourceRetiredOnPush: SourceRetiredResponse? = null
        // Sources the server no longer accepts for push, resolve or acknowledgment, mapped to the
        // replacement it names. A replacement source is never refused by these fixtures.
        val retiredSources = linkedMapOf<String, String>()
        // Sources whose upload sequence the server cannot continue, mapped to the refusal code.
        val sequenceRefusedSources = linkedMapOf<String, String>()
        var sourceRetiredOnSnapshotCreate: SourceRetiredResponse? = null
        var sourceReplacementInvalidOnSnapshotCreate: ErrorResponse? = null
        var bundleChangeWatchSupported: Boolean = false
        var protocolVersion: String = "v1"
        val discoverySourceIds = mutableListOf<String>()
        val connectSourceIds = mutableListOf<String>()
        val requestTrace = mutableListOf<String>()
        val pushBodies = mutableListOf<String>()

        fun remoteRowsForBaselineTest(): Map<String, String> =
            liveRows.values.associate { row -> "${row.table}/${canonicalKey(row.key)}" to row.payload.toString() }
        var registeredTableSpecs: List<RegisteredTableSpec> = testRegisteredTableSpecs("users", "posts")
        var connectRequestCount: Int = 0
        var pullRequestCount: Int = 0
        var pushRequestCount: Int = 0
        val snapshotChunkRequests = mutableListOf<Pair<String, Long>>()
        fun expireSnapshotSessionForTest(id: String) { snapshotSessions.remove(id) }
        fun snapshotSessionIdsForTest(): Set<String> = snapshotSessions.keys.toSet()
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
                if (request.method.value == "GET" && request.url.encodedPath.startsWith("/sync/snapshot-sessions/")) {
                    beforeSnapshotChunkRequest?.let { hook -> beforeSnapshotChunkRequest = null; hook() }
                }
                if (request.method.value == "POST" && request.url.encodedPath == "/sync/snapshot-sessions") {
                    beforeSnapshotCreateRequest?.let { hook -> beforeSnapshotCreateRequest = null; hook() }
                }
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
                    tombstones[key] = row.rowVersion
                } else {
                    tombstones.remove(key)
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

        fun pruneBundleHistoryForSnapshotTest() {
            check(snapshotSessions.isEmpty()) { "finish the prior snapshot before pruning mock history" }
            val latest = bundles.lastOrNull() ?: error("no bundle history to prune")
            // Snapshot sessions read liveRows. Keep the stable sequence and a
            // pruned-history response without retaining obsolete bundle trees.
            retainedBundleFloor = latest.bundleSeq
            bundles.clear()
            bundles += latest.copy(rows = emptyList())
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
            request.method.value == "GET" && (request.url.encodedPath == "/sync/generations/1" || request.url.encodedPath.removePrefix("/sync/generations/") in generationDiscoveryOverrides) ->
                run {
                    discoverySourceIds += request.headers[sourceIdHeaderName].orEmpty()
                    if(unavailableSecondGeneration && request.url.encodedPath=="/sync/generations/2") jsonResponse("""{"error":"unknown_schema_generation","message":"unavailable","retryable":false,"remediation":"upgrade_or_reconfigure_client","schema_generation_id":"2"}""",HttpStatusCode.Conflict)
                    else if(retireOldGeneration && request.url.encodedPath=="/sync/generations/1") jsonResponse("""{"error":"retired_schema_generation","message":"retired","retryable":false,"remediation":"upgrade_application","schema_generation_id":"1"}""",HttpStatusCode.UpgradeRequired)
                    else jsonResponse(
                        json.encodeToString(
                            GenerationDiscovery.serializer(),
                            GenerationDiscovery(
                                protocolVersion = protocolVersion,
                                generation = generationDiscoveryOverrides[request.url.encodedPath.removePrefix("/sync/generations/")] ?: generationOverride ?: testGeneration(registeredTableSpecs),
                                limits = wireLimits,
                                watchEnabled = bundleChangeWatchSupported,
                            ),
                        ),
                    )
                }

            request.method.value == "POST" && request.url.encodedPath == "/sync/connect" -> {
                connectRequestCount++
                if(rejectMutableAdmission) errorResponse("schema_generation_retired","retired",HttpStatusCode.Gone) else handleConnect(request)
            }

            request.method.value == "POST" && request.url.encodedPath == "/sync/push" -> handlePush(request)
            request.method.value == "POST" && request.url.encodedPath == "/sync/push-outcomes/resolve" -> handleResolve(request)
            request.method.value == "POST" && request.url.encodedPath == "/sync/push-outcomes/retained" -> handleRetained(request)
            request.method.value == "POST" && request.url.encodedPath == "/sync/push-outcomes/ack" -> handleAck(request)

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
                connectSourceIds += sourceId
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
                refusedSourceResponse(sourceId)?.let { return it }
                val selectedGeneration = generationOverride ?: testGeneration(registeredTableSpecs)
                originalGenerations[selectedGeneration.fingerprint]=selectedGeneration
                val rows = OversqliteCompactCodec.decodeBundleRows(selectedGeneration, push.rows).map { row ->
                    PushRequestRow(row.schema, row.table, row.key, row.op, row.rowVersion, row.payload)
                }
                val computedHash = computePushRequestHashForTest(rows, selectedGeneration)
                if (computedHash != push.canonicalRequestHash) {
                    return errorResponse("push_invalid", "canonical request hash mismatch")
                }
                val replayKey = "$sourceId\u0000${push.sourceBundleId}"
                latestAck[sourceId]?.let { if(push.sourceBundleId<=it.second.sourceBundleId) return errorResponse("push_outcome_acknowledged","acked",HttpStatusCode.Gone) }
                terminalOutcomes[replayKey]?.let { existing ->
                    if(existing.canonicalRequestHash!=push.canonicalRequestHash || existing.initializationId!=push.initializationId)
                        return errorResponse("push_outcome_mismatch","mismatch",HttpStatusCode.Conflict)
                    return jsonResponse(checkNotNull(terminalBytes[replayKey]))
                }
                val expectedInitializationId = initializationId
                if (expectedInitializationId != null && push.initializationId != expectedInitializationId) {
                    return errorResponse("initialization_stale", "initialization_id is required", HttpStatusCode.Conflict)
                }
                if (expectedInitializationId == null && push.initializationId != null) {
                    return errorResponse("initialization_stale", "scope is no longer initializing", HttpStatusCode.Conflict)
                }
                beforePushCommit?.let { hook -> beforePushCommit = null; hook() }
                applicationRejectionCode?.let { code -> return terminalRejection(sourceId,push,selectedGeneration,PushOutcomeRejection(code)) }
                for (row in rows) {
                    val key = liveKey(row.table, row.key)
                    val liveRow = liveRows[key]
                    val tombstoneVersion = if (liveRow == null) tombstones[key] else null
                    val currentVersion = liveRow?.rowVersion ?: tombstoneVersion ?: 0L
                    val forcedConflict = conflictOverride?.invoke(row, currentVersion, liveRow?.payload)
                    if (forcedConflict != null) {
                        // Conflict observations are authoritative history; subsequent commits must be newer.
                        // The real server never skips a sequence, so the jump is filled with empty envelopes.
                        while (nextBundleSeq <= forcedConflict.serverRowVersion) {
                            val fillerSeq = nextBundleSeq++
                            bundles += StoredBundle(
                                bundleSeq = fillerSeq, sourceId = "mock-forced-history", sourceBundleId = fillerSeq,
                                rows = emptyList(), bundleHash = computeCommittedBundleHash(emptyList()),
                            )
                        }
                        if (forcedConflict.serverRowDeleted) {
                            liveRows.remove(key)
                            tombstones[key] = forcedConflict.serverRowVersion
                        } else if (forcedConflict.serverRow != null) {
                            liveRows[key] = LiveRow(
                                table = row.table,
                                key = row.key,
                                rowVersion = forcedConflict.serverRowVersion,
                                payload = forcedConflict.serverRow,
                            )
                        }
                        return terminalRejection(sourceId,push,selectedGeneration,PushOutcomeRejection("push_conflict",wireConflict(forcedConflict)))
                    }
                    if (currentVersion != row.baseRowVersion) {
                        return terminalRejection(sourceId,push,selectedGeneration,PushOutcomeRejection("push_conflict",wireConflict(
                            PushConflictDetails(
                                schema = row.schema,
                                table = row.table,
                                key = row.key,
                                op = row.op,
                                baseRowVersion = row.baseRowVersion,
                                serverRowVersion = currentVersion,
                                serverRowDeleted = tombstoneVersion != null,
                                serverRow = liveRow?.payload,
                            ),
                        )))
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
                        tombstones[key] = row.rowVersion
                    } else {
                        tombstones.remove(key)
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
                    initializationId = push.initializationId,
                )
                bundles += stored
                committedBySourceBundle[replayKey] = stored
                if (initializingSourceId == sourceId) {
                    scopeInitialized = true
                    initializingSourceId = null
                    initializationId = null
                }
                val response=completePushResponse(stored, selectedGeneration)
                beforePushResponse?.let { hook -> beforePushResponse = null; hook() }
                response
            } catch (t: Throwable) {
                if (t is CancellationException) throw t
                errorResponse("invalid_request", t.message ?: "push failed")
            }

        private fun MockRequestHandleScope.completePushResponse(bundle: StoredBundle,generation: SyncGeneration) = publishOutcome(
            TerminalPushOutcome(bundle.sourceId,generation.id,generation.fingerprint,bundle.sourceBundleId,bundle.canonicalRequestHash,bundle.initializationId,"committed","",
                committed=CommittedPushEffect(bundle.bundleSeq,bundle.bundleHash,bundle.rows.size.toLong(),OversqliteCompactCodec.encodeBundleRows(generation,bundle.rows))),generation)

        private fun wireConflict(local: PushConflictDetails)=local.copy(op=mapOf("INSERT" to "i","UPDATE" to "u","DELETE" to "d").getValue(local.op))

        private suspend fun MockRequestHandleScope.terminalRejection(sourceId: String,push: CompletePushRequest,generation: SyncGeneration,rejection: PushOutcomeRejection): io.ktor.client.request.HttpResponseData {
            val response=publishOutcome(TerminalPushOutcome(sourceId,generation.id,generation.fingerprint,push.sourceBundleId,push.canonicalRequestHash,push.initializationId,"rejected","",rejection=rejection),generation)
            beforePushResponse?.let {beforePushResponse=null;it()}
            return response
        }

        private fun MockRequestHandleScope.publishOutcome(value: TerminalPushOutcome,generation: SyncGeneration): io.ktor.client.request.HttpResponseData {
            originalGenerations[generation.fingerprint]=generation
            val outcome=value.copy(outcomeHash=hashPushOutcome(value,generation))
            val fields=json.encodeToJsonElement(outcome).jsonObject.toMutableMap()
            if(outcome.committed==null)fields.remove("committed")
            if(outcome.rejection==null)fields.remove("rejection")
            else if(outcome.rejection.conflict==null) fields["rejection"]=buildJsonObject{put("code",JsonPrimitive(outcome.rejection.code))}
            val raw=JsonObject(fields).toString()
            val key="${outcome.sourceId}\u0000${outcome.sourceBundleId}"
            terminalOutcomes[key]=outcome;terminalBytes[key]=raw;terminalStatuses[key]=outcome.status
            val override=outcomeResponseOverride;outcomeResponseOverride=null
            return jsonResponse(override?.invoke(raw) ?: raw)
        }

        private suspend fun MockRequestHandleScope.handleResolve(request: HttpRequestData): io.ktor.client.request.HttpResponseData {
            val selected=decodeOutcomeResolve(request.bodyText());resolveRequests+=selected
            val source=request.headers[sourceIdHeaderName].orEmpty()
            beforeResolve?.let { beforeResolve=null;it() }
            refusedSourceResponse(source)?.let { return it }
            if(resolveCapacityUnavailable)return jsonResponse("""{"error":"history_projection_capacity_exceeded","message":"capacity unavailable","retryable":true,"remediation":"retry"}""",HttpStatusCode.ServiceUnavailable)
            latestAck[source]?.let {if(selected.sourceBundleId<=it.second.sourceBundleId)return errorResponse("push_outcome_acknowledged","acked",HttpStatusCode.Gone)}
            val key="$source\u0000${selected.sourceBundleId}"
            terminalOutcomes[key]?.let { existing ->
                if(existing.canonicalRequestHash!=selected.canonicalRequestHash || existing.initializationId!=selected.initializationId)
                    return errorResponse("push_outcome_mismatch","mismatch",HttpStatusCode.Conflict)
                return jsonResponse(checkNotNull(terminalBytes[key]))
            }
            val generation=originalGenerations[request.headers["Oversync-Schema-Generation-Fingerprint"]] ?: generationOverride ?: testGeneration(registeredTableSpecs)
            return publishOutcome(TerminalPushOutcome(source,generation.id,generation.fingerprint,selected.sourceBundleId,selected.canonicalRequestHash,selected.initializationId,"cancelled",""),generation)
        }

        // Sequence-only, read-only lookup of the owning source's retained outcome (r0001 section 3.2.1).
        private suspend fun MockRequestHandleScope.handleRetained(request: HttpRequestData): io.ktor.client.request.HttpResponseData {
            val selected=decodeOutcomeRetained(request.bodyText());retainedRequests+=selected
            val source=request.headers[sourceIdHeaderName].orEmpty()
            val acknowledged=latestAck[source]?.second?.sourceBundleId ?: 0L
            if(selected.sourceBundleId<=acknowledged)return errorResponse("push_outcome_acknowledged","acked",HttpStatusCode.Gone)
            val retained=terminalOutcomes.values.filter { it.sourceId==source }.minByOrNull { it.sourceBundleId }
            if(retained==null)return errorResponse(
                if(selected.sourceBundleId==acknowledged+1)"push_outcome_absent" else "source_sequence_out_of_order","nothing retained",HttpStatusCode.Conflict)
            if(selected.sourceBundleId!=retained.sourceBundleId)return errorResponse("push_outcome_pending","pending",HttpStatusCode.Conflict)
            if(request.headers["Oversync-Schema-Generation-ID"]!=retained.generationId || request.headers["Oversync-Schema-Generation-Fingerprint"]!=retained.generationFingerprint)
                return errorResponse("push_outcome_mismatch","mismatch",HttpStatusCode.Conflict)
            return jsonResponse(checkNotNull(terminalBytes["$source\u0000${retained.sourceBundleId}"]))
        }

        private suspend fun MockRequestHandleScope.handleAck(request: HttpRequestData): io.ktor.client.request.HttpResponseData {
            val selected=decodeOutcomeAck(request.bodyText());ackRequests+=selected
            val source=request.headers[sourceIdHeaderName].orEmpty()
            beforeAck?.let { beforeAck=null;it() }
            refusedSourceResponse(source)?.let { return it }
            val key="$source\u0000${selected.sourceBundleId}"
            val outcome=terminalOutcomes[key]
            if(outcome!=null) {
                require(outcome.canonicalRequestHash==selected.canonicalRequestHash && outcome.outcomeHash==selected.outcomeHash)
                require(request.headers["Oversync-Schema-Generation-ID"]==outcome.generationId && request.headers["Oversync-Schema-Generation-Fingerprint"]==outcome.generationFingerprint)
                latestAck[source]=PushOperationBinding(source,outcome.generationId,outcome.generationFingerprint,outcome.sourceBundleId,outcome.canonicalRequestHash,outcome.initializationId) to selected
                terminalOutcomes.remove(key);terminalBytes.remove(key)
            } else {
                val latest=checkNotNull(latestAck[source]);require(latest.second==selected)
                require(request.headers["Oversync-Schema-Generation-ID"]==latest.first.generationId && request.headers["Oversync-Schema-Generation-Fingerprint"]==latest.first.generationFingerprint)
            }
            afterAck?.let {afterAck=null;it()}
            return respond("",HttpStatusCode.NoContent)
        }

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
            if (createRequest?.recovery?.minimumBundleSeq?.let { it > stableBundleSeq } == true)
                return errorResponse("recovery_floor_ahead", "minimum boundary is ahead", HttpStatusCode.Conflict)
            val effectiveSource = createRequest?.sourceReplacement?.newSourceId ?: request.headers[sourceIdHeaderName].orEmpty()
            snapshotSessions[snapshotId] = SnapshotSessionState(
                snapshotId = snapshotId,
                snapshotBundleSeq = stableBundleSeq,
                sourceId = createRequest?.sourceReplacement?.newSourceId ?: request.headers[sourceIdHeaderName].orEmpty(),
                rows = snapshotRows,
            )
            beforeSnapshotCreateResponse?.let { hook -> beforeSnapshotCreateResponse = null; hook() }
            val sealed=sealedSnapshotForTest(generationOverride ?: testGeneration(registeredTableSpecs),effectiveSource,snapshotId,stableBundleSeq,snapshotRows,createRequest?.recovery)
            val fields=json.encodeToJsonElement(SnapshotSession.serializer(),sealed).jsonObject.toMutableMap()
            if(sealed.recovery==null)fields.remove("recovery")
            jsonResponse(JsonObject(fields).toString())
        } catch (t: Throwable) {
            errorResponse("invalid_request", t.message ?: "snapshot create failed")
        }

        private suspend fun MockRequestHandleScope.handleSnapshotChunk(request: HttpRequestData) : io.ktor.client.request.HttpResponseData {
            return try {
                val snapshotId = request.url.encodedPath.removePrefix("/sync/snapshot-sessions/")
                val session = snapshotSessions[snapshotId]
                    ?: return errorResponse("snapshot_session_not_found", "unknown snapshot", HttpStatusCode.NotFound)
                require(session.sourceId == request.headers[sourceIdHeaderName]) { "snapshot chunks require effective source" }
                val afterRowOrdinal = request.url.parameters["after_row_ordinal"]?.toLongOrNull() ?: 0L
                snapshotChunkRequests += snapshotId to afterRowOrdinal
                if (snapshotChunkTooSmallOnceForTest) {
                    snapshotChunkTooSmallOnceForTest = false
                    val requested = checkNotNull(request.url.parameters["max_bytes"]).toLong()
                    return jsonResponse(json.encodeToString(ErrorResponse.serializer(), ErrorResponse(
                        error = "snapshot_chunk_too_small", message = "synthetic one-row fallback",
                        requiredDecodedBodyBytes = requested + 1L)), HttpStatusCode.BadRequest)
                }
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

        /** The real server refuses every upload operation of a source it retired or cannot sequence. */
        private fun MockRequestHandleScope.refusedSourceResponse(sourceId: String): io.ktor.client.request.HttpResponseData? =
            retiredSources[sourceId]?.let { replacement ->
                sourceRetiredResponse(SourceRetiredResponse("source_retired", "source is retired", sourceId, replacement))
            } ?: sequenceRefusedSources[sourceId]?.let { code ->
                errorResponse(code, "source sequence refused", HttpStatusCode.Conflict)
            }

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
            val initializationId: String? = null,
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
