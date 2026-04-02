package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.oversqlite.platformsupport.canonicalizeJsonElement
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
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
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject

internal open class PlatformCrossTargetTestSupport {
    private companion object {
        const val sourceIdHeaderName = "Oversync-Source-ID"
    }

    protected val json = Json { ignoreUnknownKeys = true }

    protected suspend fun newDb(): SafeSQLiteConnection {
        return BundledSqliteConnectionProvider.openConnection(":memory:", debug = true)
    }

    protected suspend fun newFileBackedDb(path: String): SafeSQLiteConnection {
        return BundledSqliteConnectionProvider.openConnection(
            dbName = path,
            debug = true,
            config = createSqliteNowTestConnectionConfig(path),
        )
    }

    protected fun newClient(
        db: SafeSQLiteConnection,
        http: HttpClient,
        uploadLimit: Int = 200,
        downloadLimit: Int = 1000,
        resolver: Resolver = ServerWinsResolver,
    ): DefaultOversqliteClient {
        return DefaultOversqliteClient(
            db = db,
            config = OversqliteConfig(
                schema = "main",
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = uploadLimit,
                downloadLimit = downloadLimit,
            ),
            http = http,
            resolver = resolver,
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
        private val json = Json { ignoreUnknownKeys = true }
        private val liveRows = linkedMapOf<String, LiveRow>()
        private val bundles = mutableListOf<StoredBundle>()
        private val committedBySourceBundle = linkedMapOf<String, StoredBundle>()
        private val pushSessions = linkedMapOf<String, PushSession>()
        private val snapshotSessions = linkedMapOf<String, SnapshotSessionState>()
        private var scopeInitialized = false
        private var initializingSourceId: String? = null
        private var initializationId: String? = null

        var retainedBundleFloor: Long = 0
        var uploadedChunkCount: Int = 0
        var conflictOverride: ((PushRequestRow, Long, kotlinx.serialization.json.JsonElement?) -> PushConflictDetails?)? = null

        private var nextBundleSeq = 1L
        private var nextPushId = 1L
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

        private suspend fun MockRequestHandleScope.handle(request: HttpRequestData) = when {
            request.method.value == "GET" && request.url.encodedPath == "/sync/capabilities" ->
                jsonResponse(
                    json.encodeToString(
                        CapabilitiesResponse.serializer(),
                        CapabilitiesResponse(
                            protocolVersion = "v1",
                            schemaVersion = 1,
                            features = mapOf("connect_lifecycle" to true),
                        ),
                    ),
                )

            request.method.value == "POST" && request.url.encodedPath == "/sync/connect" ->
                handleConnect(request)

            request.method.value == "POST" && request.url.encodedPath == "/sync/push-sessions" ->
                handleCreatePushSession(request)

            request.method.value == "POST" &&
                request.url.encodedPath.startsWith("/sync/push-sessions/") &&
                request.url.encodedPath.endsWith("/chunks") ->
                handlePushChunk(request)

            request.method.value == "POST" &&
                request.url.encodedPath.startsWith("/sync/push-sessions/") &&
                request.url.encodedPath.endsWith("/commit") ->
                handlePushCommit(request)

            request.method.value == "GET" &&
                request.url.encodedPath.startsWith("/sync/committed-bundles/") &&
                request.url.encodedPath.endsWith("/rows") ->
                handleCommittedBundleRows(request)

            request.method.value == "DELETE" &&
                request.url.encodedPath.startsWith("/sync/push-sessions/") -> {
                val pushId = request.url.encodedPath.removePrefix("/sync/push-sessions/")
                pushSessions.remove(pushId)
                jsonResponse("""{}""")
            }

            request.method.value == "POST" && request.url.encodedPath == "/sync/snapshot-sessions" ->
                handleCreateSnapshotSession()

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

        private suspend fun MockRequestHandleScope.handleConnect(request: HttpRequestData) =
            try {
                val connect = json.decodeFromString(ConnectRequest.serializer(), request.bodyText())
                val sourceId = request.headers[sourceIdHeaderName].orEmpty()
                when {
                    scopeInitialized || bundles.isNotEmpty() -> {
                        scopeInitialized = true
                        jsonResponse(
                            json.encodeToString(
                                ConnectResponse.serializer(),
                                ConnectResponse(resolution = "remote_authoritative"),
                            ),
                        )
                    }

                    initializingSourceId != null -> {
                        if (initializingSourceId == sourceId) {
                            if (connect.hasLocalPendingRows) {
                                jsonResponse(
                                    json.encodeToString(
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
                                    json.encodeToString(
                                        ConnectResponse.serializer(),
                                        ConnectResponse(resolution = "initialize_empty"),
                                    ),
                                )
                            }
                        } else {
                            jsonResponse(
                                json.encodeToString(
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
                        val nextId = "init-${nextInitializationId++}"
                        initializingSourceId = sourceId
                        initializationId = nextId
                        jsonResponse(
                            json.encodeToString(
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
                            json.encodeToString(
                                ConnectResponse.serializer(),
                                ConnectResponse(resolution = "initialize_empty"),
                            ),
                        )
                    }
                }
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "connect failed")
            }

        private suspend fun MockRequestHandleScope.handleCreatePushSession(
            request: HttpRequestData,
        ): io.ktor.client.request.HttpResponseData {
            return try {
                val body = request.bodyText()
                val create = json.decodeFromString(PushSessionCreateRequest.serializer(), body)
                val sourceId = request.headers[sourceIdHeaderName].orEmpty()
                val expectedInitializationId = initializationId
                if (expectedInitializationId != null) {
                    if (create.initializationId.isNullOrBlank()) {
                        return errorResponse("initialization_stale", "initialization_id is required", HttpStatusCode.Conflict)
                    }
                    if (create.initializationId != expectedInitializationId) {
                        return errorResponse("initialization_stale", "initialization_id does not match active lease", HttpStatusCode.Conflict)
                    }
                } else if (!create.initializationId.isNullOrBlank()) {
                    return errorResponse("initialization_stale", "scope is no longer initializing", HttpStatusCode.Conflict)
                }
                val existing = committedBySourceBundle["$sourceId\u0000${create.sourceBundleId}"]
                if (existing != null) {
                    jsonResponse(
                        json.encodeToString(
                            PushSessionCreateResponse.serializer(),
                            PushSessionCreateResponse(
                                status = "already_committed",
                                bundleSeq = existing.bundleSeq,
                                sourceId = existing.sourceId,
                                sourceBundleId = existing.sourceBundleId,
                                rowCount = existing.rows.size.toLong(),
                                bundleHash = existing.bundleHash,
                            ),
                        ),
                    )
                } else {
                    val pushId = "push-${nextPushId++}"
                    pushSessions[pushId] = PushSession(
                        pushId = pushId,
                        sourceId = sourceId,
                        sourceBundleId = create.sourceBundleId,
                        plannedRowCount = create.plannedRowCount,
                    )
                    jsonResponse(
                        json.encodeToString(
                            PushSessionCreateResponse.serializer(),
                            PushSessionCreateResponse(
                                pushId = pushId,
                                status = "staging",
                                plannedRowCount = create.plannedRowCount,
                                nextExpectedRowOrdinal = 0,
                            ),
                        ),
                    )
                }
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "push create failed")
            }
        }

        private suspend fun MockRequestHandleScope.handlePushChunk(request: HttpRequestData) : io.ktor.client.request.HttpResponseData {
            return try {
                val pushId = request.url.encodedPath
                    .removePrefix("/sync/push-sessions/")
                    .substringBefore("/chunks")
                val session = pushSessions[pushId]
                    ?: return errorResponse("push_session_not_found", "unknown push session", HttpStatusCode.NotFound)
                val chunk = json.decodeFromString(PushSessionChunkRequest.serializer(), request.bodyText())
                if (chunk.startRowOrdinal != session.rows.size.toLong()) {
                    return errorResponse("push_chunk_out_of_order", "expected ordinal ${session.rows.size}", HttpStatusCode.Conflict)
                }
                session.rows += chunk.rows
                uploadedChunkCount++
                jsonResponse(
                    json.encodeToString(
                        PushSessionChunkResponse.serializer(),
                        PushSessionChunkResponse(pushId = pushId, nextExpectedRowOrdinal = session.rows.size.toLong()),
                    ),
                )
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "push chunk failed")
            }
        }

        private suspend fun MockRequestHandleScope.handlePushCommit(request: HttpRequestData) : io.ktor.client.request.HttpResponseData {
            return try {
                val pushId = request.url.encodedPath
                    .removePrefix("/sync/push-sessions/")
                    .substringBefore("/commit")
                val session = pushSessions[pushId]
                    ?: return errorResponse("push_session_not_found", "unknown push session", HttpStatusCode.NotFound)

                val existing = committedBySourceBundle["${session.sourceId}\u0000${session.sourceBundleId}"]
                if (existing != null) {
                    return jsonResponse(
                        json.encodeToString(
                            PushSessionCommitResponse.serializer(),
                            PushSessionCommitResponse(
                                bundleSeq = existing.bundleSeq,
                                sourceId = existing.sourceId,
                                sourceBundleId = existing.sourceBundleId,
                                rowCount = existing.rows.size.toLong(),
                                bundleHash = existing.bundleHash,
                            ),
                        ),
                    )
                }

                if (session.rows.size.toLong() != session.plannedRowCount) {
                    return errorResponse("push_session_incomplete", "expected ${session.plannedRowCount} rows", HttpStatusCode.Conflict)
                }

                for (row in session.rows) {
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
                val committedRows = session.rows.map { row ->
                    BundleRow(
                        schema = row.schema,
                        table = row.table,
                        key = row.key,
                        op = row.op,
                        rowVersion = bundleSeq,
                        payload = row.payload,
                    )
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
                    sourceId = session.sourceId,
                    sourceBundleId = session.sourceBundleId,
                    rows = committedRows,
                    bundleHash = computeCommittedBundleHash(committedRows),
                )
                bundles += stored
                committedBySourceBundle["${stored.sourceId}\u0000${stored.sourceBundleId}"] = stored
                if (initializingSourceId == session.sourceId) {
                    scopeInitialized = true
                    initializingSourceId = null
                    initializationId = null
                }
                jsonResponse(
                    json.encodeToString(
                        PushSessionCommitResponse.serializer(),
                        PushSessionCommitResponse(
                            bundleSeq = stored.bundleSeq,
                            sourceId = stored.sourceId,
                            sourceBundleId = stored.sourceBundleId,
                            rowCount = stored.rows.size.toLong(),
                            bundleHash = stored.bundleHash,
                        ),
                    ),
                )
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "push commit failed")
            }
        }

        private suspend fun MockRequestHandleScope.handleCommittedBundleRows(request: HttpRequestData) : io.ktor.client.request.HttpResponseData {
            return try {
                val bundleSeq = request.url.encodedPath
                    .removePrefix("/sync/committed-bundles/")
                    .substringBefore("/rows")
                    .toLong()
                val bundle = bundles.firstOrNull { it.bundleSeq == bundleSeq }
                    ?: return errorResponse("bundle_not_found", "unknown bundle", HttpStatusCode.NotFound)
                val afterRowOrdinal = request.url.parameters["after_row_ordinal"]?.toLongOrNull()
                val maxRows = request.url.parameters["max_rows"]?.toIntOrNull() ?: 1000
                val startIndex = if (afterRowOrdinal == null) 0 else (afterRowOrdinal + 1).toInt()
                val rows = bundle.rows.drop(startIndex).take(maxRows)
                val logicalAfter = afterRowOrdinal ?: -1L
                val nextRowOrdinal = if (rows.isEmpty()) logicalAfter else logicalAfter + rows.size
                jsonResponse(
                    json.encodeToString(
                        CommittedBundleRowsResponse.serializer(),
                        CommittedBundleRowsResponse(
                            bundleSeq = bundle.bundleSeq,
                            sourceId = bundle.sourceId,
                            sourceBundleId = bundle.sourceBundleId,
                            rowCount = bundle.rows.size.toLong(),
                            bundleHash = bundle.bundleHash,
                            rows = rows,
                            nextRowOrdinal = nextRowOrdinal,
                            hasMore = startIndex + rows.size < bundle.rows.size,
                        ),
                    ),
                )
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "bundle fetch failed")
            }
        }

        private suspend fun MockRequestHandleScope.handleCreateSnapshotSession() = try {
            val snapshotRows = currentSnapshotRows()
            val snapshotId = "snapshot-${nextSnapshotId++}"
            val stableBundleSeq = bundles.lastOrNull()?.bundleSeq ?: if (scopeInitialized) 0L else 0L
            snapshotSessions[snapshotId] = SnapshotSessionState(
                snapshotId = snapshotId,
                snapshotBundleSeq = stableBundleSeq,
                rows = snapshotRows,
            )
            jsonResponse(
                json.encodeToString(
                    SnapshotSession.serializer(),
                    SnapshotSession(
                        snapshotId = snapshotId,
                        snapshotBundleSeq = stableBundleSeq,
                        rowCount = snapshotRows.size.toLong(),
                        byteCount = 0,
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
                val afterRowOrdinal = request.url.parameters["after_row_ordinal"]?.toLongOrNull() ?: 0L
                val maxRows = request.url.parameters["max_rows"]?.toIntOrNull() ?: 1000
                val startIndex = afterRowOrdinal.toInt()
                val rows = session.rows.drop(startIndex).take(maxRows)
                jsonResponse(
                    json.encodeToString(
                        SnapshotChunkResponse.serializer(),
                        SnapshotChunkResponse(
                            snapshotId = snapshotId,
                            snapshotBundleSeq = session.snapshotBundleSeq,
                            rows = rows,
                            nextRowOrdinal = afterRowOrdinal + rows.size,
                            hasMore = startIndex + rows.size < session.rows.size,
                        ),
                    ),
                )
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "snapshot fetch failed")
            }
        }

        private suspend fun MockRequestHandleScope.handlePull(request: HttpRequestData) : io.ktor.client.request.HttpResponseData {
            return try {
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
                val selected = bundles
                    .filter { it.bundleSeq > afterBundleSeq && it.bundleSeq <= target }
                    .take(maxBundles)
                    .map { bundle ->
                        Bundle(
                            bundleSeq = bundle.bundleSeq,
                            sourceId = bundle.sourceId,
                            sourceBundleId = bundle.sourceBundleId,
                            rows = bundle.rows,
                        )
                    }
                val lastReturned = selected.lastOrNull()?.bundleSeq ?: afterBundleSeq
                jsonResponse(
                    json.encodeToString(
                        PullResponse.serializer(),
                        PullResponse(
                            stableBundleSeq = target,
                            bundles = selected,
                            hasMore = lastReturned < target,
                        ),
                    ),
                )
            } catch (t: Throwable) {
                errorResponse("invalid_request", t.message ?: "pull failed")
            }
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

        private fun computeCommittedBundleHash(rows: List<BundleRow>): String {
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

        private data class LiveRow(
            val table: String,
            val key: SyncKey,
            val rowVersion: Long,
            val payload: kotlinx.serialization.json.JsonElement,
        )

        private data class PushSession(
            val pushId: String,
            val sourceId: String,
            val sourceBundleId: Long,
            val plannedRowCount: Long,
            val rows: MutableList<PushRequestRow> = mutableListOf(),
        )

        private data class StoredBundle(
            val bundleSeq: Long,
            val sourceId: String,
            val sourceBundleId: Long,
            val rows: List<BundleRow>,
            val bundleHash: String,
        )

        private data class SnapshotSessionState(
            val snapshotId: String,
            val snapshotBundleSeq: Long,
            val rows: List<SnapshotRow>,
        )
    }
}
