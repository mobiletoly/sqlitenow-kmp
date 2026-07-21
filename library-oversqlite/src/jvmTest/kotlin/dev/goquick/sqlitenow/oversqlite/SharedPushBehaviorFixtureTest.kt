package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpServer
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SharedPushBehaviorFixtureTest : BundleClientContractTestSupport() {
    private val fixtureFiles = listOf(
        oversqliteContractFixture("push/behavior/recovery.json"),
        oversqliteContractFixture("push/behavior/apply.json"),
    )

    @Test
    fun kmpSharedPushBehaviorFixturesExecuteAgainstRuntime() = runBlocking {
        for (fixtureFile in fixtureFiles) {
            val spec = json.decodeFromString(PushBehaviorSpec.serializer(), fixtureFile.readText())
            assertEquals(1, spec.formatVersion)
            for (case in spec.cases) {
                runCase(case)
            }
        }
    }

    private suspend fun runCase(case: PushBehaviorCase) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createSchema(db, case.schema)
        val syncTables = case.syncTables.map { SyncTable(it.tableName, syncKeyColumnName = it.syncKeyColumnName) }
        val server = newServer(registeredTableSpecsFor(syncTables))
        val pushServer = configureServer(case, server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = syncTables,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
                resolver = resolverFor(case.resolver),
            )
            client.openAndConnect("user-1").getOrThrow()
            executeSetupSql(db, case.localSetupSql)
            executeApplyModeSql(db, case.localApplyModeSql)
            for (step in case.steps) {
                val error = when (step.action) {
                    "pushPending" -> client.pushPending().exceptionOrNull()
                    "localSql" -> {
                        executeSetupSql(db, step.sql)
                        null
                    }
                    "applyModeSql" -> {
                        executeApplyModeSql(db, step.sql)
                        null
                    }
                    else -> error("${case.name}: unknown action ${step.action}")
                }
                assertExpectedException(case.name, step.expectedException, error)
                step.expectedState?.let { assertState(case.name, db, it) }
                step.expectedAppState?.let { assertAppState(case.name, db, it) }
                step.expectedServerState?.let { expected ->
                    expected.createRequestCount?.let {
                        assertEquals(it, pushServer?.createRequests?.size, "${case.name}: create request count")
                    }
                    expected.uploadedRows?.let {
                        assertEquals(it, pushServer?.uploadedChunks?.lastOrNull()?.rows?.map(::uploadedRowSummary), "${case.name}: uploaded rows")
                    }
                }
            }
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    private fun configureServer(case: PushBehaviorCase, server: HttpServer): FakeChunkedSyncServer? {
        return when (case.serverScript.kind) {
            "echo_committed_rows" -> {
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    install(server)
                }
            }
            "committed_bundle_seq_gap" -> {
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    nextBundleSeq = case.serverScript.bundleSeq ?: error("${case.name}: missing bundleSeq")
                    install(server)
                }
            }
            "committed_replay_first_fetch_http_error" -> {
                var failed = false
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    committedBundleChunkError = { _, _ ->
                        if (!failed) {
                            failed = true
                            500 to """{"error":"temporary","message":"retry"}"""
                        } else {
                            null
                        }
                    }
                    install(server)
                }
            }
            "already_committed_request_hash_mismatch" -> {
                server.installAlreadyCommittedRequestHashMismatchRoutes(case)
                null
            }
            "committed_remote_request_hash_mismatch" -> {
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    committedRequestHashTransform = { "b".repeat(64) }
                    install(server)
                }
            }
            "committed_replay_pruned" -> {
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    committedBundleChunkError = { _, _ ->
                        409 to """{"error":"history_pruned","message":"committed replay is below retained floor"}"""
                    }
                    install(server)
                }
            }
            "committed_replay_bad_bundle_hash" -> {
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    bundleHashTransform = { _, _ -> "bad-hash" }
                    install(server)
                }
            }
            "committed_bundle_not_found_then_success" -> {
                var remaining = case.serverScript.notFoundCount
                    ?: error("${case.name}: missing notFoundCount")
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    committedBundleChunkError = { _, afterRowOrdinal ->
                        if (afterRowOrdinal == null && remaining > 0) {
                            remaining--
                            404 to """{"error":"committed_bundle_not_found","message":"committed bundle not visible yet"}"""
                        } else {
                            null
                        }
                    }
                    install(server)
                }
            }
            "precommit_retry" -> {
                var failed = false
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    commitError = { _, _ ->
                        if (!failed) {
                            failed = true
                            500 to """{"error":"temporary","message":"retry"}"""
                        } else {
                            null
                        }
                    }
                    install(server)
                }
            }
            "conflict_once" -> {
                var failed = false
                val conflict = case.serverScript.conflict ?: error("${case.name}: missing conflict")
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    commitError = { _, _ ->
                        if (!failed) {
                            failed = true
                            409 to """{"error":"push_conflict","message":"fixture conflict","conflict":$conflict}"""
                        } else {
                            null
                        }
                    }
                    install(server)
                }
            }
            "conflict_once_on_row_count" -> {
                var failed = false
                val expectedRowCount = case.serverScript.rowCount ?: error("${case.name}: missing rowCount")
                val conflict = case.serverScript.conflict ?: error("${case.name}: missing conflict")
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    commitError = { _, rowCount ->
                        if (!failed && rowCount == expectedRowCount) {
                            failed = true
                            409 to """{"error":"push_conflict","message":"fixture conflict","conflict":$conflict}"""
                        } else {
                            null
                        }
                    }
                    install(server)
                }
            }
            "already_committed_rows" -> {
                server.installAlreadyCommittedRowsRoutes(case)
                null
            }
            else -> error("${case.name}: unknown server script ${case.serverScript.kind}")
        }
    }

    private suspend fun createSchema(db: SafeSQLiteConnection, schema: String) {
        when (schema) {
            "users" -> createUsersTable(db)
            "users-posts" -> createUsersAndPostsTables(db)
            "immediate-authors-profiles-cycle" -> createImmediateAuthorsAndProfilesCycleTables(db)
            "typed-rows" -> createTypedRowsTable(db)
            else -> error("unknown behavior fixture schema $schema")
        }
    }

    private fun resolverFor(name: String): Resolver {
        return when (name) {
            "server_wins" -> ServerWinsResolver
            "client_wins" -> ClientWinsResolver
            "keep_local" -> Resolver { MergeResult.KeepLocal }
            "keep_merged_user" -> Resolver {
                MergeResult.KeepMerged(
                    buildJsonObject {
                        put("id", JsonPrimitive("user-1"))
                        put("name", JsonPrimitive("Merged"))
                    },
                )
            }
            else -> error("unknown resolver $name")
        }
    }

    private fun uploadedRowSummary(row: PushRequestRow): String {
        val key = row.key["id"] ?: row.key.values.firstOrNull().orEmpty()
        return "${row.table}:${row.op}:$key"
    }

    private fun HttpServer.installAlreadyCommittedRequestHashMismatchRoutes(case: PushBehaviorCase) {
        var sourceId = ""
        val payload = case.serverScript.committedPayload ?: error("${case.name}: missing committedPayload")
        val committedRows = listOf(
            BundleRow(
                schema = "main",
                table = "users",
                key = mapOf("id" to "user-1"),
                op = "INSERT",
                rowVersion = 1,
                payload = payload,
            ),
        )
        val committedHash = computeCommittedBundleHashForTest(committedRows)
        createContext("/sync/push-sessions") { exchange ->
            sourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
            respondJson(
                exchange,
                200,
                json.encodeToString(
                    PushSessionCreateResponse.serializer(),
                    PushSessionCreateResponse(
                        status = "already_committed",
                        pushId = "",
                        nextExpectedRowOrdinal = 0,
                        bundleSeq = 1,
                        sourceId = sourceId,
                        sourceBundleId = 1,
                        rowCount = 1,
                        bundleHash = committedHash,
                        canonicalRequestHash = "b".repeat(64),
                    ),
                ),
            )
        }
        createContext("/sync/committed-bundles/1/rows") { exchange ->
            respondJson(
                exchange,
                200,
                json.encodeToString(
                    CommittedBundleRowsResponse.serializer(),
                    CommittedBundleRowsResponse(
                        bundleSeq = 1,
                        sourceId = sourceId,
                        sourceBundleId = 1,
                        rowCount = 1,
                        bundleHash = committedHash,
                        canonicalRequestHash = "b".repeat(64),
                        rows = committedRows,
                        nextRowOrdinal = 0,
                        hasMore = false,
                    ),
                ),
            )
        }
    }

    private fun HttpServer.installAlreadyCommittedRowsRoutes(case: PushBehaviorCase) {
        var sourceId = ""
        var sourceBundleId = 1L
        var canonicalRequestHash = ""
        val committedRows = case.serverScript.committedRows
        require(committedRows.isNotEmpty()) { "${case.name}: missing committedRows" }
        val committedHash = computeCommittedBundleHashForTest(committedRows)
        createContext("/sync/push-sessions") { exchange ->
            val request = json.decodeFromString(
                PushSessionCreateRequest.serializer(),
                exchange.requestBody.readBytes().decodeToString(),
            )
            sourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
            sourceBundleId = request.sourceBundleId
            canonicalRequestHash = request.canonicalRequestHash
            respondJson(
                exchange,
                200,
                json.encodeToString(
                    PushSessionCreateResponse.serializer(),
                    PushSessionCreateResponse(
                        status = "already_committed",
                        pushId = "",
                        nextExpectedRowOrdinal = 0,
                        bundleSeq = 1,
                        sourceId = sourceId,
                        sourceBundleId = sourceBundleId,
                        rowCount = committedRows.size.toLong(),
                        bundleHash = committedHash,
                        canonicalRequestHash = canonicalRequestHash,
                    ),
                ),
            )
        }
        createContext("/sync/committed-bundles/1/rows") { exchange ->
            respondJson(
                exchange,
                200,
                json.encodeToString(
                    CommittedBundleRowsResponse.serializer(),
                    CommittedBundleRowsResponse(
                        bundleSeq = 1,
                        sourceId = sourceId,
                        sourceBundleId = sourceBundleId,
                        rowCount = committedRows.size.toLong(),
                        bundleHash = committedHash,
                        canonicalRequestHash = canonicalRequestHash,
                        rows = committedRows,
                        nextRowOrdinal = committedRows.size.toLong() - 1L,
                        hasMore = false,
                    ),
                ),
            )
        }
    }

    private suspend fun assertAppState(
        caseName: String,
        db: SafeSQLiteConnection,
        expected: ExpectedAppState,
    ) {
        expected.users?.let {
            val rows = mutableListOf<UserRow>()
            db.prepare("SELECT id, name FROM users ORDER BY id").use { st ->
                while (st.step()) rows += UserRow(st.getText(0), st.getText(1))
            }
            assertEquals(it, rows, "$caseName: users")
        }
        expected.posts?.let {
            val rows = mutableListOf<PostRow>()
            db.prepare("SELECT id, user_id, title FROM posts ORDER BY id").use { st ->
                while (st.step()) rows += PostRow(st.getText(0), st.getText(1), st.getText(2))
            }
            assertEquals(it, rows, "$caseName: posts")
        }
        expected.authors?.let {
            val rows = mutableListOf<AuthorRow>()
            db.prepare("SELECT id, profile_id, name FROM authors ORDER BY id").use { st ->
                while (st.step()) rows += AuthorRow(st.getText(0), st.getText(1), st.getText(2))
            }
            assertEquals(it, rows, "$caseName: authors")
        }
        expected.profiles?.let {
            val rows = mutableListOf<ProfileRow>()
            db.prepare("SELECT id, author_id, bio FROM profiles ORDER BY id").use { st ->
                while (st.step()) rows += ProfileRow(st.getText(0), st.getText(1), st.getText(2))
            }
            assertEquals(it, rows, "$caseName: profiles")
        }
        expected.typedRows?.let {
            val rows = mutableListOf<JsonObject>()
            db.prepare(
                """
                SELECT id, name, note, count_value, enabled_flag, rating, data, created_at
                FROM typed_rows
                ORDER BY id
                """.trimIndent(),
            ).use { st ->
                while (st.step()) {
                    rows += buildJsonObject {
                        put("id", JsonPrimitive(st.getText(0)))
                        put("name", JsonPrimitive(st.getText(1)))
                        put("note", if (st.isNull(2)) JsonNull else JsonPrimitive(st.getText(2)))
                        put("count_value", if (st.isNull(3)) JsonNull else JsonPrimitive(st.getLong(3)))
                        put("enabled_flag", JsonPrimitive(st.getLong(4)))
                        put("rating", if (st.isNull(5)) JsonNull else JsonPrimitive(st.getDouble(5)))
                        put("data", if (st.isNull(6)) JsonNull else JsonPrimitive(bytesToHexLower(st.getBlob(6))))
                        put("created_at", if (st.isNull(7)) JsonNull else JsonPrimitive(st.getText(7)))
                    }
                }
            }
            assertEquals(it, rows, "$caseName: typed_rows")
        }
    }

    private suspend fun assertState(
        caseName: String,
        db: SafeSQLiteConnection,
        expected: ExpectedPushState,
    ) {
        expected.rebuildRequired?.let {
            assertEquals(
                if (it) 1L else 0L,
                scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"),
                "$caseName: rebuild_required",
            )
        }
        expected.outboxState?.let {
            assertEquals(
                it,
                scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"),
                "$caseName: outbox state",
            )
        }
        expected.outboxRowCount?.let {
            assertEquals(it.toLong(), scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"), "$caseName: outbox rows")
        }
        expected.dirtyRowCount?.let {
            assertEquals(it.toLong(), scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"), "$caseName: dirty rows")
        }
        expected.nextSourceBundleId?.let {
            assertEquals(
                it,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
                "$caseName: next source bundle id",
            )
        }
        expected.lastBundleSeqSeen?.let {
            assertEquals(
                it,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1"),
                "$caseName: last bundle seq seen",
            )
        }
        expected.applyMode?.let {
            assertEquals(
                it,
                scalarLong(db, "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1"),
                "$caseName: apply mode",
            )
        }
    }

    private fun assertExpectedException(caseName: String, expected: String, error: Throwable?) {
        when (expected) {
            "none" -> assertNull(error, "$caseName: expected success")
            "source_sequence_mismatch" -> assertIs<SourceSequenceMismatchException>(error, "$caseName: expected source sequence mismatch")
            "rebuild_required" -> assertIs<RebuildRequiredException>(error, "$caseName: expected rebuild required")
            "protocol_error" -> {
                assertNotNull(error, "$caseName: expected protocol error")
                assertTrue(
                    error !is SourceSequenceMismatchException && error !is RebuildRequiredException,
                    "$caseName: expected protocol error, got ${error::class.simpleName}",
                )
                val category = (error as? OversqliteCategorizedException)?.category
                assertTrue(
                    category in setOf(OversqliteErrorCategory.PROTOCOL, OversqliteErrorCategory.CONFLICT) ||
                        (category == null && error is IllegalArgumentException),
                    "$caseName: expected categorized protocol/conflict error, got ${error::class.simpleName}",
                )
            }
            "http_error" -> {
                assertNotNull(error, "$caseName: expected http error")
                assertTrue(
                    error !is SourceSequenceMismatchException && error !is RebuildRequiredException,
                    "$caseName: expected http error, got ${error::class.simpleName}",
                )
            }
            else -> error("$caseName: unknown expected exception $expected")
        }
    }

    @Serializable
    private data class PushBehaviorSpec(
        val formatVersion: Int,
        val cases: List<PushBehaviorCase>,
    )

    @Serializable
    private data class PushBehaviorCase(
        val name: String,
        val description: String,
        val schema: String = "users",
        val resolver: String = "server_wins",
        val syncTables: List<FixtureSyncTable> = listOf(FixtureSyncTable("users", "id")),
        val localSetupSql: List<String> = emptyList(),
        val localApplyModeSql: List<String> = emptyList(),
        val serverScript: PushServerScript,
        val steps: List<PushStep>,
    )

    @Serializable
    private data class FixtureSyncTable(
        val tableName: String,
        val syncKeyColumnName: String,
    )

    @Serializable
    private data class PushServerScript(
        val kind: String,
        val bundleSeq: Long? = null,
        val notFoundCount: Int? = null,
        val rowCount: Int? = null,
        val conflict: JsonObject? = null,
        val committedPayload: JsonElement? = null,
        val committedRows: List<BundleRow> = emptyList(),
        val commitResponse: JsonObject? = null,
        val committedRowsResponse: JsonObject? = null,
    )

    @Serializable
    private data class PushStep(
        val action: String,
        val sql: List<String> = emptyList(),
        val expectedException: String,
        val expectedState: ExpectedPushState? = null,
        val expectedAppState: ExpectedAppState? = null,
        val expectedServerState: ExpectedPushServerState? = null,
    )

    @Serializable
    private data class ExpectedPushState(
        val rebuildRequired: Boolean? = null,
        val outboxState: String? = null,
        val outboxRowCount: Int? = null,
        val dirtyRowCount: Int? = null,
        val nextSourceBundleId: Long? = null,
        val lastBundleSeqSeen: Long? = null,
        val applyMode: Long? = null,
    )

    @Serializable
    private data class ExpectedPushServerState(
        val createRequestCount: Int? = null,
        val uploadedRows: List<String>? = null,
    )

    @Serializable
    private data class ExpectedAppState(
        val users: List<UserRow>? = null,
        val posts: List<PostRow>? = null,
        val authors: List<AuthorRow>? = null,
        val profiles: List<ProfileRow>? = null,
        val typedRows: List<JsonObject>? = null,
    )

    @Serializable
    private data class UserRow(
        val id: String,
        val name: String,
    )

    @Serializable
    private data class PostRow(
        val id: String,
        val user_id: String,
        val title: String,
    )

    @Serializable
    private data class AuthorRow(
        val id: String,
        val profile_id: String,
        val name: String,
    )

    @Serializable
    private data class ProfileRow(
        val id: String,
        val author_id: String,
        val bio: String,
    )
}
