package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpServer
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SharedPushBehaviorFixtureTest : BundleClientContractTestSupport() {
    private val fixtureFile = oversqliteContractFixture("push/behavior/recovery.json")

    @Test
    fun kmpSharedPushBehaviorFixturesExecuteAgainstRuntime() = runBlocking {
        val spec = json.decodeFromString(PushBehaviorSpec.serializer(), fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            runCase(case)
        }
    }

    private suspend fun runCase(case: PushBehaviorCase) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = configureServer(case, server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            client.openAndConnect("user-1").getOrThrow()
            for (sql in case.localSetupSql) {
                db.execSQL(sql)
            }
            for (step in case.steps) {
                val error = when (step.action) {
                    "pushPending" -> client.pushPending().exceptionOrNull()
                    else -> error("${case.name}: unknown action ${step.action}")
                }
                assertExpectedException(case.name, step.expectedException, error)
                step.expectedState?.let { assertState(case.name, db, it) }
                step.expectedServerState?.let { expected ->
                    expected.createRequestCount?.let {
                        assertEquals(it, pushServer?.createRequests?.size, "${case.name}: create request count")
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
            "already_committed_mismatch" -> {
                server.installAlreadyCommittedMismatchRoutes(case)
                null
            }
            "committed_remote_mismatch" -> {
                FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                    committedRowsTransform = { rows ->
                        rows.reversed()
                    }
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
            else -> error("${case.name}: unknown server script ${case.serverScript.kind}")
        }
    }

    private fun HttpServer.installAlreadyCommittedMismatchRoutes(case: PushBehaviorCase) {
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
                        rows = committedRows,
                        nextRowOrdinal = 0,
                        hasMore = false,
                    ),
                ),
            )
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
    }

    private fun assertExpectedException(caseName: String, expected: String, error: Throwable?) {
        when (expected) {
            "none" -> assertNull(error, "$caseName: expected success")
            "source_sequence_mismatch" -> assertIs<SourceSequenceMismatchException>(error, "$caseName: expected source sequence mismatch")
            "rebuild_required" -> assertIs<RebuildRequiredException>(error, "$caseName: expected rebuild required")
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
        val localSetupSql: List<String> = emptyList(),
        val serverScript: PushServerScript,
        val steps: List<PushStep>,
    )

    @Serializable
    private data class PushServerScript(
        val kind: String,
        val committedPayload: JsonElement? = null,
        val commitResponse: JsonObject? = null,
        val committedRowsResponse: JsonObject? = null,
    )

    @Serializable
    private data class PushStep(
        val action: String,
        val expectedException: String,
        val expectedState: ExpectedPushState? = null,
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
    )

    @Serializable
    private data class ExpectedPushServerState(
        val createRequestCount: Int? = null,
    )
}
