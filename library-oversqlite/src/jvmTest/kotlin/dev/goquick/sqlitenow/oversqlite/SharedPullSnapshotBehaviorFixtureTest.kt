package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpServer
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SharedPullSnapshotBehaviorFixtureTest : BundleClientContractTestSupport() {
    private val fixtureFile = oversqliteContractFixture("pull-snapshot/behavior/apply.json")

    @Test
    fun kmpSharedPullSnapshotBehaviorFixturesExecuteAgainstRuntime() = runBlocking {
        val spec = json.decodeFromString(PullBehaviorSpec.serializer(), fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            runCase(case)
        }
    }

    private suspend fun runCase(case: PullBehaviorCase) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createSchema(db, case.schema)
        val server = newServer()
        configureServer(case, server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = case.syncTables.map { SyncTable(it.tableName, syncKeyColumnName = it.syncKeyColumnName) },
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            client.openAndConnect("user-1").getOrThrow()
            for (step in case.steps) {
                executeSetupSql(db, step.setupSql)
                val sourceBeforeStep = scalarText(
                    db,
                    "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1",
                )
                val error = when (step.action) {
                    "pullToStable" -> client.pullToStable().exceptionOrNull()
                    "rebuild" -> client.rebuild().exceptionOrNull()
                    else -> error("${case.name}: unknown action ${step.action}")
                }
                assertExpectedException(case.name, step.expectedException, step.expectedErrorContains, error)
                step.expectedState?.let { assertState(case.name, db, it, sourceBeforeStep) }
                step.expectedAppState?.let { assertAppState(case.name, db, it) }
            }
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    private fun configureServer(case: PullBehaviorCase, server: HttpServer) {
        when (case.serverScript.kind) {
            "pull_incremental_bundles" -> {
                val response = case.serverScript.response ?: error("${case.name}: missing pull response")
                server.createContext("/sync/pull") { exchange ->
                    respondJson(exchange, 200, response.toString())
                }
            }
            "pull_sequence" -> {
                val responses = case.serverScript.responses.ifEmpty {
                    error("${case.name}: missing pull responses")
                }
                var index = 0
                server.createContext("/sync/pull") { exchange ->
                    val response = responses.getOrNull(index)
                        ?: error("${case.name}: exhausted pull responses at request ${index + 1}")
                    index++
                    respondJson(exchange, 200, response.toString())
                }
            }
            "snapshot_sequence" -> server.installSnapshotSequenceRoutes(case)
            else -> error("${case.name}: unknown server script ${case.serverScript.kind}")
        }
    }

    private fun HttpServer.installSnapshotSequenceRoutes(case: PullBehaviorCase) {
        val sessions = case.serverScript.sessions.ifEmpty {
            error("${case.name}: missing snapshot sessions")
        }
        val chunks = case.serverScript.chunks ?: error("${case.name}: missing snapshot chunks")
        var sessionIndex = 0
        createContext("/sync/snapshot-sessions") { exchange ->
            if (exchange.requestMethod != "POST") {
                exchange.sendResponseHeaders(405, -1)
                exchange.close()
                return@createContext
            }
            val session = sessions.getOrNull(sessionIndex)
                ?: error("${case.name}: exhausted snapshot sessions at request ${sessionIndex + 1}")
            sessionIndex++
            respondJson(exchange, 200, session.toString())
        }
        for ((snapshotId, chunkEntries) in chunks) {
            val chunksByOrdinal = chunkEntries.jsonObject
            createContext("/sync/snapshot-sessions/$snapshotId") { exchange ->
                when (exchange.requestMethod) {
                    "GET" -> {
                        val afterRowOrdinal = queryParam(exchange, "after_row_ordinal")
                        val response = chunksByOrdinal[afterRowOrdinal]
                            ?: error("${case.name}: missing $snapshotId chunk after_row_ordinal=$afterRowOrdinal")
                        respondFixtureJson(exchange, response)
                    }
                    "DELETE" -> {
                        exchange.sendResponseHeaders(204, -1)
                        exchange.close()
                    }
                    else -> {
                        exchange.sendResponseHeaders(405, -1)
                        exchange.close()
                    }
                }
            }
        }
    }

    private fun respondFixtureJson(exchange: com.sun.net.httpserver.HttpExchange, element: JsonElement) {
        val responseObject = element.jsonObject
        val status = responseObject["status"]?.jsonPrimitive?.int ?: 200
        val body = responseObject["body"] ?: element
        respondJson(exchange, status, body.toString())
    }

    private suspend fun createSchema(db: SafeSQLiteConnection, schema: String) {
        when (schema) {
            "users" -> createUsersTable(db)
            "immediate-authors-profiles-cycle" -> createImmediateAuthorsAndProfilesCycleTables(db)
            else -> error("unknown behavior fixture schema $schema")
        }
    }

    private suspend fun assertState(
        caseName: String,
        db: SafeSQLiteConnection,
        expected: ExpectedPullState,
        sourceBeforeStep: String,
    ) {
        expected.lastBundleSeqSeen?.let {
            assertEquals(
                it,
                scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state WHERE singleton_key = 1"),
                "$caseName: last bundle seq seen",
            )
        }
        expected.dirtyRowCount?.let {
            assertEquals(it.toLong(), scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"), "$caseName: dirty rows")
        }
        expected.snapshotStageCount?.let {
            assertEquals(it.toLong(), scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"), "$caseName: snapshot stage")
        }
        expected.rowStateCount?.let {
            assertEquals(it.toLong(), scalarLong(db, "SELECT COUNT(*) FROM _sync_row_state"), "$caseName: row state")
        }
        expected.outboxRowCount?.let {
            assertEquals(it.toLong(), scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"), "$caseName: outbox rows")
        }
        expected.rebuildRequired?.let {
            assertEquals(
                it.toLong(),
                scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"),
                "$caseName: rebuild required",
            )
        }
        expected.currentSourceChangedFromStepStart?.let { changed ->
            val currentSource = scalarText(
                db,
                "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1",
            )
            assertEquals(changed, currentSource != sourceBeforeStep, "$caseName: source changed")
        }
        expected.oldSourceReplacedByCurrent?.let { shouldReplace ->
            val currentSource = scalarText(
                db,
                "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1",
            )
            val replacedBy = scalarText(
                db,
                "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$sourceBeforeStep'",
            )
            assertEquals(
                shouldReplace,
                replacedBy == currentSource && currentSource != sourceBeforeStep,
                "$caseName: old source replacement",
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
    }

    private fun assertExpectedException(
        caseName: String,
        expected: String,
        expectedMessage: String?,
        error: Throwable?,
    ) {
        when (expected) {
            "none" -> assertNull(error, "$caseName: expected success")
            "any_error" -> {
                val actual = assertNotNull(error, "$caseName: expected error")
                expectedMessage?.let {
                    assertTrue(
                        actual.message?.contains(it) == true,
                        "$caseName: expected error containing '$it' but was '${actual.message}'",
                    )
                }
            }
            else -> error("$caseName: unknown expected exception $expected")
        }
    }

    @Serializable
    private data class PullBehaviorSpec(
        val formatVersion: Int,
        val cases: List<PullBehaviorCase>,
    )

    @Serializable
    private data class PullBehaviorCase(
        val name: String,
        val description: String,
        val schema: String = "users",
        val syncTables: List<FixtureSyncTable> = listOf(FixtureSyncTable("users", "id")),
        val serverScript: PullServerScript,
        val steps: List<PullStep>,
    )

    @Serializable
    private data class FixtureSyncTable(
        val tableName: String,
        val syncKeyColumnName: String,
    )

    @Serializable
    private data class PullServerScript(
        val kind: String,
        val response: JsonObject? = null,
        val responses: List<JsonObject> = emptyList(),
        val sessions: List<JsonObject> = emptyList(),
        val chunks: JsonObject? = null,
    )

    @Serializable
    private data class PullStep(
        val setupSql: List<String> = emptyList(),
        val action: String,
        val expectedException: String,
        val expectedErrorContains: String? = null,
        val expectedState: ExpectedPullState? = null,
        val expectedAppState: ExpectedAppState? = null,
    )

    @Serializable
    private data class ExpectedPullState(
        val lastBundleSeqSeen: Long? = null,
        val dirtyRowCount: Int? = null,
        val snapshotStageCount: Int? = null,
        val rowStateCount: Int? = null,
        val outboxRowCount: Int? = null,
        val rebuildRequired: Int? = null,
        val currentSourceChangedFromStepStart: Boolean? = null,
        val oldSourceReplacedByCurrent: Boolean? = null,
    )

    @Serializable
    private data class ExpectedAppState(
        val users: List<UserRow>? = null,
        val authors: List<AuthorRow>? = null,
        val profiles: List<ProfileRow>? = null,
    )

    @Serializable
    private data class UserRow(
        val id: String,
        val name: String,
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
