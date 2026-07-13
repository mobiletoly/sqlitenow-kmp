package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpServer
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import java.net.InetSocketAddress
import kotlin.io.path.createDirectories
import kotlin.io.path.readText
import kotlin.io.path.writeText
import kotlin.math.min
import kotlin.test.Test
import kotlin.test.assertEquals

class SharedLifecycleBehaviorFixtureTest : SharedRuntimeStateFixtureSupport() {
    private val fixtureFiles = listOf(
        oversqliteContractFixture("lifecycle/behavior/basic.json"),
        oversqliteContractFixture("lifecycle/behavior/protocol-gates.json"),
        oversqliteContractFixture("lifecycle/behavior/source-recovery-replacement.json"),
    )

    @Test
    fun kmpSharedLifecycleBehaviorFixtureMatchesRuntime() = runBlocking {
        for (fixtureFile in fixtureFiles) {
            runFixture(fixtureFile)
        }
    }

    private suspend fun runFixture(fixtureFile: java.nio.file.Path) {
        val spec = contractJson.decodeFromString<LifecycleBehaviorSpec>(fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        val updatedCases = mutableListOf<LifecycleBehaviorCase>()
        for (case in spec.cases) {
            updatedCases += runCase(case)
        }
        if (updateRuntimeStateExpected) {
            fixtureFile.parent.createDirectories()
            fixtureFile.writeText(
                contractJson.encodeToString(LifecycleBehaviorSpec(formatVersion = 1, cases = updatedCases)) + "\n",
            )
        }
    }

    private suspend fun runCase(case: LifecycleBehaviorCase): LifecycleBehaviorCase {
        val updatedSteps = mutableListOf<LifecycleBehaviorStep>()
        withLifecycleUsersDatabase(case) {
            for (step in case.steps) {
                val (error, actualResult) = runStep(step)
                assertRuntimeStateExpectedException(case.name, step.expectedException, error)
                step.expectedResult?.let { expected ->
                    assertEquals(expected, actualResult, "${case.name}/${step.action}: result")
                }
                step.expectedServerState?.let { expected ->
                    assertExpectedServerState(case.name, expected)
                }
                val actualState = dumpRuntimeState(db) as JsonObject
                if (updateRuntimeStateExpected) {
                    updatedSteps += step.copy(expectedState = actualState)
                } else {
                    val expected = step.expectedState ?: error("${case.name}/${step.action}: missing expectedState")
                    assertEquals(expected, actualState, "${case.name}/${step.action}: runtime state")
                    updatedSteps += step
                }
            }
        }
        return case.copy(steps = updatedSteps)
    }

    private suspend fun withLifecycleUsersDatabase(
        case: LifecycleBehaviorCase,
        block: suspend LifecycleUsersEnv.() -> Unit,
    ) {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val serverHandle = newLifecycleServer(case, db)
        serverHandle.server.start()
        try {
            val client = newRuntimeStateClient(db, serverHandle.http)
            LifecycleUsersEnv(
                db = db,
                http = serverHandle.http,
                client = client,
                chunkedServer = serverHandle.chunkedServer,
                recorder = serverHandle.recorder,
            ).block()
        } finally {
            serverHandle.http.close()
            serverHandle.server.stop(0)
            db.close()
        }
    }

    private fun newLifecycleServer(
        case: LifecycleBehaviorCase,
        db: SafeSQLiteConnection,
    ): LifecycleServerHandle {
        val recorder = LifecycleServerRecorder()
        val customConnect = case.serverScript.connectResolution != "default"
        val customCapabilities = case.serverScript.protocolVersions != listOf("v0")
        val server = if (customConnect || customCapabilities) {
            newConnectScriptServer(
                connectResolution = case.serverScript.connectResolution,
                protocolVersions = case.serverScript.protocolVersions,
            )
        } else {
            newServer()
        }
        installSnapshotRoutes(server, case.serverScript, recorder)
        val chunkedServer = when (case.serverScript.kind) {
            "chunked_sync" -> FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
                var pullIndex = 0
                beforePullResponse = { _, _ ->
                    val statements = case.serverScript.lateWritesByPull.getOrNull(pullIndex)
                    pullIndex += 1
                    if (!statements.isNullOrEmpty()) {
                        runBlocking { executeSetupSql(db, statements) }
                    }
                }
                install(server)
            }
            "default", "snapshot_rebuild", "remote_authoritative_partial_snapshot" -> null
            else -> error("${case.name}: unknown lifecycle server script ${case.serverScript.kind}")
        }
        return LifecycleServerHandle(
            server = server,
            http = newHttpClient(server),
            chunkedServer = chunkedServer,
            recorder = recorder,
        )
    }

    private fun newConnectScriptServer(
        connectResolution: String,
        protocolVersions: List<String>,
    ): HttpServer {
        val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        var capabilityRequestIndex = 0
        server.createContext("/sync/capabilities") { exchange ->
            if (exchange.requestMethod != "GET") {
                exchange.sendResponseHeaders(405, -1)
                exchange.close()
                return@createContext
            }
            val protocolVersion = protocolVersions.getOrElse(capabilityRequestIndex) {
                protocolVersions.last()
            }
            capabilityRequestIndex += 1
            respondJson(
                exchange,
                200,
                """{"protocol_version":"$protocolVersion","schema_version":1,"features":{"connect_lifecycle":true}}""",
            )
        }
        server.createContext("/sync/connect") { exchange ->
            if (exchange.requestMethod != "POST") {
                exchange.sendResponseHeaders(405, -1)
                exchange.close()
                return@createContext
            }
            val resolution = connectResolution.takeUnless { it == "default" } ?: "initialize_empty"
            respondJson(exchange, 200, """{"resolution":"$resolution"}""")
        }
        return server
    }

    private fun installSnapshotRoutes(
        server: HttpServer,
        script: LifecycleServerScript,
        recorder: LifecycleServerRecorder,
    ) {
        if (script.snapshots.isEmpty() && script.snapshotSessionErrors.isEmpty()) {
            return
        }
        var nextSnapshotIndex = 0
        val snapshotsById = script.snapshots.associateBy { it.id }
        server.createContext("/sync/snapshot-sessions") { exchange ->
            if (exchange.requestMethod != "POST") {
                exchange.sendResponseHeaders(405, -1)
                exchange.close()
                return@createContext
            }
            recorder.snapshotSessionCreateCount += 1
            val requestJson = exchange.requestBody.readBytes().decodeToString().ifBlank { "{}" }
            recorder.snapshotSourceReplacementRequests +=
                contractJson.parseToJsonElement(requestJson).jsonObject["source_replacement"]
                    ?.jsonObject
                    ?.let(::canonicalizeJsonElement)
                    .orEmpty()
            script.snapshotSessionErrors.getOrNull(nextSnapshotIndex)?.let { error ->
                nextSnapshotIndex += 1
                val response = buildJsonObject {
                    put("error", JsonPrimitive(error.error))
                    put("message", JsonPrimitive(error.message))
                    put("source_id", JsonPrimitive(error.sourceId))
                    error.replacedBySourceId?.let { put("replaced_by_source_id", JsonPrimitive(it)) }
                }
                respondJson(
                    exchange,
                    error.status,
                    contractJson.encodeToString(response),
                )
                return@createContext
            }
            val snapshot = script.snapshots[min(nextSnapshotIndex, script.snapshots.lastIndex)]
            nextSnapshotIndex += 1
            respondJson(
                exchange,
                200,
                """
                {
                  "snapshot_id": "${snapshot.id}",
                  "snapshot_bundle_seq": ${snapshot.bundleSeq},
                  "row_count": ${snapshot.rows.size},
                  "byte_count": 0,
                  "expires_at": "2099-01-01T00:00:00Z"
                }
                """.trimIndent(),
            )
        }
        server.createContext("/sync/snapshot-sessions/") { exchange ->
            val snapshotId = exchange.requestURI.path.removePrefix("/sync/snapshot-sessions/")
            val snapshot = snapshotsById[snapshotId]
                ?: error("unknown snapshot id $snapshotId")
            when (exchange.requestMethod) {
                "GET" -> {
                    val after = queryParam(exchange, "after_row_ordinal").toLongOrNull() ?: 0L
                    if (snapshot.failAfterRowOrdinal == after) {
                        respondJson(exchange, 500, """{"error":"snapshot_failed","message":"forced snapshot failure"}""")
                        return@createContext
                    }
                    val maxRows = queryParam(exchange, "max_rows").toIntOrNull() ?: 1000
                    val rowLimit = min(maxRows, snapshot.rowsPerChunk ?: maxRows)
                    val rows = snapshot.rows.drop(after.toInt()).take(rowLimit)
                    val nextRowOrdinal = after + rows.size
                    val rowsJson = rows.joinToString(separator = ",") { row ->
                        """
                        {
                          "schema": "main",
                          "table": "users",
                          "key": ${row.key},
                          "row_version": ${row.rowVersion},
                          "payload": ${row.payload}
                        }
                        """.trimIndent()
                    }
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "snapshot_id": "${snapshot.id}",
                          "snapshot_bundle_seq": ${snapshot.bundleSeq},
                          "rows": [$rowsJson],
                          "next_row_ordinal": $nextRowOrdinal,
                          "has_more": ${nextRowOrdinal < snapshot.rows.size}
                        }
                        """.trimIndent(),
                    )
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

    private suspend fun LifecycleUsersEnv.runStep(step: LifecycleBehaviorStep): StepResult {
        var actualResult: JsonObject? = null
        val error = runCatching {
            actualResult = when (step.action) {
                "open" -> {
                    client.open().getOrThrow()
                    null
                }
                "attach" -> attachResultJson(client.attach("user-1").getOrThrow())
                "localSql" -> {
                    executeSetupSql(db, step.sql)
                    null
                }
                "applyModeSql" -> {
                    executeApplyModeSql(db, step.sql)
                    null
                }
                "sync" -> syncResultJson(client.sync().getOrThrow())
                "syncThenDetach" -> syncThenDetachResultJson(client.syncThenDetach().getOrThrow())
                "rebuild" -> remoteSyncResultJson(client.rebuild().getOrThrow())
                "detach" -> detachResultJson(client.detach().getOrThrow())
                "sourceInfo" -> sourceInfoResultJson(client.sourceInfo().getOrThrow())
                "reopenOpen" -> {
                    client.close()
                    client = newRuntimeStateClient(db, http)
                    client.open().getOrThrow()
                    null
                }
                else -> error("unknown lifecycle step action ${step.action}")
            }
        }.exceptionOrNull()
        return StepResult(error, actualResult)
    }

    private fun LifecycleUsersEnv.assertExpectedServerState(
        caseName: String,
        expected: ExpectedLifecycleServerState,
    ) {
        expected.createRequestCount?.let {
            assertEquals(it, chunkedServer?.createRequests?.size ?: 0, "$caseName: create request count")
        }
        expected.createSourceBundleIds?.let {
            assertEquals(it, chunkedServer?.createRequests?.map { request -> request.sourceBundleId }.orEmpty(), "$caseName: source bundle ids")
        }
        expected.snapshotSessionCreateCount?.let {
            assertEquals(it, recorder.snapshotSessionCreateCount, "$caseName: snapshot session count")
        }
        expected.snapshotSourceReplacementRequests?.let {
            assertEquals(it, recorder.snapshotSourceReplacementRequests, "$caseName: snapshot source replacement requests")
        }
    }

    private fun attachResultJson(result: AttachResult): JsonObject = when (result) {
        is AttachResult.Connected -> buildJsonObject {
            put("kind", JsonPrimitive("attach"))
            put("outcome", JsonPrimitive(result.outcome.name.lowercase()))
        }
        is AttachResult.RetryLater -> buildJsonObject {
            put("kind", JsonPrimitive("attach_retry_later"))
            put("retryAfterSeconds", JsonPrimitive(result.retryAfterSeconds))
        }
    }

    private fun syncResultJson(result: SyncReport): JsonObject = buildJsonObject {
        put("kind", JsonPrimitive("sync"))
        put("pushOutcome", JsonPrimitive(result.pushOutcome.name.lowercase()))
        put("remoteOutcome", JsonPrimitive(result.remoteOutcome.name.lowercase()))
    }

    private fun syncThenDetachResultJson(result: SyncThenDetachResult): JsonObject = buildJsonObject {
        put("kind", JsonPrimitive("sync_then_detach"))
        put("isSuccess", JsonPrimitive(result.isSuccess()))
        put("detach", JsonPrimitive(detachOutcomeName(result.detach)))
        put("syncRounds", JsonPrimitive(result.syncRounds))
        put("remainingPendingRowCount", JsonPrimitive(result.remainingPendingRowCount))
        put("pushOutcome", JsonPrimitive(result.lastSync.pushOutcome.name.lowercase()))
        put("remoteOutcome", JsonPrimitive(result.lastSync.remoteOutcome.name.lowercase()))
    }

    private fun remoteSyncResultJson(result: RemoteSyncReport): JsonObject = buildJsonObject {
        put("kind", JsonPrimitive("remote_sync"))
        put("outcome", JsonPrimitive(result.outcome.name.lowercase()))
    }

    private fun detachResultJson(result: DetachOutcome): JsonObject = buildJsonObject {
        put("kind", JsonPrimitive("detach"))
        put("outcome", JsonPrimitive(detachOutcomeName(result)))
    }

    private fun detachOutcomeName(result: DetachOutcome): String = when (result) {
        DetachOutcome.DETACHED -> "detached"
        DetachOutcome.BLOCKED_UNSYNCED_DATA -> "blocked_unsynced_data"
    }

    private fun sourceInfoResultJson(result: SourceInfo): JsonObject = buildJsonObject {
        put("kind", JsonPrimitive("source_info"))
        put("currentSourceIdPresent", JsonPrimitive(result.currentSourceId.isNotBlank()))
        put("rebuildRequired", JsonPrimitive(result.rebuildRequired))
        put("sourceRecoveryRequired", JsonPrimitive(result.sourceRecoveryRequired))
        result.sourceRecoveryReason?.let { put("sourceRecoveryReason", JsonPrimitive(it.name.lowercase())) }
    }

    @Serializable
    private data class LifecycleBehaviorSpec(
        val formatVersion: Int,
        val cases: List<LifecycleBehaviorCase>,
    )

    @Serializable
    private data class LifecycleBehaviorCase(
        val name: String,
        val description: String = "",
        val serverScript: LifecycleServerScript = LifecycleServerScript(),
        val steps: List<LifecycleBehaviorStep>,
    )

    @Serializable
    private data class LifecycleServerScript(
        val kind: String = "default",
        val connectResolution: String = "default",
        val protocolVersions: List<String> = listOf("v0"),
        val lateWritesByPull: List<List<String>> = emptyList(),
        val snapshots: List<LifecycleSnapshot> = emptyList(),
        val snapshotSessionErrors: List<LifecycleSnapshotSessionError> = emptyList(),
    )

    @Serializable
    private data class LifecycleSnapshotSessionError(
        val status: Int = 409,
        val error: String,
        val message: String,
        val sourceId: String = "current-source",
        val replacedBySourceId: String? = null,
    )

    @Serializable
    private data class LifecycleSnapshot(
        val id: String,
        val bundleSeq: Long,
        val failAfterRowOrdinal: Long? = null,
        val rowsPerChunk: Int? = null,
        val rows: List<LifecycleSnapshotRow>,
    )

    @Serializable
    private data class LifecycleSnapshotRow(
        val key: JsonElement,
        val rowVersion: Long,
        val payload: JsonElement,
    )

    @Serializable
    private data class LifecycleBehaviorStep(
        val action: String,
        val sql: List<String> = emptyList(),
        val expectedException: String = "none",
        val expectedResult: JsonObject? = null,
        val expectedServerState: ExpectedLifecycleServerState? = null,
        val expectedState: JsonObject? = null,
    )

    @Serializable
    private data class ExpectedLifecycleServerState(
        val createRequestCount: Int? = null,
        val createSourceBundleIds: List<Long>? = null,
        val snapshotSessionCreateCount: Int? = null,
        val snapshotSourceReplacementRequests: List<String>? = null,
    )

    private data class StepResult(
        val error: Throwable?,
        val result: JsonObject?,
    )

    private data class LifecycleServerHandle(
        val server: HttpServer,
        val http: HttpClient,
        val chunkedServer: FakeChunkedSyncServer?,
        val recorder: LifecycleServerRecorder,
    )

    private data class LifecycleServerRecorder(
        var snapshotSessionCreateCount: Int = 0,
        val snapshotSourceReplacementRequests: MutableList<String> = mutableListOf(),
    )

    private data class LifecycleUsersEnv(
        val db: SafeSQLiteConnection,
        val http: HttpClient,
        var client: DefaultOversqliteClient,
        val chunkedServer: FakeChunkedSyncServer?,
        val recorder: LifecycleServerRecorder,
    )
}
