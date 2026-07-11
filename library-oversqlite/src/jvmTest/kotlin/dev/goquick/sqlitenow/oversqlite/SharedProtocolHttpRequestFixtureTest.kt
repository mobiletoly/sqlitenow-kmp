package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.HttpRequestData
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

class SharedProtocolHttpRequestFixtureTest {
    private val fixtureFile = oversqliteContractFixture("protocol-http/requests.json")
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun kmpSharedProtocolHttpRequestFixturesMatchRuntime() = runTest {
        val spec = json.decodeFromString(ProtocolRequestSpec.serializer(), fixtureFile.readText())
        assertEquals(1, spec.formatVersion)
        for (case in spec.cases) {
            runCase(spec.baseUri, case)
        }
    }

    private suspend fun runCase(baseUri: String, case: ProtocolRequestCase) {
        var captured: CapturedRequest? = null
        val http = HttpClient(
            MockEngine { request ->
                captured = request.capture()
                respond(
                    content = responseBodyFor(case),
                    status = HttpStatusCode.OK,
                    headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
                )
            },
        ) {
            install(ContentNegotiation) {
                json(json)
            }
            defaultRequest {
                url(baseUri)
            }
        }
        try {
            executeOperation(OversqliteRemoteApi(http, json, log = {}), case)
            assertCaptured(case, captured ?: error("${case.name}: request was not captured"))
        } finally {
            http.close()
        }
    }

    private suspend fun executeOperation(api: OversqliteRemoteApi, case: ProtocolRequestCase) {
        when (case.operation) {
            "capabilities" -> api.fetchCapabilities(case.sourceId)
            "connect" -> api.connect(
                sourceId = case.sourceId,
                hasLocalPendingRows = case.booleanArg("hasLocalPendingRows"),
            )
            "pull" -> api.sendPullRequest(
                sourceId = case.sourceId,
                afterBundleSeq = case.longArg("afterBundleSeq"),
                maxBundles = case.intArg("maxBundles"),
                targetBundleSeq = case.longArg("targetBundleSeq"),
            )
            "pushSessionCreate" -> api.createPushSession(
                sourceId = case.sourceId,
                sourceBundleId = case.longArg("sourceBundleId"),
                plannedRowCount = case.longArg("plannedRowCount"),
                canonicalRequestHash = "a".repeat(64),
                initializationId = case.stringArg("initializationId"),
            )
            "pushSessionChunk" -> api.uploadPushChunk(
                pushId = case.stringArg("pushId"),
                sourceId = case.sourceId,
                request = PushSessionChunkRequest(
                    startRowOrdinal = case.longArg("startRowOrdinal"),
                    rows = listOf(fixturePushRow()),
                ),
            )
            "pushSessionCommit" -> api.commitPushSession(
                pushId = case.stringArg("pushId"),
                sourceId = case.sourceId,
            )
            "committedRows" -> api.fetchCommittedBundleChunk(
                bundleSeq = case.longArg("bundleSeq"),
                sourceId = case.sourceId,
                afterRowOrdinal = case.longArg("afterRowOrdinal"),
                maxRows = case.intArg("maxRows"),
            )
            "snapshotSession" -> api.createSnapshotSession(sourceId = case.sourceId)
            "snapshotChunk" -> api.fetchSnapshotChunk(
                snapshotId = case.stringArg("snapshotId"),
                sourceId = case.sourceId,
                snapshotBundleSeq = case.longArg("snapshotBundleSeq"),
                afterRowOrdinal = case.longArg("afterRowOrdinal"),
                maxRows = case.intArg("maxRows"),
            )
            "sourceReplacement" -> api.createSnapshotSession(
                sourceId = case.sourceId,
                request = SnapshotSessionCreateRequest(
                    sourceReplacement = SnapshotSourceReplacement(
                        previousSourceId = case.stringArg("previousSourceId"),
                        newSourceId = case.stringArg("newSourceId"),
                        reason = case.stringArg("reason"),
                    ),
                ),
            )
            "watch" -> api.watchBundleChanges(
                sourceId = case.sourceId,
                afterBundleSeq = case.longArg("afterBundleSeq"),
                onEvent = {},
            )
            else -> error("${case.name}: unknown operation ${case.operation}")
        }
    }

    private fun assertCaptured(case: ProtocolRequestCase, captured: CapturedRequest) {
        assertEquals(case.expected.method, captured.method, "${case.name}: method")
        assertEquals(case.expected.path, captured.path, "${case.name}: path")
        assertEquals(case.expected.query, captured.query, "${case.name}: query")
        assertEquals(case.expected.sourceHeader, captured.sourceHeader, "${case.name}: source header")
        if (case.expected.body == null || case.expected.body == JsonNull) {
            assertNull(captured.body, "${case.name}: body")
        } else {
            assertEquals(case.expected.body, captured.body, "${case.name}: body")
        }
        for (key in case.expected.bodyMustNotContain) {
            assertFalse(captured.body.containsKeyRecursive(key), "${case.name}: body must not contain $key")
        }
    }

    private suspend fun HttpRequestData.capture(): CapturedRequest {
        val rawBody = bodyText().trim()
        return CapturedRequest(
            method = method.value,
            path = url.encodedPath,
            query = url.parameters.entries().associate { (key, values) -> key to values.single() },
            sourceHeader = headers["Oversync-Source-ID"],
            body = if (rawBody.isBlank()) null else json.parseToJsonElement(rawBody),
        )
    }

    private fun responseBodyFor(case: ProtocolRequestCase): String {
        return when (case.operation) {
            "capabilities" -> json.encodeToString(
                CapabilitiesResponse.serializer(),
                CapabilitiesResponse(
                    protocolVersion = "v1",
                    schemaVersion = 1,
                    features = mapOf("connect_lifecycle" to true, "bundle_change_watch" to true),
                ),
            )
            "connect" -> json.encodeToString(
                ConnectResponse.serializer(),
                ConnectResponse(resolution = "initialize_empty"),
            )
            "pull" -> json.encodeToString(
                PullResponse.serializer(),
                PullResponse(stableBundleSeq = case.longArg("targetBundleSeq"), bundles = emptyList(), hasMore = false),
            )
            "pushSessionCreate" -> json.encodeToString(
                PushSessionCreateResponse.serializer(),
                PushSessionCreateResponse(
                    pushId = "push-fixture",
                    status = "staging",
                    plannedRowCount = case.longArg("plannedRowCount"),
                    nextExpectedRowOrdinal = 0,
                    canonicalRequestHash = "a".repeat(64),
                ),
            )
            "pushSessionChunk" -> json.encodeToString(
                PushSessionChunkResponse.serializer(),
                PushSessionChunkResponse(
                    pushId = case.stringArg("pushId"),
                    nextExpectedRowOrdinal = 1,
                ),
            )
            "pushSessionCommit" -> json.encodeToString(
                PushSessionCommitResponse.serializer(),
                PushSessionCommitResponse(
                    bundleSeq = 1,
                    sourceId = case.sourceId,
                    sourceBundleId = 1,
                    rowCount = 1,
                    bundleHash = "fixture-hash",
                    canonicalRequestHash = "a".repeat(64),
                ),
            )
            "committedRows" -> json.encodeToString(
                CommittedBundleRowsResponse.serializer(),
                CommittedBundleRowsResponse(
                    bundleSeq = case.longArg("bundleSeq"),
                    sourceId = case.sourceId,
                    sourceBundleId = 1,
                    rowCount = 0,
                    bundleHash = "fixture-hash",
                    canonicalRequestHash = "a".repeat(64),
                    rows = emptyList(),
                    nextRowOrdinal = case.longArg("afterRowOrdinal"),
                    hasMore = false,
                ),
            )
            "snapshotSession", "sourceReplacement" -> json.encodeToString(
                SnapshotSession.serializer(),
                SnapshotSession(
                    snapshotId = "snapshot-fixture",
                    snapshotBundleSeq = case.args["snapshotBundleSeq"]?.jsonPrimitive?.contentOrNull?.toLongOrNull() ?: 6L,
                    rowCount = 0,
                    byteCount = 0,
                    expiresAt = "2099-01-01T00:00:00Z",
                ),
            )
            "snapshotChunk" -> json.encodeToString(
                SnapshotChunkResponse.serializer(),
                SnapshotChunkResponse(
                    snapshotId = case.stringArg("snapshotId"),
                    snapshotBundleSeq = case.longArg("snapshotBundleSeq"),
                    rows = emptyList(),
                    nextRowOrdinal = case.longArg("afterRowOrdinal"),
                    hasMore = false,
                ),
            )
            "watch" -> ""
            else -> error("${case.name}: unknown operation ${case.operation}")
        }
    }

    private fun fixturePushRow(): PushRequestRow {
        return PushRequestRow(
            schema = "main",
            table = "users",
            key = mapOf("id" to "user-1"),
            op = "INSERT",
            baseRowVersion = 0,
            payload = JsonObject(
                mapOf(
                    "id" to JsonPrimitive("user-1"),
                    "name" to JsonPrimitive("Ada"),
                ),
            ),
        )
    }

    private fun ProtocolRequestCase.longArg(name: String): Long {
        return args[name]?.jsonPrimitive?.contentOrNull?.toLongOrNull()
            ?: error("${this.name}: missing long arg $name")
    }

    private fun ProtocolRequestCase.intArg(name: String): Int = longArg(name).toInt()

    private fun ProtocolRequestCase.stringArg(name: String): String {
        return args[name]?.jsonPrimitive?.contentOrNull
            ?: error("${this.name}: missing string arg $name")
    }

    private fun ProtocolRequestCase.booleanArg(name: String): Boolean {
        return args[name]?.jsonPrimitive?.boolean
            ?: error("${this.name}: missing boolean arg $name")
    }

    private fun JsonElement?.containsKeyRecursive(key: String): Boolean {
        return when (this) {
            is JsonObject -> containsKey(key) || values.any { it.containsKeyRecursive(key) }
            else -> false
        }
    }

    private data class CapturedRequest(
        val method: String,
        val path: String,
        val query: Map<String, String>,
        val sourceHeader: String?,
        val body: JsonElement?,
    )

    @Serializable
    private data class ProtocolRequestSpec(
        val formatVersion: Int,
        val baseUri: String,
        val cases: List<ProtocolRequestCase>,
    )

    @Serializable
    private data class ProtocolRequestCase(
        val name: String,
        val description: String,
        val operation: String,
        val sourceId: String,
        val args: Map<String, JsonElement> = emptyMap(),
        val expected: ExpectedRequest,
    )

    @Serializable
    private data class ExpectedRequest(
        val method: String,
        val path: String,
        val query: Map<String, String> = emptyMap(),
        val sourceHeader: String,
        val body: JsonElement? = null,
        val bodyMustNotContain: List<String> = emptyList(),
    )
}
