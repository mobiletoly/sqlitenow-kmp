package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
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
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class OversqliteRemoteApiPathResolutionTest {
    private data class CapturedJsonRequest(
        val encodedPath: String?,
        val headerSourceId: String?,
        val body: JsonObject?,
    )

    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun fetchCapabilities_preserves_base_path_prefix() = runTest {
        var encodedPath: String? = null
        var headerSourceId: String? = null
        val http = HttpClient(
            MockEngine { request ->
                encodedPath = request.url.encodedPath
                headerSourceId = request.headers["Oversync-Source-ID"]
                respondJson(
                    body = json.encodeToString(
                        CapabilitiesResponse.serializer(),
                        CapabilitiesResponse(
                            protocolVersion = "v1",
                            schemaVersion = 1,
                            features = mapOf("connect_lifecycle" to true),
                        ),
                    ),
                )
            },
        ) {
            install(ContentNegotiation) {
                json(json)
            }
            defaultRequest {
                url("https://sync.test/api/v1/")
            }
        }

        try {
            OversqliteRemoteApi(http, json, log = {}).fetchCapabilities("source-a")
            assertEquals("/api/v1/sync/capabilities", encodedPath)
            assertEquals("source-a", headerSourceId)
        } finally {
            http.close()
        }
    }

    @Test
    fun sendPullRequest_preserves_base_path_prefix_and_query_parameters() = runTest {
        var encodedPath: String? = null
        var afterBundleSeq: String? = null
        var maxBundles: String? = null
        var targetBundleSeq: String? = null
        var headerSourceId: String? = null
        val http = HttpClient(
            MockEngine { request ->
                encodedPath = request.url.encodedPath
                afterBundleSeq = request.url.parameters["after_bundle_seq"]
                maxBundles = request.url.parameters["max_bundles"]
                targetBundleSeq = request.url.parameters["target_bundle_seq"]
                headerSourceId = request.headers["Oversync-Source-ID"]
                respondJson(
                    body = json.encodeToString(
                        PullResponse.serializer(),
                        PullResponse(
                            stableBundleSeq = 12L,
                            bundles = emptyList(),
                            hasMore = false,
                        ),
                    ),
                )
            },
        ) {
            install(ContentNegotiation) {
                json(json)
            }
            defaultRequest {
                url("https://sync.test/api/v1/")
            }
        }

        try {
            OversqliteRemoteApi(http, json, log = {}).sendPullRequest(
                afterBundleSeq = 4L,
                maxBundles = 10,
                targetBundleSeq = 12L,
                sourceId = "source-b",
            )
            assertEquals("/api/v1/sync/pull", encodedPath)
            assertEquals("4", afterBundleSeq)
            assertEquals("10", maxBundles)
            assertEquals("12", targetBundleSeq)
            assertEquals("source-b", headerSourceId)
        } finally {
            http.close()
        }
    }

    @Test
    fun connect_sends_source_header_and_omits_body_source_id() = runTest {
        val captured = captureJsonRequest(
            responseBody = json.encodeToString(
                ConnectResponse.serializer(),
                ConnectResponse(resolution = "initialize_empty"),
            ),
        ) { api ->
            api.connect(
                sourceId = "source-c",
                hasLocalPendingRows = true,
            )
        }

        assertEquals("/api/v1/sync/connect", captured.encodedPath)
        assertEquals("source-c", captured.headerSourceId)
        assertEquals(true, captured.body?.get("has_local_pending_rows")?.toString()?.toBooleanStrict())
        assertFalse(captured.body!!.containsKey("source_id"))
    }

    @Test
    fun createPushSession_sends_source_header_and_omits_body_source_id() = runTest {
        val captured = captureJsonRequest(
            responseBody = json.encodeToString(
                PushSessionCreateResponse.serializer(),
                PushSessionCreateResponse(
                    pushId = "push-1",
                    status = "staging",
                    plannedRowCount = 2,
                    nextExpectedRowOrdinal = 0,
                    canonicalRequestHash = "a".repeat(64),
                ),
            ),
        ) { api ->
            api.createPushSession(
                sourceBundleId = 7L,
                plannedRowCount = 2L,
                canonicalRequestHash = "a".repeat(64),
                sourceId = "source-d",
                initializationId = "init-1",
            )
        }

        assertEquals("/api/v1/sync/push-sessions", captured.encodedPath)
        assertEquals("source-d", captured.headerSourceId)
        assertEquals("7", captured.body?.get("source_bundle_id")?.toString()?.trim('"'))
        assertEquals("2", captured.body?.get("planned_row_count")?.toString()?.trim('"'))
        assertFalse(captured.body!!.containsKey("source_id"))
    }

    private suspend fun captureJsonRequest(
        responseBody: String,
        call: suspend (OversqliteRemoteApi) -> Unit,
    ): CapturedJsonRequest {
        var captured: CapturedJsonRequest? = null
        val http = HttpClient(
            MockEngine { request ->
                captured = CapturedJsonRequest(
                    encodedPath = request.url.encodedPath,
                    headerSourceId = request.headers["Oversync-Source-ID"],
                    body = json.parseToJsonElement(request.bodyText()).jsonObject,
                )
                respondJson(body = responseBody)
            },
        ) {
            install(ContentNegotiation) {
                json(json)
            }
            defaultRequest {
                url("https://sync.test/api/v1/")
            }
        }

        try {
            call(OversqliteRemoteApi(http, json, log = {}))
            return captured ?: error("expected a captured request")
        } finally {
            http.close()
        }
    }

    private fun MockRequestHandleScope.respondJson(body: String) = respond(
        content = body,
        status = HttpStatusCode.OK,
        headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
    )

}
