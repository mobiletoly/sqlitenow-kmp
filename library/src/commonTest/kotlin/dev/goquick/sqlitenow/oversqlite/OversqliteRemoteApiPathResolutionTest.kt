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
import io.ktor.http.content.OutgoingContent
import io.ktor.http.content.TextContent
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import io.ktor.utils.io.ByteChannel
import io.ktor.utils.io.readRemaining
import io.ktor.utils.io.core.readText
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class OversqliteRemoteApiPathResolutionTest {
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
        var encodedPath: String? = null
        var headerSourceId: String? = null
        var requestBody: JsonObject? = null
        val http = HttpClient(
            MockEngine { request ->
                encodedPath = request.url.encodedPath
                headerSourceId = request.headers["Oversync-Source-ID"]
                requestBody = json.parseToJsonElement(request.bodyText()).jsonObject
                respondJson(
                    body = json.encodeToString(
                        ConnectResponse.serializer(),
                        ConnectResponse(resolution = "initialize_empty"),
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
            OversqliteRemoteApi(http, json, log = {}).connect(
                sourceId = "source-c",
                hasLocalPendingRows = true,
            )
            assertEquals("/api/v1/sync/connect", encodedPath)
            assertEquals("source-c", headerSourceId)
            assertEquals(true, requestBody?.get("has_local_pending_rows")?.toString()?.toBooleanStrict())
            assertFalse(requestBody!!.containsKey("source_id"))
        } finally {
            http.close()
        }
    }

    @Test
    fun createPushSession_sends_source_header_and_omits_body_source_id() = runTest {
        var encodedPath: String? = null
        var headerSourceId: String? = null
        var requestBody: JsonObject? = null
        val http = HttpClient(
            MockEngine { request ->
                encodedPath = request.url.encodedPath
                headerSourceId = request.headers["Oversync-Source-ID"]
                requestBody = json.parseToJsonElement(request.bodyText()).jsonObject
                respondJson(
                    body = json.encodeToString(
                        PushSessionCreateResponse.serializer(),
                        PushSessionCreateResponse(
                            pushId = "push-1",
                            status = "staging",
                            plannedRowCount = 2,
                            nextExpectedRowOrdinal = 0,
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
            OversqliteRemoteApi(http, json, log = {}).createPushSession(
                sourceBundleId = 7L,
                plannedRowCount = 2L,
                sourceId = "source-d",
                initializationId = "init-1",
            )
            assertEquals("/api/v1/sync/push-sessions", encodedPath)
            assertEquals("source-d", headerSourceId)
            assertEquals("7", requestBody?.get("source_bundle_id")?.toString()?.trim('"'))
            assertEquals("2", requestBody?.get("planned_row_count")?.toString()?.trim('"'))
            assertFalse(requestBody!!.containsKey("source_id"))
        } finally {
            http.close()
        }
    }

    private fun MockRequestHandleScope.respondJson(body: String) = respond(
        content = body,
        status = HttpStatusCode.OK,
        headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
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
}
