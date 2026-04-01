package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.MockRequestHandleScope
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals

class OversqliteRemoteApiPathResolutionTest {
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun fetchCapabilities_preserves_base_path_prefix() = runTest {
        var encodedPath: String? = null
        val http = HttpClient(
            MockEngine { request ->
                encodedPath = request.url.encodedPath
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
            OversqliteRemoteApi(http, json, log = {}).fetchCapabilities()
            assertEquals("/api/v1/sync/capabilities", encodedPath)
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
        val http = HttpClient(
            MockEngine { request ->
                encodedPath = request.url.encodedPath
                afterBundleSeq = request.url.parameters["after_bundle_seq"]
                maxBundles = request.url.parameters["max_bundles"]
                targetBundleSeq = request.url.parameters["target_bundle_seq"]
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
            )
            assertEquals("/api/v1/sync/pull", encodedPath)
            assertEquals("4", afterBundleSeq)
            assertEquals("10", maxBundles)
            assertEquals("12", targetBundleSeq)
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
