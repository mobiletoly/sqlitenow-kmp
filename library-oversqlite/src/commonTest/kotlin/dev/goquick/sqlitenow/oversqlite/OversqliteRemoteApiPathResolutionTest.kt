package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.HttpRequestData
import io.ktor.client.request.get
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.headersOf
import io.ktor.utils.io.ByteReadChannel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class OversqliteRemoteApiPathResolutionTest {
    private val json = Json { ignoreUnknownKeys = true }

    @Test
    fun derivedClientRejectsRedirectWithoutReplayingRequest() = runTest {
        for (location in listOf("/same-origin", "https://other.test/cross-authority", "http://sync.test/downgrade")) {
            val requests = mutableListOf<HttpRequestData>()
            val source = HttpClient(MockEngine { request ->
                requests += request
                respond("", HttpStatusCode.Found, headersOf(HttpHeaders.Location, location))
            }) { defaultRequest { url("https://sync.test") } }
            val client = oversqliteRedirectSafeHttpClient(source)
            try {
                assertFailsWith<DownloadHttpException> { client.get("https://sync.test/start") }
                assertEquals(1, requests.size)
            } finally {
                client.close()
                source.close()
            }
        }
    }

    @Test
    fun pullRedirectBodiesAreFailuresWithoutReplay() = runTest {
        val cases = listOf(
            byteArrayOf() to headersOf(HttpHeaders.Location, "/moved"),
            byteArrayOf(0xC3.toByte(), 0x28) to headersOf(HttpHeaders.Location, "/moved"),
            "ignored".encodeToByteArray() to headersOf(
                HttpHeaders.Location to listOf("/moved"),
                HttpHeaders.ContentEncoding to listOf("br"),
            ),
            ByteArray(70_000) { 'x'.code.toByte() } to headersOf(HttpHeaders.Location, "/moved"),
        )
        for (response in cases) {
            var requests = 0
            val source = HttpClient(MockEngine {
                requests++
                respond(ByteReadChannel(response.first), HttpStatusCode.Found, response.second)
            }) {
                followRedirects = false
                defaultRequest { url("https://sync.test") }
            }
            val http = oversqliteRedirectSafeHttpClient(source)
            try {
                assertFailsWith<DownloadHttpException> {
                    OversqliteRemoteApi(http, json, generation = { testGeneration() }, log = {})
                        .sendPullRequest(0, 1, 0, "source-a", oversqlitePullResponseBodyHardLimit)
                }
                assertEquals(1, requests)
            } finally {
                http.close()
                source.close()
            }
        }
    }

    @Test
    fun snapshotDeleteRedirectUsesBestEffortFailurePathWithoutReplay() = runTest {
        var requests = 0
        val logs = mutableListOf<String>()
        val source = HttpClient(MockEngine {
            requests++
            respond("", HttpStatusCode.Found, headersOf(HttpHeaders.Location, "/moved"))
        }) {
            followRedirects = false
            defaultRequest { url("https://sync.test/api/") }
        }
        val http = oversqliteRedirectSafeHttpClient(source)
        try {
            withContext(Dispatchers.Default) {
                OversqliteRemoteApi(http, json, generation = { testGeneration() }, log = { logs += it() })
                    .deleteSnapshotSessionBestEffort("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "source-a")
            }
            assertEquals(1, requests)
            assertTrue(logs.any { "best-effort failure" in it })
        } finally {
            http.close()
            source.close()
        }
    }

    @Test
    fun completePushPreservesBasePathAndUsesOnlyOnePost() = runTest {
        val generation = testGeneration()
        var requests = 0
        val http = HttpClient(MockEngine { request ->
            requests++
            assertEquals("/api/sync/push", request.url.encodedPath)
            assertEquals("source-a", request.headers["Oversync-Source-ID"])
            respond(
                buildJsonObject {
                    put("bundle_seq", JsonPrimitive(1))
                    put("bundle_hash", JsonPrimitive("a".repeat(64)))
                    put("rows", OversqliteCompactCodec.encodeBundleRows(
                        generation,
                        listOf(BundleRow(
                            "main", "users", mapOf("id" to "u1"), "INSERT", 1,
                            buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("Ada")) },
                        )),
                    ))
                }.toString(),
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
            )
        }) { defaultRequest { url("https://sync.test/api/") } }
        try {
            val result = OversqliteRemoteApi(http, json, generation = { generation }, log = {}).push(
                sourceId = "source-a",
                sourceBundleId = 1,
                canonicalRequestHash = "b".repeat(64),
                initializationId = null,
                rows = listOf(PushRequestRow(
                    "main", "users", mapOf("id" to "u1"), "INSERT", 0,
                    buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("Ada")) },
                )),
                maxRequestDecodedBytes = oversqlitePushResponseBodyHardLimit,
                maxResponseDecodedBytes = oversqlitePushResponseBodyHardLimit,
            )
            assertEquals(1, requests)
            assertEquals(1L, result.bundleSeq)
            assertEquals("u1", result.rows.single().key.getValue("id"))
        } finally {
            http.close()
        }
    }
}
