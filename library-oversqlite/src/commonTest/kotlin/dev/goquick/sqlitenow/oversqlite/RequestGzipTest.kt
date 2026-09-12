/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.client.request.HttpRequestData
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.content.OutgoingContent
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

class RequestGzipTest {
    @Test
    fun runtimeEngineSelectsAtThresholdAndRejectsIncompressibleBody() = runTest {
        for (size in listOf(2_047, 2_048, 2_049)) {
            val decoded = ByteArray(size) { 'a'.code.toByte() }
            val selected = encodeOversqliteRequestBody(decoded, size.toLong(), size.toLong())
            assertEquals(if (size < 2_048) null else "gzip", selected.contentEncoding)
            if (selected.contentEncoding == null) assertContentEquals(decoded, selected.bytes)
            else {
                assertTrue(selected.bytes.size < decoded.size)
                assertEquals(0x1f, selected.bytes[0].toInt() and 0xff)
                assertEquals(0x8b, selected.bytes[1].toInt() and 0xff)
                if (!oversqliteFetchManagesResponseCompression) {
                    assertContentEquals(decoded, decodeOversqliteSingleMemberGzip(selected.bytes, size.toLong()))
                }
            }
        }
        val noisy = ByteArray(4_096)
        val prefix = "{\"pad\":\"".encodeToByteArray()
        prefix.copyInto(noisy)
        noisy[noisy.size - 2] = '"'.code.toByte()
        noisy[noisy.size - 1] = '}'.code.toByte()
        var state = 0x6f76657273716c69L
        for (index in prefix.size until noisy.size - 2) {
            state = state xor (state ushr 12)
            state = state xor (state shl 25)
            state = state xor (state ushr 27)
            noisy[index] = ((state * 2685821657736338717L) ushr 56).toByte()
        }
        val identity = encodeOversqliteRequestBody(noisy, noisy.size.toLong(), noisy.size.toLong())
        assertNull(identity.contentEncoding)
        assertContentEquals(noisy, identity.bytes)
    }

    @Test
    fun inclusiveCapAndOverflowAreDifferentResults() = runTest {
        val decoded = ByteArray(2_048) { 1 }
        val atCap = requireNotNull(compressOversqliteRequestGzip(decoded, decoded.size - 1))
        val selected = encodeOversqliteRequestBody(
            decoded, decoded.size.toLong(), atCap.size.toLong(),
            OversqliteRequestGzipEngine { _, cap ->
                assertEquals(atCap.size, cap)
                atCap
            },
        )
        assertEquals("gzip", selected.contentEncoding)
        assertContentEquals(atCap, selected.bytes)
        val overflow = encodeOversqliteRequestBody(
            decoded, decoded.size.toLong(), decoded.size.toLong(),
            OversqliteRequestGzipEngine { _, cap ->
                assertEquals(decoded.size - 1, cap)
                null
            },
        )
        assertNull(overflow.contentEncoding)
        assertContentEquals(decoded, overflow.bytes)
    }

    @Test
    fun decodedAdmissionStopsBeforeEngineAtControlAndPushLimits() = runTest {
        for (limit in listOf(65_536, 1_048_576)) {
            var calls = 0
            val engine = OversqliteRequestGzipEngine { _, _ -> calls++; null }
            val exact = ByteArray(limit) { 'a'.code.toByte() }
            assertNull(encodeOversqliteRequestBody(exact, limit.toLong(), limit.toLong(), engine).contentEncoding)
            assertEquals(1, calls)
            assertFailsWith<IllegalArgumentException> {
                encodeOversqliteRequestBody(ByteArray(limit + 1), limit.toLong(), limit.toLong(), engine)
            }
            assertEquals(1, calls)
        }
    }

    @Test
    fun cancellationAndEngineFailureRemainDistinct() = runTest {
        val decoded = ByteArray(2_048) { 1 }
        var released = false
        assertFailsWith<CancellationException> {
            encodeOversqliteRequestBody(decoded, 2_048, 2_048, OversqliteRequestGzipEngine { _, _ ->
                try { throw CancellationException("caller cancelled") } finally { released = true }
            })
        }
        assertTrue(released)
        assertFailsWith<OversqliteRequestCompressionException> {
            encodeOversqliteRequestBody(decoded, 2_048, 2_048, OversqliteRequestGzipEngine { _, _ ->
                error("engine failed")
            })
        }
        assertFailsWith<OversqliteRequestCompressionUnavailableException> {
            encodeOversqliteRequestBody(decoded, 2_048, 2_048, OversqliteRequestGzipEngine { _, _ ->
                throw OversqliteRequestCompressionUnavailableException("missing")
            })
        }
    }

    @Test
    fun endpointRequestLimitsAdmitExactBodiesAndStopOverLimitBeforeEngineAndHttp() = runTest {
        val generation = testGeneration()
        var engineCalls = 0
        var capturedSize = 0
        var capturedCap = 0
        var httpCalls = 0
        val engine = OversqliteRequestGzipEngine { decoded, cap ->
            engineCalls++
            capturedSize = decoded.size
            capturedCap = cap
            null
        }
        val http = HttpClient(MockEngine {
            httpCalls++
            respond("{}", HttpStatusCode.BadRequest)
        }) { defaultRequest { url("https://sync.test/") } }
        val api = OversqliteRemoteApi(http, Json, generation = { generation }, requestGzipEngine = engine, log = {})
        try {
            // Connect has a fixed small body, so it cannot reach the control cap.
            assertFailsWith<DownloadHttpException> { api.connect("source-a", false) }
            assertEquals(1, httpCalls)
            assertEquals(0, engineCalls)
            val connectExact = api.prepareConnectBody("x".repeat(65_536))
            assertEquals(65_536, connectExact.decodedByteCount)
            assertEquals(65_536, capturedSize)
            assertEquals(65_535, capturedCap)
            assertEquals(1, engineCalls)
            assertEquals(1, httpCalls)
            assertFailsWith<IllegalArgumentException> {
                api.prepareConnectBody("x".repeat(65_537))
            }
            assertEquals(1, engineCalls)
            assertEquals(1, httpCalls)

            val emptyReason = SnapshotSessionCreateRequest(
                SnapshotSourceReplacement("source-a", "source-b", ""),
            )
            val baseSize = Json.encodeToString(SnapshotSessionCreateRequest.serializer(), emptyReason)
                .encodeToByteArray().size
            val reason = "x".repeat(65_536 - baseSize)
            fun snapshot(extra: String = "") = SnapshotSessionCreateRequest(
                SnapshotSourceReplacement("source-a", "source-b", reason + extra),
            )
            assertFails { api.createSnapshotSession("source-a", snapshot()) }
            assertEquals(65_536, capturedSize)
            assertEquals(65_535, capturedCap)
            assertEquals(2, engineCalls)
            assertEquals(2, httpCalls)
            assertFailsWith<IllegalArgumentException> {
                api.createSnapshotSession("source-a", snapshot("x"))
            }
            assertEquals(2, engineCalls)
            assertEquals(2, httpCalls)

            val row = PushRequestRow(
                "main", "users", mapOf("id" to "u1"), "INSERT", 0,
                buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("x".repeat(4_096))) },
            )
            suspend fun push(limit: Long) {
                api.push("source-a", 1, "a".repeat(64), null, listOf(row), limit, 1_048_576)
            }
            assertFailsWith<UploadHttpException> { push(1_048_576) }
            val pushSize = capturedSize
            assertTrue(pushSize > oversqliteGzipThresholdBytes)
            assertFailsWith<UploadHttpException> { push(pushSize.toLong()) }
            assertEquals(pushSize - 1, capturedCap)
            assertEquals(4, engineCalls)
            assertEquals(4, httpCalls)
            assertFailsWith<IllegalArgumentException> { push(pushSize.toLong() - 1L) }
            assertEquals(4, engineCalls)
            assertEquals(4, httpCalls)
        } finally {
            http.close()
        }
    }

    @Test
    fun pushDoesNotStartHttpAfterCompressionFailureOrAdmissionFailure() = runTest {
        val generation = testGeneration()
        var requests = 0
        val http = HttpClient(MockEngine {
            requests++
            respond("{}")
        }) { defaultRequest { url("https://sync.test/") } }
        val row = PushRequestRow(
            "main", "users", mapOf("id" to "u1"), "INSERT", 0,
            buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("x".repeat(4_096))) },
        )
        suspend fun push(engine: OversqliteRequestGzipEngine, limit: Long) {
            OversqliteRemoteApi(http, Json, generation = { generation }, requestGzipEngine = engine, log = {}).push(
                sourceId = "source-a",
                sourceBundleId = 1,
                canonicalRequestHash = "a".repeat(64),
                initializationId = null,
                rows = listOf(row),
                maxRequestDecodedBytes = limit,
                maxResponseDecodedBytes = 1_048_576,
            )
        }
        try {
            assertFailsWith<OversqliteRequestCompressionException> {
                push(OversqliteRequestGzipEngine { _, _ -> error("engine failed") }, 1_048_576)
            }
            assertEquals(0, requests)
            var engineCalls = 0
            assertFailsWith<IllegalArgumentException> {
                push(OversqliteRequestGzipEngine { _, _ -> engineCalls++; null }, 100)
            }
            assertEquals(0, engineCalls)
            assertEquals(0, requests)
            assertFailsWith<CancellationException> {
                push(OversqliteRequestGzipEngine { _, _ -> throw CancellationException("stop") }, 1_048_576)
            }
            assertEquals(0, requests)
        } finally {
            http.close()
        }
    }

    @Test
    fun preparedBytesAndHeadersAgreeOnPushAndConnect() = runTest {
        val generation = testGeneration()
        val requests = mutableListOf<HttpRequestData>()
        val http = HttpClient(MockEngine { request ->
            requests += request
            respond("", HttpStatusCode.BadRequest)
        }) { defaultRequest { url("https://sync.test/") } }
        val api = OversqliteRemoteApi(http, Json, generation = { generation }, log = {})
        try {
            assertFailsWith<DownloadHttpException> { api.connect("source-a", false) }
            val connect = requests.single()
            assertNull(connect.headers[HttpHeaders.ContentEncoding])
            val connectBytes = (connect.body as OutgoingContent.ByteArrayContent).bytes()
            assertTrue(connectBytes.decodeToString().contains("has_local_pending_rows"))

            val row = PushRequestRow(
                "main", "users", mapOf("id" to "u1"), "INSERT", 0,
                buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("x".repeat(4_096))) },
            )
            assertFailsWith<UploadHttpException> {
                api.push("source-a", 1, "a".repeat(64), null, listOf(row), 1_048_576, 1_048_576)
            }
            val push = requests.last()
            assertEquals(2, requests.size)
            assertEquals("gzip", push.headers[HttpHeaders.ContentEncoding])
            val bytes = (push.body as OutgoingContent.ByteArrayContent).bytes()
            assertTrue(bytes.size < 4_096)
            push.headers[HttpHeaders.ContentLength]?.let { assertEquals(bytes.size.toString(), it) }
            if (!oversqliteFetchManagesResponseCompression) {
                val decoded = decodeOversqliteSingleMemberGzip(bytes, 1_048_576)
                assertTrue(decoded.decodeToString().contains("canonical_request_hash"))
            }
        } finally {
            http.close()
        }
    }
}
