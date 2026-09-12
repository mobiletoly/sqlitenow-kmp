/*
 * Copyright 2026 Toly Pochkin
 * SPDX-License-Identifier: Apache-2.0
 */
package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mergeHeaders
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.content.OutgoingContent
import io.ktor.http.headersOf
import io.ktor.utils.io.InternalAPI
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

internal class SnapshotRequestMediaTypeTest {
    @Test
    fun bodylessSnapshotPostDeclaresJsonWithoutAddingBody() = runTest {
        verifyRequest(null, byteArrayOf())
    }

    @Test
    fun explicitSnapshotRequestKeepsItsJsonBodyAndSingleMediaType() = runTest {
        verifyRequest(SnapshotSessionCreateRequest(), "{}".encodeToByteArray())
    }

    @OptIn(InternalAPI::class)
    private suspend fun verifyRequest(request: SnapshotSessionCreateRequest?, expectedBody: ByteArray) {
        var calls = 0
        val generation = testGeneration()
        val http = HttpClient(MockEngine { outgoing ->
            calls++
            assertEquals(HttpMethod.Post, outgoing.method)
            assertEquals("/api/sync/snapshot-sessions", outgoing.url.encodedPath)
            val mediaTypes = mutableListOf<String>()
            mergeHeaders(outgoing.headers, outgoing.body) { name, value ->
                if (name.equals(HttpHeaders.ContentType, ignoreCase = true)) mediaTypes += value
            }
            assertEquals(listOf("application/json"), mediaTypes)
            assertEquals("source-a", outgoing.headers["Oversync-Source-ID"])
            assertEquals(generation.id, outgoing.headers["Oversync-Schema-Generation-ID"])
            assertEquals(generation.fingerprint, outgoing.headers["Oversync-Schema-Generation-Fingerprint"])
            assertNull(outgoing.headers[HttpHeaders.ContentEncoding])
            val bytes = when (val body = outgoing.body) {
                is OutgoingContent.NoContent -> byteArrayOf()
                is OutgoingContent.ByteArrayContent -> body.bytes()
                else -> error("Unexpected snapshot request body: ${body::class}")
            }
            assertContentEquals(expectedBody, bytes)
            respond(
                """{"snapshot_id":"snapshot-media-type","snapshot_bundle_seq":7,"row_count":1,"logical_byte_count":24,"expires_at":"2099-01-01T00:00:00Z"}""",
                headers = headersOf(HttpHeaders.ContentType, ContentType.Application.Json.toString()),
            )
        }) { defaultRequest { url("https://sync.test/api/") } }
        try {
            val session = OversqliteRemoteApi(http, Json, generation = { generation }, log = {})
                .createSnapshotSession("source-a", request)
            assertEquals("snapshot-media-type", session.snapshotId)
            assertEquals(7L, session.snapshotBundleSeq)
            assertEquals(1, calls)
        } finally {
            http.close()
        }
    }
}
