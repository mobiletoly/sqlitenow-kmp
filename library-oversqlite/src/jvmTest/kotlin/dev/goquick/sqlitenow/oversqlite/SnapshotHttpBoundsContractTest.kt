/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package dev.goquick.sqlitenow.oversqlite

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import io.ktor.client.engine.cio.endpoint
import io.ktor.client.plugins.compression.ContentEncoding
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.HttpStatusCode
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.DeflaterOutputStream
import java.util.zip.GZIPOutputStream
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

class SnapshotHttpBoundsContractTest : BundleClientContractTestSupport() {
    private companion object {
        const val streamingProbeBytes = 32 * 1024 * 1024
        const val retirementTimeoutProbeMillis = 500L
        const val retirementTimeoutCompletionBoundMillis = 5_000L
    }

    @Test
    fun gzipExpansionIsBoundedAfterDecodingAndUnsupportedEncodingIsRejected() = runBlocking {
        val oversized = """{"snapshot_id":"oversized","snapshot_bundle_seq":1,"row_count":1,"byte_count":1,"expires_at":"2099-01-01T00:00:00Z","padding":"${"x".repeat(65_536)}"}"""
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondBytes(
                        exchange = exchange,
                        status = 200,
                        bytes = gzip(oversized.encodeToByteArray()),
                        contentEncoding = "gzip",
                    )
                }
            },
        ) { api ->
            assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api.createSnapshotSession("source")
            }
        }

        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondBytes(
                        exchange = exchange,
                        status = 200,
                        bytes = "{}".encodeToByteArray(),
                        contentEncoding = "br",
                    )
                }
            },
        ) { api ->
            assertFailsWith<SnapshotUnsupportedContentEncodingException> {
                api.createSnapshotSession("source")
            }
        }
    }

    @Test
    fun explicitIdentityEncodingIsAccepted() = runBlocking {
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondBytes(
                        exchange = exchange,
                        status = 200,
                        bytes = """{"snapshot_id":"identity","snapshot_bundle_seq":0,"row_count":0,"byte_count":0,"expires_at":"2099-01-01T00:00:00Z"}"""
                            .encodeToByteArray(),
                        contentEncoding = "identity",
                    )
                }
            },
        ) { api ->
            assertEquals("identity", api.createSnapshotSession("source").snapshotId)
        }
    }

    @Test
    fun boundedSnapshotErrorsAndDecodeFailuresRedactRemoteBodyText() = runBlocking {
        val sentinel = "customer-secret-sentinel"
        val logs = mutableListOf<String>()
        withSnapshotApi(
            log = logs::add,
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondJson(exchange, 500, """{"error":"$sentinel","message":"$sentinel"}""")
                }
            },
        ) { api ->
            val error = assertFailsWith<SnapshotHttpException> {
                api.createSnapshotSession("source")
            }
            assertEquals("invalid_error_response", error.errorCode)
            assertTrue(sentinel !in error.toString())
        }
        assertTrue(logs.none { sentinel in it })

        logs.clear()
        withSnapshotApi(
            log = logs::add,
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondJson(exchange, 200, """{"snapshot_id":"$sentinel","broken":true}""")
                }
            },
        ) { api ->
            val error = assertFailsWith<SnapshotResponseDecodeException> {
                api.createSnapshotSession("source")
            }
            assertTrue(sentinel !in error.toString())
        }
        assertTrue(logs.none { sentinel in it })
    }

    @Test
    fun snapshotSessionLimitErrorIsStructuredRedactedAndValidated() = runBlocking {
        val sentinel = "customer-secret-snapshot-limit"
        val logs = mutableListOf<String>()
        withSnapshotApi(
            log = logs::add,
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondJson(
                        exchange,
                        409,
                        """{"error":"snapshot_session_limit_exceeded","message":"$sentinel","dimension":"byte_count","actual":257,"limit":256}""",
                    )
                }
            },
        ) { api ->
            val error = assertFailsWith<SnapshotSessionLimitExceededException> {
                api.createSnapshotSession("source")
            }
            assertEquals("byte_count", error.dimension)
            assertEquals(257L, error.actual)
            assertEquals(256L, error.limit)
            assertTrue(sentinel !in error.toString())
        }
        assertTrue(logs.none { sentinel in it })

        val malformedBodies = listOf(
            """{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"other","actual":2,"limit":1}""",
            """{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"row_count","actual":1,"limit":1}""",
            """{"error":"snapshot_session_limit_exceeded","message":"hostile","dimension":"row_byte_count","actual":1,"limit":0}""",
        )
        for (body in malformedBodies) {
            withSnapshotApi(
                configure = {
                    createContext("/sync/snapshot-sessions") { exchange ->
                        respondJson(exchange, 409, body)
                    }
                },
            ) { api ->
                val error = assertFailsWith<SnapshotHttpException> {
                    api.createSnapshotSession("source")
                }
                assertEquals("invalid_error_response", error.errorCode)
                assertTrue("hostile" !in error.toString())
            }
        }
    }

    @Test
    fun bundleChangeWatchLinesAndCompleteEventsAreBounded() = runBlocking {
        val repeated = "x".repeat(bundleChangeWatchMaxEventBytes / 2)
        val cases = listOf(
            "line" to ("x".repeat(bundleChangeWatchMaxLineBytes + 1) + "\n"),
            "event" to ("event: bundle\ndata: $repeated\ndata: $repeated\n\n"),
        )
        for ((name, stream) in cases) {
            val received = AtomicInteger()
            withSnapshotApi(
                configure = {
                    createContext("/sync/watch") { exchange ->
                        val bytes = stream.encodeToByteArray()
                        exchange.responseHeaders.add("Content-Type", "text/event-stream")
                        exchange.sendResponseHeaders(200, bytes.size.toLong())
                        exchange.responseBody.use { it.write(bytes) }
                    }
                },
            ) { api ->
                assertFailsWith<RemoteResponseDecodeException>(name) {
                    api.watchBundleChanges("source", 0) { received.incrementAndGet() }
                }
            }
            assertEquals(0, received.get(), name)
        }
    }

    @Test
    fun duplicateSnapshotMembersAreRejectedBeforeLossyObjectDecoding() = runBlocking {
        val sentinel = "customer-secret-member"
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """{"snapshot_id":"$sentinel","snapshot_id":"other","snapshot_bundle_seq":0,"row_count":0,"byte_count":0,"expires_at":"2099-01-01T00:00:00Z"}""",
                    )
                }
            },
        ) { api ->
            val error = assertFailsWith<SnapshotSemanticException> {
                api.createSnapshotSession("source")
            }
            assertEquals(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER, error.failure)
            assertEquals(null, error.cause)
            assertTrue(sentinel !in error.toString())
        }

        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """
                        {
                          "snapshot_id":"snapshot",
                          "snapshot_bundle_seq":1,
                          "rows":[{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"$sentinel","\u0069d":"other"},
                            "row_version":1,
                            "payload":{"id":"$sentinel","name":"Ada"}
                          }],
                          "next_row_ordinal":1,
                          "has_more":false,
                          "byte_count":32
                        }
                        """.trimIndent(),
                    )
                }
            },
        ) { api ->
            val error = assertFailsWith<SnapshotSemanticException> {
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 32)
            }
            assertEquals(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER, error.failure)
            assertTrue(sentinel !in error.toString())
        }
    }

    @Test
    fun malformedUtf8IsRejectedBeforeChunkAndControlJsonParsingWithoutDisclosure() = runBlocking {
        val sentinel = "customer-secret-invalid-utf8"
        val logs = mutableListOf<String>()
        val chunkBody = AtomicReference<ByteArray>()
        val malformedChunkCases = listOf(
            RawBodyCase(
                "chunk string value",
                invalidUtf8Between(
                    """{"snapshot_id":"snapshot","snapshot_bundle_seq":1,"rows":[{"schema":"$sentinel""" + "-",
                    """","table":"users","key":{"id":"1"},"row_version":1,"payload":{"id":"1"}}],"next_row_ordinal":1,"has_more":false,"byte_count":1}""",
                ),
            ),
            RawBodyCase(
                "key name",
                invalidUtf8Between(
                    """{"snapshot_id":"snapshot","snapshot_bundle_seq":1,"rows":[{"schema":"main","table":"users","key":{"$sentinel""" + "-",
                    """":"1"},"row_version":1,"payload":{"id":"1"}}],"next_row_ordinal":1,"has_more":false,"byte_count":1}""",
                ),
            ),
            RawBodyCase(
                "key value",
                invalidUtf8Between(
                    """{"snapshot_id":"snapshot","snapshot_bundle_seq":1,"rows":[{"schema":"main","table":"users","key":{"id":"$sentinel""" + "-",
                    """"},"row_version":1,"payload":{"id":"1"}}],"next_row_ordinal":1,"has_more":false,"byte_count":1}""",
                ),
            ),
            RawBodyCase(
                "payload name",
                invalidUtf8Between(
                    """{"snapshot_id":"snapshot","snapshot_bundle_seq":1,"rows":[{"schema":"main","table":"users","key":{"id":"1"},"row_version":1,"payload":{"$sentinel""" + "-",
                    """":"value"}}],"next_row_ordinal":1,"has_more":false,"byte_count":1}""",
                ),
            ),
            RawBodyCase(
                "payload value",
                invalidUtf8Between(
                    """{"snapshot_id":"snapshot","snapshot_bundle_seq":1,"rows":[{"schema":"main","table":"users","key":{"id":"1"},"row_version":1,"payload":{"name":"$sentinel""" + "-",
                    """"}}],"next_row_ordinal":1,"has_more":false,"byte_count":1}""",
                ),
            ),
        )

        withSnapshotApi(
            log = logs::add,
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    respondBytes(exchange, 200, chunkBody.get())
                }
            },
        ) { api ->
            for (case in malformedChunkCases) {
                chunkBody.set(case.body)
                val error = assertFailsWith<SnapshotResponseDecodeException>(case.name) {
                    api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 4_096)
                }
                assertThrowableTreeRedacted(error, sentinel, case.name)
            }
        }
        assertTrue(logs.none { sentinel in it })

        logs.clear()
        val controlBody = invalidUtf8Between(
            """{"error":"remote_failure","message":"$sentinel""" + "-",
            "\"}",
        )
        withSnapshotApi(
            log = logs::add,
            configure = {
                createContext("/sync/pull") { exchange ->
                    respondBytes(exchange, 500, controlBody)
                }
            },
        ) { api ->
            val error = assertFailsWith<RemoteResponseDecodeException> {
                api.sendPullRequest(0, 1, 0, "source")
            }
            assertThrowableTreeRedacted(error, sentinel, "bounded control error response")
        }
        assertTrue(logs.none { sentinel in it })
    }

    @Test
    fun chunkEnvelopeAndEveryFixedRowMemberRejectExactAndDecodedNameDuplicates() = runBlocking {
        val cases = listOf(
            ChunkJsonCase("envelope snapshot_id exact", chunkJson(duplicateAfter = "snapshot_id" to "\"snapshot_id\":\"other\"")),
            ChunkJsonCase("envelope snapshot_id escaped", chunkJson(duplicateAfter = "snapshot_id" to "\"\\u0073napshot_id\":\"other\"")),
            ChunkJsonCase("envelope snapshot_bundle_seq exact", chunkJson(duplicateAfter = "snapshot_bundle_seq" to "\"snapshot_bundle_seq\":1")),
            ChunkJsonCase("envelope snapshot_bundle_seq escaped", chunkJson(duplicateAfter = "snapshot_bundle_seq" to "\"snapshot_\\u0062undle_seq\":1")),
            ChunkJsonCase("envelope rows exact", chunkJson(duplicateAfter = "rows" to "\"rows\":[]")),
            ChunkJsonCase("envelope rows escaped", chunkJson(duplicateAfter = "rows" to "\"r\\u006fws\":[]")),
            ChunkJsonCase("envelope next_row_ordinal exact", chunkJson(duplicateAfter = "next_row_ordinal" to "\"next_row_ordinal\":0")),
            ChunkJsonCase("envelope next_row_ordinal escaped", chunkJson(duplicateAfter = "next_row_ordinal" to "\"next_row_\\u006frdinal\":0")),
            ChunkJsonCase("envelope has_more exact", chunkJson(duplicateAfter = "has_more" to "\"has_more\":false")),
            ChunkJsonCase("envelope has_more escaped", chunkJson(duplicateAfter = "has_more" to "\"h\\u0061s_more\":false")),
            ChunkJsonCase("envelope byte_count exact", chunkJson(duplicateAfter = "byte_count" to "\"byte_count\":0")),
            ChunkJsonCase("envelope byte_count escaped", chunkJson(duplicateAfter = "byte_count" to "\"byte_c\\u006funt\":0")),
            ChunkJsonCase("row schema exact", chunkJson(rowJson = rowJson(duplicateAfter = "schema" to "\"schema\":\"other\""))),
            ChunkJsonCase("row schema escaped", chunkJson(rowJson = rowJson(duplicateAfter = "schema" to "\"\\u0073chema\":\"other\""))),
            ChunkJsonCase("row table exact", chunkJson(rowJson = rowJson(duplicateAfter = "table" to "\"table\":\"other\""))),
            ChunkJsonCase("row table escaped", chunkJson(rowJson = rowJson(duplicateAfter = "table" to "\"t\\u0061ble\":\"other\""))),
            ChunkJsonCase("row key exact", chunkJson(rowJson = rowJson(duplicateAfter = "key" to "\"key\":{\"id\":\"other\"}"))),
            ChunkJsonCase("row key escaped", chunkJson(rowJson = rowJson(duplicateAfter = "key" to "\"k\\u0065y\":{\"id\":\"other\"}"))),
            ChunkJsonCase("row row_version exact", chunkJson(rowJson = rowJson(duplicateAfter = "row_version" to "\"row_version\":2"))),
            ChunkJsonCase("row row_version escaped", chunkJson(rowJson = rowJson(duplicateAfter = "row_version" to "\"row_\\u0076ersion\":2"))),
            ChunkJsonCase("row payload exact", chunkJson(rowJson = rowJson(duplicateAfter = "payload" to "\"payload\":{\"id\":\"other\"}"))),
            ChunkJsonCase("row payload escaped", chunkJson(rowJson = rowJson(duplicateAfter = "payload" to "\"p\\u0061yload\":{\"id\":\"other\"}"))),
        )
        assertChunkCasesFailWithDuplicate(cases)
    }

    @Test
    fun keyPayloadAndNestedPayloadObjectsRejectExactAndDecodedNameDuplicates() = runBlocking {
        val cases = listOf(
            ChunkJsonCase(
                "key exact",
                chunkJson(rowJson = rowJson(keyJson = "{\"id\":\"1\",\"id\":\"2\"}")),
            ),
            ChunkJsonCase(
                "key escaped",
                chunkJson(rowJson = rowJson(keyJson = "{\"id\":\"1\",\"\\u0069d\":\"2\"}")),
            ),
            ChunkJsonCase(
                "payload exact",
                chunkJson(rowJson = rowJson(payloadJson = "{\"id\":\"1\",\"id\":\"2\"}")),
            ),
            ChunkJsonCase(
                "payload escaped",
                chunkJson(rowJson = rowJson(payloadJson = "{\"id\":\"1\",\"\\u0069d\":\"2\"}")),
            ),
            ChunkJsonCase(
                "nested payload object exact",
                chunkJson(rowJson = rowJson(payloadJson = "{\"outer\":{\"name\":\"a\",\"name\":\"b\"}}")),
            ),
            ChunkJsonCase(
                "nested payload object escaped",
                chunkJson(rowJson = rowJson(payloadJson = "{\"outer\":{\"name\":\"a\",\"\\u006eame\":\"b\"}}")),
            ),
            ChunkJsonCase(
                "array-contained payload object exact",
                chunkJson(rowJson = rowJson(payloadJson = "{\"items\":[{\"name\":\"a\",\"name\":\"b\"}]}")),
            ),
            ChunkJsonCase(
                "array-contained payload object escaped",
                chunkJson(rowJson = rowJson(payloadJson = "{\"items\":[{\"name\":\"a\",\"\\u006eame\":\"b\"}]}")),
            ),
        )
        assertChunkCasesFailWithDuplicate(cases)
    }

    @Test
    fun chunkEnvelopeAndRowsRejectEveryUnknownOrMissingFixedMember() = runBlocking {
        val cases = buildList {
            add(ChunkJsonCase("unknown envelope member", chunkJson(extraMember = "\"unknown\":true")))
            for (field in listOf("snapshot_id", "snapshot_bundle_seq", "rows", "next_row_ordinal", "has_more", "byte_count")) {
                add(ChunkJsonCase("missing envelope $field", chunkJson(omitMember = field)))
            }
            add(ChunkJsonCase("unknown row member", chunkJson(rowJson = rowJson(extraMember = "\"unknown\":true"))))
            for (field in listOf("schema", "table", "key", "row_version", "payload")) {
                add(ChunkJsonCase("missing row $field", chunkJson(rowJson = rowJson(omitMember = field))))
            }
        }
        val body = AtomicReference<String>()
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    respondBytes(exchange, 200, body.get().encodeToByteArray())
                }
            },
        ) { api ->
            for (case in cases) {
                body.set(case.body)
                if (case.name == "missing envelope byte_count") {
                    val error = assertFailsWith<SnapshotSemanticException>(case.name) {
                        api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 4_096)
                    }
                    assertEquals(SnapshotSemanticFailure.INVALID_CHUNK, error.failure)
                } else {
                    assertFailsWith<SnapshotResponseDecodeException>(case.name) {
                        api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 4_096)
                    }
                }
            }
        }
    }

    @Test
    fun chunkNestingDepthUsesTheLockedZeroBased128Limit() = runBlocking {
        val body = AtomicReference<String>()
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    respondBytes(exchange, 200, body.get().encodeToByteArray())
                }
            },
        ) { api ->
            body.set(chunkJson(rowJson = rowJson(payloadJson = nestedPayloadJson(arrayDepth = 124))))
            assertEquals(
                1,
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 32_768).rows.size,
                "root object is depth 0 and the deepest primitive is depth 128",
            )

            body.set(chunkJson(rowJson = rowJson(payloadJson = nestedPayloadJson(arrayDepth = 125))))
            val error = assertFailsWith<SnapshotSemanticException> {
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 32_768)
            }
            assertEquals(SnapshotSemanticFailure.EXCESSIVE_NESTING, error.failure)
        }
    }

    @Test
    fun chunkBodyExactlyAtLimitPassesAndOneByteAboveFails() = runBlocking {
        val maxRows = 1
        val maxBytes = 1L
        val limit = maxBytes + maxRows + 64L * 1024L
        val minimal = chunkJson().encodeToByteArray()
        val exact = minimal + ByteArray((limit - minimal.size).toInt()) { ' '.code.toByte() }
        val body = AtomicReference(exact)
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    respondBytes(exchange, 200, body.get())
                }
            },
        ) { api ->
            assertEquals(0, api.fetchSnapshotChunk("snapshot", "source", 1, 0, maxRows, maxBytes).rows.size)

            body.set(exact + byteArrayOf(' '.code.toByte()))
            val error = assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, maxRows, maxBytes)
            }
            assertEquals(limit, error.limit)
        }
    }

    @Test
    fun validLargeLogicalChunkUsesTheBoundedChunkIngressPath() = runBlocking {
        val rowCount = 1_000
        val rows = (1..rowCount).joinToString(",") { id ->
            rowJson(
                keyJson = "{\"id\":\"$id\"}",
                payloadJson = "{\"id\":\"$id\",\"content\":\"${"x".repeat(256)}\"}",
            )
        }
        val body = chunkJson(
            rowsJson = "[$rows]",
            nextRowOrdinal = rowCount.toLong(),
            byteCount = rowCount.toLong(),
        )
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    respondBytes(exchange, 200, body.encodeToByteArray())
                }
            },
        ) { api ->
            val chunk = api.fetchSnapshotChunk(
                snapshotId = "snapshot",
                sourceId = "source",
                snapshotBundleSeq = 1,
                afterRowOrdinal = 0,
                maxRows = rowCount,
                maxBytes = 1_000_000,
            )
            assertEquals(rowCount, chunk.rows.size)
            assertEquals("1000", chunk.rows.last().key["id"])
        }
    }

    @Test
    fun derivedClientForcesBoundedStatusHandlingWhenCallerExpectsSuccess() = runBlocking {
        val server = HttpServer.create(java.net.InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/sync/capabilities") { exchange ->
            respondJson(exchange, 500, "x".repeat(65_537))
        }
        server.start()
        val base = HttpClient(CIO) {
            expectSuccess = true
            defaultRequest {
                url("http://127.0.0.1:${server.address.port}")
            }
        }
        val db = dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val client = newClient(db, base, listOf(SyncTable("users", syncKeyColumnName = "id")))
        try {
            client.open().getOrThrow()
            val error = assertIs<SnapshotResponseBodyTooLargeException>(client.attach("user-1").exceptionOrNull())
            assertEquals(64L * 1024L, error.limit)
        } finally {
            client.close()
            base.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun snapshotIdsAreEncodedAsOneStrictPathSegmentIncludingDotsAndUnicode() = runBlocking {
        val cases = listOf(
            "a.b/c?d#e%f" to "a%2Eb%2Fc%3Fd%23e%25f",
            ".." to "%2E%2E",
            "雪.é" to "%E9%9B%AA%2E%C3%A9",
        )

        for ((snapshotId, encodedSnapshotId) in cases) {
            val fetchPath = AtomicReference<String>()
            val deletePath = AtomicReference<String>()
            withSnapshotApi(
                configure = {
                    createContext("/sync/snapshot-sessions") { exchange ->
                        when (exchange.requestMethod) {
                            "GET" -> {
                                fetchPath.set(exchange.requestURI.rawPath)
                                respondJson(
                                    exchange,
                                    200,
                                    """{"snapshot_id":"$snapshotId","snapshot_bundle_seq":0,"rows":[],"next_row_ordinal":0,"has_more":false,"byte_count":0}""",
                                )
                            }
                            "DELETE" -> {
                                deletePath.set(exchange.requestURI.rawPath)
                                exchange.sendResponseHeaders(204, -1)
                                exchange.close()
                            }
                            else -> error("unexpected method")
                        }
                    }
                },
            ) { api ->
                api.fetchSnapshotChunk(snapshotId, "source", 0, 0, 1, 1)
                api.deleteSnapshotSessionBestEffort(snapshotId, "source")
            }

            val expected = "/sync/snapshot-sessions/$encodedSnapshotId"
            assertEquals(expected, fetchPath.get(), snapshotId)
            assertEquals(expected, deletePath.get(), snapshotId)
            assertEquals(encodedSnapshotId, encodeOversqliteSessionIdPathSegment(snapshotId), snapshotId)
        }
    }

    @Test
    fun ordinaryHttpErrorsAndDecodeFailuresKeepRemoteTextOutOfErrorsAndLogs() = runBlocking {
        val sentinel = "customer-secret-http-body"
        val logs = mutableListOf<String>()
        val connectAttempts = AtomicInteger()
        withSnapshotApi(
            log = logs::add,
            configure = {
                createContext("/sync/connect") { exchange ->
                    if (connectAttempts.incrementAndGet() == 1) {
                        respondJson(exchange, 500, """{"error":"remote_failure","message":"$sentinel"}""")
                    } else {
                        respondJson(exchange, 200, """{"resolution":{"value":"$sentinel"}}""")
                    }
                }
                createContext("/sync/pull") { exchange ->
                    respondJson(exchange, 409, """{"error":"history_pruned","message":"$sentinel"}""")
                }
                createContext("/sync/watch") { exchange ->
                    respondJson(exchange, 500, """{"error":"watch_failed","message":"$sentinel"}""")
                }
            },
        ) { api ->
            val connectError = assertFailsWith<DownloadHttpException> {
                api.connect("source", hasLocalPendingRows = false)
            }
            assertEquals("", connectError.rawBody)
            assertTrue(sentinel !in connectError.toString())

            val decodeError = assertFailsWith<RemoteResponseDecodeException> {
                api.connect("source", hasLocalPendingRows = false)
            }
            assertEquals(null, decodeError.cause)
            assertTrue(sentinel !in decodeError.toString())

            val historyError = assertFailsWith<HistoryPrunedException> {
                api.sendPullRequest(0, 1, 0, "source")
            }
            assertEquals("remote history is no longer available", historyError.message)

            val watchError = assertFailsWith<DownloadHttpException> {
                api.watchBundleChanges("source", 0) { }
            }
            assertEquals("", watchError.rawBody)
            assertTrue(sentinel !in watchError.toString())
        }
        assertTrue(logs.none { sentinel in it })
    }

    @Test
    fun invalidSourceIdsFailBeforeAnyOutgoingRequestOrReplacementBody() = runBlocking {
        val requests = AtomicInteger()
        withSnapshotApi(
            configure = {
                createContext("/") { exchange ->
                    requests.incrementAndGet()
                    respondJson(exchange, 500, "{}")
                }
            },
        ) { api ->
            assertFailsWith<InvalidOversqliteSourceIdException> {
                api.fetchCapabilities(" invalid")
            }
            assertFailsWith<InvalidOversqliteSourceIdException> {
                api.createSnapshotSession(
                    sourceId = "source",
                    request = SnapshotSessionCreateRequest(
                        sourceReplacement = SnapshotSourceReplacement(
                            previousSourceId = "source",
                            newSourceId = "invalid source",
                            reason = "source_retired",
                        ),
                    ),
                )
            }
        }
        assertEquals(0, requests.get())
    }

    @Test
    fun derivedClientReplacesPreconfiguredResponseDecodersWithGzipAndIdentity() = runBlocking<Unit> {
        val server = HttpServer.create(java.net.InetSocketAddress("127.0.0.1", 0), 0)
        server.createContext("/sync/capabilities") { exchange ->
            respondBytes(
                exchange = exchange,
                status = 200,
                bytes = deflate(capabilitiesJson(mapOf("connect_lifecycle" to true)).encodeToByteArray()),
                contentEncoding = "deflate",
            )
        }
        server.start()
        val base = newHttpClient(server).config {
            install(ContentEncoding) {
                deflate()
                identity()
            }
        }
        val db = dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        try {
            val client = newClient(db, base, listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.open().getOrThrow()
            assertIs<SnapshotUnsupportedContentEncodingException>(client.attach("user-1").exceptionOrNull())
        } finally {
            base.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun oversizedCapabilitiesBodyClosesTheTransportBeforeTheServerFinishesWriting() = runBlocking {
        val probe = StreamingResponseProbe(streamingProbeBytes)
        withSnapshotApi(
            configure = {
                createContext("/sync/capabilities") { exchange ->
                    probe.respond(exchange)
                }
            },
        ) { api ->
            assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api.fetchCapabilities("source")
            }
            probe.assertClosedEarly()
        }
    }

    @Test
    fun cancellationClosesAStreamingChunkResponseBeforeTheServerFinishesWriting() = runBlocking {
        val probe = StreamingResponseProbe(streamingProbeBytes)
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    probe.respond(exchange)
                }
            },
        ) { api ->
            val request = async(Dispatchers.Default) {
                api.fetchSnapshotChunk(
                    snapshotId = "snapshot",
                    sourceId = "source",
                    snapshotBundleSeq = 1,
                    afterRowOrdinal = 0,
                    maxRows = 1,
                    maxBytes = streamingProbeBytes.toLong(),
                )
            }
            assertTrue(probe.firstWrite.await(10, TimeUnit.SECONDS), "server did not begin the chunk response")
            request.cancelAndJoin()
            probe.assertClosedEarly()
        }
    }

    @Test
    fun retirementDoesNotDrainTheResponseBody() = runBlocking {
        val probe = StreamingResponseProbe(streamingProbeBytes)
        val logs = mutableListOf<String>()
        withSnapshotApi(
            log = logs::add,
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    assertEquals("DELETE", exchange.requestMethod)
                    probe.respond(exchange)
                }
            },
        ) { api ->
            api.deleteSnapshotSessionBestEffort("snapshot", "source")
            probe.assertClosedEarly()
        }
        assertTrue(logs.none { "best-effort failure" in it })
    }

    @Test
    fun retryableResponseReleasesTheSingleConnectionForTheNextAttempt() = runBlocking {
        val attempts = AtomicInteger()
        withSnapshotApi(
            singleConnection = true,
            configure = {
                createContext("/sync/capabilities") { exchange ->
                    when (attempts.incrementAndGet()) {
                        1 -> respondJson(
                            exchange,
                            429,
                            """{"error":"snapshot_build_capacity","message":"retry"}""",
                        )
                        else -> respondJson(exchange, 200, capabilitiesJson(emptyMap()))
                    }
                }
            },
        ) { api ->
            assertFailsWith<SnapshotCapacityException> {
                api.fetchCapabilities("source")
            }
            withTimeout(2_000) {
                api.fetchCapabilities("source")
            }
            assertEquals(2, attempts.get())
        }
    }

    @Test
    fun retirementTimeoutPreservesPrimarySuccessAndFailure() = runBlocking {
        assertEquals(5_000L, defaultSnapshotRetirementTimeoutMillis)

        val successProbe = HangingResponseProbe()
        withSnapshotApi(
            retirementTimeoutMillis = retirementTimeoutProbeMillis,
            configure = {
                createContext("/sync/snapshot-sessions/success") { exchange ->
                    successProbe.respond(exchange)
                }
            },
        ) { api ->
            val startedNanos = System.nanoTime()
            val result = try {
                "primary-result"
            } finally {
                api.deleteSnapshotSessionBestEffort("success", "source")
            }
            assertEquals("primary-result", result)
            assertTrue(
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos) <
                    retirementTimeoutCompletionBoundMillis,
                "best-effort retirement exceeded its completion bound",
            )
            successProbe.assertStarted()
            successProbe.release()
        }

        val failureProbe = HangingResponseProbe()
        withSnapshotApi(
            retirementTimeoutMillis = retirementTimeoutProbeMillis,
            configure = {
                createContext("/sync/snapshot-sessions/failure") { exchange ->
                    failureProbe.respond(exchange)
                }
            },
        ) { api ->
            val primary = IllegalStateException("primary failure")
            val startedNanos = System.nanoTime()
            val error = runCatching {
                try {
                    throw primary
                } finally {
                    api.deleteSnapshotSessionBestEffort("failure", "source")
                }
            }.exceptionOrNull()
            assertSame(primary, error)
            assertTrue(
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos) <
                    retirementTimeoutCompletionBoundMillis,
                "best-effort retirement exceeded its completion bound",
            )
            failureProbe.assertStarted()
            failureProbe.release()
        }
    }

    @Test
    fun cancelledCallerStillAttemptsNonCancellableRetirement() = runBlocking {
        val probe = HangingResponseProbe()
        withSnapshotApi(
            retirementTimeoutMillis = retirementTimeoutProbeMillis,
            configure = {
                createContext("/sync/snapshot-sessions/cancelled") { exchange ->
                    probe.respond(exchange)
                }
            },
        ) { api ->
            val entered = CompletableDeferred<Unit>()
            val request = async(Dispatchers.Default) {
                entered.complete(Unit)
                try {
                    awaitCancellation()
                } finally {
                    api.deleteSnapshotSessionBestEffort("cancelled", "source")
                }
            }
            entered.await()
            request.cancelAndJoin()
            assertTrue(request.isCancelled)
            probe.assertStarted()
            probe.release()
        }
    }

    @Test
    fun capabilitiesCreateChunkAndErrorBodiesUseIndependentLimits() = runBlocking {
        withSnapshotApi(
            configure = {
                createContext("/sync/capabilities") { exchange ->
                    respondJson(exchange, 500, """{"error":"terminal","message":"${"x".repeat(65_536)}"}""")
                }
            },
        ) { api ->
            val error = assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api.fetchCapabilities("source")
            }
            assertEquals(64L * 1024L, error.limit)
        }

        val maxBytes = 32L
        val maxRows = 2
        val exactChunkLimit = maxBytes + maxRows + (64L * 1024L)
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    respondJson(exchange, 200, "x".repeat(exactChunkLimit.toInt() + 1))
                }
            },
        ) { api ->
            val error = assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, maxRows, maxBytes)
            }
            assertEquals(exactChunkLimit, error.limit)
        }
    }

    @Test
    fun ordinaryPullSuccessIsNotCappedAtSnapshotControlLimit() = runBlocking {
        val largeSourceId = "x".repeat(80 * 1024)
        withSnapshotApi(
            configure = {
                createContext("/sync/pull") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """{"stable_bundle_seq":1,"bundles":[{"bundle_seq":1,"source_id":"$largeSourceId","source_bundle_id":1,"rows":[]}],"has_more":false}""",
                    )
                }
            },
        ) { api ->
            val response = api.sendPullRequest(0, 1000, 0, "source")
            assertEquals(1, response.stableBundleSeq)
        }
    }

    @Test
    fun onlyLockedCapacityCodesRetryAndTooSmallIsActionable() = runBlocking {
        for (code in listOf("snapshot_build_capacity", "snapshot_chunk_capacity")) {
            withSnapshotApi(
                configure = {
                    createContext("/sync/capabilities") { exchange ->
                        respondJson(exchange, 429, """{"error":"$code","message":"busy"}""")
                    }
                },
            ) { api ->
                assertIs<SnapshotCapacityException>(runCatching { api.fetchCapabilities("source") }.exceptionOrNull())
            }
        }
        withSnapshotApi(
            configure = {
                createContext("/sync/capabilities") { exchange ->
                    respondJson(exchange, 429, """{"error":"snapshot_capacity","message":"legacy"}""")
                }
            },
        ) { api ->
            assertIs<SnapshotHttpException>(runCatching { api.fetchCapabilities("source") }.exceptionOrNull())
        }
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    respondJson(
                        exchange,
                        400,
                        """{"error":"snapshot_chunk_too_small","message":"increase budget","required_byte_count":257}""",
                    )
                }
            },
        ) { api ->
            val error = assertFailsWith<SnapshotChunkTooSmallException> {
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 256)
            }
            assertEquals(257, error.requiredBytes)
        }
    }

    @Test
    fun createdSessionIsRetiredWhenSuccessPayloadCannotDecode() = runBlocking {
        val retirements = AtomicInteger()
        withSnapshotApi(
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondJson(
                        exchange,
                        200,
                        """{"snapshot_id":"retire-me","snapshot_bundle_seq":1,"row_count":1,"byte_count":1}""",
                    )
                }
                createContext("/sync/snapshot-sessions/retire-me") { exchange ->
                    assertEquals("DELETE", exchange.requestMethod)
                    retirements.incrementAndGet()
                    exchange.sendResponseHeaders(204, -1)
                    exchange.close()
                }
            },
        ) { api ->
            assertTrue(runCatching { api.createSnapshotSession("source") }.isFailure)
            assertEquals(1, retirements.get())
        }
    }

    @Test
    fun diagnosticsCountCompleteRetryAndTerminalBodiesButNotOversize() = runBlocking {
        val attempts = AtomicInteger()
        val recorder = SnapshotDiagnosticsRecorder().also { it.markRestoreAttempt() }
        val retryBody = """{"error":"snapshot_chunk_capacity","message":"retry"}"""
        val terminalBody = """{"error":"terminal","message":"stop-now"}"""
        withSnapshotApi(
            recorder = recorder,
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    when (attempts.incrementAndGet()) {
                        1 -> respondJson(exchange, 429, retryBody)
                        2 -> respondJson(exchange, 409, terminalBody)
                        else -> respondJson(exchange, 500, "x".repeat(65_537))
                    }
                }
            },
        ) { api ->
            assertFailsWith<SnapshotCapacityException> {
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 256)
            }
            assertFailsWith<SnapshotHttpException> {
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 256)
            }
            val beforeOversize = recorder.snapshot().maxCompletelyDecodedChunkBodyBytes
            assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 256)
            }
            assertEquals(
                maxOf(retryBody.encodeToByteArray().size, terminalBody.encodeToByteArray().size).toLong(),
                beforeOversize,
            )
            assertEquals(beforeOversize, recorder.snapshot().maxCompletelyDecodedChunkBodyBytes)
        }
    }

    @Test
    fun diagnosticsExcludeTransportPartialBodies() = runBlocking {
        val recorder = SnapshotDiagnosticsRecorder().also { it.markRestoreAttempt() }
        withSnapshotApi(
            recorder = recorder,
            configure = {
                createContext("/sync/snapshot-sessions/snapshot") { exchange ->
                    val partial = """{"snapshot_id":"snapshot""".encodeToByteArray()
                    exchange.responseHeaders.add("Content-Type", "application/json")
                    exchange.sendResponseHeaders(200, partial.size.toLong() + 100L)
                    exchange.responseBody.write(partial)
                    exchange.responseBody.close()
                }
            },
        ) { api ->
            assertTrue(
                runCatching {
                    api.fetchSnapshotChunk("snapshot", "source", 1, 0, 1, 256)
                }.isFailure,
            )
            assertEquals(0, recorder.snapshot().maxCompletelyDecodedChunkBodyBytes)
        }
    }

    private suspend fun withSnapshotApi(
        recorder: SnapshotDiagnosticsRecorder? = null,
        retirementTimeoutMillis: Long = defaultSnapshotRetirementTimeoutMillis,
        singleConnection: Boolean = false,
        log: ((String) -> Unit)? = null,
        configure: HttpServer.() -> Unit,
        block: suspend (OversqliteRemoteApi) -> Unit,
    ) {
        val server = HttpServer.create(java.net.InetSocketAddress("127.0.0.1", 0), 0).apply(configure)
        server.start()
        val base = if (singleConnection) {
            HttpClient(CIO) {
                engine {
                    maxConnectionsCount = 1
                    endpoint {
                        maxConnectionsPerRoute = 1
                        pipelineMaxSize = 1
                    }
                }
                defaultRequest {
                    url("http://127.0.0.1:${server.address.port}")
                }
            }
        } else {
            newHttpClient(server)
        }
        val decoded = base.config {
            installOrReplace(ContentEncoding) {
                gzip()
                identity()
            }
        }
        try {
            block(
                OversqliteRemoteApi(
                    http = decoded,
                    json = Json { ignoreUnknownKeys = true },
                    snapshotDiagnostics = { recorder },
                    snapshotRetirementTimeoutMillis = retirementTimeoutMillis,
                    log = { message -> log?.invoke(message()) },
                ),
            )
        } finally {
            decoded.close()
            base.close()
            server.stop(0)
        }
    }

    private suspend fun assertChunkCasesFailWithDuplicate(cases: List<ChunkJsonCase>) {
        val sentinel = "customer-secret-duplicate"
        val logs = mutableListOf<String>()
        val body = AtomicReference<String>()
        withSnapshotApi(
            log = logs::add,
            configure = {
                createContext("/sync/snapshot-sessions") { exchange ->
                    respondBytes(
                        exchange,
                        200,
                        body.get()
                            .replace("\"snapshot_id\":\"snapshot\"", "\"snapshot_id\":\"$sentinel\"")
                            .encodeToByteArray(),
                    )
                }
            },
        ) { api ->
            for (case in cases) {
                body.set(case.body)
                val error = assertFailsWith<SnapshotSemanticException>(case.name) {
                    api.fetchSnapshotChunk(sentinel, "source", 1, 0, 1, 4_096)
                }
                assertEquals(SnapshotSemanticFailure.DUPLICATE_OBJECT_MEMBER, error.failure, case.name)
                assertThrowableTreeRedacted(error, sentinel, case.name)
            }
        }
        assertTrue(logs.none { sentinel in it })
    }

    private fun chunkJson(
        rowJson: String? = null,
        rowsJson: String = rowJson?.let { "[$it]" } ?: "[]",
        nextRowOrdinal: Long = if (rowJson == null && rowsJson == "[]") 0 else 1,
        byteCount: Long = if (rowJson == null && rowsJson == "[]") 0 else 1,
        duplicateAfter: Pair<String, String>? = null,
        omitMember: String? = null,
        extraMember: String? = null,
    ): String {
        val members = listOf(
            "snapshot_id" to "\"snapshot_id\":\"snapshot\"",
            "snapshot_bundle_seq" to "\"snapshot_bundle_seq\":1",
            "rows" to "\"rows\":$rowsJson",
            "next_row_ordinal" to "\"next_row_ordinal\":$nextRowOrdinal",
            "has_more" to "\"has_more\":false",
            "byte_count" to "\"byte_count\":$byteCount",
        )
        return objectWithMemberVariation(members, duplicateAfter, omitMember, extraMember)
    }

    private fun rowJson(
        keyJson: String = "{\"id\":\"1\"}",
        payloadJson: String = "{\"id\":\"1\"}",
        duplicateAfter: Pair<String, String>? = null,
        omitMember: String? = null,
        extraMember: String? = null,
    ): String {
        val members = listOf(
            "schema" to "\"schema\":\"main\"",
            "table" to "\"table\":\"users\"",
            "key" to "\"key\":$keyJson",
            "row_version" to "\"row_version\":1",
            "payload" to "\"payload\":$payloadJson",
        )
        return objectWithMemberVariation(members, duplicateAfter, omitMember, extraMember)
    }

    private fun objectWithMemberVariation(
        members: List<Pair<String, String>>,
        duplicateAfter: Pair<String, String>?,
        omitMember: String?,
        extraMember: String?,
    ): String {
        val rendered = buildList {
            for ((name, member) in members) {
                if (name == omitMember) continue
                add(member)
                if (duplicateAfter?.first == name) add(duplicateAfter.second)
            }
            extraMember?.let(::add)
        }
        return rendered.joinToString(prefix = "{", postfix = "}", separator = ",")
    }

    private fun nestedPayloadJson(arrayDepth: Int): String =
        "{\"nested\":" + "[".repeat(arrayDepth) + "0" + "]".repeat(arrayDepth) + "}"

    private fun invalidUtf8Between(prefix: String, suffix: String): ByteArray =
        prefix.encodeToByteArray() + byteArrayOf(0xC3.toByte(), 0x28) + suffix.encodeToByteArray()

    private fun assertThrowableTreeRedacted(error: Throwable, sentinel: String, context: String) {
        val pending = java.util.ArrayDeque<Throwable>()
        val visited = java.util.Collections.newSetFromMap(java.util.IdentityHashMap<Throwable, Boolean>())
        pending.add(error)
        while (pending.isNotEmpty()) {
            val current = pending.removeFirst()
            if (!visited.add(current)) continue
            assertTrue(sentinel !in current.toString(), "$context leaked through ${current::class.simpleName}")
            current.cause?.let(pending::add)
            current.suppressed.forEach(pending::add)
        }
    }

    private data class RawBodyCase(val name: String, val body: ByteArray)

    private data class ChunkJsonCase(val name: String, val body: String)

    private fun respondBytes(
        exchange: HttpExchange,
        status: Int,
        bytes: ByteArray,
        contentEncoding: String? = null,
    ) {
        exchange.responseHeaders.add("Content-Type", "application/json")
        contentEncoding?.let { exchange.responseHeaders.add("Content-Encoding", it) }
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    private fun gzip(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        GZIPOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private fun deflate(bytes: ByteArray): ByteArray = ByteArrayOutputStream().use { output ->
        DeflaterOutputStream(output).use { it.write(bytes) }
        output.toByteArray()
    }

    private class StreamingResponseProbe(private val totalBytes: Int) {
        val firstWrite = CountDownLatch(1)
        private val finished = CountDownLatch(1)
        private val writtenBytes = AtomicLong()

        fun respond(exchange: HttpExchange) {
            val chunk = ByteArray(16 * 1024) { 'x'.code.toByte() }
            exchange.responseHeaders.add("Content-Type", "application/json")
            try {
                exchange.sendResponseHeaders(200, totalBytes.toLong())
                while (writtenBytes.get() < totalBytes) {
                    val remaining = totalBytes - writtenBytes.get().toInt()
                    val count = minOf(chunk.size, remaining)
                    exchange.responseBody.write(chunk, 0, count)
                    exchange.responseBody.flush()
                    writtenBytes.addAndGet(count.toLong())
                    firstWrite.countDown()
                }
            } catch (_: IOException) {
                // The client closing the bounded streaming response is the expected path.
            } finally {
                exchange.close()
                finished.countDown()
            }
        }

        fun assertClosedEarly() {
            assertTrue(finished.await(10, TimeUnit.SECONDS), "server writer did not observe response closure")
            assertTrue(
                writtenBytes.get() < totalBytes,
                "client drained the complete $totalBytes-byte response",
            )
        }
    }

    private class HangingResponseProbe {
        private val started = CountDownLatch(1)
        private val released = CountDownLatch(1)

        fun respond(exchange: HttpExchange) {
            started.countDown()
            try {
                released.await(10, TimeUnit.SECONDS)
                exchange.sendResponseHeaders(204, -1)
            } catch (_: IOException) {
                // The retirement timeout closing the response is the expected path.
            } finally {
                exchange.close()
            }
        }

        fun assertStarted() {
            assertTrue(started.await(10, TimeUnit.SECONDS), "server did not observe retirement")
        }

        fun release() {
            released.countDown()
        }
    }
}
