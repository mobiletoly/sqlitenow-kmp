package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import io.ktor.http.HttpStatusCode
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import kotlinx.serialization.encodeToString
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

class RemoteApiReviewRegressionTest {
    @Test
    fun strictObjectScannerAcceptsJsonScalarsAndRejectsInvalidTokens() {
        requireUniqueSnapshotJsonObjectMembers(
            """{"yes":true,"no":false,"nothing":null,"zero":-0,"fraction":1.25e-3}""",
        )
        listOf("""{"value":oops}""", """{"value":01}""", """{"value":1e}""").forEach { raw ->
            assertFailsWith<IllegalArgumentException> { requireUniqueSnapshotJsonObjectMembers(raw) }
        }
    }

    @Test
    fun nonJsonSnapshotErrorsRetainHttpStatusAndCategory() = runTest {
        for (status in listOf(HttpStatusCode.Unauthorized, HttpStatusCode.Forbidden, HttpStatusCode.BadGateway)) {
            val http = client(status, "Invalid token\n")
            try {
                val remote = api(http)
                val failures = listOf(
                    assertFailsWith<SnapshotHttpException> { remote.createSnapshotSession("source-a") },
                    assertFailsWith<SnapshotHttpException> {
                        remote.fetchSnapshotChunk("session", "source-a", 1, 0, 1, 4096, 4096)
                    },
                )
                for (error in failures) {
                    assertEquals(status, error.status)
                    assertEquals(if (status == HttpStatusCode.BadGateway) OversqliteErrorCategory.NETWORK
                        else OversqliteErrorCategory.AUTH, error.category)
                    assertFalse(error.message.orEmpty().contains("Invalid token"))
                }
            } finally { http.close() }
        }
    }

    @Test
    fun malformedSnapshotJsonRemainsProtocolFailure() = runTest {
        for (status in listOf(HttpStatusCode.OK, HttpStatusCode.Unauthorized)) {
            for (body in listOf("{\"error\":oops}", "[]", "{\"error\":\"a\",\"error\":\"b\"}")) {
                val http = client(status, body)
                try {
                    val error = runCatching { api(http).createSnapshotSession("source-a") }.exceptionOrNull()
                    assertEquals(OversqliteErrorCategory.PROTOCOL, (error as? OversqliteCategorizedException)?.category)
                } finally { http.close() }
            }
        }
    }

    private fun client(status: HttpStatusCode, body: String): HttpClient = HttpClient(MockEngine {
        respond(body, status)
    }) {
        defaultRequest { url("https://sync.test/") }
    }

    private fun api(http: HttpClient): OversqliteRemoteApi =
        OversqliteRemoteApi(http, Json { ignoreUnknownKeys = true }, generation = { testGeneration() }, log = {})

    private suspend fun OversqliteRemoteApi.pushOne(maxBytes: Long = 4096): CompletePushResult = push(
        sourceId = "source-a",
        sourceBundleId = 1,
        canonicalRequestHash = "a".repeat(64),
        initializationId = null,
        rows = listOf(PushRequestRow(
            schema = "main", table = "users", key = mapOf("id" to "u1"),
            op = "INSERT", baseRowVersion = 0, payload = Json.parseToJsonElement("""{"id":"u1"}"""),
        )),
        maxRequestDecodedBytes = oversqlitePushResponseBodyHardLimit,
        maxResponseDecodedBytes = maxBytes,
    )

    @Test
    fun actualHttpErrorsPreserveTerminalCodesAndRetryability() = runTest {
        val cases = listOf(
            Triple(HttpStatusCode.Conflict, "push_replay_mismatch", false),
            Triple(HttpStatusCode.UnprocessableEntity, "bundle_projection_too_large", false),
            Triple(HttpStatusCode.PayloadTooLarge, "push_outcome_too_large", false),
            Triple(HttpStatusCode.ServiceUnavailable, "service_not_ready", true),
        )
        cases.forEach { (status, code, retryable) ->
            val http = client(status, """{"error":"$code","message":"safe"}""")
            try {
                val error = assertFailsWith<OversqliteRemoteContractException> { api(http).pushOne() }
                assertEquals(code, error.code)
                assertEquals(retryable, error.retryable)
                assertEquals(!retryable, error.isTerminalSyncCompatibilityFailure())
            } finally {
                http.close()
            }
        }
        val http = client(HttpStatusCode.InternalServerError,
            """{"error":"committed_bundle_unrepresentable","message":"safe"}""")
        try {
            val error = assertFailsWith<OversqliteRemoteContractException> {
                api(http).sendPullRequest(0, 1, 0, "source-a", 4096)
            }
            assertEquals("committed_bundle_unrepresentable", error.code)
            assertFalse(error.retryable)
        } finally {
            http.close()
        }
    }

    @Test
    fun malformedRecognizedHttpErrorIsProtocolFailure() = runTest {
        val http = client(HttpStatusCode.InternalServerError,
            """{"error":"committed_bundle_unrepresentable"}""")
        try {
            val error = assertFailsWith<RemoteResponseSemanticException> {
                api(http).sendPullRequest(0, 1, 0, "source-a", 4096)
            }
            assertEquals(OversqliteErrorCategory.PROTOCOL, error.category)
            assertEquals(true, error.isTerminalSyncCompatibilityFailure())
        } finally {
            http.close()
        }
    }

    @Test
    fun malformedJsonShapedHttpErrorsDoNotBecomeRetryableTransportFailures() = runTest {
        val pullBodies = listOf(
            """{"error":"committed_bundle_unrepresentable","message":oops}""",
            """{"error":"committed_bundle_unrepresentable","message":"safe","extra":oops}""",
            "{\"error\":\"committed_bundle_unrepresentable\",\"message\":\"safe",
            "[]",
            "\"upstream\"",
            "123",
            "true",
        )
        pullBodies.forEach { body ->
            val http = client(HttpStatusCode.InternalServerError, body)
            try {
                val error = assertFailsWith<RemoteResponseDecodeException> {
                    api(http).sendPullRequest(0, 1, 0, "source-a", 4096)
                }
                assertEquals(OversqliteErrorCategory.PROTOCOL, error.category)
                assertEquals(true, error.isTerminalSyncCompatibilityFailure())
            } finally {
                http.close()
            }
        }
        val pushHttp = client(HttpStatusCode.InternalServerError,
            """{"error":"push_failed","message":oops}""")
        try {
            val error = assertFailsWith<RemoteResponseDecodeException> { api(pushHttp).pushOne() }
            assertEquals(OversqliteErrorCategory.PROTOCOL, error.category)
            assertEquals(true, error.isTerminalSyncCompatibilityFailure())
        } finally {
            pushHttp.close()
        }
    }

    @Test
    fun nonJsonHttpFailureRetainsTransportFallback() = runTest {
        val http = client(HttpStatusCode.InternalServerError, "upstream-unavailable")
        try {
            assertFailsWith<DownloadHttpException> {
                api(http).sendPullRequest(0, 1, 0, "source-a", 4096)
            }
        } finally {
            http.close()
        }
    }

    @Test
    fun malformedUtf8BodyRemainsAProtocolDecodeFailure() = runTest {
        val invalidBodies = listOf(
            byteArrayOf(0xc0.toByte(), 0x80.toByte()),
            byteArrayOf(0xed.toByte(), 0xa0.toByte(), 0x80.toByte()),
            byteArrayOf(0xf4.toByte(), 0x90.toByte(), 0x80.toByte(), 0x80.toByte()),
            byteArrayOf(0xe2.toByte(), 0x82.toByte()),
        )
        invalidBodies.forEach { bytes ->
            val http = HttpClient(MockEngine { respond(bytes, HttpStatusCode.OK) }) {
                defaultRequest { url("https://sync.test/") }
            }
            try {
                assertFailsWith<RemoteResponseDecodeException> {
                    api(http).sendPullRequest(0, 1, 0, "source-a", 4096)
                }
            } finally {
                http.close()
            }
        }
    }

    @Test
    fun actualHttpSuccessBodiesRespectAdmittedAndRequestedLimits() = runTest {
        val oversized = "x".repeat(65)
        val pushHttp = client(HttpStatusCode.OK, oversized)
        try {
            assertEquals(64, assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api(pushHttp).pushOne(64)
            }.limit)
        } finally {
            pushHttp.close()
        }
        val pullHttp = client(HttpStatusCode.OK, oversized)
        try {
            assertEquals(64, assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api(pullHttp).sendPullRequest(0, 1, 0, "source-a", 64)
            }.limit)
        } finally {
            pullHttp.close()
        }
        val snapshotHttp = client(HttpStatusCode.OK, oversized)
        try {
            assertEquals(32, assertFailsWith<SnapshotResponseBodyTooLargeException> {
                api(snapshotHttp).fetchSnapshotChunk(
                    snapshotId = "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaaa", sourceId = "source-a",
                    snapshotBundleSeq = 1, afterRowOrdinal = 0, maxRows = 1,
                    maxBytes = 32, admittedMaxBytes = 64,
                )
            }.limit)
        } finally {
            snapshotHttp.close()
        }
    }

    @Test
    fun pullRejectsRemovedAndCaseCollidingBundleFields() = runTest {
        val base = """{"stable_bundle_seq":1,"bundles":[{"bundle_seq":1,"source_id":"source-a","source_bundle_id":1,"bundle_hash":"${"a".repeat(64)}","rows":[]%s}],"has_more":false}"""
        listOf(",\"row_count\":0", ",\"Rows\":[]").forEach { extra ->
            val http = client(HttpStatusCode.OK, base.replace("%s", extra))
            try {
                assertFailsWith<RemoteResponseDecodeException> {
                    api(http).sendPullRequest(0, 1, 0, "source-a", 4096)
                }
            } finally {
                http.close()
            }
        }
    }

    @Test
    fun conflictRejectsExtraNestedAndOuterFields() = runTest {
        val generation=testGeneration()
        val detail=PushConflictDetails("main","users",mapOf("id" to "u1"),"i",0,1,false,
            Json.parseToJsonElement("""{"id":"u1","name":"server"}"""))
        val unsealed=TerminalPushOutcome("source-a",generation.id,generation.fingerprint,1,"a".repeat(64),null,
            "rejected","",rejection=PushOutcomeRejection("push_conflict",detail))
        val outcome=unsealed.copy(outcomeHash=hashPushOutcome(unsealed,generation))
        val fields=Json.encodeToJsonElement(outcome).jsonObject.toMutableMap()
        fields.remove("committed")
        val valid=JsonObject(fields)
        val rejection=valid.getValue("rejection").jsonObject
        listOf(
            JsonObject(fields+ ("rejection" to JsonObject(rejection+("conflict" to JsonObject(rejection.getValue("conflict").jsonObject+("row_count" to JsonPrimitive(1))))))),
            JsonObject(fields+("Message" to JsonPrimitive("safe"))),
        ).forEach { body ->
            val http = client(HttpStatusCode.OK, body.toString())
            try {
                val failure=assertFailsWith<RemoteResponseDecodeException> { api(http).pushOne() }
                assertEquals(OversqliteErrorCategory.PROTOCOL,failure.category)
                assertTrue(failure.isTerminalSyncCompatibilityFailure())
            } finally {
                http.close()
            }
        }
    }
}
