package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.HttpClient
import io.ktor.client.engine.mock.MockEngine
import io.ktor.client.engine.mock.respond
import io.ktor.client.plugins.defaultRequest
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.*
import kotlin.test.*

class RecoveryProtocolTest {
    @Test fun nullableInitializationIsPresentOnResolveWire() {
        val wire = Json { explicitNulls = true }.encodeToString(PushOutcomeResolveRequest(1, "a".repeat(64), null))
        assertTrue(Json.parseToJsonElement(wire).jsonObject.containsKey("initialization_id"))
        assertEquals(JsonNull, Json.parseToJsonElement(wire).jsonObject.getValue("initialization_id"))
    }
    @Test fun snapshotObjectsRejectMissingNullQuotedAndUnknownFloor() {
        for (raw in listOf("""{"recovery":{}}""", """{"recovery":null}""", """{"recovery":{"minimum_bundle_seq":"0"}}""",
            """{"recovery":{"minimum_bundle_seq":0,"outcome":{}}}""")) assertFails { decodeRecoverySnapshotRequest(raw) }
        assertEquals(Long.MAX_VALUE, decodeRecoverySnapshotRequest("""{"recovery":{"minimum_bundle_seq":9223372036854775807}}""").recovery!!.minimumBundleSeq)
    }
    @Test fun actualSdkProtocolHeaderFollowsDefaultRequestAndFailsClosed() = runTest {
        for (configured in listOf<String?>(null, "v1", "v2", " v1")) {
            var requests = 0
            val http = HttpClient(MockEngine { request ->
                requests++
                assertEquals(listOf("v1"), request.headers.getAll("Oversync-Protocol-Version"))
                respond("""{"resolution":"initialize_empty"}""")
            }) {
                defaultRequest {
                    url("https://sync.test/")
                    if (configured != null) headers.append("Oversync-Protocol-Version", configured)
                }
            }
            try {
                val api = OversqliteRemoteApi(http, Json, generation = { testGeneration() }, log = {})
                val result = runCatching { api.connect("source-a", false) }
                assertEquals(configured == null || configured == "v1", result.isSuccess)
                assertEquals(if (configured == null || configured == "v1") 1 else 0, requests)
            } finally { http.close() }
        }
    }
}
