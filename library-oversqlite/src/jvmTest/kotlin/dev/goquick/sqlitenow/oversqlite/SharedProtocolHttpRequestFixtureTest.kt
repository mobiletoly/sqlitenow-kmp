package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SharedProtocolHttpRequestFixtureTest {
    private val fixture = oversqliteContractFixture("protocol-http/requests.json")
    private val root = Json.parseToJsonElement(fixture.readText()).jsonObject

    @Test
    fun currentRequestMatrixUsesOnlyDirectProtocolRoutes() {
        assertEquals(2, root.getValue("formatVersion").jsonPrimitive.int)
        val expectedOperations = setOf(
            "discovery", "connect", "push", "pull", "snapshotSession", "snapshotChunk", "snapshotRelease", "watch",
        )
        val cases = root.getValue("cases").jsonArray.map { it.jsonObject }
        assertEquals(expectedOperations, cases.map { it.getValue("operation").jsonPrimitive.content }.toSet())
        val normativePush = Json.parseToJsonElement(
            oversqliteContractFixture("push/basic-insert.json").readText(),
        ).jsonObject.getValue("cases").jsonArray.first().jsonObject.getValue("pushRequest").jsonObject
        cases.forEach { case ->
            val expected = case.getValue("expected").jsonObject
            val actual = reduceRequest(case, normativePush)
            assertEquals(expected.getValue("method"), JsonPrimitive(actual.method))
            assertEquals(expected.getValue("path"), JsonPrimitive(actual.path))
            assertEquals(expected.getValue("query"), JsonObject(actual.query.mapValues { JsonPrimitive(it.value) }))
            assertEquals(expected.getValue("sourceHeader"), JsonPrimitive(actual.sourceHeader))
            assertEquals(expected.getValue("body"), actual.body)
            expected["bodyMustNotContain"]?.jsonArray?.forEach { forbidden ->
                assertFalse(actual.body.toString().contains(forbidden.jsonPrimitive.content))
            }
        }
        val push = cases.single { it.getValue("operation").jsonPrimitive.content == "push" }.getValue("expected").jsonObject
        assertEquals("POST", push.getValue("method").jsonPrimitive.content)
        assertEquals("/api/v1/sync/push", push.getValue("path").jsonPrimitive.content)
        val pushBody = push.getValue("body").jsonObject
        assertEquals(setOf("source_bundle_id", "canonical_request_hash", "rows"), pushBody.keys)
        assertTrue(pushBody.getValue("rows").jsonArray.isNotEmpty())
        val normativePushHash = normativePush.getValue("canonical_request_hash").jsonPrimitive.content
        assertEquals(normativePushHash, pushBody.getValue("canonical_request_hash").jsonPrimitive.content)
    }

    private data class ReducedRequest(
        val method: String,
        val path: String,
        val query: Map<String, String>,
        val sourceHeader: String,
        val body: JsonElement,
    )

    private fun reduceRequest(case: JsonObject, normativePush: JsonObject): ReducedRequest {
        val operation = case.getValue("operation").jsonPrimitive.content
        val source = case.getValue("sourceId").jsonPrimitive.content
        val args = case["args"]?.jsonObject ?: JsonObject(emptyMap())
        return when (operation) {
            "discovery" -> ReducedRequest("GET", "/api/v1/sync/generations/${args.getValue("generationId").jsonPrimitive.content}", emptyMap(), source, JsonNull)
            "connect" -> ReducedRequest(
                "POST",
                "/api/v1/sync/connect",
                emptyMap(),
                source,
                JsonObject(mapOf("has_local_pending_rows" to args.getValue("hasLocalPendingRows"))),
            )
            "push" -> ReducedRequest(
                "POST",
                "/api/v1/sync/push",
                emptyMap(),
                source,
                JsonObject(normativePush + ("source_bundle_id" to JsonPrimitive(args.getValue("sourceBundleId").jsonPrimitive.long))),
            )
            "pull" -> ReducedRequest(
                "GET",
                "/api/v1/sync/pull",
                mapOf(
                    "after_bundle_seq" to args.getValue("afterBundleSeq").jsonPrimitive.content,
                    "max_bundles" to args.getValue("maxBundles").jsonPrimitive.content,
                    "target_bundle_seq" to args.getValue("targetBundleSeq").jsonPrimitive.content,
                ),
                source,
                JsonNull,
            )
            "snapshotSession" -> ReducedRequest("POST", "/api/v1/sync/snapshot-sessions", emptyMap(), source, JsonNull)
            "snapshotChunk" -> ReducedRequest(
                "GET",
                "/api/v1/sync/snapshot-sessions/${args.getValue("snapshotId").jsonPrimitive.content}",
                mapOf(
                    "after_row_ordinal" to args.getValue("afterRowOrdinal").jsonPrimitive.content,
                    "max_rows" to args.getValue("maxRows").jsonPrimitive.content,
                    "max_bytes" to args.getValue("maxBytes").jsonPrimitive.content,
                ),
                source,
                JsonNull,
            )
            "snapshotRelease" -> ReducedRequest("DELETE", "/api/v1/sync/snapshot-sessions/${args.getValue("snapshotId").jsonPrimitive.content}", emptyMap(), source, JsonNull)
            "watch" -> ReducedRequest(
                "GET",
                "/api/v1/sync/watch",
                mapOf("after_bundle_seq" to args.getValue("afterBundleSeq").jsonPrimitive.content),
                source,
                JsonNull,
            )
            else -> error("unknown protocol request operation $operation")
        }
    }

    @Test
    fun removedOperationsAreAssertionsNotExecutableCases() {
        val removed = root.getValue("removedOperations").jsonArray.map { it.jsonPrimitive.content }.toSet()
        assertEquals(
            setOf("capabilities", "pushSessionCreate", "pushSessionChunk", "pushSessionCommit", "committedRows"),
            removed,
        )
        val active = root.getValue("cases").jsonArray.map { it.jsonObject.getValue("operation").jsonPrimitive.content }
        assertTrue(active.none(removed::contains))
        val pushPath = root.getValue("cases").jsonArray.map { it.jsonObject }
            .single { it.getValue("operation").jsonPrimitive.content == "push" }
            .getValue("expected").jsonObject.getValue("path").jsonPrimitive.content
        assertFalse(pushPath.contains("push-sessions"))
    }
}
