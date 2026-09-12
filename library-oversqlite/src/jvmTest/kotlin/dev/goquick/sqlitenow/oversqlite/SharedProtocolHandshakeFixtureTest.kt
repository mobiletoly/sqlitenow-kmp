package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SharedProtocolHandshakeFixtureTest {
    private val fixture = oversqliteContractFixture("protocol-handshake/connect.json")
    private val root = Json.parseToJsonElement(fixture.readText()).jsonObject

    @Test
    fun currentDiscoveryAndConnectCasesDescribeTheDirectProtocol() {
        assertEquals(2, root.getValue("formatVersion").jsonPrimitive.int)
        val allowedDiscovery = setOf("protocol_version", "schema_generation", "limits", "watch_enabled")
        val requiredLimits = setOf(
            "max_push_rows",
            "max_push_decoded_bytes",
            "max_pull_bundles",
            "max_pull_decoded_bytes",
            "max_snapshot_rows_per_chunk",
            "max_snapshot_decoded_bytes_per_chunk",
            "max_snapshot_rows",
            "max_snapshot_logical_bytes",
            "max_row_decoded_bytes",
        )

        root.getValue("discoveryCases").jsonArray.map { it.jsonObject }.forEach { case ->
            val discovery = case.getValue("discovery").jsonObject
            val expectedKind = case.getValue("expected").jsonObject.getValue("kind").jsonPrimitive.content
            val limits = discovery.getValue("limits").jsonObject
            val isAdmissible = discovery.keys == allowedDiscovery &&
                discovery.getValue("protocol_version").jsonPrimitive.content == "v1" &&
                limits.keys == requiredLimits && limits.values.all { it.jsonPrimitive.content.toLong() > 0 }

            if (expectedKind == "ready") {
                assertTrue(isAdmissible, case.getValue("name").jsonPrimitive.content)
                assertEquals(0, case.getValue("expected").jsonObject.getValue("ordinaryOperationPreflights").jsonPrimitive.int)
                assertEquals(setOf("has_local_pending_rows"), case.getValue("connect").jsonObject.keys)
                val tables = discovery.getValue("schema_generation").jsonObject.getValue("tables").jsonArray
                case.getValue("expected").jsonObject.getValue("tableIndices").jsonObject.forEach { (identity, index) ->
                    val table = tables[index.jsonPrimitive.int].jsonObject
                    assertEquals(identity, "${table.getValue("schema").jsonPrimitive.content}.${table.getValue("table").jsonPrimitive.content}")
                }
            } else {
                assertEquals("invalid_admitted_contract", expectedKind)
                assertFalse(isAdmissible, case.getValue("name").jsonPrimitive.content)
            }
        }
    }

    @Test
    fun activeHandshakeFixtureContainsNoLegacyDiscoverySurface() {
        val text = fixture.readText()
        listOf("capabilities", "bundle_limits", "push_session").forEach { legacy ->
            assertFalse(text.contains(legacy), legacy)
        }
    }
}
