package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.*
import kotlin.test.*

/** Local decision vectors only; callers and Swift remain separate runtime obligations. */
internal class SharedPendingContractTest {
    @Test
    fun contract() {
        val fixture = localApplyContractFixture("a3-pending")
        fixture.assertContractFields("formatVersion classification family cases runtimeExecuted")
        executeContractCases(fixture, listOf("detail-relationship_unknown", "detail-dependency_unavailable", "detail-historical_repair_required", "sample-count", "sample-bytes", "precedence-cancel", "precedence-wire", "precedence-missing-base", "precedence-capacity", "precedence-work", "precedence-mandatory", "precedence-unknown", "precedence-replay-first"), ::pendingResult)
    }

    private fun pendingResult(kind: String, input: JsonObject): JsonObject = when (kind) {
        "pending" -> {
            input.assertContractFields("detail unit refs progress")
            val detail = input.contractText("detail")
            assertTrue(detail in listOf("relationship_unknown", "dependency_unavailable", "historical_repair_required"))
            val unit = input.contractText("unit"); assertTrue(unit.matches(Regex("[0-9a-f]{64}")))
            val progress = input.getValue("progress").jsonObject
            progress.assertContractFields("dirtyRows outboundRows committedBundles")
            listOf("dirtyRows", "outboundRows", "committedBundles").forEach { progress.contractNumber(it) }
            val sample = mutableListOf<JsonElement>(); var bytes = 2; var more = false
            for (value in input.getValue("refs").jsonArray) {
                val ref = value.jsonObject; ref.assertContractFields("schema table key")
                assertTrue(ref.contractText("schema").isNotEmpty() && ref.contractText("table").isNotEmpty())
                val key = ref.getValue("key").jsonObject; key.assertContractFields("storage value")
                assertEquals("text", key.contractText("storage")); key.contractText("value")
                val required = canonicalContractValue(ref).toString().encodeToByteArray().size + if (sample.isEmpty()) 0 else 1
                if (sample.size == 16 || required > 4096 - bytes) { more = true; continue }
                sample += ref; bytes += required
            }
            buildJsonObject { put("detail", detail); put("unit", unit); put("refs", JsonArray(sample))
                put("hasMore", more); put("progress", progress) }
        }
        "precedence" -> {
            input.assertContractFields("flags state")
            val flags = input.getValue("flags").jsonArray.map { it.jsonPrimitive.content }
            assertEquals(flags.size, flags.toSet().size)
            val precedence = linkedMapOf("cancelled" to "cancelled", "frozen_outbox" to "replay", "malformed" to "integrity",
                "missing_base" to "resolution_base_unavailable", "capacity" to "sync_workspace_limit",
                "work" to "sync_work_limit", "mandatory_missing" to "integrity", "unknown" to "relationship_unknown")
            assertTrue(flags.all { it in precedence })
            val result = precedence.entries.first { it.key in flags }.value
            val state = input.getValue("state").jsonObject
            state.assertContractFields("A B L clean dirty outbox sourceNext checkpoint confirmedPrefix")
            buildJsonObject { put("result", result); put("state", state); put("automaticActions", JsonArray(emptyList())) }
        }
        else -> error("unsupported A3 pending case kind: $kind")
    }
}
