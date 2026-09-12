package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.*
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/** Pure contract checks, not execution of a persistence or graph implementation. */
internal class SharedDependencyPlanningContractTest {
    @Test
    fun dependencyPlanningContract() {
        val fixture = Json.parseToJsonElement(oversqliteContractFixture("local-planning/dependency-planning.json").readText()).jsonObject
        assertEquals(1, fixture.getValue("formatVersion").jsonPrimitive.int)
        assertEquals("contract-candidate-runtime-pending", fixture.getValue("status").jsonPrimitive.content)
        assertFalse(fixture.getValue("httpChanged").jsonPrimitive.boolean)
        assertEquals(listOf("state-discriminators", "full-base-copy-witnesses", "hard-boundary-arithmetic", "resume-guard-equality", "scenario-ownership", "rekey-relationship-ordering"),
            fixture.getValue("executedContractChecks").jsonArray.map { it.jsonPrimitive.content })
        val witnesses = fixture.getValue("baseWitnesses").jsonArray
        assertEquals(4, witnesses.size)
        for (entry in witnesses) {
            val row = entry.jsonObject
            val event = row.getValue("event").jsonPrimitive.content
            assertTrue(event in listOf("local_edit", "requeue", "settlement_rebase", "snapshot_preserve_outbox"))
            val wantA = if (event in listOf("settlement_rebase", "snapshot_preserve_outbox")) row.getValue("server") else row.getValue("beforeA")
            val wantB = if (event == "settlement_rebase") row.getValue("server") else row.getValue("beforeB")
            assertEquals(wantA, row.getValue("afterA"), event)
            assertEquals(wantB, row.getValue("afterB"), event)
            assertTrue(row.getValue("local") != row.getValue("beforeB"))
            assertEquals(7, row.getValue("beforeB").jsonObject.size)
        }
        val states = fixture.getValue("stateKinds").jsonArray.map { it.jsonObject }
        assertEquals(listOf("live", "deleted", "absent", "unpublished", "unknown"), states.map { it.getValue("kind").jsonPrimitive.content })
        for (state in states) {
            val kind = state.getValue("kind").jsonPrimitive.content
            assertEquals(if (kind == "unknown") "unknown" else "known", state.getValue("relationshipKnowledge").jsonPrimitive.content)
            assertEquals(kind == "live", state.getValue("hasPayload").jsonPrimitive.boolean)
            assertEquals(kind == "live" || kind == "deleted", state.getValue("hasVersion").jsonPrimitive.boolean)
        }
        val boundaries = fixture.getValue("hardBoundaryCases").jsonArray
        assertEquals(6, boundaries.size)
        for (entry in boundaries) {
            val row = entry.jsonObject
            val rows = row.getValue("componentRows").jsonPrimitive.long
            val bytes = row.getValue("componentDecodedBytes").jsonPrimitive.long
            val maxRows = row.getValue("maxRows").jsonPrimitive.long
            val maxBytes = row.getValue("maxBytes").jsonPrimitive.long
            assertTrue(rows > 1 && bytes > 0 && maxRows > 0 && maxBytes > 0)
            val result = when { rows > maxRows -> "rows"; bytes > maxBytes -> "decoded_bytes"; else -> "fits" }
            assertEquals(row.getValue("expected").jsonPrimitive.content, result, row.getValue("id").jsonPrimitive.content)
        }
        val resumes = fixture.getValue("resumeCases").jsonArray
        assertEquals(4, resumes.size)
        for (entry in resumes) {
            val row = entry.jsonObject
            assertEquals(row.getValue("saved") == row.getValue("current"), row.getValue("resume").jsonPrimitive.boolean)
        }
        val scenarios = fixture.getValue("runtimeScenarios").jsonArray.map { it.jsonObject }
        val ids = scenarios.map { it.getValue("id").jsonPrimitive.content }.toSet()
        assertEquals(33, scenarios.size)
        assertEquals(scenarios.size, ids.size)
        for (row in scenarios) {
            assertTrue(row.getValue("owner").jsonPrimitive.content in listOf("baseline-lifecycle", "dependency-planning", "public-sync-live"))
            assertTrue(row.getValue("given").jsonPrimitive.content.isNotEmpty())
            assertTrue(row.getValue("expected").jsonPrimitive.content.isNotEmpty())
            assertFalse(row.getValue("executed").jsonPrimitive.boolean, "ownership is not runtime proof")
        }
        assertTrue(ids.containsAll(listOf("typed-base", "snapshot-outbox", "conflict-rebase", "format-rejection", "beyond-hard-lookahead", "independent-pairs", "oversized-plus-independent", "diagnostics", "one-call-live", "rekey-without-dependents", "rekey-unrelated-insert", "rekey-chain-coalescing", "rekey-restart", "rekey-partial-failure")))
        // Bounded acyclic contract model; not SQLite capture/restart or a runtime SCC planner.
        val rekeys = fixture.getValue("rekeyWitnesses").jsonArray
        assertEquals(6, rekeys.size)
        for (input in rekeys) {
            // Only serialized persisted facts participate; human-readable edit history does not.
            val witness = Json.parseToJsonElement(input.toString()).jsonObject
            val rows = witness.getValue("rows").jsonArray.map { it.jsonObject }
            assertTrue(rows.size <= 4)
            fun JsonObject.text(name: String) = getValue(name).jsonPrimitive.content
            val pending = rows.map { it.text("key") }.toMutableSet()
            assertEquals(rows.size, pending.size)
            val live = witness.getValue("serverLive").jsonArray.map { it.jsonPrimitive.content }.toSet()
            for (row in rows) {
                val key = row.text("key")
                assertEquals(1, key.length) // Fixture labels, not production key encoding.
                assertTrue(row.text("op") in listOf("upsert", "delete"))
                val serverParent = row.text("serverParent")
                if (serverParent.isNotEmpty()) {
                    assertTrue(key in live, "server-parent owner must be live: $key")
                    assertTrue(serverParent in live, "server-parent target must be live: $serverParent")
                }
            }
            val edges = mutableListOf<String>()
            for (row in rows) {
                val key = row.text("key")
                val parent = row.text("desiredParent")
                if (row.text("op") == "upsert" && parent.isNotEmpty() && parent !in live) {
                    assertTrue(parent in pending)
                    edges += "$key>$parent"
                }
                if (row.text("op") == "delete") for (child in rows) {
                    if (child.text("serverParent") == key && (child.text("op") == "delete" || child.text("desiredParent") != key)) {
                        edges += "$key>${child.text("key")}"
                    }
                }
            }
            assertEquals(witness.getValue("expectedEdges").jsonArray.map { it.jsonPrimitive.content }, edges.sorted())
            val ordered = rows.sortedBy { it.getValue("ordinal").jsonPrimitive.int }
            val batches = mutableListOf<List<String>>()
            repeat(rows.size) {
                val ready = ordered.firstOrNull { row ->
                    val key = row.text("key")
                    key in pending && edges.none { it.substringBefore('>') == key && it.substringAfter('>') in pending }
                }
                assertTrue(ready != null, "witness must be acyclic")
                val key = ready.text("key")
                pending.remove(key)
                batches += listOf(key) // Hard row limit is one.
            }
            assertEquals(witness.getValue("expectedBatches").jsonArray.map { batch -> batch.jsonArray.map { it.jsonPrimitive.content } }, batches)
        }
        val classification = Json.parseToJsonElement(oversqliteContractFixture("CLASSIFICATIONS.json").readText()).jsonObject
        val candidate = classification.getValue("localContractCandidates").jsonArray.map { it.jsonObject }.single { it.getValue("contract").jsonPrimitive.content == "dependency-planning" }
        assertEquals("local-planning/dependency-planning.json", candidate.getValue("path").jsonPrimitive.content)
        assertEquals("dependency-planning", candidate.getValue("contract").jsonPrimitive.content)
        assertEquals("oversync/dependency_planning_contract_test.go:TestContractCorpus_DependencyPlanning", candidate.getValue("goConsumer").jsonPrimitive.content)
        assertEquals("SharedDependencyPlanningContractTest.dependencyPlanningContract", candidate.getValue("jvmConsumer").jsonPrimitive.content)
        assertEquals(listOf("baseline-lifecycle", "dependency-planning", "public-sync-live"), candidate.getValue("runtimeOwners").jsonArray.map { it.jsonPrimitive.content })
    }
}
