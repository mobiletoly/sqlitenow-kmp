package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.*
import kotlin.io.path.readText
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.test.assertIs
import java.lang.management.ManagementFactory

internal class SharedLocalApplyFixtureTest : CrossTargetSyncTestSupport() {
    @Test
    fun refusedWirePayloadDoesNotAllocateEncodedCopy() = runOwnedWebDatabaseTest {
        val db = newDb()
        val http = MockSyncServer().newHttpClient()
        val resources = OversqliteClientResourceOwner(db, http)
        try {
            initializeOversqliteControlTables(db)
            db.execSQL("INSERT INTO _sync_apply_plan(singleton_key,unit,guard,phase,root_cursor,visit_cursor,table_cursor,fk_cursor,entries,bytes) VALUES(1,'allocation','guard','roots',0,0,0,0,0,0)")
            val config = OversqliteConfig(1, listOf(SyncTable("nodes", "id")), "fixture", "main", maxSyncWorkspaceBytes = 8192)
            val owner = OversqliteApplySurvivors(db,
                OversqliteLocalStore(db, Json) { error("wire admission must not read business rows") }, config,
                OversqliteClientOperationController(config, resources, SnapshotDiagnosticsState()) {}, Json)
            val payload = buildJsonObject { put("name", "x".repeat(8 * 1024 * 1024)) }
            owner.wire("nodes", "C", buildJsonObject { put("name", "warm") })
            val bean = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
            assertTrue(bean.isThreadAllocatedMemorySupported)
            bean.isThreadAllocatedMemoryEnabled = true
            val thread = Thread.currentThread().id
            val before = bean.getThreadAllocatedBytes(thread)
            val error = runCatching { owner.wire("nodes", "C", payload) }.exceptionOrNull()
            val allocated = bean.getThreadAllocatedBytes(thread) - before
            assertEquals("bytes", assertIs<SyncWorkspaceLimitException>(error).dimension)
            assertTrue(allocated < 4 * 1024 * 1024, "refusal allocated $allocated bytes before admission")
            assertEquals(0L, scalarLong(db, "SELECT entries FROM _sync_apply_plan"))
        } finally {
            resources.closeFromCoordinatedOwner()
            http.close()
            db.close()
        }
    }

    @Test
    fun localApplyContractExecutesDefaultsResultsAndSurvivorWitness() {
        val fixture = Json.parseToJsonElement(oversqliteContractFixture("local-apply/a1.json").readText()).jsonObject
        val compatibility = fixture.getValue("executionContract").jsonObject.getValue("databaseCompatibility").jsonObject
        assertEquals(mapOf("olderDatabaseCompatibilityRequired" to false, "recreationPermitted" to true,
            "currentDatabaseReopenRequired" to true, "currentDatabaseRestartReplayRequired" to true,
            "legacyUpgradeQualificationRequired" to false), compatibility.mapValues { it.value.jsonPrimitive.boolean })
        val defaults = fixture.getValue("defaults").jsonObject
        val config = OversqliteConfig(1, listOf(SyncTable("nodes", "id")), "fixture", "main")
        assertEquals(defaults.getValue("entries").jsonPrimitive.long, config.maxSyncWorkspaceEntries)
        assertEquals(defaults.getValue("bytes").jsonPrimitive.long, config.maxSyncWorkspaceBytes)
        assertEquals(defaults.getValue("work").jsonPrimitive.long, config.maxSyncWorkUnitsPerInvocation)
        assertEquals(3, fixture.getValue("results").jsonArray.size)
        for (entry in fixture.getValue("results").jsonArray) {
            val row = entry.jsonObject
            val work = row.getValue("reason").jsonPrimitive.content == "sync_work_limit"
            assertEquals(work, row.getValue("resumable").jsonPrimitive.boolean)
            assertFalse(row.getValue("advancesCheckpoint").jsonPrimitive.boolean)
            assertEquals(if (work) "internal-slice" else "public-operation", row.getValue("executionScope").jsonPrimitive.content)
            if (work) assertEquals("work", row.getValue("dimension").jsonPrimitive.content)
            else {
                val error = SyncWorkspaceLimitException("unit", row.getValue("dimension").jsonPrimitive.content, 1, 1, 1, false)
                assertEquals(row.getValue("resumable").jsonPrimitive.boolean, error.resumable)
            }
        }
        for (entry in fixture.getValue("revisionCases").jsonArray) {
            val row = entry.jsonObject
            assertEquals(row.getValue("resume").jsonPrimitive.boolean, row["saved"] == row["current"])
        }
        val witness = fixture.getValue("survivorWitness").jsonObject
        val rows = witness.getValue("before").jsonObject.toMutableMap()
        for (effect in witness.getValue("wire").jsonArray.map { it.jsonObject }) {
            val key = effect.getValue("key").jsonPrimitive.content
            if (effect.getValue("op").jsonPrimitive.content == "DELETE") rows.remove(key)
            else rows[key] = effect.getValue("parent")
        }
        assertEquals(witness.getValue("after"), JsonObject(rows))
        val format = fixture.getValue("persistedFormat").jsonObject
        assertEquals(listOf("_sync_apply_plan", "_sync_apply_rows", "_sync_apply_edges"),
            format.getValue("tables").jsonArray.map { it.jsonPrimitive.content })
        assertEquals(listOf("clear", "roots", "index", "walk", "ready"),
            format.getValue("phases").jsonArray.map { it.jsonPrimitive.content })
        assertTrue(format.getValue("guardBindsRegistrationOrder").jsonPrimitive.boolean)
        assertTrue(format.getValue("allRootsExpand").jsonPrimitive.boolean)
        assertEquals(1, format.getValue("relationshipBuildPassesPerUnit").jsonPrimitive.int)
        for (entry in fixture.getValue("allocationCases").jsonArray) {
            val row = entry.jsonObject
            val payload = row.getValue("payload")
            val expected = row.getValue("encodedBytes").jsonPrimitive.long
            assertEquals(expected, encodedJsonSize(payload))
            assertEquals(expected, payload.toString().encodeToByteArray().size.toLong())
        }
    }
}
