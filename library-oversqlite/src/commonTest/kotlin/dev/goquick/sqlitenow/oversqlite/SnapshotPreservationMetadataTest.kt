package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import kotlin.test.*

internal class SnapshotPreservationMetadataTest : SurvivorTestSupport() {
    @Test fun optionalMetadataIsChargedAndFallsBackAtEntryAndByteLimits() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, client ->
            try {
                val base = OversqliteConfig(1, nodeTables, "fixture", "main")
                for (dimension in listOf("entries", "bytes")) {
                    val slice = OversqliteWorkSlice()
                    withContext(slice) {
                        val cache = StatementCache(db)
                        try {
                            val initial = OversqliteWorkspace(db, base).usage("metadata")
                            val bounded = if (dimension == "entries") base.copy(maxSyncWorkspaceEntries = initial.entries + 4)
                                else base.copy(maxSyncWorkspaceBytes = initial.bytes + 128)
                            val workspace = OversqliteWorkspace(db, bounded)
                            val memo = SnapshotPreservationMetadata(db, cache, workspace, "metadata", null)
                            slice.preservationMetadata = memo
                            assertTrue(memo.retain(WorkspaceCharge(4, 128)))
                            assertEquals(workspace.add("metadata", initial, WorkspaceCharge(4, 128)), workspace.usage("metadata"))
                            assertFalse(memo.retain(WorkspaceCharge(1, 1)))
                            assertEquals(WorkspaceCharge(4, 128), memo.charge)
                            memo.disable()
                            assertEquals(initial, workspace.usage("metadata"))
                            assertFalse(memo.retain(WorkspaceCharge()))
                        } finally { slice.preservationMetadata = null; cache.close() }
                    }
                }
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun mainAndTemporaryTriggersAreRecheckedAfterFailedWriterAndCancellation() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            try {
                server.addRemoteBundleForTest(listOf(changed("P", "changed")))
                for (temporary in listOf(false, true)) {
                    db.execSQL("CREATE ${if (temporary) "TEMP " else ""}TRIGGER unsafe_preservation AFTER UPDATE ON nodes BEGIN SELECT 1; END")
                    val before = scalarText(db, "SELECT name FROM nodes WHERE id='P'")
                    assertIs<FinalStateApplyBlockedException>(client.rebuild().exceptionOrNull())
                    assertEquals(before, scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                    db.execSQL("DROP TRIGGER ${if (temporary) "temp." else ""}unsafe_preservation")
                }
                client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(beforeCommit = { throw CancellationException("fixture") }))
                assertFailsWith<CancellationException> { client.rebuild().getOrThrow() }
                client.setSnapshotApplyFaultInjectorForTest(null)
                // A changed schema must be inspected again after the interrupted invocation.
                db.execSQL("CREATE TEMP TRIGGER unsafe_preservation AFTER INSERT ON nodes BEGIN SELECT 1; END")
                assertIs<FinalStateApplyBlockedException>(client.rebuild().exceptionOrNull())
                db.execSQL("DROP TRIGGER temp.unsafe_preservation")
                client.rebuild().getOrThrow()
                assertEquals("changed", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { client.setSnapshotApplyFaultInjectorForTest(null); client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun compositeIncomingReferencesKeepRowSpecificRefusalsAndSafeRows() = runOwnedWebDatabaseTest {
        for (action in listOf("NO ACTION", "RESTRICT", "CASCADE", "SET NULL")) withSurvivorFixture { server, db, _, client ->
            try {
                db.execSQL("CREATE UNIQUE INDEX nodes_pair ON nodes(id,name)")
                db.execSQL("CREATE TABLE local_refs(id INTEGER PRIMARY KEY,parent_id TEXT,parent_name TEXT,FOREIGN KEY(parent_id,parent_name) REFERENCES nodes(id,name) ON UPDATE $action ON DELETE $action)")
                db.execSQL("INSERT INTO local_refs VALUES(1,'P','parent')")
                server.addRemoteBundleForTest(listOf(changed("P", "changed")))
                assertIs<FinalStateApplyBlockedException>(client.rebuild().exceptionOrNull())
                assertEquals("parent", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                assertEquals("parent", scalarText(db, "SELECT parent_name FROM local_refs WHERE id=1"))
                db.execSQL("DELETE FROM local_refs")
                client.rebuild().getOrThrow()
                assertEquals("changed", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    private fun changed(id: String, name: String) = BundleRow("main", "nodes", mapOf("id" to id), "UPDATE", 2,
        buildJsonObject { put("id", id); put("name", name); put("parent_id", JsonNull) })
}
