package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import androidx.sqlite.async.step
import kotlinx.serialization.json.*
import kotlin.test.*

internal class WorkspaceCountingTest : SurvivorTestSupport() {
    @Test fun snapshotPreparationSkipsOldScopeReservationScans() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
            for (index in 0..99) db.execSQL("INSERT INTO nodes VALUES('Z${index.toString().padStart(4, '0')}','unrelated',NULL)")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            db.execSQL("""INSERT INTO _sync_row_state(schema_name,table_name,key_json,row_version,server_kind,server_payload,server_provenance,relationships_known,server_revision)
                SELECT a.schema_name,a.table_name,json_object('id',n.id),a.row_version,a.server_kind,
                json_set(a.server_payload,'$.id.value',n.id,'$.name.value',n.name),a.server_provenance,1,0
                FROM nodes n CROSS JOIN _sync_row_state a WHERE n.id LIKE 'Z%' AND a.key_json='{"id":"P"}'""")
            val config = OversqliteConfig(1, nodeTables, "fixture", "main", maxSyncWorkUnitsPerInvocation = 1)
            val generation = OversqliteContractStore(db).readGeneration(1)
            val validated = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db).withGeneration(generation)
            val state = RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false),
                "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
            val resources = OversqliteClientResourceOwner(db, http)
            val local = OversqliteLocalStore(db, json) { validated }
            val unit = applyUnit("snapshot", 2, "bounded-empty")
            val operations = OversqliteClientOperationController(config, resources, SnapshotDiagnosticsState()) {}
            val owner = OversqliteApplySurvivors(db, local, config, operations, json)
            db.transaction(TransactionMode.IMMEDIATE) {
                owner.prepare(state, unit, 0, "snapshot", 2) { error("empty snapshot has no roots") }
            }
            assertEquals("ready", scalarText(db, "SELECT phase FROM _sync_apply_plan"))
            assertEquals(0L, scalarLong(db, "SELECT atomic_work FROM _sync_apply_plan"))
            assertTrue(operations.usedSyncWork() > config.maxSyncWorkUnitsPerInvocation)
            val nextOperations = OversqliteClientOperationController(config, resources, SnapshotDiagnosticsState()) {}
            val nextOwner = OversqliteApplySurvivors(db, local, config, nextOperations, json)
            nextOwner.prepare(state, unit, 0, "snapshot", 2) { error("empty") }
            assertTrue(nextOperations.usedSyncWork() > config.maxSyncWorkUnitsPerInvocation)
            assertEquals(104L, scalarLong(db, "SELECT count(*) FROM nodes"))
            assertEquals(104L, scalarLong(db, "SELECT count(*) FROM _sync_row_state"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        }
    }

    @Test fun staleConflictPrecedesWorkspaceAdmission() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val client = newClient(db, http, syncTables = nodeTables,
                resolver = object : Resolver {
                    override fun resolve(conflict: ConflictContext) = MergeResult.DeferConflict
                })
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("UPDATE nodes SET name='local' WHERE id='P'")
            server.conflictOverride = { row, _, _ ->
                PushConflictDetails(row.schema, row.table, row.key, row.op, row.baseRowVersion, 2, false,
                    buildJsonObject { put("id", "P"); put("name", "remote"); put("parent_id", JsonNull) })
            }
            assertIs<ConflictNeedsResolutionException>(client.pushPending().exceptionOrNull())
            val selected = checkNotNull(client.pendingConflict().getOrThrow())
            db.execSQL("UPDATE nodes SET name='newer local' WHERE id='P'")
            client.closeFromCoordinatedOwner()
            val limited = newClient(db, http, syncTables = nodeTables, workspaceBytes = 128 * 1024)
            limited.openAndConnect("user-1").getOrThrow()
            val hash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
            assertIs<StaleConflictResolutionException>(limited.resolveConflict(selected, MergeResult.AcceptServer).exceptionOrNull())
            val current = checkNotNull(limited.pendingConflict().getOrThrow())
            for (obsolete in listOf(
                current.copy(revision = current.revision - 1),
                current.copy(baselineRevision = current.baselineRevision - 1),
                current.copy(sourceId = "obsolete-source"),
                current.copy(sourceBundleId = current.sourceBundleId + 1),
                current.copy(canonicalRequestHash = "0".repeat(64)),
                current.copy(generationId = current.generationId + 1),
                current.copy(generationFingerprint = "0".repeat(64)),
                current.copy(conflict = current.conflict.copy(schema = "obsolete")),
                current.copy(conflict = current.conflict.copy(table = "obsolete")),
                current.copy(conflict = current.conflict.copy(key = mapOf("id" to "Q"))),
                current.copy(conflict = current.conflict.copy(key = emptyMap())),
                current.copy(conflict = current.conflict.copy(key = mapOf("other_id" to "P"))),
                current.copy(conflict = current.conflict.copy(key = mapOf("id" to "P", "extra" to "Q"))),
                current.copy(conflict = current.conflict.copy(localOp = "DELETE")),
                current.copy(conflict = current.conflict.copy(baseRowVersion = current.conflict.baseRowVersion + 1)),
                current.copy(conflict = current.conflict.copy(serverRowVersion = current.conflict.serverRowVersion + 1)),
                current.copy(conflict = current.conflict.copy(serverRowDeleted = !current.conflict.serverRowDeleted)),
            )) assertIs<StaleConflictResolutionException>(limited.resolveConflict(obsolete, MergeResult.AcceptServer).exceptionOrNull())
            assertIs<SyncWorkspaceLimitException>(limited.resolveConflict(current, MergeResult.AcceptServer).exceptionOrNull(),
                "a current handle must still obey workspace admission")
            assertEquals(current, limited.pendingConflict().getOrThrow())
            assertEquals(hash, scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
            assertEquals("newer local", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries FROM _sync_operation_state"))
            limited.closeFromCoordinatedOwner()
        }
    }

    @Test fun countingQueriesUseIndexedSeeks() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            for (query in listOf(
                """SELECT CAST(id AS TEXT) FROM nodes WHERE id>'C' ORDER BY id LIMIT 1""",
                """SELECT key_json FROM _sync_row_state WHERE schema_name='main' AND table_name='nodes' AND key_json>'{"id":"C"}' ORDER BY key_json LIMIT 1""",
            )) {
                val detail = db.withPreparedStatement("EXPLAIN QUERY PLAN $query") { st ->
                    buildString { while (st.step()) append(st.getText(3)) }
                }
                assertTrue("SEARCH" in detail, detail)
                assertFalse("TEMP B-TREE" in detail, detail)
            }
        }
    }
}
