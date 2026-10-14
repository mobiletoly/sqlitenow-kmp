package dev.goquick.sqlitenow.oversqlite

import kotlin.test.*

internal class PushPlanCleanupProgressTest : SurvivorTestSupport() {
    @Test fun lowTargetCleanupCommitsEachDeletedNode() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val config = OversqliteConfig(1, nodeTables, "fixture", "main", maxSyncWorkUnitsPerInvocation = 1)
            val unit = applyUnit("cleanup", 1, "nodes")
            db.execSQL("INSERT INTO _sync_push_plan VALUES(1,'$unit','$unit','clear','{}',3,0)")
            for (ordinal in 1..3) db.execSQL("""INSERT INTO _sync_push_nodes(unit,schema_name,table_name,key_json,ordinal,intent_revision,dirty_ordinal,status,charge)
                VALUES('$unit','main','nodes','{"id":"cleanup-$ordinal"}',$ordinal,0,0,'pending',0)""")
            var previous = 3L
            repeat(4) {
                val operations = OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}
                val local = OversqliteLocalStore(db, json) { error("cleanup never loads payloads") }
                val stage = OversqliteStageStore(db, local, OversqliteSyncStateStore(db) { local }, json, config, operations)
                val failure = runCatching { stage.clearPushPlanForLifecycleSlice(false, null, OversqliteStageStore.PushCleanupIdentity(unit)) }.exceptionOrNull()
                val remaining = scalarLong(db, "SELECT count(*) FROM _sync_push_nodes")
                assertEquals(maxOf(0, previous - 1), remaining)
                if (previous > 0) assertIs<SyncWorkLimitException>(failure) else assertEquals(null, failure)
                previous = remaining
            }
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_push_plan"))
        }
    }

    @Test fun tinyTargetDiagnosticReturnsUsefulPages() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, http, seed ->
            db.execSQL("UPDATE nodes SET parent_id='C' WHERE id='P'")
            seed.pushPending().getOrThrow(); seed.closeFromCoordinatedOwner()
            server.wireLimits = testWireLimits().copy(maxPushRows = 1)
            val builder = newClient(db, http, maxPushRows = 1, syncTables = nodeTables)
            builder.openAndConnect("user-1").getOrThrow()
            db.execSQL("DELETE FROM nodes WHERE id='P'")
            // The independent grandchild deletion forms the first admitted bundle.
            builder.pushPending().getOrThrow()
            assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            val blocked = assertIs<PushDependencyGroupExceedsAdmittedLimitException>(builder.pushPending().exceptionOrNull())
            builder.closeFromCoordinatedOwner()
            val client = newClient(db, http, maxPushRows = 1, syncTables = nodeTables, workUnits = 1)
            try {
                client.openAndConnect("user-1").getOrThrow()
                val before = baselineQualificationVector(db, listOf("nodes"))
                var cursor: String? = null
                val refs = mutableListOf<PushDependencyRecordRef>()
                do {
                    val page = client.readPushDependencyDiagnostic(blocked.diagnosticId, cursor, 10).getOrThrow()
                    assertEquals(1, page.identities.size, "one bounded record may overshoot target")
                    refs += page.identities; cursor = page.nextCursor
                    assertTrue(refs.size <= blocked.componentRows)
                } while (cursor != null)
                assertEquals(blocked.componentRows, refs.size.toLong())
                assertEquals(refs.size, refs.toSet().size)
                assertEquals(before, baselineQualificationVector(db, listOf("nodes")))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    @Test fun tinyTargetsCompleteOneCleanupStep() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            for (target in 1L..7L) {
                val config = OversqliteConfig(1, nodeTables, "fixture", "main", maxSyncWorkUnitsPerInvocation = target)
                val resources = OversqliteClientResourceOwner(db, http)
                val operations = OversqliteClientOperationController(config, resources, SnapshotDiagnosticsState()) {}
                val local = OversqliteLocalStore(db, json) { error("cleanup does not load payloads") }
                val unit = applyUnit("cleanup", target, "retained")
                db.execSQL("INSERT INTO _sync_push_plan VALUES(1,'$unit','$unit','clear','{}',0,0)")
                val stage = OversqliteStageStore(db, local, OversqliteSyncStateStore(db) { local }, json, config, operations)
                stage.clearPushPlanForLifecycle()
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_push_plan"), "target=$target")
                assertEquals(8L, operations.usedSyncWork())
            }
        }
    }
}
