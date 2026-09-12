package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import kotlin.test.*

internal class ApplyPlanPersistenceTest : SurvivorTestSupport() {
    private val columns = listOf("unit", "guard", "phase", "root_cursor", "visit_cursor",
        "table_cursor", "fk_cursor", "row_cursor", "entries", "bytes", "edge_entries",
        "edge_bytes", "operation_kind", "preserve_schema", "preserve_table", "preserve_key",
        "atomic_work", "preservation_complete")
    private val integerColumns = setOf(3, 4, 5, 6, 8, 9, 10, 11, 16, 17)
    private val config = OversqliteConfig(1, nodeTables, "fixture", "main",
        maxSyncWorkUnitsPerInvocation = 16)

    @Test fun initialCheckpointPersistsEveryColumnForAllOperationKinds() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val state = runtimeState(db)
            for (kind in listOf("pull", "settlement", "snapshot", "conflict")) {
                db.execSQL("DELETE FROM _sync_apply_plan")
                val unit = "plan-é-猫-$kind"
                val guard = OversqliteWorkspace(db, config).contextGuard(state, unit)
                checkpoint(db, http, state, unit, kind, exhaustBudget = true)
                assertEquals(listOf(unit, guard, "roots", 0L, 0L, 0L, 0L, null,
                    0L, 0L, 0L, 0L, kind, "", "", "", 0L, 0L), readPlan(db))
                assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
            }
        }
    }

    @Test fun loadedCheckpointPreservesInt64AndTextThenContextChangeClearsOldFields() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val state = runtimeState(db)
            val unit = "loaded-é-猫"
            val guard = OversqliteWorkspace(db, config).contextGuard(state, unit)
            val values = listOf(unit, guard, "roots", 3L, 9_007_199_254_740_993L, 7L, -1L, "",
                9L, 113L, 2L, 31L, "conflict", "schéma", "表", "{\"id\":\"🙂\"}", 1234L, 1L)
            db.withPreparedStatement("INSERT INTO _sync_apply_plan(singleton_key,${columns.joinToString(",")}) VALUES(1,${columns.joinToString(",") { "?" }})") { st ->
                values.forEachIndexed { i, value ->
                    when (value) {
                        is Long -> st.bindLong(i + 1, value)
                        is String -> st.bindText(i + 1, value)
                        null -> st.bindNull(i + 1)
                    }
                }
                st.step()
            }
            checkpoint(db, http, state, unit, "conflict", exhaustBudget = true)
            assertEquals(values, readPlan(db), "existing durable plan survives a complete save")
            db.execSQL("UPDATE _sync_apply_plan SET row_cursor='é🙂'")
            checkpoint(db, http, state, unit, "conflict", exhaustBudget = true)
            assertEquals(values.toMutableList().also { it[7] = "é🙂" }, readPlan(db))

            val nextUnit = "changed-context"
            val nextGuard = OversqliteWorkspace(db, config).contextGuard(state, nextUnit)
            checkpoint(db, http, state, nextUnit, "pull", exhaustBudget = false)
            assertEquals(listOf(nextUnit, nextGuard, "roots", 0L, 0L, 0L, 0L, null,
                0L, 0L, 0L, 0L, "pull", "", "", "", 0L, 0L), readPlan(db),
                "conflict update clears prior nonzero, nonempty and true fields")
        }
    }

    @Test fun actualPlanWriteFaultRollsBackCompleteStateAndConnectionResumes() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, _ ->
            val state = runtimeState(db)
            val tables = listOf("nodes") + oversqliteRetainedTableDdl.keys
            val before = baselineQualificationVector(db, tables)
            db.execSQL("CREATE TEMP TRIGGER fail_apply_plan BEFORE INSERT ON _sync_apply_plan BEGIN SELECT RAISE(ABORT,'plan-write-fault'); END")
            var laterWork = false
            try {
                val failure = assertFailsWith<Throwable> {
                    val operations = OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}
                    val owner = OversqliteApplySurvivors(db, OversqliteLocalStore(db, json) { state.validated }, config, operations, json)
                    OversqliteApplyExecutor(db, OversqliteApplyStateStore(db)).inApplyModeTransaction(state) {
                        db.execSQL("UPDATE nodes SET name='must roll back' WHERE id='P'")
                        owner.prepare(state, "failed-plan", 1) { laterWork = true; error("seed must not run") }
                        laterWork = true
                    }
                }
                assertTrue(generateSequence(failure) { it.cause }.any { it.message?.contains("plan-write-fault") == true }, failure.toString())
                assertFalse(laterWork)
                assertEquals(before, baselineQualificationVector(db, tables))
            } finally { db.execSQL("DROP TRIGGER fail_apply_plan") }
            checkpoint(db, http, state, "resumed-plan", "pull", exhaustBudget = true)
            assertEquals("resumed-plan", readPlan(db).first())
        }
    }

    private suspend fun runtimeState(db: SafeSQLiteConnection): RuntimeState {
        val generation = OversqliteContractStore(db).readGeneration(1)
        val validated = SyncRuntimeInitializer(config, TableInfoCache()).validateConfig(db).withGeneration(generation)
        return RuntimeState(validated, AdmittedOversqliteContract("v1", generation, testWireLimits(), false),
            "user-1", scalarText(db, "SELECT current_source_id FROM _sync_attachment_state"))
    }

    private suspend fun checkpoint(db: SafeSQLiteConnection, http: HttpClient, state: RuntimeState,
        unit: String, kind: String, exhaustBudget: Boolean) {
        val operations = OversqliteClientOperationController(config, OversqliteClientResourceOwner(db, http), SnapshotDiagnosticsState()) {}
        if (exhaustBudget) operations.reserveSyncWork(unit, 16)
        val owner = OversqliteApplySurvivors(db, OversqliteLocalStore(db, json) { state.validated }, config, operations, json)
        val failure = assertFailsWith<SyncWorkLimitException> {
            OversqliteApplyExecutor(db, OversqliteApplyStateStore(db)).inApplyModeTransaction(state) {
                owner.prepare(state, unit, 5, kind) { error("work-limit checkpoint precedes seed") }
            }
        }
        assertEquals(16L, failure.used)
    }

    private suspend fun readPlan(db: SafeSQLiteConnection): List<Any?> =
        db.withPreparedStatement("SELECT ${columns.joinToString(",")} FROM _sync_apply_plan WHERE singleton_key=1") { st ->
            assertTrue(st.step())
            columns.indices.map { i -> if (i in integerColumns) st.getLong(i) else if (st.isNull(i)) null else st.getText(i) }
                .also { assertFalse(st.step()) }
        }
}
