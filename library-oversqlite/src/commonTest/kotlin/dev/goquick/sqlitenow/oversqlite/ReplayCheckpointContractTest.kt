package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import io.ktor.client.HttpClient
import kotlinx.coroutines.CancellationException
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

internal abstract class ReplayCheckpointSupport : SurvivorTestSupport() {
    protected val planColumns = listOf("unit", "guard", "phase", "root_cursor", "visit_cursor", "table_cursor", "fk_cursor", "row_cursor", "entries", "bytes", "edge_entries", "edge_bytes", "operation_kind", "preserve_schema", "preserve_table", "preserve_key", "atomic_work", "preservation_complete")
    private val planIntegers = setOf(3,4,5,6,8,9,10,11,16,17)
    protected val checkpointConfig get() = OversqliteConfig(1,nodeTables,"fixture","main",maxSyncWorkUnitsPerInvocation=1_000_000)
    protected suspend fun checkpointState(db: SafeSQLiteConnection, config: OversqliteConfig): RuntimeState {
        val generation=OversqliteContractStore(db).readGeneration(1)
        val validated=SyncRuntimeInitializer(config,TableInfoCache()).validateConfig(db).withGeneration(generation)
        return RuntimeState(validated,AdmittedOversqliteContract("v1",generation,testWireLimits(),false),"user-1",scalarText(db,"SELECT current_source_id FROM _sync_attachment_state"))
    }
    protected fun checkpointOwner(db: SafeSQLiteConnection,http: HttpClient,state: RuntimeState,config: OversqliteConfig): Pair<OversqliteApplySurvivors,OversqliteClientOperationController> {
        val operations=OversqliteClientOperationController(config,OversqliteClientResourceOwner(db,http),SnapshotDiagnosticsState()) {}
        return OversqliteApplySurvivors(db,OversqliteLocalStore(db,json) {state.validated},config,operations,json) to operations
    }
    protected suspend fun readCheckpoint(db: SafeSQLiteConnection): List<Any?> = db.withPreparedStatement("SELECT ${planColumns.joinToString(",")} FROM _sync_apply_plan WHERE singleton_key=1") { st ->
        assertTrue(st.step());planColumns.indices.map { if(it in planIntegers) st.getLong(it) else if(st.isNull(it)) null else st.getText(it) }.also {assertFalse(st.step())}
    }
    protected suspend fun assertCheckpointTotals(db: SafeSQLiteConnection,plan: List<Any?>) {
        assertEquals(18,plan.size)
        assertEquals(scalarLong(db,"SELECT count(*) FROM _sync_apply_rows"),plan[8])
        assertEquals(scalarLong(db,"SELECT coalesce(sum(bytes),0) FROM _sync_apply_rows"),plan[9])
        assertEquals(scalarLong(db,"SELECT count(*) FROM _sync_apply_edges"),plan[10])
        assertEquals(scalarLong(db,"SELECT coalesce(sum(bytes),0) FROM _sync_apply_edges"),plan[11])
        assertTrue((plan[16] as Long)>=0);assertTrue(plan[17] in listOf(0L,1L))
        assertEquals(0L,scalarLong(db,"SELECT apply_mode FROM _sync_apply_state"))
    }
    protected suspend fun fullCheckpointVector(db: SafeSQLiteConnection) = baselineQualificationVector(db,listOf("nodes")+oversqliteRetainedTableDdl.keys)
    protected suspend fun primaryCheckpointVector(db: SafeSQLiteConnection) = baselineQualificationVector(db,listOf("nodes")+oversqliteRetainedTableDdl.keys.filterNot { it in setOf("_sync_apply_plan","_sync_apply_rows","_sync_apply_edges") })
    protected suspend fun prepareCheckpoint(db: SafeSQLiteConnection,http: HttpClient,state: RuntimeState,config: OversqliteConfig,unit: String,kind: String="pull",budget: Long=config.maxSyncWorkUnitsPerInvocation,keys: List<String> = listOf("P","Q"),executor: OversqliteApplyExecutor=OversqliteApplyExecutor(db,OversqliteApplyStateStore(db)),seed: (suspend (Int,OversqliteApplySurvivors)->ApplySurvivor)?=null): OversqliteClientOperationController {
        val (owner,operations)=checkpointOwner(db,http,state,config)
        operations.reserveSyncWork(unit,config.maxSyncWorkUnitsPerInvocation-budget)
        executor.inApplyModeTransaction(state) { cache -> owner.prepare(state,unit,keys.size,kind,2,cache,cache) { index -> seed?.invoke(index,owner) ?: owner.capture(state,"nodes",keys[index],cache,cache) } }
        return operations
    }
}

internal class ReplayCheckpointContractTest : ReplayCheckpointSupport() {
    @Test fun rootSeedsReadCurrentCheckpointForAllKinds() = runOwnedWebDatabaseTest(timeout=180.seconds) {
        withSurvivorFixture { _,db,http,_ ->
            val config=checkpointConfig;val state=checkpointState(db,config)
            for(kind in listOf("pull","settlement","snapshot","conflict")) {
                db.execSQL("DELETE FROM _sync_apply_edges");db.execSQL("DELETE FROM _sync_apply_rows");db.execSQL("DELETE FROM _sync_apply_plan")
                val before=primaryCheckpointVector(db);val keys=listOf("P","P","Q");var callbacks=0
                prepareCheckpoint(db,http,state,config,"roots-$kind",kind,keys=keys,seed={ index,owner ->
                    val plan=readCheckpoint(db)
                    assertEquals(index.toLong(),plan[3]);assertEquals(keys.take(index).distinct().size.toLong(),plan[8])
                    assertEquals(scalarLong(db,"SELECT coalesce(sum(bytes),0) FROM _sync_apply_rows"),plan[9])
                    assertEquals("roots",plan[2]);callbacks++;owner.capture(state,"nodes",keys[index])
                })
                assertEquals(keys.size,callbacks)
                val plan=readCheckpoint(db);assertEquals("ready",plan[2]);assertEquals(keys.size.toLong(),plan[3]);assertCheckpointTotals(db,plan)
                assertEquals(before,primaryCheckpointVector(db))
            }
        }
    }
    @Test fun boundedPreparationResumesWithExactRetainedTotalsForAllKinds() = runOwnedWebDatabaseTest(timeout=180.seconds) {
        withSurvivorFixture { _,db,http,_ ->
            db.execSQL("UPDATE nodes SET name=name||'-newer'")
            val config=checkpointConfig;val state=checkpointState(db,config)
            for(kind in listOf("pull","settlement","snapshot","conflict")) {
                db.execSQL("DELETE FROM _sync_apply_edges");db.execSQL("DELETE FROM _sync_apply_rows");db.execSQL("DELETE FROM _sync_apply_plan")
                prepareCheckpoint(db,http,state,config,"obsolete-$kind",kind)
                val primary=primaryCheckpointVector(db);val phases=mutableSetOf<String>();val unit="resumed-$kind"
                var previous: List<Any?>?=null;var previousVector: BaselineQualificationVector?=null;var ready=false;var callbacks=0
                repeat(120) {
                    if(!ready) {
                        val refusal=assertFailsWith<SyncWorkLimitException> { prepareCheckpoint(db,http,state,config,unit,kind,budget=32,seed={ i,owner -> callbacks++;owner.capture(state,"nodes",listOf("P","Q")[i]) }) }
                        assertEquals(unit,refusal.unit);assertEquals(config.maxSyncWorkUnitsPerInvocation,refusal.limit);assertTrue(refusal.used in (config.maxSyncWorkUnitsPerInvocation-32)..config.maxSyncWorkUnitsPerInvocation)
                        val plan=readCheckpoint(db);assertCheckpointTotals(db,plan);assertEquals(unit,plan[0]);assertEquals(kind,plan[12]);assertEquals(OversqliteWorkspace(db,config).contextGuard(state,unit),plan[1])
                        phases+=plan[2] as String
                        val progressVector=fullCheckpointVector(db)
                        assertNotEquals(previousVector,progressVector,"an admitted step advances retained cursors or staging row state")
                        previousVector=progressVector
                        previous=plan;ready=plan[2]=="ready" && (kind !in listOf("settlement","conflict") || plan[17]==1L)
                        assertEquals(primary,primaryCheckpointVector(db))
                    }
                }
                assertTrue(ready,"bounded preparation must complete $kind: $previous")
                assertTrue(phases.containsAll(listOf("clear","roots","index","walk","ready")),"$kind phases $phases")
                assertEquals(2,callbacks,"completed roots do not repeat after reopen-like resume")
                prepareCheckpoint(db,http,state,config,unit,kind,seed={ _,_->error("ready checkpoint must skip seeds") })
                assertEquals(previous,readCheckpoint(db));assertEquals(primary,primaryCheckpointVector(db))
            }
        }
    }
    @Test fun readyPlanResumeSkipsDiscoveryAndPreservation() = runOwnedWebDatabaseTest(timeout=180.seconds) {
        withSurvivorFixture { _,db,http,_ ->
            db.execSQL("UPDATE nodes SET name=name||'-newer'")
            val config=checkpointConfig;val state=checkpointState(db,config)
            for(kind in listOf("pull","settlement","snapshot","conflict")) {
                db.execSQL("DELETE FROM _sync_apply_edges");db.execSQL("DELETE FROM _sync_apply_rows");db.execSQL("DELETE FROM _sync_apply_plan")
                val unit="ready-$kind";prepareCheckpoint(db,http,state,config,unit,kind)
                val before=fullCheckpointVector(db)
                val refusal=assertFailsWith<SyncWorkLimitException> { prepareCheckpoint(db,http,state,config,unit,kind,budget=0,seed={ _,_->error("ready resume called seed") }) }
                assertEquals(config.maxSyncWorkUnitsPerInvocation,refusal.used);assertEquals(before,fullCheckpointVector(db))
                val operations=prepareCheckpoint(db,http,state,config,unit,kind,seed={ _,_->error("ready retry called seed") })
                assertEquals(0L,operations.a3Stats.primaryReads);assertEquals(0L,operations.a3Stats.indexedVisits);assertEquals(before,fullCheckpointVector(db))
            }
        }
    }
    @Test fun commitCancellationRollsBackCurrentCheckpoint() = runOwnedWebDatabaseTest(timeout=180.seconds) {
        withSurvivorFixture { _,db,http,_ ->
            val config=checkpointConfig;val state=checkpointState(db,config);val unit="commit-cancel"
            assertFailsWith<SyncWorkLimitException> {prepareCheckpoint(db,http,state,config,unit,budget=32)}
            val before=fullCheckpointVector(db);var barrier=0
            val executor=OversqliteApplyExecutor(db,OversqliteApplyStateStore(db)).also { it.faultInjector=SnapshotApplyFaultInjector(beforeCommit={barrier++;throw CancellationException("checkpoint-commit-cancel")}) }
            val failure=assertFailsWith<CancellationException> {prepareCheckpoint(db,http,state,config,unit,budget=64,executor=executor)}
            assertTrue(generateSequence(failure as Throwable) {it.cause}.any {it.message=="checkpoint-commit-cancel"});assertEquals(1,barrier);assertEquals(before,fullCheckpointVector(db))
            assertFailsWith<SyncWorkLimitException> {prepareCheckpoint(db,http,state,config,unit,budget=64)}
            assertNotEquals(before,fullCheckpointVector(db))
        }
    }
}
