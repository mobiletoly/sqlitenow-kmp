package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.launch
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

/** Uses the accepted raw-connection test seam; no native pointer or product accessor. */
private class CheckpointFaultObservation private constructor(private val db: SafeSQLiteConnection,private val delegate: SQLiteConnection,private val before: (String,Map<Int,Any?>)->Unit,private val after: (String,Map<Int,Any?>)->Unit,private val closeFault: Boolean) : SQLiteConnection by delegate {
    var planCloses=0
    var planResetFailures=0
    private var closeFaultInjected=false
    private val controlledCloseFailure=IllegalStateException("checkpoint-close-fault")
    override fun prepare(sql: String): SQLiteStatement {
        val statement=delegate.prepare(sql);val values=mutableMapOf<Int,Any?>()
        return object : SQLiteStatement by statement {
            override fun bindText(index: Int,value: String) {statement.bindText(index,value);values[index]=value}
            override fun bindLong(index: Int,value: Long) {statement.bindLong(index,value);values[index]=value}
            override fun bindNull(index: Int) {statement.bindNull(index);values[index]=null}
            override fun step(): Boolean {before(sql,values);return statement.step().also {after(sql,values)}}
            override fun reset() {try {statement.reset()} catch(failure: Throwable){if(sql.startsWith("INSERT INTO _sync_apply_plan("))planResetFailures++;throw failure}}
            override fun clearBindings() {statement.clearBindings();values.clear()}
            override fun close() {statement.close();if(sql.startsWith("INSERT INTO _sync_apply_plan(")){planCloses++;if(closeFault && !closeFaultInjected){closeFaultInjected=true;throw controlledCloseFailure}}}
        }
    }
    suspend fun restore() = db.withExclusiveAccess { val f=SafeSQLiteConnection::class.java.getDeclaredField("ref").also {it.isAccessible=true};assertSame(this@CheckpointFaultObservation,f.get(db));f.set(db,delegate) }
    companion object {
        suspend fun install(db: SafeSQLiteConnection,before: (String,Map<Int,Any?>)->Unit={_,_->},after: (String,Map<Int,Any?>)->Unit={_,_->},closeFault: Boolean=false): CheckpointFaultObservation = db.withExclusiveAccess {
            val f=SafeSQLiteConnection::class.java.getDeclaredField("ref").also {it.isAccessible=true};CheckpointFaultObservation(db,f.get(db) as SQLiteConnection,before,after,closeFault).also {f.set(db,it)}
        }
    }
}

internal class ReplayCheckpointObservationTest : ReplayCheckpointSupport() {
    @Test fun pendingCheckpointFaultPrecedesWorkRefusalAndKeepsCloseOrdering() = runOwnedWebDatabaseTest(timeout=180.seconds) {
        for(closeFault in listOf(false,true)) {
            val path=createTempSqliteNowTestDbPath("checkpoint-fault")
            try {
                withSurvivorFixture(dbPath=path) { _,db,http,_ ->
                    val config=checkpointConfig;val state=checkpointState(db,config);val before=fullCheckpointVector(db)
                    db.execSQL("""CREATE TEMP TRIGGER checkpoint_write_fault BEFORE INSERT ON _sync_apply_plan
                    WHEN NEW.phase='index' AND NEW.row_cursor='C' AND NEW.fk_cursor=-1
                    BEGIN SELECT RAISE(ABORT,'checkpoint-write-fault'); END""")
                    var reached=0
                    val observer=CheckpointFaultObservation.install(db,before={ sql,values -> if(sql.startsWith("INSERT INTO _sync_apply_plan(") && values[3]=="index" && values[8]=="C" && values[7]==-1L)reached++ },closeFault=closeFault)
                    val failure=try {assertFailsWith<Throwable> {prepareCheckpoint(db,http,state,config,"fault-before-refusal",budget=64,keys=emptyList())}}
                    finally {
                        assertEquals("SQLite connection is closed",assertFailsWith<IllegalStateException>{observer.restore()}.message)
                        // The failed cached step's reset is itself a cleanup failure.
                        // Core fences/closes both variants; its close also removes the TEMP trigger.
                    }
                    assertEquals(1,reached,"the real last checkpoint before work refusal was attempted")
                    val chain=generateSequence(failure) {it.cause}.toList()
                    assertTrue(chain.any {it.message.orEmpty().contains("checkpoint-write-fault")},failure.toString())
                    assertFalse(chain.any {it is SyncWorkLimitException},"write failure precedes the next work reservation")
                    val failureGraph=mutableListOf<Throwable>()
                    val seen=java.util.IdentityHashMap<Throwable,Boolean>()
                    fun visit(error: Throwable) {if(seen.put(error,true)!=null)return;failureGraph+=error;error.cause?.let(::visit);error.suppressedExceptions.forEach(::visit)}
                    visit(failure)
                    println("CHECKPOINT_FAULT_GRAPH "+buildJsonObject {put("closeFault",closeFault);put("planCloses",observer.planCloses);put("resetFailures",observer.planResetFailures);put("errors",buildJsonArray {failureGraph.forEach {add(it::class.simpleName+":"+it.message)}})})
                    assertEquals(if(closeFault)1 else 0,failureGraph.count {it.message=="checkpoint-close-fault"},"close diagnostic retained once through wrapped/nested cleanup causes")
                    assertTrue(failureGraph.none {it is SyncWorkLimitException},"no new work-refusal suppression policy")
                    assertEquals(if(closeFault)2 else 1,observer.planCloses,"a one-shot uncertain close is retried once by the fatal cleanup owner")
                    assertEquals(1,observer.planResetFailures,"reset reports the failed step and triggers the existing fatal policy")
                    assertEquals("SQLite connection is closed",assertFailsWith<IllegalStateException>{db.prepare("SELECT 1")}.message)
                    val peer=newFileBackedDb(path);try {assertEquals(before,fullCheckpointVector(peer))} finally {peer.close()}
                }
            } finally {deleteTempSqliteNowTestDbArtifacts(path)}
        }
    }
    @Test fun cancellationAtPrivateAndCheckpointStepsRetainsPreviousCommittedPlan() = runOwnedWebDatabaseTest(timeout=180.seconds) {
        for(location in listOf("private-edge","checkpoint-before","checkpoint-after")) {
            val path=createTempSqliteNowTestDbPath("checkpoint-cancel")
            try {
                withSurvivorFixture(dbPath=path) { _,db,http,_ ->
                    val config=checkpointConfig;val state=checkpointState(db,config);val unit="cancel-$location"
                    assertFailsWith<SyncWorkLimitException> {prepareCheckpoint(db,http,state,config,unit,budget=32,keys=emptyList())}
                    val before=fullCheckpointVector(db);var observed: Throwable?=null;var reached=0
                    val job=launch {
                        val caller=currentCoroutineContext()[Job]!!
                        fun hit(sql: String,values: Map<Int,Any?>): Boolean = if(location=="private-edge")sql.startsWith("INSERT OR IGNORE INTO _sync_apply_edges") else sql.startsWith("INSERT INTO _sync_apply_plan(") && values[3]=="index" && values[7]==-1L && values[8]=="C"
                        fun cancel() {reached++;caller.cancel(CancellationException("cancel-$location"))}
                        val observer=CheckpointFaultObservation.install(db,before={ sql,values -> if(location=="checkpoint-before" && hit(sql,values)){cancel();throw CancellationException("cancel-$location")} },after={ sql,values -> if(location!="checkpoint-before" && hit(sql,values))cancel() })
                        try {prepareCheckpoint(db,http,state,config,unit,budget=32,keys=emptyList())}
                        catch(failure: Throwable){observed=failure;throw failure}
                        finally {kotlinx.coroutines.withContext(kotlinx.coroutines.NonCancellable){observer.restore()}}
                    }
                    job.join();assertTrue(job.isCancelled);assertEquals(1,reached);assertIs<CancellationException>(observed)
                    assertEquals(before,fullCheckpointVector(db))
                    val peer=newFileBackedDb(path);try {assertEquals(before,fullCheckpointVector(peer))} finally {peer.close()}
                    assertFailsWith<SyncWorkLimitException> {prepareCheckpoint(db,http,state,config,unit,budget=32,keys=emptyList())}
                    assertNotEquals(before,fullCheckpointVector(db),"connection and prior checkpoint remain reusable")
                }
            } finally {deleteTempSqliteNowTestDbArtifacts(path)}
        }
    }
}
