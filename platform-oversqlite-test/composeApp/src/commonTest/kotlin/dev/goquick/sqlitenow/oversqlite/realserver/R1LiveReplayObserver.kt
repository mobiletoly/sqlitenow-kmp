package dev.goquick.sqlitenow.oversqlite.realserver

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.common.SqliteNowLogger
import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import kotlinx.serialization.json.*
import kotlin.test.*

/** Test-owned observations, enabled only for the selected synchronous native live lanes. */
internal class R1LiveReplayObserver(private val connection: SafeSQLiteConnection) {
    private val workspacePreparations = linkedMapOf<String, Long>()
    private val selectedShapes = linkedSetOf<String>()
    private var selectedPreparations = 0L
    private var selectedSteps = 0L
    private var selectedResets = 0L
    private var selectedClears = 0L
    private var selectedCloses = 0L
    private var planSavePreparations = 0L

    private fun workspace(sql: String) = sql.startsWith("SELECT ") && sql.contains("FROM _sync_operation_state o")
    private fun selected(sql: String) = workspace(sql) && sql.contains("NULL,0,0,0,0,0") && sql.contains("LEFT JOIN _sync_push_plan")

    suspend fun replay(client: DefaultOversqliteClient) {
        val previousLogger = sqliteNowLogger
        sqliteNowLogger = object : SqliteNowLogger by previousLogger {
            override fun d(throwable: Throwable?, message: () -> String) {
                val text = message()
                val prefix = "SafeSQLiteConnection.prepare: "
                if (!text.startsWith(prefix)) return
                val sql = text.removePrefix(prefix)
                if (workspace(sql)) workspacePreparations[sql] = workspacePreparations.getOrElse(sql) { 0 } + 1
                if (sql.startsWith("INSERT INTO _sync_apply_plan(")) planSavePreparations++
            }
        }
        client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
            statementCacheOperations = StatementCacheOperations(prepare = { owner, sql ->
                val statement = owner.prepare(sql)
                if (!selected(sql)) statement else {
                    assertSame(connection, owner, "selected workspace cache belongs to the reopened writer connection")
                    selectedShapes += sql
                    selectedPreparations++
                    object : SQLiteStatement by observeR1LiveReplaySteps(statement, { selectedSteps++ }) {
                        override fun reset() { selectedResets++; statement.reset() }
                        override fun clearBindings() { selectedClears++; statement.clearBindings() }
                        override fun close() { selectedCloses++; statement.close() }
                    }
                }
            }),
        ))
        try {
            client.pushPending().getOrThrow()
        } finally {
            client.setSnapshotApplyFaultInjectorForTest(null)
            sqliteNowLogger = previousLogger
        }
    }

    fun verify(size: Int, width: Int, client: DefaultOversqliteClient, identity: String) {
        assertEquals(16, width, "this frozen accounting oracle covers the width-16 slice")
        assertEquals(1L, selectedPreparations)
        assertEquals(1, selectedShapes.size)
        assertEquals(1L, workspacePreparations.getValue(selectedShapes.single()), "actual Safe connection preparation agrees with cache seam")
        // The preserved four-node structural fixture contributes 59 reads/28 saves;
        // each unrelated NULL-FK intent contributes twelve checks/four plan saves.
        assertEquals(12L * size + 59, selectedSteps, "original workspace reads retained")
        assertEquals(4L * size + 28, planSavePreparations, "original complete plan-save cadence retained")
        assertEquals(selectedSteps, selectedResets)
        assertEquals(selectedSteps, selectedClears)
        assertEquals(1L, selectedCloses, "owning transaction finalized its selected handle")
        val expected = "A3AccountingStats(primaryReads=${4L * size}, primaryBytes=${2444L * size}, indexedVisits=${8L * size}, duplicateChecks=${4L * size}, restorationPasses=3, savedCopies=3, savedBytes=1328, serializedBytes=${676L * size + 1718}, peakPrimaryCopies=14, peakPrimaryBytes=8968, work=${1104L * size + 3864})"
        assertEquals(expected, client.workspaceDiagnostics().toString(), "sealed F1 original accounting on the unchanged real-workload fixture")
        println("R1_LIVE_STRUCTURE " + buildJsonObject {
            put("identity", identity); put("runtime", webRuntimeKind()); put("size", size); put("width", width)
            put("selectedPreparations", selectedPreparations); put("selectedSteps", selectedSteps)
            put("selectedResets", selectedResets); put("selectedClears", selectedClears); put("selectedCloses", selectedCloses)
            put("planSavePreparations", planSavePreparations); put("accounting", client.workspaceDiagnostics().toString())
            put("workspacePreparations", buildJsonObject {
                workspacePreparations.forEach { (sql, count) -> put(sha256Hex(sql.encodeToByteArray()), count) }
            })
            put("selectedShape", sha256Hex(selectedShapes.single().encodeToByteArray()))
        })
    }
}

// Bundled drivers step synchronously; the worker driver exposes a suspend step.
internal expect fun observeR1LiveReplaySteps(
    statement: SQLiteStatement,
    onStep: () -> Unit,
): SQLiteStatement
