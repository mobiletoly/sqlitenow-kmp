package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.*
import kotlin.test.*

/** Counts real SQLite calls; the baseline switch is test-only and never changes product behavior. */
internal class R1SettlementStructureTest : SurvivorTestSupport() {
    @Test fun committedSettlementKeepsAccountingAndBoundsWorkspacePreparations() = runBlocking {
        val baseline = System.getenv("R1_STRUCTURE_BASELINE") == "true"
        withSurvivorFixture { server, db, _, client ->
            try {
                db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
                server.normalizePushEffects = { effects() }
                server.beforePushResponse = { throw CancellationException("lost committed response") }
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                val frozen = server.pushBodies.last()
                val observer = observe(db)
                repeat(2) { invocation ->
                    if (invocation == 1) {
                        db.execSQL("UPDATE nodes SET name='changed other' WHERE id='Q'")
                        server.normalizePushEffects = { listOf(BundleRow("main", "nodes", mapOf("id" to "Q"), "UPDATE", 3,
                            Json.parseToJsonElement("""{"id":"Q","name":"changed other","parent_id":null}"""))) }
                    }
                    observer.counts.clear()
                    val cachedShapes = mutableListOf<String>()
                    client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
                        statementCacheOperations = StatementCacheOperations(prepare = { connection, sql ->
                            if (selected(sql)) cachedShapes += sql
                            connection.prepare(sql)
                        }),
                    ))
                    client.pushPending().getOrThrow()
                    requireSurvivors(db)
                    if (invocation == 0) assertEquals(frozen, server.pushBodies.last(), "exact frozen replay request")
                    val selected = observer.counts.filterKeys(::selected).values
                    assertEquals(1, selected.size, "one activated workspace SQL shape")
                    val count = selected.single()
                    assertTrue(count.executions > 1, "fixture traverses repeated workspace checks")
                    assertEquals(if (baseline) count.executions else 4, count.preparations, "one cached owning handle plus three uncached dependency validation checks")
                    assertEquals(if (baseline) 0 else 1, cachedShapes.size, "fresh statement in each writer transaction")
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                    assertEquals(0L, scalarLong(db, "SELECT reserved_entries FROM _sync_operation_state"))
                    assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state"))
                    assertEquals(3L + invocation, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
                    assertEquals(2L + invocation, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
                    println("R1_STRUCTURE_RECEIPT " + buildJsonObject {
                        put("baseline", baseline); put("invocation", invocation)
                        put("selectedPreparations", count.preparations); put("selectedExecutions", count.executions)
                        put("cachedShapes", cachedShapes.distinct().size)
                        put("uncachedWorkspace", buildJsonObject {
                            observer.counts.filterKeys { workspace(it) && !selected(it) }.forEach { (sql, c) ->
                                put(sha256Hex(sql.encodeToByteArray()), buildJsonObject { put("preparations", c.preparations); put("executions", c.executions) })
                            }
                        })
                        put("planSaves", observer.counts.filterKeys { it.startsWith("INSERT INTO _sync_apply_plan(") }.values.sumOf { it.executions })
                        put("accounting", client.workspaceDiagnostics().toString())
                    })
                }
            } finally { client.closeFromCoordinatedOwner() }
        }
    }

    private fun workspace(sql: String) = sql.startsWith("SELECT ") && sql.contains("FROM _sync_operation_state o")
    private fun selected(sql: String) = workspace(sql) && sql.contains("NULL,0,0,0,0,0") && sql.contains("LEFT JOIN _sync_push_plan")
    private data class Count(var preparations: Int = 0, var executions: Int = 0)
    private class Observer(private val delegate: SQLiteConnection) : SQLiteConnection by delegate {
        val counts = linkedMapOf<String, Count>()
        override fun prepare(sql: String): SQLiteStatement {
            val count = counts.getOrPut(sql) { Count() }; count.preparations++
            val statement = delegate.prepare(sql)
            return object : SQLiteStatement by statement {
                override fun step(): Boolean { count.executions++; return statement.step() }
            }
        }
    }
    private fun observe(db: SafeSQLiteConnection): Observer {
        // Same test-only raw-connection seam as JvmStatementCleanupFault, preserving Safe ownership.
        val field = SafeSQLiteConnection::class.java.getDeclaredField("ref").also { it.isAccessible = true }
        return Observer(field.get(db) as SQLiteConnection).also { field.set(db, it) }
    }
}
