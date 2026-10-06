package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import java.util.IdentityHashMap
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.*
import kotlin.test.assertEquals

/** Aggregate JVM-only measurements; uses the existing transaction owner and hooks. */
internal class SnapshotStatementObservation {
    val preparedSql = mutableListOf<String>()
    val closedSql = mutableListOf<String>()
    var logicalSteps: Map<String, Long> = emptyMap()
        private set
    var physicalPrepares: Map<String, Long> = emptyMap()
        private set
    var work = 0L
        private set
    private val handles = IdentityHashMap<SQLiteStatement, String>()
    private var started = 0L
    private var firstPage = 0L
    private var lastRow = 0L
    private var closed = 0L
    private var rows = 0L

    private val operations = StatementCacheOperations(
        prepare = { db, sql ->
            if (started == 0L) started = System.nanoTime()
            db.prepare(sql).also { statement -> preparedSql += sql; handles[statement] = sql }
        },
        close = { statement ->
            statement.close()
            closedSql += checkNotNull(handles.remove(statement))
        },
    )

    suspend fun <T> observe(db: SafeSQLiteConnection, client: DefaultOversqliteClient, scope: String,
        sqlCounts: Boolean = false, block: suspend () -> T): T {
        val raw = if (sqlCounts) ReplaySqlObservation.install(db) else null
        client.setSnapshotApplyFaultInjectorForTest(SnapshotApplyFaultInjector(
            statementCacheOperations = operations,
            afterApplyPageLoaded = { if (firstPage == 0L) firstPage = System.nanoTime() },
            afterAppliedRow = { rows++; lastRow = System.nanoTime() },
            afterStatementCacheClosed = { closed = System.nanoTime() },
        ))
        var outcome = "complete"
        try {
            return block()
        } catch (failure: Throwable) {
            outcome = if (failure is SyncWorkLimitException) "yield" else failure::class.simpleName.orEmpty()
            throw failure
        } finally {
            withContext(NonCancellable) {
                client.setSnapshotApplyFaultInjectorForTest(null)
                if (raw != null) {
                    logicalSteps = raw.counts.mapValues { it.value.steps }
                    physicalPrepares = raw.counts.mapValues { it.value.prepares }
                    raw.counts.forEach { (sql, count) -> assertEquals(count.prepares, count.closes, "raw handle cleanup: $sql") }
                    raw.restore()
                }
            }
            work = client.workspaceDiagnostics().work
            val end = if (closed != 0L) closed else System.nanoTime()
            val preparationEnd = if (firstPage != 0L) firstPage else end
            val installationEnd = if (lastRow != 0L) lastRow else preparationEnd
            println("snapshot_statement_observation " + buildJsonObject {
                put("scope", scope); put("outcome", outcome); put("rows", rows); put("work", work)
                put("preparationNanos", if (started == 0L) 0 else preparationEnd - started)
                put("installationNanos", installationEnd - preparationEnd)
                put("finalizationNanos", end - installationEnd)
                put("cacheHandles", preparedSql.size)
                put("retainedSqlUtf8Bytes", preparedSql.sumOf { it.encodeToByteArray().size })
                if (raw != null) {
                    put("physicalPrepares", physicalPrepares.values.sum())
                    put("logicalSteps", logicalSteps.values.sum())
                    put("sql", buildJsonObject { raw.counts.toSortedMap().forEach { (sql, count) ->
                        put(sql, buildJsonObject {
                            put("prepares", count.prepares); put("steps", count.steps)
                            put("resets", count.resets); put("clears", count.clears); put("closes", count.closes)
                        })
                    } })
                }
            })
            assertEquals(preparedSql.asReversed(), closedSql, "reverse transaction-owned handle disposal")
            check(handles.isEmpty()) { "transaction left cached handles live" }
        }
    }

    companion object {
        suspend fun <T> ifEnabled(db: SafeSQLiteConnection, client: DefaultOversqliteClient,
            scope: String, block: suspend () -> T): T {
            val sql = System.getenv("OVERSQLITE_CAPACITY_SQL_OBSERVE") == "true"
            val timing = System.getenv("OVERSQLITE_CAPACITY_APPLY_TIMINGS") == "true"
            if (!sql && !timing) return block()
            return SnapshotStatementObservation().observe(db, client, scope, sql, block)
        }
    }
}
