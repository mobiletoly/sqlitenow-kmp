package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.supervisorScope
import kotlin.test.*

internal class ProgressFinalizationJvmTest : SurvivorTestSupport() {
    @Test fun failedProgressReadRetainsTypedCauseAndState() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, seed ->
            db.execSQL("UPDATE nodes SET name='retained local intent' WHERE id='Q'")
            seed.closeFromCoordinatedOwner()
            val before = baselineQualificationVector(db, listOf("nodes"))
            val resources = OversqliteClientResourceOwner(db, http)
            val operations = OversqliteClientOperationController(
                OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "main", syncTables = nodeTables),
                resources, SnapshotDiagnosticsState(), {},
            )
            val raw = db.ref
            val rawField = db.javaClass.getDeclaredField("ref").apply { isAccessible = true }
            try {
                for (query in listOf("BEGIN IMMEDIATE", "SELECT count(*) FROM _sync_dirty_rows", "SELECT count(*) FROM _sync_outbox_rows")) {
                    for (failure in listOf(
                        SyncWorkLimitException("a".repeat(64), 32, 32),
                        SyncWorkspaceLimitException("b".repeat(64), "bytes", 700, 600, 200, true),
                        FKReconciliationRequiredException("retained evidence", "c".repeat(64), emptyList(), false),
                        ResolutionBaseUnavailableException("d".repeat(64)),
                        PushDependencyGroupExceedsAdmittedLimitException("rows", 1, 2, 2, 200, "e".repeat(64), emptyList(), false),
                    )) {
                        val readFailure = IllegalStateException("injected progress failure: $query")
                        var intercepted = 0
                        rawField.set(db, object : SQLiteConnection by raw {
                            override fun prepare(sql: String): SQLiteStatement {
                                if (sql.equals(query, ignoreCase = true)) {
                                    intercepted++
                                    throw readFailure
                                }
                                return raw.prepare(sql)
                            }
                        })
                        val result = operations.runExclusiveOperation<Unit> { throw failure }
                        rawField.set(db, raw)
                        assertEquals(1, intercepted, query)
                        assertSame(failure, result.exceptionOrNull(), query)
                        assertEquals(1, failure.suppressedExceptions.size)
                        assertTrue(generateSequence(failure.suppressedExceptions.single()) { it.cause }
                            .any { it === readFailure }, "progress failure must remain observable")
                        assertEquals(before, baselineQualificationVector(db, listOf("nodes")))
                        assertTrue(operations.runExclusiveOperation { Unit }.isSuccess, "operation ownership must be released")
                    }
                }
            } finally { rawField.set(db, raw); resources.closeFromCoordinatedOwner() }
        }
    }

    @Test fun progressCancellationRetainsPrecedence() = checkCancellation(false)

    @Test fun callerCancellationDuringFailedProgressReadRetainsPrecedence() = checkCancellation(true)

    private fun checkCancellation(cancelCaller: Boolean) = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, http, seed ->
            seed.closeFromCoordinatedOwner()
            val before = baselineQualificationVector(db, listOf("nodes"))
            val resources = OversqliteClientResourceOwner(db, http)
            val operations = OversqliteClientOperationController(
                OversqliteConfig(generationId = 1, databaseId = "fixture", serviceId = "main", syncTables = nodeTables),
                resources, SnapshotDiagnosticsState(), {},
            )
            val raw = db.ref
            val rawField = db.javaClass.getDeclaredField("ref").apply { isAccessible = true }
            val cancellation = CancellationException("caller/progress cancellation")
            try {
                assertFailsWith<CancellationException> {
                    supervisorScope {
                        async {
                            val job = currentCoroutineContext()[Job]!!
                            rawField.set(db, object : SQLiteConnection by raw {
                                override fun prepare(sql: String): SQLiteStatement {
                                    if (sql == "SELECT count(*) FROM _sync_dirty_rows") {
                                        if (cancelCaller) {
                                            job.cancel(cancellation)
                                            throw IllegalStateException("read failed while caller cancelled")
                                        }
                                        throw cancellation
                                    }
                                    return raw.prepare(sql)
                                }
                            })
                            operations.runExclusiveOperation<Unit> { throw SyncWorkLimitException("a".repeat(64), 32, 32) }
                        }.await()
                    }
                }
                rawField.set(db, raw)
                assertEquals(before, baselineQualificationVector(db, listOf("nodes")))
                assertTrue(operations.runExclusiveOperation { Unit }.isSuccess)
            } finally { rawField.set(db, raw); resources.closeFromCoordinatedOwner() }
        }
    }
}
