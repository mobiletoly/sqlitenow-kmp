package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.supervisorScope
import kotlin.test.*

internal class PublicProgressFailureJvmTest : CrossTargetSyncTestSupport() {
    @Test fun publicResourceExitsRetainCauseWhenFinalReadFails() = runOwnedWebDatabaseTest {
        withBlockedDiagnostic { db, client, diagnostic ->
            val before = baselineQualificationVector(db, listOf("authors", "profiles", "users"))
            val raw = db.ref
            val rawField = db.javaClass.getDeclaredField("ref").apply { isAccessible = true }
            val operations = client.field("operationController") as OversqliteClientOperationController
            val stages = client.field("stageStore") as OversqliteStageStore
            val operationConfig = operations.field("config") as OversqliteConfig
            val stageConfig = stages.field("config") as OversqliteConfig
            val unit = scalarText(db, "SELECT unit FROM _sync_push_plan")
            try {
                for (exit in listOf("readWork", "readWorkspace", "releaseWork", "pushWorkspace", "syncWorkspace")) {
                    operations.setField("config", operationConfig.copy(maxSyncWorkUnitsPerInvocation = if (exit == "readWorkspace") 1000000 else 1))
                    stages.setField("config", if (exit == "readWorkspace") stageConfig.copy(maxSyncWorkspaceBytes = 1) else stageConfig)
                    for (query in listOf("BEGIN IMMEDIATE", "SELECT COUNT(*) FROM _sync_dirty_rows", "SELECT COUNT(*) FROM _sync_outbox_rows")) {
                        val readFailure = IllegalStateException("injected final progress failure: $query")
                        var rolledBack = false
                        var reservationReleased = false
                        var intercepted = 0
                        rawField.set(db, object : SQLiteConnection by raw {
                            override fun prepare(sql: String): SQLiteStatement {
                                if (sql == "ROLLBACK") rolledBack = true
                                if (rolledBack && sql.startsWith("UPDATE _sync_operation_state SET workspace_context=")) reservationReleased = true
                                val needsRelease = exit == "pushWorkspace" || exit == "syncWorkspace"
                                if (rolledBack && (!needsRelease || reservationReleased) && sql.equals(query, ignoreCase = true)) {
                                    intercepted++
                                    throw readFailure
                                }
                                return raw.prepare(sql)
                            }
                        })
                        val error = when (exit) {
                            "readWork", "readWorkspace" -> client.readPushDependencyDiagnostic(diagnostic.diagnosticId).exceptionOrNull()
                            "releaseWork" -> client.releasePushDependencyDiagnostic(diagnostic.diagnosticId).exceptionOrNull()
                            "pushWorkspace" -> client.pushPending().exceptionOrNull()
                            else -> client.sync().exceptionOrNull()
                        }
                        rawField.set(db, raw)
                        assertEquals(1, intercepted, "$exit / $query")
                        if (exit == "pushWorkspace" || exit == "syncWorkspace") assertTrue(reservationReleased, "response cleanup must precede final capture")
                        if (exit.endsWith("Work")) {
                            val work = assertIs<SyncWorkLimitException>(error, "$exit / $query")
                            assertEquals(unit, work.unit)
                            assertEquals(1L, work.limit)
                            assertEquals(1L, work.used)
                        } else {
                            val workspace = assertIs<SyncWorkspaceLimitException>(error, "$exit / $query")
                            assertEquals(unit, workspace.unit)
                            assertEquals(1L, workspace.limit)
                            assertEquals(if (exit == "readWorkspace") "bytes" else "atomic_apply_work", workspace.dimension)
                            assertTrue(workspace.reserved > 0)
                            assertFalse(workspace.resumable)
                        }
                        assertNotNull(error)
                        assertEquals(1, error.suppressedExceptions.size)
                        assertTrue(generateSequence(error.suppressedExceptions.single()) { it.cause }.any { it === readFailure })
                        assertEquals(before, baselineQualificationVector(db, listOf("authors", "profiles", "users")))
                        assertTrue(operations.runExclusiveOperation { Unit }.isSuccess)
                    }
                }
            } finally {
                rawField.set(db, raw)
                operations.setField("config", operationConfig)
                stages.setField("config", stageConfig)
            }
        }
    }

    @Test fun publicDiagnosticProgressCancellationWins() = checkPublicCancellation(false)

    @Test fun publicDiagnosticCallerCancellationWins() = checkPublicCancellation(true)

    private fun checkPublicCancellation(cancelCaller: Boolean) = runOwnedWebDatabaseTest {
        withBlockedDiagnostic { db, client, diagnostic ->
            val before = baselineQualificationVector(db, listOf("authors", "profiles", "users"))
            val operations = client.field("operationController") as OversqliteClientOperationController
            val config = operations.field("config") as OversqliteConfig
            operations.setField("config", config.copy(maxSyncWorkUnitsPerInvocation = 1))
            val raw = db.ref
            val rawField = db.javaClass.getDeclaredField("ref").apply { isAccessible = true }
            try {
                for (release in listOf(false, true)) {
                    var rolledBack = false
                    assertFailsWith<CancellationException> {
                        supervisorScope {
                            async {
                                val job = currentCoroutineContext()[Job]!!
                                rawField.set(db, object : SQLiteConnection by raw {
                                    override fun prepare(sql: String): SQLiteStatement {
                                        if (sql == "ROLLBACK") rolledBack = true
                                        if (rolledBack && sql.equals("SELECT COUNT(*) FROM _sync_dirty_rows", ignoreCase = true)) {
                                            val cancellation = CancellationException("diagnostic final read cancelled")
                                            if (cancelCaller) {
                                                job.cancel(cancellation)
                                                throw IllegalStateException("read failed while caller cancelled")
                                            }
                                            throw cancellation
                                        }
                                        return raw.prepare(sql)
                                    }
                                })
                                if (release) client.releasePushDependencyDiagnostic(diagnostic.diagnosticId)
                                else client.readPushDependencyDiagnostic(diagnostic.diagnosticId)
                            }.await()
                        }
                    }
                    rawField.set(db, raw)
                    assertTrue(rolledBack, "primary diagnostic refusal must precede cancellation")
                    assertEquals(before, baselineQualificationVector(db, listOf("authors", "profiles", "users")))
                    assertTrue(operations.runExclusiveOperation { Unit }.isSuccess)
                }
            } finally { rawField.set(db, raw); operations.setField("config", config) }
        }
    }

    private suspend fun withBlockedDiagnostic(block: suspend (SafeSQLiteConnection, DefaultOversqliteClient, PushDependencyGroupExceedsAdmittedLimitException) -> Unit) {
        val server = MockSyncServer().apply { registeredTableSpecs = testRegisteredTableSpecs("authors", "profiles", "users") }
        val db = newDb()
        val http = server.newHttpClient()
        var client: DefaultOversqliteClient? = null
        try {
            db.execSQL("CREATE TABLE authors(id TEXT PRIMARY KEY NOT NULL,profile_id TEXT NOT NULL REFERENCES profiles(id) DEFERRABLE INITIALLY DEFERRED,name TEXT NOT NULL)")
            db.execSQL("CREATE TABLE profiles(id TEXT PRIMARY KEY NOT NULL,author_id TEXT NOT NULL REFERENCES authors(id) DEFERRABLE INITIALLY DEFERRED,bio TEXT NOT NULL)")
            db.execSQL("CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)")
            val tables = listOf("users", "authors", "profiles").map { SyncTable(it, syncKeyColumnName = "id") }
            client = newClient(db, http, syncTables = tables)
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN")
            db.execSQL("INSERT INTO authors VALUES('a','p','Ada')")
            db.execSQL("INSERT INTO profiles VALUES('p','a','bio')")
            db.execSQL("COMMIT")
            client.pushPending().getOrThrow()
            client.closeFromCoordinatedOwner()
            server.wireLimits = testWireLimits().copy(maxPushRows = 1)
            client = newClient(db, http, maxPushRows = 1, syncTables = tables)
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("BEGIN")
            db.execSQL("DELETE FROM authors WHERE id='a'")
            db.execSQL("DELETE FROM profiles WHERE id='p'")
            db.execSQL("COMMIT")
            val diagnostic = assertIs<PushDependencyGroupExceedsAdmittedLimitException>(client.pushPending().exceptionOrNull())
            assertEquals(SyncProgress(dirtyRows = 2), diagnostic.progress)
            block(db, client, diagnostic)
        } finally { client?.closeFromCoordinatedOwner(); http.close(); db.close() }
    }

    private fun Any.field(name: String): Any = javaClass.getDeclaredField(name).apply { isAccessible = true }.get(this)
    private fun Any.setField(name: String, value: Any) = javaClass.getDeclaredField(name).apply { isAccessible = true }.set(this, value)
}
