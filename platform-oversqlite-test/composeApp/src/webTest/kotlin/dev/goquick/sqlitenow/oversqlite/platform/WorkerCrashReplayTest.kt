package dev.goquick.sqlitenow.oversqlite.platform

import dev.goquick.sqlitenow.core.worker.SqliteWorkerSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.SqliteException
import dev.goquick.sqlitenow.oversqlite.*
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.*
import kotlin.test.*
import kotlin.time.Duration.Companion.seconds

internal class WorkerCrashReplayTest : PlatformCrossTargetTestSupport() {
    @Test fun replaysCommittedResponseAfterWorkerCrashAndCallerCleanup() = runTest(timeout = 180.seconds) {
        val identity = "dev.goquick.sqlitenow.oversqlite.platform.WorkerCrashReplayTest.replaysCommittedResponseAfterWorkerCrashAndCallerCleanup"
        val runtime = webRuntimeKind()
        if (!platformSuiteEnabled()) {
            println("WORKER_REPLAY_NOT_EXECUTED $runtime $identity opt_in_disabled")
            return@runTest
        }
        if (runtime == "js-node") {
            println("WORKER_REPLAY_NOT_APPLICABLE $runtime $identity durable_restart_unavailable")
            return@runTest
        }
        check(runtime in setOf("js-browser", "wasm-browser")) { "unsupported default browser death runtime $runtime" }
        println("WORKER_REPLAY_ENTER $runtime $identity")
        val dbName = workerOwnedStorage.newDatabaseName()
        val schema = mutableListOf(
            "CREATE TABLE users(id TEXT PRIMARY KEY NOT NULL,name TEXT NOT NULL)",
            "CREATE TABLE posts(id TEXT PRIMARY KEY NOT NULL,user_id TEXT NOT NULL REFERENCES users(id),title TEXT NOT NULL)",
        )
        fun database() = PlatformTestDatabase(dbName, oversqliteTestConnectionProvider(), schema.toMutableList(), null)
        val config = OversqliteConfig(
            generationId = 1, databaseId = "platform-sync", serviceId = "platform-mock",
            syncTables = listOf(SyncTable("users", syncKeyColumnName = "id"), SyncTable("posts", syncKeyColumnName = "id")),
        )
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val interrupted = database()
        var successor: PlatformTestDatabase? = null
        var terminated = false
        var frozenRequest = ""
        var frozenState: Map<String, List<String>>? = null
        var frozenJournal: String? = null
        var primary: Throwable? = null
        try {
            val client = interrupted.openClient(config, http, ServerWinsResolver)
            client.attach("user-1").getOrThrow()
            insertUser(interrupted, "P", "parent")
            insertPost(interrupted, "C", "P", "clean child")
            client.pushPending().getOrThrow()
            updateUserName(interrupted, "P", "local intent")
            server.authoritativePayloadTransform = { row ->
                if (row.table == "users" && row.key["id"] == "P")
                    JsonObject(row.payload!!.jsonObject + ("name" to JsonPrimitive("normalized parent")))
                else row.payload
            }
            server.beforePushResponse = {
                server.beforePushResponse = null
                frozenRequest = server.requestBodies.last { it.first == "/sync/push" }.second
                frozenState = databaseStateVector(interrupted.connection(), listOf("users", "posts"),
                    ignoreDerivedCharges = true)
                frozenJournal = scalarText(interrupted, "SELECT json_array(binding,lower(hex(request_body)),request_digest,lower(hex(outcome_body)),outcome_hash,phase) FROM _sync_upload_journal")
                assertEquals(2L, scalarLong(interrupted, "SELECT source_bundle_id FROM _sync_outbox_bundle"))
                (interrupted.connection().ref as SqliteWorkerSQLiteConnection).forceTerminateOwnerForTest()
                terminated = true
                throw CancellationException("default browser worker terminated after server commit")
            }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            assertTrue(terminated, "the default worker was forcibly terminated")
            assertTrue(sameOriginIframeCanAcquireLifecycleLock(dbName), "dead worker released origin lock")
            val interruptedConnection = interrupted.connection()
            // The worker has already died. The surviving caller still owns its client lease.
            // Close through the existing coordinator and retain the crash diagnostic.
            val cleanupFailure = assertFailsWith<SqliteException> { interrupted.close() }
            assertEquals("SQLite worker was force-terminated during cleanup.", cleanupFailure.message)
            assertTrue(cleanupFailure.suppressedExceptions.isEmpty(),
                "unexpected cleanup diagnostics: ${cleanupFailure.suppressedExceptions}")
            assertFalse(client.cleanupPending, "caller operations, HTTP and active-client lease drained")
            assertTrue(interruptedConnection.ownedCloseComplete, "core connection and execution context closed")
            println("WORKER_CRASH_CLEANUP $runtime ${cleanupFailure.message}")
            val reopened = database()
            successor = reopened
            val resumed = reopened.openClient(config, http, ServerWinsResolver)
            assertNotSame(interruptedConnection, reopened.connection())
            assertEquals(frozenState, databaseStateVector(reopened.connection(), listOf("users", "posts"),
                ignoreDerivedCharges = true), "same stored database and frozen replay state")
            val beforeResume = server.requestCounts.toMap()
            val frozenBinding = Json.parseToJsonElement(frozenRequest).jsonObject
            assertNotNull(frozenJournal)
            assertEquals(frozenJournal, scalarText(reopened, "SELECT json_array(binding,lower(hex(request_body)),request_digest,lower(hex(outcome_body)),outcome_hash,phase) FROM _sync_upload_journal"), "complete original F bytes/binding/digest/outcome/phase survive the dead worker")
            resumed.attach("user-1").getOrThrow()
            resumed.pushPending().getOrThrow()
            assertEquals(frozenRequest, server.requestBodies.last { it.first == "/sync/push" }.second)
            assertEquals("normalized parent", scalarText(reopened, "SELECT name FROM users WHERE id='P'"))
            assertEquals("clean child", scalarText(reopened, "SELECT title FROM posts WHERE id='C'"))
            assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(reopened, "SELECT reserved_entries+reserved_bytes+length(workspace_context) FROM _sync_operation_state"))
            assertEquals(3L, scalarLong(reopened, "SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(2L, scalarLong(reopened, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM pragma_foreign_key_check"))
            assertEquals(2, server.pushRequestCount, "seed and committed loss only; original tuple resolves without resend")
            assertEquals((beforeResume["/sync/push-outcomes/resolve"] ?: 0) + 1, server.requestCounts["/sync/push-outcomes/resolve"])
            val resolved = Json.parseToJsonElement(server.requestBodies.last { it.first == "/sync/push-outcomes/resolve" }.second).jsonObject
            for (field in listOf("source_bundle_id", "canonical_request_hash")) assertEquals(frozenBinding.getValue(field), resolved.getValue(field), "original resolve $field")
            assertEquals(frozenBinding["initialization_id"]?.jsonPrimitive?.contentOrNull, resolved["initialization_id"]?.jsonPrimitive?.contentOrNull, "original nullable initialization binding; push may omit absent while resolve requires explicit null")
            assertEquals((beforeResume["/sync/push-outcomes/ack"] ?: 0) + 1, server.requestCounts["/sync/push-outcomes/ack"])
            val ack = Json.parseToJsonElement(server.requestBodies.last { it.first == "/sync/push-outcomes/ack" }.second).jsonObject
            for (field in listOf("source_bundle_id", "canonical_request_hash")) assertEquals(frozenBinding.getValue(field), ack.getValue(field), "original ack $field")
            assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_upload_journal"))
        } catch (failure: Throwable) {
            primary = failure
            throw failure
        } finally {
            server.beforePushResponse = null
            withContext(NonCancellable) {
                val failures = listOfNotNull(
                    runCatching { successor?.close() }.exceptionOrNull(),
                    runCatching { interrupted.close() }.exceptionOrNull(),
                    runCatching { http.close() }.exceptionOrNull(),
                    runCatching { cleanupOwnedWorkerStorage() }.exceptionOrNull(),
                )
                val first = primary ?: failures.firstOrNull()
                failures.filter { it !== first }.forEach { first?.addSuppressed(it) }
                if (primary == null && first != null) throw first
            }
        }
        println("WORKER_REPLAY_COMPLETE $runtime $identity committed_response/worker_crash/caller_cleanup/same_database/exact_replay")
    }
}
