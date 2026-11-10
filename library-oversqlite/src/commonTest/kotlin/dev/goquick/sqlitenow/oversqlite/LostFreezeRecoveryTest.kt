package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.Json
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A freeze commit that never reached disk leaves the server holding a terminal outcome the client
 * has no local evidence for. The client re-freezes different content under the same source
 * sequence, receives `push_outcome_mismatch`, and must converge through the sequence-only retained
 * lookup with no lost row and no stuck upload (go-oversync r0001 sections 3.2.1 and 6.1).
 */
internal class LostFreezeRecoveryTest : CrossTargetSyncTestSupport() {
    private val bodyJson = Json { ignoreUnknownKeys = true }

    private suspend fun syncTables(db: SafeSQLiteConnection): List<String> = db.queryList(
        "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE '\\_sync\\_%' ESCAPE '\\' ORDER BY name",
    ) { it.getText(0) }

    // Stored generated columns are derived on insert; copy only the columns SQLite persists.
    private suspend fun storedColumns(db: SafeSQLiteConnection, table: String): String =
        db.queryList("SELECT name FROM pragma_table_xinfo('$table') WHERE hidden=0 ORDER BY cid") { it.getText(0) }
            .joinToString(",") { "\"$it\"" }

    /**
     * Captures every sync control table: the durable state a lost WAL commit falls back to.
     * [holder] is `temp` for one connection, or `main` when the capture must survive a reopen.
     */
    private suspend fun captureDurableSyncState(db: SafeSQLiteConnection, holder: String = "temp"): List<String> {
        val tables = syncTables(db)
        for (table in tables) {
            db.execSQL("CREATE TABLE $holder.\"durable_$table\" AS SELECT ${storedColumns(db, table)} FROM \"$table\"")
        }
        return tables
    }

    /**
     * Discards every sync-state commit made after [captureDurableSyncState], exactly as a power
     * failure before the WAL reached disk would. Only tables whose content changed are rewritten,
     * so revision triggers do not disturb untouched tables; tables without triggers are reconciled
     * once more so no trigger side effect survives.
     */
    private suspend fun loseSyncCommitsSince(db: SafeSQLiteConnection, tables: List<String>, holder: String = "temp") {
        val triggered = db.queryList("SELECT DISTINCT tbl_name FROM sqlite_schema WHERE type='trigger'") { it.getText(0) }.toSet()
        db.execSQL("PRAGMA foreign_keys = OFF")
        try {
            db.transaction(TransactionMode.IMMEDIATE) {
                for (table in tables + tables.filterNot { it in triggered }) {
                    val columns = storedColumns(db, table)
                    val durable = "$holder.\"durable_$table\""
                    val changed = scalarLong(db, "SELECT EXISTS(SELECT $columns FROM main.\"$table\" EXCEPT SELECT $columns FROM $durable) " +
                        "OR EXISTS(SELECT $columns FROM $durable EXCEPT SELECT $columns FROM main.\"$table\")") != 0L
                    if (!changed) continue
                    db.execSQL("DELETE FROM main.\"$table\"")
                    db.execSQL("INSERT INTO main.\"$table\"($columns) SELECT $columns FROM $durable")
                }
            }
        } finally {
            db.execSQL("PRAGMA foreign_keys = ON")
        }
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
        for (table in tables) db.execSQL("DROP TABLE $holder.\"durable_$table\"")
    }

    private suspend fun lostExchange(client: OversqliteClient): Throwable? =
        try { client.pushPending().getOrThrow(); null } catch (failure: Throwable) { failure }

    private fun pushedRows(server: MockSyncServer, body: String): Set<String> {
        val request = bodyJson.decodeFromString(CompletePushRequest.serializer(), body)
        return OversqliteCompactCodec.decodeBundleRows(testGeneration(server.registeredTableSpecs), request.rows)
            .map { "${it.table}:${it.op}:${it.key.getValue("id")}:${it.rowVersion}" }.toSet()
    }

    private suspend fun lostFreezeConverges(retainedStatus: String) {
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        createUsersAndPostsTables(db)
        var client = newClient(db, http)
        try {
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db, "seed", "Seed")
            client.sync().getOrThrow()
            val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val sequence = scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'")
            assertEquals(2L, sequence)

            // Local intent that the lost freeze uploads: one new row and one update of a synced row.
            insertUser(db, "ada", "Ada")
            updateUserName(db, "seed", "Seed v2")
            val durable = recoveryJournalVector(db)
            val durableTables = captureDurableSyncState(db)

            when (retainedStatus) {
                "committed" -> server.beforePushResponse = { throw CancellationException("committed response lost") }
                "rejected" -> {
                    server.applicationRejectionCode = "business_rule_rejected"
                    server.beforePushResponse = { throw CancellationException("rejected response lost") }
                }
                "cancelled" -> server.beforePushCommit = { throw CancellationException("request lost before commit") }
            }
            assertNotNull(lostExchange(client), "$retainedStatus: the first exchange must be lost")
            if (retainedStatus == "cancelled") {
                // The resumed request resolves, the server durably cancels it, and that answer is lost too.
                server.outcomeResponseOverride = { throw CancellationException("cancelled response lost") }
                assertNotNull(lostExchange(client), "cancelled: the resolve answer must be lost")
            }
            assertEquals(retainedStatus, server.terminalStatuses["$source\u0000$sequence"])
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(0, server.ackRequests.count { it.sourceBundleId == sequence })

            // Power loss: the freeze commit (and everything after it) never reached disk.
            client.closeFromCoordinatedOwner()
            loseSyncCommitsSince(db, durableTables)
            assertEquals(durable, recoveryJournalVector(db), "$retainedStatus: restore must reproduce the pre-freeze durable state")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(sequence, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'"))
            server.applicationRejectionCode = null

            // Later edits make the re-frozen request differ from the one the server already answered.
            updateUserName(db, "ada", "Ada v2")
            insertPost(db, "post", "ada", "Kept")

            client = newClient(db, http)
            client.openAndConnect("user-1").getOrThrow()
            val traceMark = server.requestTrace.size
            val pushMark = server.pushBodies.size
            client.sync().getOrThrow()

            // The mismatch is answered by one retained lookup, one ack under the retained binding,
            // and a fresh bundle at the next sequence; nothing is resolved or cancelled.
            assertEquals(
                listOf(
                    "POST /sync/push", "POST /sync/push-outcomes/retained", "POST /sync/push-outcomes/ack",
                    "POST /sync/push", "POST /sync/push-outcomes/ack",
                ),
                server.requestTrace.drop(traceMark).filter { it.startsWith("POST /sync/push") },
                retainedStatus,
            )
            assertEquals(listOf(sequence), server.retainedRequests.map { it.sourceBundleId })
            assertEquals(listOf(sequence, sequence + 1), server.ackRequests.takeLast(2).map { it.sourceBundleId })
            assertEquals(if (retainedStatus == "cancelled") 1 else 0, server.resolveRequests.size)
            assertEquals("committed", server.terminalStatuses["$source\u0000${sequence + 1}"])

            val successor = pushedRows(server, server.pushBodies.drop(pushMark).last())
            if (retainedStatus == "committed") {
                // Committed versions became the bases: the newer edit is rebased onto bundle 2, the
                // satisfied update is consumed, and the row outside the retained outcome is kept.
                assertEquals(setOf("users:UPDATE:ada:2", "posts:INSERT:post:0"), successor)
            } else {
                // No business effect was committed: every frozen row is re-uploaded on its original base.
                assertEquals(setOf("users:INSERT:ada:0", "users:UPDATE:seed:1", "posts:INSERT:post:0"), successor)
            }

            // No lost row: local and server state agree on every value the application wrote.
            assertEquals("Ada v2", scalarText(db, "SELECT name FROM users WHERE id='ada'"))
            assertEquals("Seed v2", scalarText(db, "SELECT name FROM users WHERE id='seed'"))
            assertEquals("Kept", scalarText(db, "SELECT title FROM posts WHERE id='post'"))
            val remote = server.remoteRowsForBaselineTest()
            assertEquals(setOf("users/id=ada", "users/id=seed", "posts/id=post"), remote.keys)
            assertTrue("Ada v2" in remote.getValue("users/id=ada"), remote.toString())
            assertTrue("Seed v2" in remote.getValue("users/id=seed"), remote.toString())
            assertTrue("Kept" in remote.getValue("posts/id=post"), remote.toString())

            // No stuck upload: nothing dirty, frozen or awaiting acknowledgment, and sequences advanced.
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(sequence + 2, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes FROM _sync_operation_state"))
            val settledPushes = server.pushRequestCount
            client.sync().getOrThrow()
            assertEquals(settledPushes, server.pushRequestCount, "$retainedStatus: a converged client must not upload again")
            println("LOST_FREEZE_RECOVERY_COMPLETE $retainedStatus")
        } finally {
            client.closeFromCoordinatedOwner()
            http.close()
            db.close()
        }
    }

    /**
     * A failure after the journal was durably rebound must resume from the stored retained
     * outcome: no second lookup, no re-dispatch of the frozen request and no lost intent.
     * A SQL failure inside local apply closes the connection, so this needs a durable database.
     */
    @Test fun interruptedLostFreezeRecoveryResumesFromStoredRetainedOutcome() = runOwnedWebDatabaseTest {
        if (!supportsDurableTestDatabaseRestart()) {
            println("NOT-APPLICABLE transient database SQL-fatal restart: interrupted lost-freeze recovery; durable-file targets own this proof")
            return@runOwnedWebDatabaseTest
        }
        val path = createTempSqliteNowTestDbPath("lost-freeze-interrupted")
        val server = MockSyncServer()
        val http = server.newHttpClient()
        var opening = recoveryJournalDatabase(path).openForSync(recoveryJournalConfig(), http).also { it.advance() }
        var raw: SafeSQLiteConnection? = null
        suspend fun reopenClient(): Pair<SafeSQLiteConnection, OversqliteClient> {
            raw?.close(); raw = null
            opening = recoveryJournalDatabase(path).openForSync(recoveryJournalConfig(), http).also { it.advance() }
            val ready = checkNotNull(opening.readyOrNull())
            ready.client.openAndConnect("user-1").getOrThrow()
            return ready.database.connection() to ready.client
        }
        suspend fun inspectClosed(): SafeSQLiteConnection {
            opening.close()
            return newFileBackedDb(path).also { raw = it }
        }
        try {
            val first = checkNotNull(opening.readyOrNull())
            var db = first.database.connection()
            var client = first.client
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db, "seed", "Seed")
            client.sync().getOrThrow()
            val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            insertUser(db, "ada", "Ada")
            updateUserName(db, "seed", "Seed v2")
            val durableTables = captureDurableSyncState(db, holder = "main")
            server.beforePushResponse = { throw CancellationException("committed response lost") }
            assertNotNull(lostExchange(client))
            assertEquals("committed", server.terminalStatuses["$source\u00002"])

            // Power loss: the freeze commit never reached disk; a later edit changes the request.
            db = inspectClosed()
            loseSyncCommitsSince(db, durableTables, holder = "main")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            updateUserName(db, "ada", "Ada v2")

            reopenClient().let { db = it.first; client = it.second }
            db.execSQL("CREATE TEMP TRIGGER fail_settlement BEFORE UPDATE ON users BEGIN SELECT RAISE(ABORT,'synthetic settlement failure');END")
            assertNotNull(lostExchange(client), "settlement must fail after the journal was rebound")

            // The retained evidence is durable under the retained binding; the frozen rows and every
            // local value are intact and nothing was acknowledged.
            db = inspectClosed()
            assertEquals("frozen", scalarText(db, "SELECT phase FROM _sync_upload_journal"))
            assertTrue(scalarLong(db, "SELECT length(outcome_body) FROM _sync_upload_journal") > 0L)
            val bound = scalarText(db, "SELECT json_extract(binding,'$.operation.canonical_request_hash') FROM _sync_upload_journal")
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertNotEquals(bound, scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
            assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(1, server.retainedRequests.size)
            assertEquals(0, server.ackRequests.count { it.sourceBundleId == 2L })
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'"))
            assertEquals("Ada v2", scalarText(db, "SELECT name FROM users WHERE id='ada'"))
            assertEquals("Seed v2", scalarText(db, "SELECT name FROM users WHERE id='seed'"))

            reopenClient().let { db = it.first; client = it.second }
            val traceMark = server.requestTrace.size
            val pushMark = server.pushBodies.size
            client.sync().getOrThrow()

            assertEquals(
                listOf("POST /sync/push-outcomes/ack", "POST /sync/push", "POST /sync/push-outcomes/ack"),
                server.requestTrace.drop(traceMark).filter { it.startsWith("POST /sync/push") },
            )
            assertEquals(1, server.retainedRequests.size, "resume must settle from the stored outcome")
            assertEquals(bound, server.ackRequests.first { it.sourceBundleId == 2L }.canonicalRequestHash)
            assertEquals(setOf("users:UPDATE:ada:2"), pushedRows(server, server.pushBodies.drop(pushMark).single()))
            assertEquals("Ada v2", scalarText(db, "SELECT name FROM users WHERE id='ada'"))
            assertEquals("Seed v2", scalarText(db, "SELECT name FROM users WHERE id='seed'"))
            val remote = server.remoteRowsForBaselineTest()
            assertTrue("Ada v2" in remote.getValue("users/id=ada"), remote.toString())
            assertTrue("Seed v2" in remote.getValue("users/id=seed"), remote.toString())
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(4L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'"))
            assertEquals(0L, scalarLong(db, "SELECT reserved_entries+reserved_bytes FROM _sync_operation_state"))
            println("LOST_FREEZE_RECOVERY_COMPLETE interrupted")
        } finally {
            raw?.close()
            opening.close()
            http.close()
            deleteTempSqliteNowTestDbArtifacts(path)
        }
    }

    @Test fun lostFreezeAfterServerCommitSettlesRetainedOutcomeAndKeepsNewerIntent() = runOwnedWebDatabaseTest {
        lostFreezeConverges("committed")
    }

    @Test fun lostFreezeAfterServerRejectionRequeuesFrozenIntent() = runOwnedWebDatabaseTest {
        lostFreezeConverges("rejected")
    }

    @Test fun lostFreezeAfterServerCancellationRequeuesFrozenIntent() = runOwnedWebDatabaseTest {
        lostFreezeConverges("cancelled")
    }

    /**
     * A lost freeze can leave no dirty row behind: when the application deletes the row the lost
     * freeze inserted, the next push prunes that no-op without consuming the sequence, so pull is
     * admitted. Pull then installs the source's own lost bundle C and a peer's later version of the
     * same key. The next freeze reuses C's sequence and recovers C through the retained lookup. C's
     * effect for that key is history the pulled version already superseded: settlement must neither
     * lower the row nor refuse, and the new intent must upload under the next sequence.
     */
    @Test fun retainedEffectSupersededByPulledVersionSettlesWithoutLoweringIt() = runOwnedWebDatabaseTest {
        val db = newDb()
        val peerDb = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val peerHttp = server.newHttpClient()
        createUsersAndPostsTables(db)
        createUsersAndPostsTables(peerDb)
        var client = newClient(db, http)
        val peer = newClient(peerDb, peerHttp)
        suspend fun adaVersion(): Long = scalarLong(db,
            "SELECT row_version FROM _sync_row_state WHERE table_name='users' AND json_extract(key_json,'$.id')='ada'")
        try {
            client.openAndConnect("user-1").getOrThrow()
            peer.openAndConnect("user-1").getOrThrow()
            insertUser(db, "seed", "Seed")
            client.sync().getOrThrow()
            val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            val sequence = scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'")

            insertUser(db, "ada", "Ada")
            val durableTables = captureDurableSyncState(db)
            server.beforePushResponse = { throw CancellationException("committed response lost") }
            assertNotNull(lostExchange(client), "the committed answer must be lost")
            assertEquals("committed", server.terminalStatuses["$source\u0000$sequence"])
            client.closeFromCoordinatedOwner()
            loseSyncCommitsSince(db, durableTables)
            assertEquals(sequence, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'"))

            // A peer replaces C's effect with a later version.
            peer.pullToStable().getOrThrow()
            updateUserName(peerDb, "ada", "peer edit")
            assertEquals(PushOutcome.COMMITTED, peer.pushPending().getOrThrow().outcome)

            // The application deletes the never-published row; the pruned no-op leaves nothing dirty
            // and the sequence unused, so pull installs C and then the peer's version.
            db.execSQL("DELETE FROM users WHERE id='ada'")
            client = newClient(db, http)
            client.openAndConnect("user-1").getOrThrow()
            val pushMark = server.pushBodies.size
            assertEquals(PushOutcome.NO_CHANGE, client.pushPending().getOrThrow().outcome)
            assertEquals(pushMark, server.pushBodies.size, "a pruned no-op dispatches nothing")
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            client.pullToStable().getOrThrow()
            val pulledVersion = adaVersion()
            assertEquals("peer edit", scalarText(db, "SELECT name FROM users WHERE id='ada'"))

            // New intent reuses the lost sequence, so the server answers with a mismatch.
            insertUser(db, "bob", "Bob")
            // A higher version is history only once pull has passed C; before that it is an integrity failure.
            val checkpoint = scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state")
            db.execSQL("UPDATE _sync_attachment_state SET last_bundle_seq_seen=1")
            val fenced = client.pushPending().exceptionOrNull()
            assertNotNull(fenced)
            assertTrue("would lower row version" in fenced.message.orEmpty(), fenced.toString())
            assertEquals(pulledVersion, adaVersion())
            db.execSQL("UPDATE _sync_attachment_state SET last_bundle_seq_seen=$checkpoint")

            assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
            assertEquals(listOf(sequence), server.retainedRequests.map { it.sourceBundleId })
            assertEquals(sequence, server.ackRequests.last().sourceBundleId)
            assertEquals(sequence + 1, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'"))
            // The superseded effect changed nothing: the pulled version and value stay authoritative.
            assertEquals(pulledVersion, adaVersion())
            assertEquals("peer edit", scalarText(db, "SELECT name FROM users WHERE id='ada'"))
            assertEquals("users/bob/INSERT/0", scalarText(db,
                "SELECT group_concat(table_name||'/'||json_extract(key_json,'$.id')||'/'||op||'/'||base_row_version) FROM _sync_dirty_rows"))

            assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
            assertEquals(setOf("users:INSERT:bob:0"), pushedRows(server, server.pushBodies.last()))
            assertEquals(sequence + 1, server.ackRequests.last().sourceBundleId)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(sequence + 2, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='$source'"))
            val remote = server.remoteRowsForBaselineTest()
            assertTrue("peer edit" in remote.getValue("users/id=ada"), remote.toString())
            assertTrue("Bob" in remote.getValue("users/id=bob"), remote.toString())
        } finally {
            client.closeFromCoordinatedOwner()
            peer.closeFromCoordinatedOwner()
            http.close()
            peerHttp.close()
            db.close()
            peerDb.close()
        }
    }
}
