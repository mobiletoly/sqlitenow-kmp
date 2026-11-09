package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Source recovery must never strand local edits. A client whose source the server refuses keeps every
 * pending edit, rebuilds under the replacement source and uploads the edits there. Edits that cannot
 * apply to the authoritative snapshot block the rebuild until the caller discards them explicitly.
 * An edit of a row that another device changed in the meantime keeps the version it was made from, so
 * its upload raises an ordinary conflict instead of overwriting that change.
 */
internal class SourceRecoveryPreservedIntentTest : CrossTargetSyncTestSupport() {
    @Test
    fun frozenUnsentUploadAndLaterEditsReachServerUnderReplacementSource() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val verifyDb = newDb()
        val http = server.newHttpClient()
        val verifyHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            createUsersAndPostsTables(verifyDb)
            val client = newClient(db, http)
            val verifyClient = newClient(verifyDb, verifyHttp)
            client.openAndConnect("user-1").getOrThrow()
            verifyClient.openAndConnect("user-1").getOrThrow()
            insertUser(db, "u1", "Synced")
            insertPost(db, "p1", "u1", "Synced post")
            client.pushPending().getOrThrow()
            val oldSource = currentSource(db)

            // An upload freezes and its request never reaches the server.
            updateUserName(db, "u1", "Edited offline")
            insertUser(db, "u2", "Created offline")
            server.cancelNextPush = true
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal WHERE phase='frozen' AND length(outcome_body)=0"))
            assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))

            // Later edits stay dirty behind the frozen upload.
            insertPost(db, "p2", "u2", "Offline post")
            db.execSQL("DELETE FROM posts WHERE id='p1'")
            assertEquals(2L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))

            // The server retires the source. Neither push nor resolve can ever settle the frozen upload.
            server.retiredSources[oldSource] = "replacement-source"
            markSourceRecoveryRequired(db, SourceRecoveryReason.SOURCE_RETIRED, "replacement-source")
            assertEquals(SourceRecoveryReason.SOURCE_RETIRED,
                assertFailsWith<SourceRecoveryRequiredException> { client.pushPending().getOrThrow() }.reason)
            assertFails { updateUserName(db, "u1", "Blocked while recovery is required") }
            val commitsBefore = server.remoteRowsForBaselineTest()

            client.rebuild().getOrThrow()

            assertEquals("replacement-source", currentSource(db))
            assertEquals(listOf(oldSource to "replacement-source"),
                server.requestedSnapshotSourceReplacements.map { it.previousSourceId to it.newSourceId })
            assertEquals(commitsBefore, server.remoteRowsForBaselineTest(), "rebuild itself uploads nothing")
            assertEquals("none", scalarText(db, "SELECT kind FROM _sync_operation_state"))
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle"))
            assertEquals(1L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='replacement-source'"))

            // Every edit is still visible locally and is pending against the installed snapshot versions.
            assertEquals("Edited offline", scalarText(db, "SELECT name FROM users WHERE id='u1'"))
            assertEquals("Created offline", scalarText(db, "SELECT name FROM users WHERE id='u2'"))
            assertEquals("Offline post", scalarText(db, "SELECT title FROM posts WHERE id='p2'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM posts WHERE id='p1'"))
            assertEquals(
                listOf("posts/p1/DELETE/1", "posts/p2/INSERT/0", "users/u1/UPDATE/1", "users/u2/INSERT/0"),
                pendingIntent(db),
            )
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))

            // Ordinary push uploads all of it as the replacement source's first bundle.
            val pushesBefore = server.pushBodies.size
            assertEquals(PushOutcome.COMMITTED, client.pushPending().getOrThrow().outcome)
            assertEquals(pushesBefore + 1, server.pushBodies.size)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id='replacement-source'"))

            verifyClient.pullToStable().getOrThrow()
            assertEquals("Edited offline", scalarText(verifyDb, "SELECT name FROM users WHERE id='u1'"))
            assertEquals("Created offline", scalarText(verifyDb, "SELECT name FROM users WHERE id='u2'"))
            assertEquals("Offline post", scalarText(verifyDb, "SELECT title FROM posts WHERE id='p2'"))
            assertEquals(0L, scalarLong(verifyDb, "SELECT count(*) FROM posts WHERE id='p1'"))
            assertEquals(2L, scalarLong(verifyDb, "SELECT count(*) FROM users"))
            assertEquals(1L, scalarLong(verifyDb, "SELECT count(*) FROM posts"))
        } finally {
            http.close()
            verifyHttp.close()
            db.close()
            verifyDb.close()
        }
    }

    @Test
    fun dirtyRowsWithoutFrozenUploadAreCarriedWhenSequenceCannotContinue() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val verifyDb = newDb()
        val http = server.newHttpClient()
        val verifyHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            createUsersAndPostsTables(verifyDb)
            val client = newClient(db, http)
            val verifyClient = newClient(verifyDb, verifyHttp)
            client.openAndConnect("user-1").getOrThrow()
            verifyClient.openAndConnect("user-1").getOrThrow()
            insertUser(db, "u1", "Synced")
            client.pushPending().getOrThrow()
            val oldSource = currentSource(db)

            updateUserName(db, "u1", "Dirty before recovery")
            server.sequenceRefusedSources[oldSource] = "source_sequence_out_of_order"
            markSourceRecoveryRequired(db, SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER, "replacement-source")
            assertFailsWith<SourceRecoveryRequiredException> { client.pushPending().getOrThrow() }

            client.rebuild().getOrThrow()

            assertEquals("replacement-source", currentSource(db))
            assertEquals("Dirty before recovery", scalarText(db, "SELECT name FROM users WHERE id='u1'"))
            assertEquals(listOf("users/u1/UPDATE/1"), pendingIntent(db))
            client.pushPending().getOrThrow()
            verifyClient.pullToStable().getOrThrow()
            assertEquals("Dirty before recovery", scalarText(verifyDb, "SELECT name FROM users WHERE id='u1'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            verifyHttp.close()
            db.close()
            verifyDb.close()
        }
    }

    @Test
    fun editThatCannotApplyToSnapshotBlocksRebuildUntilExplicitDiscard() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val otherDb = newDb()
        val http = server.newHttpClient()
        val otherHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            createUsersAndPostsTables(otherDb)
            val client = newClient(db, http)
            val other = newClient(otherDb, otherHttp)
            client.openAndConnect("user-1").getOrThrow()
            other.openAndConnect("user-1").getOrThrow()
            insertUser(db, "u1", "Parent")
            client.pushPending().getOrThrow()
            other.pullToStable().getOrThrow()
            val oldSource = currentSource(db)

            // A local child is pending while another device deletes its parent on the server.
            insertPost(db, "p9", "u1", "Child of a remotely deleted parent")
            otherDb.execSQL("DELETE FROM users WHERE id='u1'")
            other.pushPending().getOrThrow()
            server.retiredSources[oldSource] = "replacement-source"
            assertFailsWith<SourceRecoveryRequiredException> { client.pushPending().getOrThrow() }
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal WHERE phase='frozen'"))

            // No discard is possible before the server has accepted the replacement source.
            assertIs<IllegalStateException>(client.discardPendingIntentForRecovery().exceptionOrNull())

            val blocked = assertFailsWith<PendingIntentUnsatisfiableException> { client.rebuild().getOrThrow() }
            assertEquals(setOf("posts"), blocked.tables)
            assertEquals(oldSource, currentSource(db))
            assertEquals("source_recovery", scalarText(db, "SELECT kind FROM _sync_operation_state"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals("Parent", scalarText(db, "SELECT name FROM users WHERE id='u1'"))
            assertEquals("Child of a remotely deleted parent", scalarText(db, "SELECT title FROM posts WHERE id='p9'"))
            assertFailsWith<SourceRecoveryRequiredException> { client.pushPending().getOrThrow() }
            // A repeated rebuild stays blocked and still changes nothing.
            assertFailsWith<PendingIntentUnsatisfiableException> { client.rebuild().getOrThrow() }
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))

            val dropped = client.discardPendingIntentForRecovery().getOrThrow()
            assertEquals(listOf(DiscardedIntentRow("main", "posts", """{"id":"p9"}""", "INSERT")), dropped.rows)
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))

            client.rebuild().getOrThrow()
            assertEquals("replacement-source", currentSource(db))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM posts"))
            assertEquals("none", scalarText(db, "SELECT kind FROM _sync_operation_state"))
            assertEquals(PushOutcome.NO_CHANGE, client.pushPending().getOrThrow().outcome)
            assertTrue(server.remoteRowsForBaselineTest().isEmpty(), "the dropped edit never reached the server")
        } finally {
            http.close()
            otherHttp.close()
            db.close()
            otherDb.close()
        }
    }

    @Test
    fun editThatCollidesWithSnapshotUniqueValueBlocksRebuildUntilExplicitDiscard() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val otherDb = newDb()
        val http = server.newHttpClient()
        val otherHttp = server.newHttpClient()
        try {
            createUniqueNameUsersAndPostsTables(db)
            createUniqueNameUsersAndPostsTables(otherDb)
            val client = newClient(db, http)
            val other = newClient(otherDb, otherHttp)
            client.openAndConnect("user-1").getOrThrow()
            other.openAndConnect("user-1").getOrThrow()
            insertUser(db, "u1", "Alpha")
            client.pushPending().getOrThrow()
            other.pullToStable().getOrThrow()
            val oldSource = currentSource(db)

            // The same unique name is pending locally and reaches the server from another device.
            insertUser(db, "u2", "Taken")
            insertUser(otherDb, "u3", "Taken")
            other.pushPending().getOrThrow()
            server.retiredSources[oldSource] = "replacement-source"
            markSourceRecoveryRequired(db, SourceRecoveryReason.SOURCE_RETIRED, "replacement-source")

            val blocked = assertFailsWith<PendingIntentUnsatisfiableException> { client.rebuild().getOrThrow() }
            assertEquals(setOf("users"), blocked.tables)
            assertEquals(oldSource, currentSource(db))
            assertEquals("Taken", scalarText(db, "SELECT name FROM users WHERE id='u2'"))
            assertEquals(0L, scalarLong(db, "SELECT count(*) FROM users WHERE id='u3'"))
            assertEquals(listOf("users/u2/INSERT/0"), pendingIntent(db))

            val dropped = client.discardPendingIntentForRecovery().getOrThrow()
            assertEquals(listOf(DiscardedIntentRow("main", "users", """{"id":"u2"}""", "INSERT")), dropped.rows)
            client.rebuild().getOrThrow()
            assertEquals("replacement-source", currentSource(db))
            assertEquals("Alpha,Taken", scalarText(db, "SELECT group_concat(name, ',') FROM (SELECT name FROM users ORDER BY id)"))
            assertEquals("u3", scalarText(db, "SELECT id FROM users WHERE name='Taken'"))
        } finally {
            http.close()
            otherHttp.close()
            db.close()
            otherDb.close()
        }
    }

    @Test
    fun editOfRowChangedByPeerDuringLockoutIsResolvedInsteadOfOverwritingPeer() = runOwnedWebDatabaseTest {
        val resolver = RecordingResolver(MergeResult.AcceptServer)
        rebuildBehindPeerChange(
            resolver = resolver,
            localEdit = { updateUserName(it, "u1", "Edited offline") },
            peerEdit = { updateUserName(it, "u1", "Edited by peer") },
        ) { f ->
            // The application still sees its edit, pending against the version it was made from.
            assertEquals("Edited offline", scalarText(f.db, "SELECT name FROM users WHERE id='u1'"))
            assertEquals(listOf("users/u1/UPDATE/${f.syncedVersion}"), pendingIntent(f.db))
            assertTrue(observedVersion(f.db, "u1") > f.syncedVersion)
            assertTrue(resolver.conflicts.isEmpty())

            f.client.pushPending().getOrThrow()

            val conflict = resolver.conflicts.single()
            assertEquals("UPDATE", conflict.localOp)
            assertEquals(f.syncedVersion, conflict.baseRowVersion)
            assertTrue(conflict.serverRowVersion > f.syncedVersion)
            assertEquals(false, conflict.serverRowDeleted)
            assertEquals("Edited by peer", conflict.serverRow!!.jsonObject.getValue("name").jsonPrimitive.content)
            assertEquals("Edited offline", conflict.localPayload!!.jsonObject.getValue("name").jsonPrimitive.content)
            assertEquals("Edited by peer", scalarText(f.db, "SELECT name FROM users WHERE id='u1'"))
            assertEquals(0L, scalarLong(f.db, "SELECT count(*) FROM _sync_dirty_rows"))
            f.peer.pullToStable().getOrThrow()
            assertEquals("Edited by peer", scalarText(f.peerDb, "SELECT name FROM users WHERE id='u1'"))
        }
    }

    @Test
    fun frozenUnsentEditOfRowChangedByPeerUploadsOnlyAfterResolverKeepsIt() = runOwnedWebDatabaseTest {
        val resolver = RecordingResolver(MergeResult.KeepLocal)
        rebuildBehindPeerChange(
            resolver = resolver,
            freezeLocalEdit = true,
            localEdit = { updateUserName(it, "u1", "Edited offline") },
            peerEdit = { updateUserName(it, "u1", "Edited by peer") },
        ) { f ->
            assertEquals("Edited offline", scalarText(f.db, "SELECT name FROM users WHERE id='u1'"))
            assertEquals(listOf("users/u1/UPDATE/${f.syncedVersion}"), pendingIntent(f.db))
            assertEquals(0L, scalarLong(f.db, "SELECT count(*) FROM _sync_outbox_rows"))

            assertEquals(PushOutcome.COMMITTED, f.client.pushPending().getOrThrow().outcome)

            val conflict = resolver.conflicts.single()
            assertEquals(f.syncedVersion, conflict.baseRowVersion)
            assertEquals("Edited by peer", conflict.serverRow!!.jsonObject.getValue("name").jsonPrimitive.content)
            assertEquals(0L, scalarLong(f.db, "SELECT count(*) FROM _sync_dirty_rows"))
            f.peer.pullToStable().getOrThrow()
            assertEquals("Edited offline", scalarText(f.peerDb, "SELECT name FROM users WHERE id='u1'"))
        }
    }

    @Test
    fun editOfRowDeletedByPeerDuringLockoutIsResolvedInsteadOfResurrectingIt() = runOwnedWebDatabaseTest {
        val resolver = RecordingResolver(MergeResult.AcceptServer)
        rebuildBehindPeerChange(
            resolver = resolver,
            localEdit = { updateUserName(it, "u1", "Edited offline") },
            peerEdit = { it.execSQL("DELETE FROM users WHERE id='u1'") },
        ) { f ->
            assertEquals("Edited offline", scalarText(f.db, "SELECT name FROM users WHERE id='u1'"))
            assertEquals(listOf("users/u1/UPDATE/${f.syncedVersion}"), pendingIntent(f.db))

            f.client.pushPending().getOrThrow()

            val conflict = resolver.conflicts.single()
            assertEquals(f.syncedVersion, conflict.baseRowVersion)
            assertEquals(true, conflict.serverRowDeleted)
            assertEquals(0L, scalarLong(f.db, "SELECT count(*) FROM users WHERE id='u1'"))
            assertEquals(0L, scalarLong(f.db, "SELECT count(*) FROM _sync_dirty_rows"))
            f.peer.pullToStable().getOrThrow()
            assertEquals(0L, scalarLong(f.peerDb, "SELECT count(*) FROM users WHERE id='u1'"))
        }
    }

    @Test
    fun deleteOfRowChangedByPeerDuringLockoutIsResolvedInsteadOfDeletingPeerChange() = runOwnedWebDatabaseTest {
        val resolver = RecordingResolver(MergeResult.AcceptServer)
        rebuildBehindPeerChange(
            resolver = resolver,
            localEdit = { it.execSQL("DELETE FROM users WHERE id='u1'") },
            peerEdit = { updateUserName(it, "u1", "Edited by peer") },
        ) { f ->
            assertEquals(0L, scalarLong(f.db, "SELECT count(*) FROM users WHERE id='u1'"))
            assertEquals(listOf("users/u1/DELETE/${f.syncedVersion}"), pendingIntent(f.db))

            f.client.pushPending().getOrThrow()

            val conflict = resolver.conflicts.single()
            assertEquals("DELETE", conflict.localOp)
            assertEquals(f.syncedVersion, conflict.baseRowVersion)
            assertEquals("Edited by peer", conflict.serverRow!!.jsonObject.getValue("name").jsonPrimitive.content)
            assertEquals("Edited by peer", scalarText(f.db, "SELECT name FROM users WHERE id='u1'"))
            f.peer.pullToStable().getOrThrow()
            assertEquals("Edited by peer", scalarText(f.peerDb, "SELECT name FROM users WHERE id='u1'"))
        }
    }

    @Test
    fun insertOfKeyCreatedByPeerDuringLockoutIsResolvedInsteadOfOverwritingPeerRow() = runOwnedWebDatabaseTest {
        val resolver = RecordingResolver(MergeResult.AcceptServer)
        rebuildBehindPeerChange(
            resolver = resolver,
            localEdit = { insertUser(it, "u2", "Created offline") },
            peerEdit = { insertUser(it, "u2", "Created by peer") },
        ) { f ->
            assertEquals("Created offline", scalarText(f.db, "SELECT name FROM users WHERE id='u2'"))
            assertEquals(listOf("users/u2/INSERT/0"), pendingIntent(f.db))

            f.client.pushPending().getOrThrow()

            val conflict = resolver.conflicts.single()
            assertEquals(0L, conflict.baseRowVersion)
            assertEquals("Created by peer", conflict.serverRow!!.jsonObject.getValue("name").jsonPrimitive.content)
            assertEquals("Created by peer", scalarText(f.db, "SELECT name FROM users WHERE id='u2'"))
            f.peer.pullToStable().getOrThrow()
            assertEquals("Created by peer", scalarText(f.peerDb, "SELECT name FROM users WHERE id='u2'"))
        }
    }

    @Test
    fun defaultResolverDefersConflictFromLockoutAndKeepsBothValues() = runOwnedWebDatabaseTest {
        rebuildBehindPeerChange(
            resolver = DeferConflictResolver,
            localEdit = { updateUserName(it, "u1", "Edited offline") },
            peerEdit = { updateUserName(it, "u1", "Edited by peer") },
        ) { f ->
            assertIs<ConflictNeedsResolutionException>(f.client.pushPending().exceptionOrNull())

            assertEquals(1L, scalarLong(f.db, "SELECT count(*) FROM _sync_conflict_state WHERE decision_state='pending'"))
            assertEquals("Edited offline", scalarText(f.db, "SELECT name FROM users WHERE id='u1'"))
            f.peer.pullToStable().getOrThrow()
            assertEquals("Edited by peer", scalarText(f.peerDb, "SELECT name FROM users WHERE id='u1'"))
        }
    }

    @Test
    fun discardIsRefusedOutsideSourceRecovery() = runOwnedWebDatabaseTest {
        val server = MockSyncServer()
        val db = newDb()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http)
            client.openAndConnect("user-1").getOrThrow()
            insertUser(db, "u1", "Ordinary pending edit")

            assertIs<IllegalStateException>(client.discardPendingIntentForRecovery().exceptionOrNull())
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
            assertEquals("Ordinary pending edit", scalarText(db, "SELECT name FROM users WHERE id='u1'"))
        } finally {
            http.close()
            db.close()
        }
    }

    private class RecordingResolver(private val result: MergeResult) : Resolver {
        val conflicts = mutableListOf<ConflictContext>()

        override fun resolve(conflict: ConflictContext): MergeResult {
            conflicts += conflict
            return result
        }
    }

    private class LockoutFixture(
        val db: SafeSQLiteConnection,
        val client: OversqliteClient,
        val peerDb: SafeSQLiteConnection,
        val peer: OversqliteClient,
        val syncedVersion: Long,
    )

    /**
     * Rebuilds one client out of source recovery after a peer changed a row the client edited offline.
     * With [freezeLocalEdit] the offline edit sits in a frozen request that never reached the server.
     */
    private suspend fun rebuildBehindPeerChange(
        resolver: Resolver,
        freezeLocalEdit: Boolean = false,
        localEdit: suspend (SafeSQLiteConnection) -> Unit,
        peerEdit: suspend (SafeSQLiteConnection) -> Unit,
        verify: suspend (LockoutFixture) -> Unit,
    ) {
        val server = MockSyncServer()
        val db = newDb()
        val peerDb = newDb()
        val http = server.newHttpClient()
        val peerHttp = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            createUsersAndPostsTables(peerDb)
            val client = newClient(db, http, resolver = resolver)
            val peer = newClient(peerDb, peerHttp)
            client.openAndConnect("user-1").getOrThrow()
            peer.openAndConnect("user-1").getOrThrow()
            insertUser(db, "u1", "Synced")
            client.pushPending().getOrThrow()
            peer.pullToStable().getOrThrow()
            val oldSource = currentSource(db)
            val syncedVersion = observedVersion(db, "u1")

            localEdit(db)
            if (freezeLocalEdit) {
                server.cancelNextPush = true
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_upload_journal WHERE phase='frozen' AND length(outcome_body)=0"))
            }
            peerEdit(peerDb)
            assertEquals(PushOutcome.COMMITTED, peer.pushPending().getOrThrow().outcome)

            server.retiredSources[oldSource] = "replacement-source"
            markSourceRecoveryRequired(db, SourceRecoveryReason.SOURCE_RETIRED, "replacement-source")
            val serverRowsBefore = server.remoteRowsForBaselineTest()

            client.rebuild().getOrThrow()

            assertEquals("replacement-source", currentSource(db))
            assertEquals(serverRowsBefore, server.remoteRowsForBaselineTest(), "rebuild itself uploads nothing")
            verify(LockoutFixture(db, client, peerDb, peer, syncedVersion))
        } finally {
            http.close()
            peerHttp.close()
            db.close()
            peerDb.close()
        }
    }

    private suspend fun observedVersion(db: SafeSQLiteConnection, userId: String): Long = scalarLong(
        db,
        "SELECT row_version FROM _sync_row_state WHERE table_name='users' AND json_extract(key_json, '$.id')='$userId'",
    )

    private suspend fun createUniqueNameUsersAndPostsTables(db: SafeSQLiteConnection) {
        db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL UNIQUE)")
        db.execSQL("CREATE TABLE posts (id TEXT PRIMARY KEY NOT NULL, user_id TEXT NOT NULL REFERENCES users(id), title TEXT NOT NULL)")
    }

    private suspend fun currentSource(db: SafeSQLiteConnection): String =
        scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")

    private suspend fun pendingIntent(db: SafeSQLiteConnection): List<String> = scalarText(
        db,
        """SELECT COALESCE(group_concat(entry, ','), '') FROM (
            SELECT table_name || '/' || json_extract(key_json, '$.id') || '/' || op || '/' || base_row_version AS entry
            FROM _sync_dirty_rows ORDER BY table_name, key_json)""",
    ).split(',').filter { it.isNotEmpty() }
}
