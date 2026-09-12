package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.DatabaseMigrations
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.SqliteNowDatabase
import io.ktor.client.HttpClient
import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class SettlementReplayTest : SurvivorTestSupport() {
    @Test fun retriesLostCommittedResponseInSameLifetime() = runOwnedWebDatabaseTest {
        withLostCommittedResponse { server, db, _, client ->
            client.pushPending().getOrThrow()
            assertSettledReplay(server, db)
        }
        println("REPLAY_COMPLETE same_lifetime")
    }

    @Test fun replaysLostCommittedResponseAfterClientRecreation() = runOwnedWebDatabaseTest {
        withLostCommittedResponse { server, db, http, client ->
            val frozen = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
            client.closeFromCoordinatedOwner()
            val restarted = newClient(db, http, syncTables = nodeTables)
            try {
                restarted.openAndConnect("user-1").getOrThrow()
                assertEquals(frozen, scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                restarted.pushPending().getOrThrow()
                assertSettledReplay(server, db)
            } finally { restarted.closeFromCoordinatedOwner() }
        }
        println("REPLAY_COMPLETE client_recreation")
    }

    @Test fun coordinatedDatabaseReopenReclaimsOnlyOrphanedReservations() = runOwnedWebDatabaseTest {
        if (!supportsDurableTestDatabaseRestart()) {
            println("REPLAY_NOT_APPLICABLE coordinated_database_reopen durable_storage_unavailable")
            return@runOwnedWebDatabaseTest
        }
        val path = createTempSqliteNowTestDbPath("settlement-replay")
        try {
            withLostCommittedResponse(dbPath = path) { server, db, http, seed ->
                val before = baselineQualificationVector(db, listOf("nodes"))
                val unit = applyUnit("terminated-buffer", 0, "")
                val ledger = """[{"token":"$unit","unit":"$unit","context":"$unit","entries":4,"bytes":4196}]"""
                db.execSQL("UPDATE _sync_operation_state SET workspace_context='$ledger',reserved_entries=4,reserved_bytes=4196")
                seed.closeFromCoordinatedOwner()
                db.close()
                val ordinaryDb = newFileBackedDb(path)
                try {
                    val ordinary = newClient(ordinaryDb, http, syncTables = nodeTables)
                    try {
                        ordinary.openAndConnect("user-1").getOrThrow()
                        assertEquals(ledger, scalarText(ordinaryDb, "SELECT workspace_context FROM _sync_operation_state"))
                    } finally { ordinary.closeFromCoordinatedOwner() }
                } finally { ordinaryDb.close() }
                // Only the coordinated opening acquires the provider lifetime lease.
                // Ordinary client.open above must not reclaim this residue.
                val database = SqliteNowDatabase(path, object : DatabaseMigrations {
                    override val targetVersion = 0
                    override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int) = 0
                }, syncManaged = true)
                val opening = database.openForSync(OversqliteConfig(
                    generationId = 1, databaseId = "fixture", serviceId = "main", syncTables = nodeTables,
                ), http)
                try {
                    assertEquals(OversqliteOpeningOutcome.READY, opening.advance().outcome)
                    val ready = assertNotNull(opening.readyOrNull())
                    val reopened = database.connection()
                    assertEquals(before, baselineQualificationVector(reopened, listOf("nodes")))
                    ready.client.openAndConnect("user-1").getOrThrow()
                    ready.client.pushPending().getOrThrow()
                    assertSettledReplay(server, reopened)
                } finally { opening.close() }
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
        println("REPLAY_COMPLETE coordinated_database_reopen")
    }

    private suspend fun withLostCommittedResponse(
        dbPath: String? = null,
        block: suspend (MockSyncServer, SafeSQLiteConnection, HttpClient, DefaultOversqliteClient) -> Unit,
    ) = withSurvivorFixture(dbPath = dbPath) { server, db, http, client ->
        try {
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.normalizePushEffects = { effects() }
            server.beforePushResponse = { throw CancellationException("lost committed response") }
            assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
            assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
            block(server, db, http, client)
        } finally { client.closeFromCoordinatedOwner() }
    }

    private suspend fun assertSettledReplay(server: MockSyncServer, db: SafeSQLiteConnection) {
        requireSurvivors(db)
        assertEquals(3, server.pushRequestCount, "seed, committed loss, replay")
        assertEquals(server.pushBodies[1], server.pushBodies[2], "exact frozen request")
        assertEquals(3L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state"))
        assertEquals(2L, scalarLong(db, "SELECT last_bundle_seq_seen FROM _sync_attachment_state"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
        assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
        assertEquals(0L, scalarLong(db, "SELECT reserved_entries FROM _sync_operation_state"))
    }
}
