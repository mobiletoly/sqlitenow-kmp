package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class HistoricalContextRetentionTest : SurvivorTestSupport() {
    @Test fun firstBaseAndFrozenWireIdentitySurviveEditsAndPhysicalReopen() = runOwnedWebDatabaseTest {
        val path = createTempSqliteNowTestDbPath("historical-context")
        try {
            withSurvivorFixture(dbPath = path) { server, db, http, client ->
                val payload = scalarText(db, "SELECT server_payload FROM _sync_row_state WHERE json_extract(key_json,'$.id')='P'")
                val context = scalarLong(db, "SELECT server_context_id FROM _sync_row_state WHERE json_extract(key_json,'$.id')='P'")
                db.execSQL("UPDATE nodes SET name='first-edit' WHERE id='P'")
                db.execSQL("UPDATE nodes SET name='frozen-edit' WHERE id='P'")
                assertEquals(payload, scalarText(db, "SELECT base_payload FROM _sync_dirty_rows"))
                assertEquals(context, scalarLong(db, "SELECT base_context_id FROM _sync_dirty_rows"))
                server.beforePushResponse = { throw CancellationException("uncertain committed push") }
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                val hash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
                val request = scalarText(db, "SELECT hex(request_body) FROM _sync_upload_journal")
                assertEquals(context, scalarLong(db, "SELECT base_context_id FROM _sync_outbox_rows"))
                db.execSQL("UPDATE nodes SET name='later-edit' WHERE id='P'")
                assertEquals(payload, scalarText(db, "SELECT base_payload FROM _sync_outbox_rows"))
                client.closeFromCoordinatedOwner(); db.close()
                server.beforePushResponse = null
                val reopened = newFileBackedDb(path)
                val next = newClient(reopened, http, syncTables = nodeTables)
                try {
                    next.openAndConnect("user-1").getOrThrow()
                    assertEquals(hash, scalarText(reopened, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                    assertEquals(request, scalarText(reopened, "SELECT hex(request_body) FROM _sync_upload_journal"))
                    assertEquals(context, scalarLong(reopened, "SELECT base_context_id FROM _sync_outbox_rows"))
                    assertEquals(payload, scalarText(reopened, "SELECT base_payload FROM _sync_outbox_rows"))
                    next.pushPending().getOrThrow()
                    assertEquals("later-edit", scalarText(reopened, "SELECT name FROM nodes WHERE id='P'"))
                    assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM _sync_outbox_rows"))
                    assertEquals(0L, scalarLong(reopened, "SELECT count(*) FROM pragma_foreign_key_check"))
                } finally { next.closeFromCoordinatedOwner(); reopened.close() }
            }
        } finally { deleteTempSqliteNowTestDbArtifacts(path) }
    }

    @Test fun frozenRootsBlockReplacementAndCleanRecoveryUsesNewSource() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server, db, _, client ->
            try {
                val oldSource = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                db.execSQL("UPDATE nodes SET name='frozen' WHERE id='P'")
                server.beforePushResponse = { throw CancellationException("freeze") }
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                val context = scalarLong(db, "SELECT base_context_id FROM _sync_outbox_rows")
                markSourceRecoveryRequired(db, SourceRecoveryReason.HISTORY_PRUNED)
                // The frozen request keeps its historical context alive for as long as it is unsettled.
                // Replacement waits for that request: it settles under the old source before any rotation.
                repeat(3) { OversqliteProvenanceContextStore(db).cleanup() }
                assertEquals(1L, scalarLong(db, "SELECT count(*) FROM _sync_provenance_contexts WHERE context_id=$context"))
                assertEquals(oldSource, scalarText(db, "SELECT origin_source_id FROM _sync_provenance_contexts WHERE context_id=$context"))
                settleFrozenBeforeCleanRecovery(db, client, server)
                client.rebuild().getOrThrow()
                val source = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
                assertNotEquals(oldSource, source)
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_row_state r JOIN _sync_provenance_contexts c ON c.context_id=r.server_context_id WHERE c.origin_source_id<>'$source'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
            } finally { client.closeFromCoordinatedOwner() }
        }
    }
}
