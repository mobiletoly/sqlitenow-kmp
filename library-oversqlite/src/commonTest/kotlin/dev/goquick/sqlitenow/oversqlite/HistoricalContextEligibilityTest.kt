package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class HistoricalContextEligibilityTest : SurvivorTestSupport() {
    @Test fun earlierVersionWithSameShapePreservesDirtyAndFrozenBases() = runOwnedWebDatabaseTest {
        for (frozen in listOf(false, true)) withSurvivorFixture { server, db, http, client ->
            db.execSQL("UPDATE nodes SET name='pending' WHERE id='P'")
            val context = scalarLong(db, "SELECT base_context_id FROM _sync_dirty_rows")
            val originalVersion = scalarLong(db, "SELECT local_version FROM _sync_provenance_contexts WHERE context_id=$context")
            var request = ""; var hash = ""
            if (frozen) {
                server.beforePushResponse = { throw CancellationException("lost response") }
                assertFailsWith<CancellationException> { client.pushPending().getOrThrow() }
                request = scalarText(db, "SELECT hex(request_body) FROM _sync_upload_journal")
                hash = scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle")
                server.beforePushResponse = null
            }
            val owner = if (frozen) "_sync_outbox_rows" else "_sync_dirty_rows"
            val payload = scalarText(db, "SELECT base_payload FROM $owner")
            val provenance = scalarText(db, "SELECT base_provenance FROM $owner")
            val pushes = server.pushRequestCount
            client.closeFromCoordinatedOwner()
            // Isolate the already-proved same-shape representation transition.
            // The JVM coordinated-opening regression separately drives the public migration API.
            db.transaction(TransactionMode.IMMEDIATE) {
                db.execSQL("UPDATE _sync_contract_identity SET local_version=${originalVersion + 1}")
                db.execSQL("PRAGMA user_version=${originalVersion + 1}")
                bindLocalBaselineContext(db)
            }
            val next = newClient(db, http, syncTables = nodeTables)
            try {
                next.openAndConnect("user-1").getOrThrow()
                assertEquals(context, scalarLong(db, "SELECT base_context_id FROM $owner"))
                assertEquals(payload, scalarText(db, "SELECT base_payload FROM $owner"))
                assertEquals(provenance, scalarText(db, "SELECT base_provenance FROM $owner"))
                assertEquals(originalVersion, scalarLong(db, "SELECT local_version FROM _sync_provenance_contexts WHERE context_id=$context"))
                val active = OversqliteProvenanceContextStore(db).activeId()
                assertNotEquals(context, active)
                assertEquals(originalVersion + 1, scalarLong(db, "SELECT local_version FROM _sync_provenance_contexts WHERE context_id=$active"))
                if (frozen) {
                    assertEquals(request, scalarText(db, "SELECT hex(request_body) FROM _sync_upload_journal"))
                    assertEquals(hash, scalarText(db, "SELECT canonical_request_hash FROM _sync_outbox_bundle"))
                }
                next.pushPending().getOrThrow()
                assertEquals("pending", scalarText(db, "SELECT name FROM nodes WHERE id='P'"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM _sync_outbox_rows"))
                if (frozen) assertEquals(pushes, server.pushRequestCount, "original F resolves without another push")
                assertEquals(0L, scalarLong(db, "SELECT count(*) FROM pragma_foreign_key_check"))
            } finally { next.closeFromCoordinatedOwner() }
        }
    }

    @Test fun futureVersionAndDifferentShapeRemainUnavailable() = runOwnedWebDatabaseTest {
        for (changed in listOf(mapOf("local_version" to 1L), mapOf("physical_shape" to "other-shape")))
            withSurvivorFixture { server, db, _, client ->
                db.execSQL("UPDATE nodes SET name='pending' WHERE id='P'")
                val context = replaceHistoricalContextForTest(db, "_sync_dirty_rows", changed)
                assertFailsWith<ResolutionBaseUnavailableException> {
                    OversqliteProvenanceContextStore(db).validateHistorical(context, true)
                }
                val before = baselineQualificationVector(db, listOf("nodes", "_sync_dirty_rows", "_sync_outbox_rows"))
                val pushes = server.pushRequestCount
                assertIs<ResolutionBaseUnavailableException>(client.pushPending().exceptionOrNull())
                assertEquals(before, baselineQualificationVector(db, listOf("nodes", "_sync_dirty_rows", "_sync_outbox_rows")))
                assertEquals(pushes, server.pushRequestCount)
            }
    }

    @Test fun activeContextRequiresExactCurrentVersion() = runOwnedWebDatabaseTest {
        withSurvivorFixture { _, db, _, _ ->
            db.execSQL("UPDATE _sync_contract_identity SET local_version=local_version+1")
            assertFailsWith<IllegalArgumentException> { OversqliteProvenanceContextStore(db).validateRetained() }
        }
    }
}
