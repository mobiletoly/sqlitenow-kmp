package dev.goquick.sqlitenow.oversqlite

import kotlinx.coroutines.CancellationException
import kotlin.test.*

internal class SettlementTest : SettlementBacklogTestSupport() {
    @Test fun backlogBound() = runSettlementBacklog { client ->
        client.pushPending().getOrThrow()
    }

    @Test fun triggerRestoration() = runOwnedWebDatabaseTest {
        withSurvivorFixture { server,db,_,client ->
            db.execSQL("UPDATE nodes SET name='changed parent' WHERE id='P'")
            server.normalizePushEffects={effects()}
            server.beforePushResponse={throw CancellationException("lost committed original")}
            assertFailsWith<CancellationException> {client.pushPending().getOrThrow()}
            db.execSQL("INSERT INTO nodes VALUES('U','new L',NULL)")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=1")
            db.execSQL("UPDATE nodes SET name='live exception' WHERE id='U'")
            db.execSQL("UPDATE _sync_apply_state SET apply_mode=0")
            db.execSQL("CREATE TRIGGER alter_u AFTER DELETE ON nodes WHEN OLD.id='P' BEGIN UPDATE nodes SET name='trigger damage' WHERE id='U'; END")
            server.normalizePushEffects={effects()}
            client.pushPending().getOrThrow()
            assertEquals("live exception",scalarText(db,"SELECT name FROM nodes WHERE id='U'"))
            assertContains(scalarText(db,"SELECT payload FROM _sync_dirty_rows WHERE key_json='{\"id\":\"U\"}'"),"new L")
            requireSurvivors(db)
            assertEquals(0L,scalarLong(db,"SELECT count(*) FROM _sync_apply_rows"))
        }
    }
}
