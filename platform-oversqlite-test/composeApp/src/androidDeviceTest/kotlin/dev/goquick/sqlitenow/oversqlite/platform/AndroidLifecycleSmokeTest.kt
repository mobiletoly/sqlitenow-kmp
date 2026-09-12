package dev.goquick.sqlitenow.oversqlite.platform

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.*
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class AndroidLifecycleSmokeTest {
    @Test
    fun openCreatesBundleMetadata_andCapturesDirtyRows() = runBlocking {
        requirePlatformSuite()
        val db = newInMemoryDb()
        val http = newNoopHttpClient()
        try {
            db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            val client = newCoordinatedClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            )
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val dirtyCount = db.connection().prepare("SELECT COUNT(*) FROM _sync_dirty_rows").use { st ->
                check(st.step())
                st.getLong(0)
            }
            val keyJson = db.connection().prepare("SELECT key_json FROM _sync_dirty_rows LIMIT 1").use { st ->
                check(st.step())
                st.getText(0)
            }
            val unexpectedControlTableCount = db.connection().prepare(
                """
                SELECT COUNT(*)
                FROM sqlite_master
                WHERE type = 'table'
                  AND name GLOB '_sync_*'
                  AND name NOT IN (
                    '_sync_apply_state',
                    '_sync_contract_identity',
                    '_sync_admitted_contracts',
                    '_sync_attachment_state',
                    '_sync_dirty_rows',
                    '_sync_conflict_state',
                    '_sync_managed_tables',
                    '_sync_opening_state',
                    '_sync_operation_state',
                    '_sync_outbox_bundle',
                    '_sync_outbox_rows',
                    '_sync_row_state',
                    '_sync_snapshot_stage',
                    '_sync_source_state'
                  )
                """.trimIndent()
            ).use { st ->
                check(st.step())
                st.getLong(0)
            }
            val parsedKey = testJson.parseToJsonElement(keyJson).jsonObject

            assertEquals(1L, dirtyCount)
            assertEquals("user-1", parsedKey["id"]?.jsonPrimitive?.content)
            assertEquals(0L, unexpectedControlTableCount)
            db.connection().prepare(
                "SELECT COUNT(*) FROM _sync_opening_state " +
                    "WHERE singleton_key=1 AND phase='ready' AND outcome='ready' AND writes_available=1",
            ).use { st ->
                check(st.step())
                assertEquals(1L, st.getLong(0))
            }
        } finally {
            http.close()
            db.close()
        }
    }
}
