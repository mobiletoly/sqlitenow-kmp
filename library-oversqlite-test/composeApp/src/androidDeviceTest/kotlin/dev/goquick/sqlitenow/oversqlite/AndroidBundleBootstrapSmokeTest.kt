package dev.goquick.sqlitenow.oversqlite

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class AndroidBundleBootstrapSmokeTest {
    @Test
    fun bootstrapCreatesBundleMetadata_andCapturesDirtyRows() = runBlocking {
        val db = newInMemoryDb()
        val http = newNoopHttpClient()
        try {
            db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            val client = DefaultOversqliteClient(
                db = db,
                config = OversqliteConfig(
                    schema = "main",
                    syncTables = listOf(SyncTable("users", syncKeyColumnName = "id"))
                ),
                http = http,
                tablesUpdateListener = { }
            )

            client.bootstrap("user-1", "device-a").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val dirtyCount = db.prepare("SELECT COUNT(*) FROM _sync_dirty_rows").use { st ->
                check(st.step())
                st.getLong(0)
            }
            val keyJson = db.prepare("SELECT key_json FROM _sync_dirty_rows LIMIT 1").use { st ->
                check(st.step())
                st.getText(0)
            }
            val applyMode = db.prepare("SELECT apply_mode FROM _sync_client_state WHERE user_id='user-1'").use { st ->
                check(st.step())
                st.getLong(0)
            }
            val parsedKey = testJson.parseToJsonElement(keyJson).jsonObject

            assertEquals(1L, dirtyCount)
            assertEquals("user-1", parsedKey["id"]?.jsonPrimitive?.content)
            assertEquals(0L, applyMode)
        } finally {
            http.close()
            db.close()
        }
    }
}
