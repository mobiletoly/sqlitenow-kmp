package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import io.ktor.client.engine.cio.CIO
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull

class SyncUploaderChangeIdBackfillTest {
    @Test
    fun prepareUpload_backfillsNullChangeIds_andKeepsPkOverridesDistinct() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        val http = HttpClient(CIO)
        try {
            db.execSQL("CREATE TABLE profile (id BLOB PRIMARY KEY NOT NULL, name TEXT) WITHOUT ROWID;")
            db.execSQL(
                """
                CREATE TABLE _sync_client_info (
                  user_id TEXT NOT NULL PRIMARY KEY,
                  source_id TEXT NOT NULL,
                  next_change_id INTEGER NOT NULL DEFAULT 1,
                  last_server_seq_seen INTEGER NOT NULL DEFAULT 0,
                  apply_mode INTEGER NOT NULL DEFAULT 0,
                  current_window_until INTEGER NOT NULL DEFAULT 0
                )
                """.trimIndent()
            )
            db.execSQL(
                """
                CREATE TABLE _sync_pending (
                  table_name TEXT NOT NULL,
                  pk_uuid TEXT NOT NULL,
                  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
                  base_version INTEGER DEFAULT 0,
                  payload TEXT,
                  change_id INTEGER,
                  queued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                  PRIMARY KEY (table_name, pk_uuid)
                )
                """.trimIndent()
            )
            db.execSQL(
                """
                INSERT INTO _sync_client_info(user_id, source_id, next_change_id, last_server_seq_seen, apply_mode, current_window_until)
                VALUES('u', 'd', 1, 0, 0, 0)
                """.trimIndent()
            )

            val pk1 = "00112233445566778899aabbccddeeff"
            val pk2 = "ffeeddccbbaa99887766554433221100"
            val payload1 = """{"id":"$pk1","name":"a"}"""
            val payload2 = """{"id":"$pk2","name":"b"}"""

            db.execSQL(
                """
                INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id, queued_at)
                VALUES('profile', '$pk1', 'INSERT', 0, '$payload1', NULL, '2020-01-01T00:00:00Z')
                """.trimIndent()
            )
            db.execSQL(
                """
                INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id, queued_at)
                VALUES('profile', '$pk2', 'INSERT', 0, '$payload2', NULL, '2020-01-01T00:00:01Z')
                """.trimIndent()
            )

            val nextChangeId = db.prepare("SELECT next_change_id FROM _sync_client_info LIMIT 1").use { st ->
                check(st.step())
                st.getLong(0)
            }

            val uploader = SyncUploader(
                http = http,
                config = OversqliteConfig(schema = "business", syncTables = listOf(SyncTable(tableName = "profile"))),
                resolver = ServerWinsResolver,
                upsertBusinessFromPayload = { _, _, _, _ -> error("not expected in prepareUpload") },
                updateRowMeta = { _, _, _, _, _ -> error("not expected in prepareUpload") },
                ioDispatcher = Dispatchers.Default,
            )

            val prepared = uploader.prepareUpload(db, nextChangeId)
            assertEquals(listOf(1L, 2L), prepared.changes.map { it.sourceChangeId })

            val pkWire1 = prepared.pkOverride[1L]
            val pkWire2 = prepared.pkOverride[2L]
            assertNotNull(pkWire1)
            assertNotNull(pkWire2)
            assertNotEquals(pkWire1, pkWire2)
            assertEquals("00112233-4455-6677-8899-aabbccddeeff", pkWire1)
            assertEquals("ffeeddcc-bbaa-9988-7766-554433221100", pkWire2)

            val dbIds = db.prepare("SELECT change_id FROM _sync_pending ORDER BY queued_at ASC").use { st ->
                val out = mutableListOf<Long>()
                while (st.step()) {
                    check(!st.isNull(0))
                    out += st.getLong(0)
                }
                out
            }
            assertEquals(listOf(1L, 2L), dbIds)
        } finally {
            http.close()
            db.close()
        }
    }
}
