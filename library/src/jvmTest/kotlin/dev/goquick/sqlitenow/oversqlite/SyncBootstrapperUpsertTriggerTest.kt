package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.sqlite.use
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking

class SyncBootstrapperUpsertTriggerTest {
    @Test
    fun bootstrap_replaces_legacy_triggers_so_unique_key_upsert_does_not_violate_row_meta_pk() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        try {
            db.execSQL(
                """
                CREATE TABLE value_registry (
                  id BLOB PRIMARY KEY NOT NULL,
                  key TEXT NOT NULL UNIQUE,
                  updated_at INTEGER NOT NULL DEFAULT 0
                ) WITHOUT ROWID;
                """.trimIndent()
            )

            // Legacy buggy trigger: plain INSERT into _sync_row_meta (will violate PK on UPSERT path).
            db.execSQL(
                """
                CREATE TABLE IF NOT EXISTS _sync_client_info (
                  user_id TEXT NOT NULL PRIMARY KEY,
                  source_id TEXT NOT NULL,
                  next_change_id INTEGER NOT NULL DEFAULT 1,
                  last_server_seq_seen INTEGER NOT NULL DEFAULT 0,
                  apply_mode INTEGER NOT NULL DEFAULT 0,
                  current_window_until INTEGER NOT NULL DEFAULT 0
                );
                """.trimIndent()
            )
            db.execSQL(
                """
                CREATE TABLE IF NOT EXISTS _sync_row_meta (
                  table_name TEXT NOT NULL,
                  pk_uuid TEXT NOT NULL,
                  server_version INTEGER NOT NULL DEFAULT 0,
                  deleted INTEGER NOT NULL DEFAULT 0,
                  updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                  PRIMARY KEY (table_name, pk_uuid)
                );
                """.trimIndent()
            )
            db.execSQL(
                """
                CREATE TABLE IF NOT EXISTS _sync_pending (
                  table_name TEXT NOT NULL,
                  pk_uuid TEXT NOT NULL,
                  op TEXT NOT NULL CHECK (op IN ('INSERT','UPDATE','DELETE')),
                  base_version INTEGER DEFAULT 0,
                  payload TEXT,
                  change_id INTEGER,
                  queued_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                  PRIMARY KEY (table_name, pk_uuid)
                );
                """.trimIndent()
            )
            db.execSQL(
                """
                INSERT INTO _sync_client_info(user_id, source_id, next_change_id, last_server_seq_seen, apply_mode, current_window_until)
                VALUES('u', 'd', 1, 0, 0, 0);
                """.trimIndent()
            )

            db.execSQL(
                """
                CREATE TRIGGER trg_value_registry_ai
                AFTER INSERT ON value_registry
                WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
                BEGIN
                  INSERT INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
                  VALUES ('value_registry', lower(hex(NEW.id)), 0, 0);
                END;
                """.trimIndent()
            )

            val bootstrapper = SyncBootstrapper(
                OversqliteConfig(
                    schema = "business",
                    syncTables = listOf(SyncTable(tableName = "value_registry")),
                    verboseLogs = false,
                )
            )

            // Should drop/recreate triggers (including the legacy buggy one).
            bootstrapper.bootstrap(db, userId = "u", sourceId = "d")

            val triggerSql = db.prepare(
                "SELECT sql FROM sqlite_master WHERE type='trigger' AND name='trg_value_registry_ai' LIMIT 1"
            ).use { st ->
                if (!st.step()) null else st.getText(0)
            }
            assertTrue(
                triggerSql?.contains("INSERT INTO _sync_row_meta") == true &&
                    triggerSql.contains("WHERE NOT EXISTS") &&
                    triggerSql.contains("_sync_row_meta"),
                "expected trg_value_registry_ai to be UPSERT-safe without requiring ON CONFLICT; sql=$triggerSql",
            )

            val idHex = "00112233445566778899aabbccddeeff"
            val idBlob = "X'$idHex'"

            db.execSQL("INSERT INTO value_registry(id, key, updated_at) VALUES ($idBlob, 'k', 1);")

            // If the legacy trigger is still present, this UPSERT will run the INSERT trigger again and crash
            // inserting into _sync_row_meta (duplicate table_name+pk_uuid). We assert it succeeds.
            db.execSQL(
                """
                INSERT INTO value_registry(id, key, updated_at)
                VALUES ($idBlob, 'k', 2)
                ON CONFLICT(key) DO UPDATE SET updated_at=excluded.updated_at;
                """.trimIndent()
            )

            val metaCount = db.prepare(
                "SELECT COUNT(*) FROM _sync_row_meta WHERE table_name='value_registry' AND pk_uuid='$idHex'"
            ).use { st ->
                check(st.step())
                st.getLong(0)
            }
            assertTrue(metaCount >= 1, "expected _sync_row_meta row; count=$metaCount")
        } finally {
            db.close()
        }
    }
}
