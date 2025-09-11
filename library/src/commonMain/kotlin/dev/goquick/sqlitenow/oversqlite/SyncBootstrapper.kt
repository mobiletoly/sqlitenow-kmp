package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.logger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection

/**
 * Handles the bootstrapping process for sync operations.
 * Separated from DefaultOversqliteClient to improve maintainability.
 */
internal class SyncBootstrapper(
    private val config: OversqliteConfig
) {

    suspend fun bootstrap(
        db: SafeSQLiteConnection,
        userId: String,
        sourceId: String
    ): Result<Unit> = runCatching {
        logger.i { "bootstrap: start userId=$userId sourceId=$sourceId" }

        configureDatabaseSettings(db)
        createMetadataTables(db)
        ensureClientRowExists(db, userId, sourceId)
        createTriggersForTables(db)
        logger.i { "bootstrap: done" }
    }.onFailure { exception ->
        logger.e(exception) { "bootstrap: failed userId=$userId sourceId=$sourceId" }
    }

    private suspend fun configureDatabaseSettings(db: SafeSQLiteConnection) {
        db.execSQL("PRAGMA foreign_keys=ON")

        // Optional: WAL and a modest busy timeout for transient contention
        try {
            db.execSQL("PRAGMA journal_mode=WAL")
        } catch (_: Throwable) {
            // Ignore if WAL mode is not supported
        }
        try {
            db.execSQL("PRAGMA busy_timeout=4000")
        } catch (_: Throwable) {
            // Ignore if busy_timeout is not supported
        }
    }

    private suspend fun createMetadataTables(db: SafeSQLiteConnection) {
        // Client info table
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_client_info (
              user_id TEXT NOT NULL PRIMARY KEY,
              source_id TEXT NOT NULL,
              next_change_id INTEGER NOT NULL DEFAULT 1,
              last_server_seq_seen INTEGER NOT NULL DEFAULT 0,
              apply_mode INTEGER NOT NULL DEFAULT 0,
              current_window_until INTEGER NOT NULL DEFAULT 0
            )
            """.trimIndent()
        )



        // Row metadata table
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS _sync_row_meta (
              table_name TEXT NOT NULL,
              pk_uuid TEXT NOT NULL,
              server_version INTEGER NOT NULL DEFAULT 0,
              deleted INTEGER NOT NULL DEFAULT 0,
              updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
              PRIMARY KEY (table_name, pk_uuid)
            )
            """.trimIndent()
        )

        // Pending changes table
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
            )
            """.trimIndent()
        )
    }

    private suspend fun ensureClientRowExists(
        db: SafeSQLiteConnection,
        userId: String,
        sourceId: String
    ) {
        val existing = db.prepare("SELECT COUNT(*) FROM _sync_client_info WHERE user_id=?").use { st ->
            st.bindText(1, userId)
            if (st.step()) st.getLong(0) else 0L
        }

        if (existing == 0L) {
            db.prepare(
                "INSERT INTO _sync_client_info(user_id, source_id, next_change_id, last_server_seq_seen, apply_mode, current_window_until) " +
                "VALUES(?,?,1,0,0,0)"
            ).use { st ->
                st.bindText(1, userId)
                st.bindText(2, sourceId)
                st.step()
            }
        } else {
            db.prepare("UPDATE _sync_client_info SET apply_mode=0 WHERE user_id=?").use { st ->
                st.bindText(1, userId)
                st.step()
            }
        }
    }

    private suspend fun createTriggersForTables(db: SafeSQLiteConnection) {
        // Create debug logging table for trigger execution tracking
        db.execSQL("""
            CREATE TABLE IF NOT EXISTS _debug_trigger_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                trigger_name TEXT,
                table_name TEXT,
                pk_uuid TEXT,
                operation TEXT,
                details TEXT
            )
        """.trimIndent())

        // Normalize table names to lowercase and create triggers
        config.tables.map { it.lowercase() }.forEach { table ->
            logger.d { "bootstrap: creating triggers for table=$table" }
            createTriggersForTable(db, table)
        }
    }

    private suspend fun createTriggersForTable(db: SafeSQLiteConnection, table: String) {
        val tableLc = table.lowercase()
        val columns = getTableColumns(db, tableLc)
        val newRowJson = jsonObjectExpr(columns, prefix = "NEW")

        createInsertTrigger(db, tableLc, newRowJson)
        createUpdateTrigger(db, tableLc, newRowJson)
        createDeleteTrigger(db, tableLc)
    }

    private suspend fun createInsertTrigger(
        db: SafeSQLiteConnection,
        tableLc: String,
        newRowJson: String
    ) {
        db.execSQL(
            """
            CREATE TRIGGER IF NOT EXISTS trg_${tableLc}_ai
            AFTER INSERT ON ${tableLc}
            WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
            BEGIN
              INSERT OR IGNORE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
              VALUES ('${tableLc}', NEW.id, 0, 0);

              -- CRITICAL FIX: Reset deleted flag when record is reinserted
              -- This handles the case where a record was deleted and then reinserted
              UPDATE _sync_row_meta SET deleted=0, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=NEW.id AND deleted=1;

              INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
              SELECT '${tableLc}', NEW.id, 'INSERT', 0, ${newRowJson}, (SELECT next_change_id FROM _sync_client_info LIMIT 1)
              WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=NEW.id);

              UPDATE _sync_pending SET
                op='INSERT',
                base_version=(SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=NEW.id),
                payload=${newRowJson},
                queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=NEW.id;

              UPDATE _sync_client_info SET next_change_id = next_change_id + 1
              WHERE changes() > 0 AND last_insert_rowid() > 0;
            END
            """.trimIndent()
        )
    }

    private suspend fun createUpdateTrigger(
        db: SafeSQLiteConnection,
        tableLc: String,
        newRowJson: String
    ) {
        db.execSQL(
            """
            CREATE TRIGGER IF NOT EXISTS trg_${tableLc}_au
            AFTER UPDATE ON ${tableLc}
            WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
            BEGIN
              INSERT OR IGNORE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
              VALUES ('${tableLc}', NEW.id, 0, 0);

              UPDATE _sync_row_meta SET deleted=0, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=NEW.id;

              INSERT INTO _debug_trigger_log(trigger_name, table_name, pk_uuid, operation, details)
              VALUES ('trg_${tableLc}_au', '${tableLc}', NEW.id, 'UPDATE', 'set deleted=0');

              INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
              SELECT '${tableLc}', NEW.id, 'UPDATE',
                     (SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=NEW.id),
                     ${newRowJson},
                     (SELECT next_change_id FROM _sync_client_info LIMIT 1)
              WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=NEW.id);

              UPDATE _sync_pending SET
                op = CASE WHEN op='INSERT' THEN 'INSERT' ELSE 'UPDATE' END,
                base_version = CASE WHEN op='INSERT' THEN base_version ELSE COALESCE((SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=NEW.id), 0) END,
                payload = ${newRowJson},
                queued_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=NEW.id;

              UPDATE _sync_client_info SET next_change_id = next_change_id + 1
              WHERE changes() > 0 AND last_insert_rowid() > 0;
            END
            """.trimIndent()
        )
    }

    private suspend fun createDeleteTrigger(db: SafeSQLiteConnection, tableLc: String) {
        db.execSQL(
            """
            CREATE TRIGGER IF NOT EXISTS trg_${tableLc}_ad
            AFTER DELETE ON ${tableLc}
            WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
            BEGIN
              INSERT OR IGNORE INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
              VALUES ('${tableLc}', OLD.id, 0, 1);

              INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
              SELECT '${tableLc}', OLD.id, 'DELETE',
                     COALESCE((SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=OLD.id), 0),
                     NULL,
                     (SELECT next_change_id FROM _sync_client_info LIMIT 1)
              WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=OLD.id);

              INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
              SELECT '${tableLc}', OLD.id, 'DELETE',
                     COALESCE((SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=OLD.id), 0),
                     NULL,
                     (SELECT next_change_id FROM _sync_client_info LIMIT 1)
              WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=OLD.id);

              UPDATE _sync_pending SET
                op = 'DELETE',
                base_version = COALESCE((SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=OLD.id), 0),
                payload = NULL,
                queued_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=OLD.id AND op != 'INSERT';

              UPDATE _sync_client_info SET next_change_id = next_change_id + 1
              WHERE changes() > 0 AND last_insert_rowid() > 0;

              DELETE FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=OLD.id AND op='INSERT';

              UPDATE _sync_row_meta SET deleted=1, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=OLD.id;

              DELETE FROM _sync_row_meta
              WHERE table_name='${tableLc}' AND pk_uuid=OLD.id AND server_version=0
                AND NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=OLD.id);
            END
            """.trimIndent()
        )
    }

    // Cache PRAGMA table_info lookups per table (lowercased key)
    private val tableColumnsCache = mutableMapOf<String, List<String>>()

    private suspend fun getTableColumns(db: SafeSQLiteConnection, table: String): List<String> {
        val key = table.lowercase()
        tableColumnsCache[key]?.let { return it }
        val cols = mutableListOf<String>()
        db.prepare("PRAGMA table_info($key)").use { st ->
            while (st.step()) cols += st.getText(1).lowercase()
        }
        tableColumnsCache[key] = cols
        return cols
    }

    private fun jsonObjectExpr(
        columns: List<String>,
        prefix: String,
    ): String =
        "json_object(" + columns.joinToString(", ") {
            "'${it.lowercase()}', $prefix.$it"
        } + ")"


}
