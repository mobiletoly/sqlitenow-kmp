/*
 * Copyright 2025 Toly Pochkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.common.sqliteNowLogger
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use

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
        // Clear per-db table info cache to avoid stale schema info across app runs/tests
        TableInfoProvider.clear(db)

        if (config.verboseLogs) {
            sqliteNowLogger.i { "bootstrap: start userId=$userId sourceId=$sourceId" }
            sqliteNowLogger.i { "bootstrap: config=$config" }
        } else {
            sqliteNowLogger.i { "bootstrap: start userId=$userId sourceId=$sourceId" }
        }

        configureDatabaseSettings(db)
        createMetadataTables(db)
        ensureClientRowExists(db, userId, sourceId)
        createTriggersForTables(db)

        if (config.verboseLogs) {
            sqliteNowLogger.i { "bootstrap: done successfully" }
        } else {
            sqliteNowLogger.i { "bootstrap: done" }
        }
    }.onFailure { exception ->
        sqliteNowLogger.e(exception) { "bootstrap: failed userId=$userId sourceId=$sourceId" }
    }

    private suspend fun configureDatabaseSettings(db: SafeSQLiteConnection) {
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
        // Create triggers for each sync table with its specific primary key
        config.syncTables.forEach { syncTable ->
            val tableLc = syncTable.tableName.lowercase()
            if (config.verboseLogs) {
                sqliteNowLogger.i { "bootstrap: creating triggers for table=$tableLc, syncKeyColumn=${syncTable.syncKeyColumnName}" }
            } else {
                sqliteNowLogger.d { "bootstrap: creating triggers for table=$tableLc" }
            }

            val primaryKeyColumn = determinePrimaryKeyColumn(db, syncTable)
            if (config.verboseLogs) {
                sqliteNowLogger.i { "bootstrap: determined primary key column='$primaryKeyColumn' for table=$tableLc" }
            }
            createTriggersForTable(db, tableLc, primaryKeyColumn)
        }
    }

    private suspend fun determinePrimaryKeyColumn(db: SafeSQLiteConnection, syncTable: SyncTable): String {
        // If syncKeyColumnName is explicitly specified, use it
        if (!syncTable.syncKeyColumnName.isNullOrBlank()) {
            return syncTable.syncKeyColumnName
        }

        // Auto-detect primary key from table schema
        val detectedPk = detectPrimaryKeyColumn(db, syncTable.tableName.lowercase())
        if (detectedPk != null) {
            return detectedPk
        }

        sqliteNowLogger.w { "No primary key detected for table '${syncTable.tableName}', falling back to 'id'" }
        return "id"
    }

    private suspend fun detectPrimaryKeyColumn(db: SafeSQLiteConnection, table: String): String? =
        TableInfoProvider.get(db, table).primaryKey?.name

    private suspend fun createTriggersForTable(db: SafeSQLiteConnection, table: String, primaryKeyColumn: String) {
        val tableLc = table.lowercase()
        val columns = getTableColumns(db, tableLc)
        val pkIsBlob = isPrimaryKeyBlob(db, tableLc)
        val pkExprNew = if (pkIsBlob) "lower(hex(NEW.$primaryKeyColumn))" else "NEW.$primaryKeyColumn"
        val pkExprOld = if (pkIsBlob) "lower(hex(OLD.$primaryKeyColumn))" else "OLD.$primaryKeyColumn"

        if (config.verboseLogs) {
            sqliteNowLogger.i { "bootstrap: table=$tableLc, primaryKey=$primaryKeyColumn, isBlob=$pkIsBlob" }
            sqliteNowLogger.i { "bootstrap: columns=${columns.joinToString(", ")}" }
        }

        // Trigger naming convention:
        // - trg_<table>_ai = AFTER INSERT
        // - trg_<table>_au = AFTER UPDATE
        // - trg_<table>_ad = AFTER DELETE
        // Build a JSON payload expression that renders BLOB columns as hex(NEW.col) text
        val tableInfo = TableInfoProvider.get(db, tableLc)
        val blobAwarePayloadExpr = buildJsonObjectExprHexAware(
            tableInfo = tableInfo,
            prefix = "NEW"
        )

        createInsertTrigger(db, tableLc, pkExprNew, blobAwarePayloadExpr)
        createUpdateTrigger(db, tableLc, pkExprNew, blobAwarePayloadExpr)
        createDeleteTrigger(db, tableLc, pkExprOld)

        if (config.verboseLogs) {
            sqliteNowLogger.i { "bootstrap: triggers created successfully for table=$tableLc" }
        }
    }

    private suspend fun createInsertTrigger(
        db: SafeSQLiteConnection,
        tableLc: String,
        pkExprNew: String,
        payloadExpr: String
    ) {
        // trg_<table>_ai = AFTER INSERT trigger (see naming convention in createTriggersForTable()).
        db.execSQL("DROP TRIGGER IF EXISTS trg_${tableLc}_ai;")
        val triggerSql =
            """
            CREATE TRIGGER trg_${tableLc}_ai
            AFTER INSERT ON ${tableLc}
            WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
            BEGIN
              INSERT INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
              SELECT '${tableLc}', ${pkExprNew}, 0, 0
              WHERE NOT EXISTS (
                SELECT 1 FROM _sync_row_meta
                WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew}
              );

              -- Reset deleted flag when record is reinserted
              -- This handles the case where a record was deleted and then reinserted
              UPDATE _sync_row_meta SET deleted=0, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew} AND deleted=1;

              INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
              SELECT '${tableLc}', ${pkExprNew}, 'INSERT', 0, ${payloadExpr}, (SELECT next_change_id FROM _sync_client_info LIMIT 1)
              WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew});

              UPDATE _sync_pending SET
                op='INSERT',
                base_version=(SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew}),
                payload=${payloadExpr},
                queued_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew};

              UPDATE _sync_client_info SET next_change_id = next_change_id + 1
              WHERE changes() > 0 AND last_insert_rowid() > 0;
            END
            """.trimIndent()

        if (config.verboseLogs) {
            sqliteNowLogger.i { "bootstrap: creating INSERT trigger for table=$tableLc" }
            sqliteNowLogger.d { "INSERT trigger SQL: $triggerSql" }
        }

        db.execSQL(triggerSql)
    }

    private suspend fun createUpdateTrigger(
        db: SafeSQLiteConnection,
        tableLc: String,
        pkExprNew: String,
        payloadExpr: String
    ) {
        // trg_<table>_au = AFTER UPDATE trigger (see naming convention in createTriggersForTable()).
        db.execSQL("DROP TRIGGER IF EXISTS trg_${tableLc}_au;")
        db.execSQL(
            """
            CREATE TRIGGER trg_${tableLc}_au
            AFTER UPDATE ON ${tableLc}
            WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
            BEGIN
              INSERT INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
              SELECT '${tableLc}', ${pkExprNew}, 0, 0
              WHERE NOT EXISTS (
                SELECT 1 FROM _sync_row_meta
                WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew}
              );

              UPDATE _sync_row_meta SET deleted=0, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew};

              INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
              SELECT '${tableLc}', ${pkExprNew}, 'UPDATE',
                     (SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew}),
                     ${payloadExpr},
                     (SELECT next_change_id FROM _sync_client_info LIMIT 1)
              WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew});

              UPDATE _sync_pending SET
                op = CASE WHEN op='INSERT' THEN 'INSERT' ELSE 'UPDATE' END,
                base_version = CASE WHEN op='INSERT' THEN base_version ELSE COALESCE((SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew}), 0) END,
                payload = ${payloadExpr},
                queued_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=${pkExprNew};

              UPDATE _sync_client_info SET next_change_id = next_change_id + 1
              WHERE changes() > 0 AND last_insert_rowid() > 0;
            END
            """.trimIndent()
        )
    }

    private suspend fun createDeleteTrigger(db: SafeSQLiteConnection, tableLc: String, pkExprOld: String) {
        // trg_<table>_ad = AFTER DELETE trigger (see naming convention in createTriggersForTable()).
        db.execSQL("DROP TRIGGER IF EXISTS trg_${tableLc}_ad;")
        db.execSQL(
            """
            CREATE TRIGGER trg_${tableLc}_ad
            AFTER DELETE ON ${tableLc}
            WHEN COALESCE((SELECT apply_mode FROM _sync_client_info LIMIT 1), 0) = 0
            BEGIN
              INSERT INTO _sync_row_meta(table_name, pk_uuid, server_version, deleted)
              SELECT '${tableLc}', ${pkExprOld}, 0, 1
              WHERE NOT EXISTS (
                SELECT 1 FROM _sync_row_meta
                WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld}
              );

              INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
              SELECT '${tableLc}', ${pkExprOld}, 'DELETE',
                     COALESCE((SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld}), 0),
                     NULL,
                     (SELECT next_change_id FROM _sync_client_info LIMIT 1)
              WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld});

              INSERT INTO _sync_pending(table_name, pk_uuid, op, base_version, payload, change_id)
              SELECT '${tableLc}', ${pkExprOld}, 'DELETE',
                     COALESCE((SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld}), 0),
                     NULL,
                     (SELECT next_change_id FROM _sync_client_info LIMIT 1)
              WHERE NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld});

              UPDATE _sync_pending SET
                op = 'DELETE',
                base_version = COALESCE((SELECT server_version FROM _sync_row_meta WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld}), 0),
                payload = NULL,
                queued_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld} AND op != 'INSERT';

              UPDATE _sync_client_info SET next_change_id = next_change_id + 1
              WHERE changes() > 0 AND last_insert_rowid() > 0;

              DELETE FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld} AND op='INSERT';

              UPDATE _sync_row_meta SET deleted=1, updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
              WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld};

              DELETE FROM _sync_row_meta
              WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld} AND server_version=0
                AND NOT EXISTS (SELECT 1 FROM _sync_pending WHERE table_name='${tableLc}' AND pk_uuid=${pkExprOld});
            END
            """.trimIndent()
        )
    }

    private suspend fun isPrimaryKeyBlob(db: SafeSQLiteConnection, table: String): Boolean =
        TableInfoProvider.get(db, table).primaryKeyIsBlob

    private suspend fun getTableColumns(db: SafeSQLiteConnection, table: String): List<String> =
        TableInfoProvider.get(db, table).columnNamesLower

    private fun buildJsonObjectExprHexAware(
        tableInfo: TableInfo,
        prefix: String,
    ): String {
        val pairs = tableInfo.columns.map { col ->
            val name = col.name.lowercase()
            val expr = if (col.declaredType.lowercase().contains("blob")) {
                "lower(hex($prefix.$name))"
            } else {
                "$prefix.$name"
            }
            "'${name}', $expr"
        }
        return "json_object(" + pairs.joinToString(", ") + ")"
    }
}
