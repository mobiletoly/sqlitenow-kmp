package dev.goquick.sqlitenow.oversqlite

import androidx.sqlite.async.step
import dev.goquick.sqlitenow.core.TransactionMode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

internal class GenerationRetainedObjectsTest : CrossTargetSyncTestSupport() {
    @Test
    fun applicationTriggerInternalReferenceRejectsBeforeMutation() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        try {
            createUsersAndPostsTables(db)
            val client = newClient(db, http)
            client.open().getOrThrow()
            client.closeFromCoordinatedOwner()
            db.execSQL("CREATE TRIGGER app_internal_read AFTER UPDATE ON users WHEN (SELECT apply_mode FROM _sync_apply_state WHERE singleton_key=1)=1 BEGIN SELECT NEW.id; END")
            db.execSQL("PRAGMA foreign_keys=OFF")
            val before = db.queryList("SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") {
                it.getText(0) + it.getText(1) + it.getText(2) + it.getText(3)
            }
            val reopened = newClient(db,http)
            assertTrue(reopened.open().exceptionOrNull()?.message?.contains("unexpected retained trigger") == true)
            assertEquals(0L,db.scalarLong("PRAGMA foreign_keys"))
            assertEquals(before,db.queryList("SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") {
                it.getText(0) + it.getText(1) + it.getText(2) + it.getText(3)
            })
            assertEquals(0,server.requestTrace.size)
            reopened.closeFromCoordinatedOwner()
        } finally { http.close(); db.close() }
    }

    @Test
    fun missingIdentityRejectsEachOwnedObjectBeforeMutation() = runOwnedWebDatabaseTest {
        val objects = (oversqliteRetainedTables - "_sync_contract_identity").map { name ->
            name to "CREATE TABLE $name(marker INTEGER); INSERT INTO $name VALUES(7)"
        } + listOf(
            "dirty index" to "CREATE TABLE app(id INTEGER); CREATE INDEX _sync_dirty_rows_order_idx ON app(id)",
            "case-variant table" to "CREATE TABLE _SYNC_APPLY_STATE(marker INTEGER); INSERT INTO _SYNC_APPLY_STATE VALUES(7)",
            "case-variant index" to "CREATE TABLE app(id INTEGER); CREATE INDEX _SYNC_DIRTY_ROWS_ORDER_IDX ON app(id)",
            "trigger" to "CREATE TABLE app(id INTEGER); CREATE TRIGGER audit AFTER INSERT ON app BEGIN UPDATE _sync_apply_state SET apply_mode=0; END",
            "case-variant trigger reference" to "CREATE TABLE app(id INTEGER); CREATE TRIGGER audit AFTER INSERT ON app BEGIN UPDATE _SYNC_APPLY_STATE SET apply_mode=0; END",
        )
        for ((name, sql) in objects) {
            val db = newDb()
            try {
                // SQLite's driver executes one statement at a time.
                if (name.contains("trigger")) {
                    db.execSQL("CREATE TABLE app(id INTEGER)")
                    db.execSQL("CREATE TRIGGER audit AFTER INSERT ON app BEGIN UPDATE _sync_apply_state SET apply_mode=0; END")
                } else {
                    sql.split(';').filter { it.isNotBlank() }.forEach { db.execSQL(it) }
                }
                val before = db.queryList("SELECT type,name,sql FROM sqlite_schema ORDER BY type,name") { it.getText(0) + it.getText(1) + it.getText(2) }
                assertFails(name) {
                    db.transaction(TransactionMode.IMMEDIATE) {
                        OversqliteContractStore(db).open(LocalContractIdentity("db", "svc", 1, 0, "shape"))
                    }
                }
                assertEquals(before, db.queryList("SELECT type,name,sql FROM sqlite_schema ORDER BY type,name") { it.getText(0) + it.getText(1) + it.getText(2) }, name)
                assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM sqlite_schema WHERE name='_sync_contract_identity'"), name)
            } finally { db.close() }
        }
    }

    @Test
    fun unrelatedSyncPrefixedApplicationIndexIsAccepted() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.execSQL("CREATE TABLE app(id INTEGER)")
            db.execSQL("CREATE INDEX _sync_people_lookup ON app(id)")
            db.transaction(TransactionMode.IMMEDIATE) {
                OversqliteContractStore(db).open(LocalContractIdentity("db", "svc", 1, 0, "shape"))
            }
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM sqlite_schema WHERE name='_sync_people_lookup'"))
        } finally { db.close() }
    }

    @Test
    fun initialCensusDoesNotChangeSQLiteConfiguration() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.execSQL("PRAGMA foreign_keys = OFF")
            db.execSQL("CREATE TABLE _SYNC_ROW_STATE(marker INTEGER)")
            val before = db.queryList("SELECT type,name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") {
                it.getText(0) + it.getText(1) + it.getText(2)
            }

            assertFails { rejectPartialRetainedRuntimeBeforeConfiguration(db) }

            assertEquals(0L, db.scalarLong("PRAGMA foreign_keys"))
            assertEquals(
                before,
                db.queryList("SELECT type,name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") {
                    it.getText(0) + it.getText(1) + it.getText(2)
                },
            )
        } finally {
            db.close()
        }
    }

    @Test
    fun identityPresentMalformedRuntimeFailsBeforeConfigurationOrRepair() = runOwnedWebDatabaseTest {
        val corruptions = listOf(
            "missing apply singleton" to listOf("DELETE FROM _sync_apply_state"),
            "invalid apply mode" to listOf("UPDATE _sync_apply_state SET apply_mode=2"),
            "text apply mode" to listOf("UPDATE _sync_apply_state SET apply_mode='invalid'"),
            "changed singleton constraint" to listOf(
                "DROP TABLE _sync_apply_state",
                "CREATE TABLE _sync_apply_state(singleton_key INTEGER NOT NULL PRIMARY KEY CHECK(singleton_key>0),apply_mode INTEGER NOT NULL DEFAULT 0)",
                "INSERT INTO _sync_apply_state VALUES(1,0)",
            ),
            "obsolete receipt-bearing outbox layout" to listOf(
                "DROP TABLE _sync_outbox_bundle",
                """
                    CREATE TABLE _sync_outbox_bundle(
                      singleton_key INTEGER NOT NULL PRIMARY KEY CHECK(singleton_key=1),
                      canonical_json_contract TEXT NOT NULL,
                      state TEXT NOT NULL CHECK(state IN ('none','prepared','committed_remote')),
                      source_id TEXT NOT NULL DEFAULT '', source_bundle_id INTEGER NOT NULL DEFAULT 0,
                      initialization_id TEXT NOT NULL DEFAULT '', canonical_request_hash TEXT NOT NULL DEFAULT '',
                      row_count INTEGER NOT NULL DEFAULT 0, remote_bundle_hash TEXT NOT NULL DEFAULT '',
                      remote_bundle_seq INTEGER NOT NULL DEFAULT 0
                    )
                """.trimIndent(),
                "INSERT INTO _sync_outbox_bundle VALUES(1,'jcs_uniform_numeric_strings_v1','none','',0,'','',0,'',0)",
            ),
            "missing table" to listOf("DROP TABLE _sync_snapshot_stage"),
            "column affinity" to listOf(
                "ALTER TABLE _sync_source_state RENAME TO old_source_state",
                "CREATE TABLE _sync_source_state(source_id INTEGER PRIMARY KEY, next_source_bundle_id INTEGER NOT NULL DEFAULT 1, replaced_by_source_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL DEFAULT '')",
            ),
            "missing index" to listOf("DROP INDEX _sync_dirty_rows_order_idx"),
            "unique index" to listOf(
                "DROP INDEX _sync_dirty_rows_order_idx",
                "CREATE UNIQUE INDEX _sync_dirty_rows_order_idx ON _sync_dirty_rows(dirty_ordinal,schema_name,table_name,key_json)",
            ),
            "partial index" to listOf(
                "DROP INDEX _sync_dirty_rows_order_idx",
                "CREATE INDEX _sync_dirty_rows_order_idx ON _sync_dirty_rows(dirty_ordinal,schema_name,table_name,key_json) WHERE dirty_ordinal >= 0",
            ),
            "descending index" to listOf(
                "DROP INDEX _sync_dirty_rows_order_idx",
                "CREATE INDEX _sync_dirty_rows_order_idx ON _sync_dirty_rows(dirty_ordinal DESC,schema_name,table_name,key_json)",
            ),
            "collated index" to listOf(
                "DROP INDEX _sync_dirty_rows_order_idx",
                "CREATE INDEX _sync_dirty_rows_order_idx ON _sync_dirty_rows(dirty_ordinal COLLATE NOCASE,schema_name,table_name,key_json)",
            ),
            "wrong trigger" to listOf(
                "DROP TRIGGER _sync_conflict_revision_insert",
                "CREATE TRIGGER _sync_conflict_revision_insert AFTER INSERT ON _sync_dirty_rows BEGIN SELECT 1; END",
            ),
            "changed trigger literal" to listOf(
                "DROP TRIGGER _sync_conflict_revision_insert",
                "CREATE TRIGGER _sync_conflict_revision_insert AFTER INSERT ON _sync_dirty_rows BEGIN UPDATE _sync_conflict_state SET dirty_revision=dirty_revision+0 WHERE singleton_key=1; END",
            ),
            "trigger when clause" to listOf(
                "DROP TRIGGER _sync_conflict_revision_insert",
                "CREATE TRIGGER _sync_conflict_revision_insert AFTER INSERT ON _sync_dirty_rows WHEN 0 BEGIN UPDATE _sync_conflict_state SET dirty_revision=dirty_revision+1 WHERE singleton_key=1; END",
            ),
            "trigger extra statement" to listOf(
                "DROP TRIGGER _sync_conflict_revision_insert",
                "CREATE TRIGGER _sync_conflict_revision_insert AFTER INSERT ON _sync_dirty_rows BEGIN UPDATE _sync_conflict_state SET dirty_revision=dirty_revision+1 WHERE singleton_key=1; SELECT 1; END",
            ),
            "case variant identity table" to listOf(
                "ALTER TABLE _sync_contract_identity RENAME TO retained_identity_temp",
                "ALTER TABLE retained_identity_temp RENAME TO _SYNC_CONTRACT_IDENTITY",
            ),
            "table and trigger reserved-name collision" to listOf(
                "CREATE TRIGGER _sync_contract_identity AFTER INSERT ON _sync_dirty_rows BEGIN SELECT 1; END",
            ),
        )
        val collisions = listOf("_sync_opening_state", "_sync_snapshot_stage").flatMap { reserved ->
            listOf(reserved, reserved.uppercase()).flatMap { name ->
                listOf(
                    "$name trigger coexistence" to listOf("CREATE TRIGGER $name AFTER INSERT ON _sync_dirty_rows BEGIN SELECT 1; END"),
                    "$name view collision" to listOf("DROP TABLE IF EXISTS $reserved", "CREATE VIEW $name AS SELECT 1 AS marker"),
                    "$name index collision" to listOf("DROP TABLE IF EXISTS $reserved", "CREATE INDEX $name ON _sync_dirty_rows(dirty_ordinal,schema_name,table_name,key_json)"),
                )
            }
        }
        for ((name, corruption) in corruptions + collisions) {
            val db = newDb()
            try {
                db.transaction(TransactionMode.IMMEDIATE) {
                    OversqliteContractStore(db).open(LocalContractIdentity("db", "svc", 1, 0, "shape"))
                }
                corruption.forEach { db.execSQL(it) }
                db.execSQL("PRAGMA foreign_keys = OFF")
                val before = db.queryList("SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") {
                    it.getText(0) + it.getText(1) + it.getText(2) + it.getText(3)
                }

                assertFails(name) { rejectPartialRetainedRuntimeBeforeConfiguration(db) }

                assertEquals(0L, db.scalarLong("PRAGMA foreign_keys"), name)
                assertEquals(before, db.queryList("SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") {
                    it.getText(0) + it.getText(1) + it.getText(2) + it.getText(3)
                }, name)
            } finally { db.close() }
        }
    }

    @Test
    fun retainedIdentityCaseVariantIsRejectedWithoutRepair() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.transaction(TransactionMode.IMMEDIATE) {
                OversqliteContractStore(db).open(LocalContractIdentity("db", "svc", 1, 0, "shape"))
            }
            db.execSQL("ALTER TABLE _sync_contract_identity RENAME TO retained_identity_temp")
            db.execSQL("ALTER TABLE retained_identity_temp RENAME TO _SYNC_CONTRACT_IDENTITY")

            val before = db.queryList("SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") {
                it.getText(0) + it.getText(1) + it.getText(2) + it.getText(3)
            }
            assertFails { rejectPartialRetainedRuntimeBeforeConfiguration(db) }
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM _SYNC_CONTRACT_IDENTITY"))
            assertEquals(before, db.queryList("SELECT type,name,tbl_name,COALESCE(sql,'') FROM sqlite_schema ORDER BY type,name") {
                it.getText(0) + it.getText(1) + it.getText(2) + it.getText(3)
            })
        } finally { db.close() }
    }

    @Test
    fun retainedConflictRequiresServerRowPresenceAndPreservesExplicitNull() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.transaction(TransactionMode.IMMEDIATE) {
                OversqliteContractStore(db).open(LocalContractIdentity("db", "svc", 1, 0, "shape"))
            }
            val missing = """{"schema":"main","table":"users","key":{"id":"u1"},"op":"DELETE","base_row_version":1,"server_row_version":2,"server_row_deleted":true}"""
            db.withPreparedStatement(
                "UPDATE _sync_conflict_state SET decision_state='pending',source_id='source-a',source_bundle_id=1,canonical_request_hash=?,generation_id=1,generation_fingerprint=?,conflict_json=? WHERE singleton_key=1",
            ) { statement ->
                statement.bindText(1, "a".repeat(64))
                statement.bindText(2, "b".repeat(64))
                statement.bindText(3, missing)
                statement.step()
            }
            assertFails { OversqliteConflictStateStore(db, json).load() }

            db.execSQL("UPDATE _sync_conflict_state SET conflict_json='${missing.dropLast(1)},\"server_row\":null}'")
            assertNotNull(OversqliteConflictStateStore(db, json).load())
        } finally { db.close() }
    }

    @Test
    fun ownershipReferencesIgnoreApplicationTextAndRecognizeQuotedTables() {
        for (body in listOf(
            "UPDATE main.\"_sync_apply_state\" SET apply_mode=0",
            "UPDATE OR ABORT [_sync_apply_state] SET apply_mode=0",
            "INSERT INTO `_sync_source_state` VALUES(1)",
            "SELECT * FROM app, _sync_row_state",
            "SELECT * FROM '_sync_row_state'",
            "SELECT 1 WHERE 1 IN _sync_row_state",
        )) assertTrue(triggerReferencesSyncTable("CREATE TRIGGER audit AFTER UPDATE ON app BEGIN $body; END"), body)
        for (body in listOf(
            "INSERT INTO audit VALUES('app_sync_event')",
            "SELECT _sync_row_state FROM app",
            "SELECT * FROM app_sync_state",
            "SELECT * FROM app JOIN other ON _sync_row_state=other.id",
            "SELECT 1 /* FROM _sync_row_state */",
            "SELECT 1 -- FROM _sync_row_state\n",
        )) assertFalse(triggerReferencesSyncTable("CREATE TRIGGER app_sync_event AFTER UPDATE ON app BEGIN $body; END"), body)
    }

    @Test
    fun applicationTriggerDoesNotPreventFreshIdentity() = runOwnedWebDatabaseTest {
        val db = newDb()
        try {
            db.execSQL("CREATE TABLE app(id INTEGER)")
            db.execSQL("CREATE TABLE audit(message TEXT)")
            db.execSQL("CREATE TRIGGER app_sync_event AFTER INSERT ON app BEGIN INSERT INTO audit VALUES('app_sync_event'); END")
            db.transaction(TransactionMode.IMMEDIATE) {
                OversqliteContractStore(db).open(LocalContractIdentity("db", "svc", 1, 0, "shape"))
            }
            val actual = db.queryList("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB '_sync_*'") { it.getText(0) }.toSet()
            assertEquals(oversqliteRetainedTables + oversqliteOpeningTable, actual)
            assertEquals(1L, db.scalarLong("SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND name='app_sync_event'"))
        } finally { db.close() }
    }
}
