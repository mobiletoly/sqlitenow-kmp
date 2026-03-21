package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BundleBootstrapContractTest : BundleClientContractTestSupport() {
    @Test
    fun bootstrap_isRequired_andConstructionIsSideEffectFree() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_client_state'"))
            val beforeBootstrap = client.pushPending().exceptionOrNull()
            assertTrue(beforeBootstrap != null)
            assertTrue(beforeBootstrap.message?.contains("bootstrap") == true)

            client.bootstrap("user-1", "device-a").getOrThrow()

            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_client_state'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_row_state'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_dirty_rows'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_snapshot_stage'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_push_outbound'"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM sqlite_master WHERE name = '_sync_push_stage'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun bootstrap_rejectsOmittedSyncKeyConfiguration() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users")))
            val error = client.bootstrap("user-1", "device-a").exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("syncKeyColumnName or syncKeyColumns explicitly") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun bootstrap_rejectsMissingSyncKeyColumn() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "missing_id")))
            val error = client.bootstrap("user-1", "device-a").exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("does not contain configured primary key column missing_id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun bootstrap_rejectsNonFkClosedManagedSchema() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("posts", syncKeyColumnName = "id")))
            val error = client.bootstrap("user-1", "device-a").exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("managed tables are not FK-closed") == true)
            assertTrue(error.message?.contains("posts.user_id -> users.id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun bootstrap_rejectsCompositeKeysAndCompositeForeignKeys() = runBlocking {
        val compositeKeyDb = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createCompositePkTable(compositeKeyDb)
        val compositeKeyServer = newServer()
        val compositeKeyHttp = newHttpClient(compositeKeyServer)
        try {
            val client = newClient(
                compositeKeyDb,
                compositeKeyHttp,
                syncTables = listOf(SyncTable("pairs", syncKeyColumns = listOf("id_a", "id_b")))
            )
            val error = client.bootstrap("user-1", "device-a").exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("must declare exactly one sync key column") == true)
        } finally {
            compositeKeyHttp.close()
            compositeKeyServer.stop(0)
            compositeKeyDb.close()
        }

        val compositeFkDb = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createCompositeForeignKeyTables(compositeFkDb)
        val compositeFkServer = newServer()
        val compositeFkHttp = newHttpClient(compositeFkServer)
        try {
            val client = newClient(
                compositeFkDb,
                compositeFkHttp,
                syncTables = listOf(
                    SyncTable("parent_pairs", syncKeyColumnName = "id"),
                    SyncTable("child_pairs", syncKeyColumnName = "id"),
                )
            )
            val error = client.bootstrap("user-1", "device-a").exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("unsupported composite foreign keys") == true)
            assertTrue(error.message?.contains("child_pairs -> parent_pairs") == true)
        } finally {
            compositeFkHttp.close()
            compositeFkServer.stop(0)
            compositeFkDb.close()
        }
    }

    @Test
    fun bootstrap_rejectsSchemaQualifiedTableRegistration() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("main.users", syncKeyColumnName = "id")))
            val error = client.bootstrap("user-1", "device-a").exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("must not include a schema qualifier") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun bootstrap_sourceChangeClearsLocalState_andMarksRebuildRequired() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.bootstrap("user-1", "device-a").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL(
                """
                INSERT INTO _sync_push_outbound(source_bundle_id, row_ordinal, schema_name, table_name, key_json, op, base_row_version, payload)
                VALUES (1, 0, 'main', 'users', '{"id":"user-1"}', 'INSERT', 0, '{"id":"user-1","name":"Ada"}')
                """.trimIndent()
            )
            db.execSQL(
                """
                INSERT INTO _sync_push_stage(bundle_seq, row_ordinal, schema_name, table_name, key_json, op, row_version, payload)
                VALUES (7, 0, 'main', 'users', '{"id":"user-1"}', 'INSERT', 7, '{"id":"user-1","name":"Ada"}')
                """.trimIndent()
            )
            db.execSQL(
                """
                INSERT INTO _sync_snapshot_stage(snapshot_id, row_ordinal, schema_name, table_name, key_json, row_version, payload)
                VALUES ('snap-1', 0, 'main', 'users', '{"id":"user-1"}', 7, '{"id":"user-1","name":"Ada"}')
                """.trimIndent()
            )

            client.bootstrap("user-1", "device-b").getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_push_outbound"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_push_stage"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_snapshot_stage"))
            assertEquals("device-b", scalarText(db, "SELECT source_id FROM _sync_client_state WHERE user_id = 'user-1'"))
            assertEquals(1L, scalarLong(db, "SELECT rebuild_required FROM _sync_client_state WHERE user_id = 'user-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun rebuildRequired_blocksNormalSyncEntrypoints() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.bootstrap("user-1", "device-a").getOrThrow()
            db.execSQL("UPDATE _sync_client_state SET rebuild_required = 1 WHERE user_id = 'user-1'")

            assertTrue(client.pushPending().exceptionOrNull() is RebuildRequiredException)
            assertTrue(client.pullToStable().exceptionOrNull() is RebuildRequiredException)
            assertTrue(client.sync().exceptionOrNull() is RebuildRequiredException)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }
}
