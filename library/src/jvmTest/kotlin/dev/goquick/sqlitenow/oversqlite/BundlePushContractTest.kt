package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.time.OffsetDateTime
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class BundlePushContractTest : BundleClientContractTestSupport() {
    @Test
    fun pauseUploads_doesNotSkipExplicitPushPending() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pauseUploads()

            val report = client.pushPending().getOrThrow()

            assertEquals(PushOutcome.COMMITTED, report.outcome)
            assertEquals(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, report.status.authority)
            assertEquals(1L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1, pushServer.bundles.size)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_withNoDirtyRows_isLocalNoOpBeforeOutboundState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            client.pushPending().getOrThrow()

            assertEquals(0, pushServer.createRequests.size)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_freezesDirtyRowsIntoOutboundAtomically() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
            db.execSQL(
                """
                CREATE TRIGGER fail_second_outbound_insert
                BEFORE INSERT ON _sync_outbox_rows
                WHEN NEW.row_ordinal = 1
                BEGIN
                  SELECT RAISE(ABORT, 'freeze abort');
                END
                """.trimIndent()
            )

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error.message?.contains("freeze abort") == true)
            assertEquals(listOf("user-1:INSERT", "user-2:INSERT"), dirtyKeysAndOps(db))
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_localInsert_uploadsDirtyPayloadAsInsert() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            val uploaded = pushServer.uploadedChunks.single().rows.single()
            assertEquals("INSERT", uploaded.op)
            assertEquals(0L, uploaded.baseRowVersion)
            assertEquals("""{"id":"user-1","name":"Ada"}""", uploaded.payload?.toString())
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_preexistingRowsUploadAsInsert() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            client.pushPending().getOrThrow()

            val uploaded = pushServer.uploadedChunks.single().rows.single()
            assertEquals("INSERT", uploaded.op)
            assertEquals(0L, uploaded.baseRowVersion)
            assertEquals("""{"id":"user-1","name":"Ada"}""", uploaded.payload?.toString())
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(1L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_syncedUpdate_uploadsUpdateWithBaseRowVersion() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            db.execSQL("UPDATE users SET name = 'Ada v2' WHERE id = 'user-1'")
            client.pushPending().getOrThrow()

            val uploaded = pushServer.uploadedChunks.last().rows.single()
            assertEquals("UPDATE", uploaded.op)
            assertEquals(1L, uploaded.baseRowVersion)
            assertEquals("""{"id":"user-1","name":"Ada v2"}""", uploaded.payload?.toString())
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_preexistingRowsUseNormalInsertConflictPath() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                409 to """
                    {
                      "error":"push_conflict",
                      "message":"insert conflict on main.users user-1",
                      "conflict":{
                        "schema":"main",
                        "table":"users",
                        "key":{"id":"user-1"},
                        "op":"INSERT",
                        "base_row_version":0,
                        "server_row_version":7,
                        "server_row_deleted":false,
                        "server_row":{"id":"user-1","name":"Server"}
                      }
                    }
                """.trimIndent()
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = ServerWinsResolver,
            )
            client.openAndConnect("user-1").getOrThrow()

            client.pushPending().getOrThrow()

            val uploaded = pushServer.uploadedChunks.single().rows.single()
            assertEquals("INSERT", uploaded.op)
            assertEquals(0L, uploaded.baseRowVersion)
            assertEquals("Server", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(7L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_unsyncedInsertThenDelete_isDroppedAsNoOp() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("DELETE FROM users WHERE id = 'user-1'")

            client.pushPending().getOrThrow()

            assertEquals(0, pushServer.createRequests.size)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_syncedDelete_uploadsDeleteWithNullPayload() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            db.execSQL("DELETE FROM users WHERE id = 'user-1'")
            client.pushPending().getOrThrow()

            val uploaded = pushServer.uploadedChunks.last().rows.single()
            assertEquals("DELETE", uploaded.op)
            assertEquals(1L, uploaded.baseRowVersion)
            assertEquals(null, uploaded.payload)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_usesPushSessions_chunksTransport_andReplaysCommittedBundle() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = 1,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")

            client.pushPending().getOrThrow()

            assertEquals(1, pushServer.createRequests.size)
            assertEquals(2L, pushServer.createRequests.single().plannedRowCount)
            assertEquals(listOf(0L, 1L), pushServer.uploadedChunks.map { it.startRowOrdinal })
            assertEquals(listOf("users:INSERT", "posts:INSERT"), pushServer.uploadedChunks.map { it.rows.single() }.map { "${it.table}:${it.op}" })
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_newLocalWritesDuringUploadRemainDirty() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val createStarted = CountDownLatch(1)
        val releaseCreate = CountDownLatch(1)
        var createdSourceId = ""
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                val request = json.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    exchange.requestBody.readBytes().decodeToString(),
                )
                createdSourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
                createStarted.countDown()
                check(releaseCreate.await(5, TimeUnit.SECONDS))
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-upload",
                      "status": "staging",
                      "planned_row_count": 1,
                      "next_expected_row_ordinal": 0
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-upload/chunks") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-upload",
                      "next_expected_row_ordinal": 1
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-upload/commit") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "d01868c80bc7b55a07137bc81d8382388e82d693216aad65d97bdd4b277ee06a"
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/committed-bundles/1/rows") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "d01868c80bc7b55a07137bc81d8382388e82d693216aad65d97bdd4b277ee06a",
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "op": "INSERT",
                          "row_version": 1,
                          "payload": {"id":"user-1","name":"Ada"}
                        }
                      ],
                      "next_row_ordinal": 0,
                      "has_more": false
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
            assertTrue(createStarted.await(5, TimeUnit.SECONDS))

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
            releaseCreate.countDown()
            inFlightPush.await().getOrThrow()

            assertEquals(listOf("user-2:INSERT"), dirtyKeysAndOps(db))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("Grace", scalarText(db, "SELECT name FROM users WHERE id = 'user-2'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_newLocalWritesDuringReplayRemainDirty() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val fetchStarted = CountDownLatch(1)
        val releaseFetch = CountDownLatch(1)
        var createdSourceId = ""
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                val request = json.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    exchange.requestBody.readBytes().decodeToString(),
                )
                createdSourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-replay",
                      "status": "staging",
                      "planned_row_count": 1,
                      "next_expected_row_ordinal": 0
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-replay/chunks") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-replay",
                      "next_expected_row_ordinal": 1
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-replay/commit") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "d01868c80bc7b55a07137bc81d8382388e82d693216aad65d97bdd4b277ee06a"
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/committed-bundles/1/rows") { exchange ->
                fetchStarted.countDown()
                check(releaseFetch.await(5, TimeUnit.SECONDS))
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "d01868c80bc7b55a07137bc81d8382388e82d693216aad65d97bdd4b277ee06a",
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "op": "INSERT",
                          "row_version": 1,
                          "payload": {"id":"user-1","name":"Ada"}
                        }
                      ],
                      "next_row_ordinal": 0,
                      "has_more": false
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
            assertTrue(fetchStarted.await(5, TimeUnit.SECONDS))

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
            releaseFetch.countDown()
            inFlightPush.await().getOrThrow()

            assertEquals(listOf("user-2:INSERT"), dirtyKeysAndOps(db))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("Grace", scalarText(db, "SELECT name FROM users WHERE id = 'user-2'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_sameKeyUpdateThenLaterUpdate_rebasesDirtyIntent() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        val fetchStarted = CountDownLatch(1)
        val releaseFetch = CountDownLatch(1)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            pushServer.beforeCommittedBundleChunkResponse = { _, _ ->
                fetchStarted.countDown()
                check(releaseFetch.await(5, TimeUnit.SECONDS))
            }
            db.execSQL("UPDATE users SET name = 'Ada uploaded' WHERE id = 'user-1'")

            val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
            assertTrue(fetchStarted.await(5, TimeUnit.SECONDS))
            db.execSQL("UPDATE users SET name = 'Ada newer' WHERE id = 'user-1'")
            releaseFetch.countDown()
            inFlightPush.await().getOrThrow()

            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            val dirtyPayload = json.parseToJsonElement(dirty.payload ?: error("expected dirty payload")).jsonObject
            assertEquals("Ada newer", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("UPDATE", dirty.op)
            assertEquals(2L, dirty.baseRowVersion)
            assertEquals("user-1", dirtyPayload["id"]?.jsonPrimitive?.content)
            assertEquals("Ada newer", dirtyPayload["name"]?.jsonPrimitive?.content)
            assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
            assertEquals(0L, scalarLong(db, "SELECT deleted FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_sameKeyUpdateThenLaterDelete_rebasesDirtyIntent() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        val fetchStarted = CountDownLatch(1)
        val releaseFetch = CountDownLatch(1)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            pushServer.beforeCommittedBundleChunkResponse = { _, _ ->
                fetchStarted.countDown()
                check(releaseFetch.await(5, TimeUnit.SECONDS))
            }
            db.execSQL("UPDATE users SET name = 'Ada uploaded' WHERE id = 'user-1'")

            val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
            assertTrue(fetchStarted.await(5, TimeUnit.SECONDS))
            db.execSQL("DELETE FROM users WHERE id = 'user-1'")
            releaseFetch.countDown()
            inFlightPush.await().getOrThrow()

            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'user-1'"))
            assertEquals("DELETE", dirty.op)
            assertEquals(2L, dirty.baseRowVersion)
            assertEquals(null, dirty.payload)
            assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
            assertEquals(0L, scalarLong(db, "SELECT deleted FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_sameKeyDeleteThenLaterRecreate_rebasesDirtyIntent() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        val fetchStarted = CountDownLatch(1)
        val releaseFetch = CountDownLatch(1)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            pushServer.beforeCommittedBundleChunkResponse = { _, _ ->
                fetchStarted.countDown()
                check(releaseFetch.await(5, TimeUnit.SECONDS))
            }
            db.execSQL("DELETE FROM users WHERE id = 'user-1'")

            val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
            assertTrue(fetchStarted.await(5, TimeUnit.SECONDS))
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada recreated')")
            releaseFetch.countDown()
            inFlightPush.await().getOrThrow()

            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            val dirtyPayload = json.parseToJsonElement(dirty.payload ?: error("expected dirty payload")).jsonObject
            assertEquals("Ada recreated", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("INSERT", dirty.op)
            assertEquals(2L, dirty.baseRowVersion)
            assertEquals("user-1", dirtyPayload["id"]?.jsonPrimitive?.content)
            assertEquals("Ada recreated", dirtyPayload["name"]?.jsonPrimitive?.content)
            assertEquals(2L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
            assertEquals(1L, scalarLong(db, "SELECT deleted FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_sameKeyInsertThenLaterUpdate_rebasesDirtyIntent() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        val fetchStarted = CountDownLatch(1)
        val releaseFetch = CountDownLatch(1)
        pushServer.beforeCommittedBundleChunkResponse = { _, _ ->
            fetchStarted.countDown()
            check(releaseFetch.await(5, TimeUnit.SECONDS))
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
            assertTrue(fetchStarted.await(5, TimeUnit.SECONDS))
            db.execSQL("UPDATE users SET name = 'Ada newer' WHERE id = 'user-1'")
            releaseFetch.countDown()
            inFlightPush.await().getOrThrow()

            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            val dirtyPayload = json.parseToJsonElement(dirty.payload ?: error("expected dirty payload")).jsonObject
            assertEquals("Ada newer", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("UPDATE", dirty.op)
            assertEquals(1L, dirty.baseRowVersion)
            assertEquals("user-1", dirtyPayload["id"]?.jsonPrimitive?.content)
            assertEquals("Ada newer", dirtyPayload["name"]?.jsonPrimitive?.content)
            assertEquals(1L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
            assertEquals(0L, scalarLong(db, "SELECT deleted FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_sameKeyInsertThenLaterDelete_rebasesDirtyIntent() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        val fetchStarted = CountDownLatch(1)
        val releaseFetch = CountDownLatch(1)
        pushServer.beforeCommittedBundleChunkResponse = { _, _ ->
            fetchStarted.countDown()
            check(releaseFetch.await(5, TimeUnit.SECONDS))
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
            assertTrue(fetchStarted.await(5, TimeUnit.SECONDS))
            db.execSQL("DELETE FROM users WHERE id = 'user-1'")
            releaseFetch.countDown()
            inFlightPush.await().getOrThrow()

            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'user-1'"))
            assertEquals("DELETE", dirty.op)
            assertEquals(1L, dirty.baseRowVersion)
            assertEquals(null, dirty.payload)
            assertEquals(1L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
            assertEquals(0L, scalarLong(db, "SELECT deleted FROM _sync_row_state WHERE table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_ordersParentFirstUpserts_andChildFirstDeletes() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = 10,
            )
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")
            client.pushPending().getOrThrow()

            assertEquals(listOf("users:INSERT", "posts:INSERT"), pushServer.uploadedChunks[0].rows.map { "${it.table}:${it.op}" })

            db.execSQL("DELETE FROM posts WHERE id = 'post-1'")
            db.execSQL("DELETE FROM users WHERE id = 'user-1'")
            client.pushPending().getOrThrow()

            assertEquals(listOf("posts:DELETE", "users:DELETE"), pushServer.uploadedChunks[1].rows.map { "${it.table}:${it.op}" })
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun localDeleteCapturesFkCascadeIntoDirtyRows() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndCascadePostsTables(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                )
            )
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")
            client.pushPending().getOrThrow()

            db.execSQL("DELETE FROM users WHERE id = 'user-1'")

            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(listOf("posts:DELETE", "users:DELETE"), dirtyOps(db))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun keyChangingLocalUpdate_emitsDeletePlusUpsert() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            db.execSQL("UPDATE users SET id = 'user-2' WHERE id = 'user-1'")

            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(listOf("user-1:DELETE", "user-2:INSERT"), dirtyKeysAndOps(db))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_keepLocalRetriesAndCommitsLatestLocalIntent() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var commitAttempt = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                commitAttempt++
                if (commitAttempt == 1) {
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"insert conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"INSERT",
                            "base_row_version":0,
                            "server_row_version":7,
                            "server_row_deleted":false,
                            "server_row":{"id":"user-1","name":"Server"}
                          }
                        }
                    """.trimIndent()
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver { MergeResult.KeepLocal },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")

            client.pushPending().getOrThrow()

            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf("INSERT", "UPDATE"), pushServer.uploadedChunks.map { it.rows.single().op })
            assertEquals("Client", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_keepLocalRetriesAndCommitsLatestLocalIntent_forBlobPrimaryKey() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobDocsTable(db)
        val server = newServer()
        var commitAttempt = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                commitAttempt++
                if (commitAttempt == 1) {
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"insert conflict on main.blob_docs 00112233-4455-6677-8899-aabbccddeeff",
                          "conflict":{
                            "schema":"main",
                            "table":"blob_docs",
                            "key":{"id":"00112233-4455-6677-8899-aabbccddeeff"},
                            "op":"INSERT",
                            "base_row_version":0,
                            "server_row_version":7,
                            "server_row_deleted":false,
                            "server_row":{
                              "id":"00112233-4455-6677-8899-aabbccddeeff",
                              "name":"Server Blob",
                              "payload":"c2VydmVy"
                            }
                          }
                        }
                    """.trimIndent()
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("blob_docs", syncKeyColumnName = "id")),
                resolver = Resolver { MergeResult.KeepLocal },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL(
                """
                INSERT INTO blob_docs(id, name, payload)
                VALUES (x'00112233445566778899aabbccddeeff', 'Client Blob', x'68656c6c6f')
                """.trimIndent()
            )

            client.pushPending().getOrThrow()

            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf("INSERT", "UPDATE"), pushServer.uploadedChunks.map { it.rows.single().op })
            val retriedPayload = (pushServer.uploadedChunks.last().rows.single().payload ?: error("expected retried payload")).jsonObject
            assertEquals("00112233-4455-6677-8899-aabbccddeeff", retriedPayload["id"]?.jsonPrimitive?.content)
            assertEquals("Client Blob", retriedPayload["name"]?.jsonPrimitive?.content)
            assertEquals("aGVsbG8=", retriedPayload["payload"]?.jsonPrimitive?.content)
            assertEquals("Client Blob", scalarText(db, "SELECT name FROM blob_docs WHERE lower(hex(id)) = '00112233445566778899aabbccddeeff'"))
            assertEquals("68656C6C6F", scalarText(db, "SELECT hex(payload) FROM blob_docs WHERE lower(hex(id)) = '00112233445566778899aabbccddeeff'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_rejectsHiddenScopeColumnInConflictServerRow() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                409 to """
                    {
                      "error":"push_conflict",
                      "message":"insert conflict on main.users user-1",
                      "conflict":{
                        "schema":"main",
                        "table":"users",
                        "key":{"id":"user-1"},
                        "op":"INSERT",
                        "base_row_version":0,
                        "server_row_version":7,
                        "server_row_deleted":false,
                        "server_row":{"id":"user-1","_sync_scope_id":"forbidden","name":"Server"}
                      }
                    }
                """.trimIndent()
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver { MergeResult.KeepLocal },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error.message?.contains("_sync_scope_id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_keepMergedRetriesMergedPayloadAndCommits() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var commitAttempt = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                commitAttempt++
                if (commitAttempt == 1) {
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"insert conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"INSERT",
                            "base_row_version":0,
                            "server_row_version":7,
                            "server_row_deleted":false,
                            "server_row":{"id":"user-1","name":"Server"}
                          }
                        }
                    """.trimIndent()
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver {
                    MergeResult.KeepMerged(
                        buildJsonObject {
                            put("id", JsonPrimitive("user-1"))
                            put("name", JsonPrimitive("Merged"))
                        }
                    )
                },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")

            client.pushPending().getOrThrow()

            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf("INSERT", "UPDATE"), pushServer.uploadedChunks.map { it.rows.single().op })
            assertEquals("""{"id":"user-1","name":"Merged"}""", pushServer.uploadedChunks.last().rows.single().payload?.toString())
            assertEquals("Merged", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_invalidKeepLocalForDeletedAuthoritativeUpdateRestoresDirtyRows() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var conflictEnabled = false
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                if (conflictEnabled) {
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"update conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"UPDATE",
                            "base_row_version":1,
                            "server_row_version":7,
                            "server_row_deleted":true,
                            "server_row":null
                          }
                        }
                    """.trimIndent()
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver { MergeResult.KeepLocal },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Seed')")
            client.pushPending().getOrThrow()

            db.execSQL("UPDATE users SET name = 'Local Name' WHERE id = 'user-1'")
            conflictEnabled = true

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error is InvalidConflictResolutionException)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Local Name", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals("UPDATE", dirty.op)
            assertEquals(1L, dirty.baseRowVersion)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_invalidKeepMergedForDeleteRestoresDirtyRows() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var conflictEnabled = false
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                if (conflictEnabled) {
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"delete conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"DELETE",
                            "base_row_version":1,
                            "server_row_version":7,
                            "server_row_deleted":false,
                            "server_row":{"id":"user-1","name":"Server"}
                          }
                        }
                    """.trimIndent()
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver {
                    MergeResult.KeepMerged(
                        buildJsonObject {
                            put("id", JsonPrimitive("user-1"))
                            put("name", JsonPrimitive("Merged"))
                        }
                    )
                },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Seed')")
            client.pushPending().getOrThrow()

            db.execSQL("DELETE FROM users WHERE id = 'user-1'")
            conflictEnabled = true

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error is InvalidConflictResolutionException)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = 'user-1'"))
            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals("DELETE", dirty.op)
            assertEquals(1L, dirty.baseRowVersion)
            assertEquals(null, dirty.payload)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_invalidKeepMergedForDeletedAuthoritativeUpdateRestoresDirtyRows() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var conflictEnabled = false
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                if (conflictEnabled) {
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"update conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"UPDATE",
                            "base_row_version":1,
                            "server_row_version":7,
                            "server_row_deleted":true,
                            "server_row":null
                          }
                        }
                    """.trimIndent()
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver {
                    MergeResult.KeepMerged(
                        buildJsonObject {
                            put("id", JsonPrimitive("user-1"))
                            put("name", JsonPrimitive("Merged"))
                        }
                    )
                },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Seed')")
            client.pushPending().getOrThrow()

            db.execSQL("UPDATE users SET name = 'Local Name' WHERE id = 'user-1'")
            conflictEnabled = true

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error is InvalidConflictResolutionException)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Local Name", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals("UPDATE", dirty.op)
            assertEquals(1L, dirty.baseRowVersion)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_preservesSiblingRowsFromRejectedBundle() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        var conflictEnabled = false
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, rowCount ->
                if (conflictEnabled && rowCount == 2) {
                    conflictEnabled = false
                    409 to """
                        {
                          "error":"push_conflict",
                          "message":"update conflict on main.users user-1",
                          "conflict":{
                            "schema":"main",
                            "table":"users",
                            "key":{"id":"user-1"},
                            "op":"UPDATE",
                            "base_row_version":1,
                            "server_row_version":7,
                            "server_row_deleted":false,
                            "server_row":{"id":"user-1","name":"Server"}
                          }
                        }
                    """.trimIndent()
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = 10,
                resolver = ClientWinsResolver,
            )
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Seed')")
            client.pushPending().getOrThrow()

            db.execSQL("UPDATE users SET name = 'Local Name' WHERE id = 'user-1'")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Sibling Post')")
            conflictEnabled = true

            client.pushPending().getOrThrow()

            assertEquals(listOf(1L, 2L, 2L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(
                listOf("users:INSERT", "users:UPDATE,posts:INSERT", "users:UPDATE,posts:INSERT"),
                pushServer.uploadedChunks.map { chunk ->
                    chunk.rows.joinToString(",") { "${it.table}:${it.op}" }
                },
            )
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Local Name", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals("Sibling Post", scalarText(db, "SELECT title FROM posts WHERE id = 'post-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_retryExhaustionLeavesReplayableDirtyState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var commitAttempt = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                commitAttempt++
                val serverVersion = 6L + commitAttempt
                val conflictOp = if (commitAttempt == 1) "INSERT" else "UPDATE"
                409 to """
                    {
                      "error":"push_conflict",
                      "message":"$conflictOp conflict on main.users user-1",
                      "conflict":{
                        "schema":"main",
                        "table":"users",
                        "key":{"id":"user-1"},
                        "op":"$conflictOp",
                        "base_row_version":0,
                        "server_row_version":$serverVersion,
                        "server_row_deleted":false,
                        "server_row":{"id":"user-1","name":"Server $commitAttempt"}
                      }
                    }
                """.trimIndent()
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = Resolver { MergeResult.KeepLocal },
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Client')")

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(error is PushConflictRetryExhaustedException)
            error as PushConflictRetryExhaustedException
            assertEquals(2, error.retryCount)
            assertEquals(1, error.remainingDirtyCount)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("Client", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            val dirty = dirtyRow(db, "users", """{"id":"user-1"}""")
            assertEquals("UPDATE", dirty.op)
            assertEquals(9L, dirty.baseRowVersion)
            assertEquals(9L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_structuredConflict_serverWinsResolverAppliesAuthoritativeRow() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                409 to """
                    {
                      "error":"push_conflict",
                      "message":"insert conflict on main.users user-1",
                      "conflict":{
                        "schema":"main",
                        "table":"users",
                        "key":{"id":"user-1"},
                        "op":"INSERT",
                        "base_row_version":0,
                        "server_row_version":7,
                        "server_row_deleted":false,
                        "server_row":{"id":"user-1","name":"Server"}
                      }
                    }
                """.trimIndent()
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                resolver = ServerWinsResolver,
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            client.pushPending().getOrThrow()

            assertEquals("Server", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(7L, scalarLong(db, "SELECT row_version FROM _sync_row_state WHERE schema_name = 'main' AND table_name = 'users' AND key_json = '{\"id\":\"user-1\"}'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_preCommitRetry_reuploadsAllChunksFromZero() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersAndPostsTables(db)
        val server = newServer()
        var failFirstCommit = true
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                if (failFirstCommit) {
                    failFirstCommit = false
                    500 to """{"error":"push_session_commit_failed","message":"simulated pre-commit failure"}"""
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("users", syncKeyColumnName = "id"),
                    SyncTable("posts", syncKeyColumnName = "id"),
                ),
                uploadLimit = 1,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            db.execSQL("INSERT INTO posts(id, user_id, title) VALUES('post-1', 'user-1', 'Notes')")

            val firstError = client.pushPending().exceptionOrNull()
            assertNotNull(firstError)
            assertEquals(listOf(0L, 1L), pushServer.uploadedChunks.map { it.startRowOrdinal })
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            client.pushPending().getOrThrow()

            assertEquals(listOf(0L, 1L, 0L, 1L), pushServer.uploadedChunks.map { it.startRowOrdinal })
            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_postCommitReplayRecovery_reusesCommittedBundleWithoutReupload() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var chunkReads = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            bundleChunkOverride = { bundle, afterRowOrdinal, maxRows ->
                chunkReads++
                val normal = buildCommittedBundleChunk(bundle, afterRowOrdinal, maxRows)
                if (chunkReads == 1) {
                    normal.copy(bundleHash = "bad-hash")
                } else {
                    normal
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = client.pushPending().exceptionOrNull()
            assertNotNull(firstError)
            assertTrue(firstError.message?.contains("bundle hash") == true || firstError.message?.contains("bundle_hash") == true)
            val uploadedChunkCount = pushServer.uploadedChunks.size
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("committed_remote", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT remote_bundle_seq FROM _sync_outbox_bundle WHERE singleton_key = 1"))

            client.pushPending().getOrThrow()

            assertEquals(uploadedChunkCount, pushServer.uploadedChunks.size)
            assertEquals(listOf(1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_postCommitReplayWaitsForCommittedBundleVisibility() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var notFoundResponsesRemaining = 2
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            committedBundleChunkError = { _, afterRowOrdinal ->
                if (afterRowOrdinal == null && notFoundResponsesRemaining > 0) {
                    notFoundResponsesRemaining--
                    404 to """{"error":"committed_bundle_not_found","message":"committed bundle not visible yet"}"""
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            client.pushPending().getOrThrow()

            assertEquals(0, notFoundResponsesRemaining)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_restartAfterRemoteCommitBeforeReplay_reusesCommittedBundleWithoutReupload() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var chunkReads = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            bundleChunkOverride = { bundle, afterRowOrdinal, maxRows ->
                chunkReads++
                val normal = buildCommittedBundleChunk(bundle, afterRowOrdinal, maxRows)
                if (chunkReads == 1) {
                    normal.copy(bundleHash = "bad-hash")
                } else {
                    normal
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val firstClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            firstClient.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = firstClient.pushPending().exceptionOrNull()

            assertNotNull(firstError)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("committed_remote", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            val uploadedChunkCount = pushServer.uploadedChunks.size

            val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            restartedClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                actual = restartedClient.attach("user-1").getOrThrow(),
            )
            restartedClient.pushPending().getOrThrow()

            assertEquals(uploadedChunkCount, pushServer.uploadedChunks.size)
            assertEquals(listOf(1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_sourceRecoveryPreservesFrozenOutboxAcrossRebuildRestartAndFollowupPush() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var sourceRecoveryResponsesRemaining = 1
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            createError = { request ->
                if (sourceRecoveryResponsesRemaining > 0) {
                    sourceRecoveryResponsesRemaining--
                    409 to """{"error":"source_sequence_out_of_order","message":"source bundle id is stale"}"""
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.createContext("/sync/snapshot-sessions") { exchange ->
            respondJson(
                exchange,
                200,
                """
                {
                  "snapshot_id": "snapshot-recover",
                  "snapshot_bundle_seq": 0,
                  "row_count": 0,
                  "byte_count": 0,
                  "expires_at": "2026-03-22T00:00:00Z"
                }
                """.trimIndent(),
            )
        }
        server.createContext("/sync/snapshot-sessions/snapshot-recover") { exchange ->
            when (exchange.requestMethod) {
                "GET" -> respondJson(
                    exchange,
                    200,
                    """
                    {
                      "snapshot_id": "snapshot-recover",
                      "snapshot_bundle_seq": 0,
                      "next_row_ordinal": 0,
                      "has_more": false,
                      "rows": []
                    }
                    """.trimIndent(),
                )

                "DELETE" -> {
                    exchange.sendResponseHeaders(204, -1)
                    exchange.close()
                }

                else -> {
                    exchange.sendResponseHeaders(405, -1)
                    exchange.close()
                }
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val originalSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = client.pushPending().exceptionOrNull()

            assertNotNull(firstError)
            assertTrue(firstError is SourceRecoveryRequiredException)
            assertEquals(SourceRecoveryReason.SOURCE_SEQUENCE_OUT_OF_ORDER, firstError.reason)
            assertEquals("source_recovery", scalarText(db, "SELECT kind FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals(
                "source_sequence_out_of_order",
                scalarText(db, "SELECT source_recovery_reason FROM _sync_operation_state WHERE singleton_key = 1"),
            )
            assertEquals(originalSourceId, scalarText(db, "SELECT source_recovery_source_id FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_recovery_source_bundle_id FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals("outbox", scalarText(db, "SELECT source_recovery_intent_state FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            client.rebuild().getOrThrow()

            val rotatedSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            assertNotEquals(originalSourceId, rotatedSourceId)
            assertEquals("none", scalarText(db, "SELECT kind FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(rotatedSourceId, scalarText(db, "SELECT source_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(
                2L,
                scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSourceId'"),
            )

            client.close()
            val resumedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            resumedClient.open().getOrThrow()
            assertTrue(resumedClient.attach("user-1").getOrThrow() is AttachResult.Connected)

            val pushReport = resumedClient.pushPending().getOrThrow()

            assertEquals(PushOutcome.COMMITTED, pushReport.outcome)
            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf(originalSourceId, rotatedSourceId), pushServer.createRequests.map { it.sourceId })
            assertEquals(rotatedSourceId, pushServer.bundles.last().sourceId)
            assertEquals(1L, pushServer.bundles.last().sourceBundleId)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(
                2L,
                scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSourceId'"),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_createTimeHistoryPruned_setsDurableSourceRecoveryState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            createError = {
                409 to """{"error":"history_pruned","message":"stale source history was pruned"}"""
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val error = client.pushPending().exceptionOrNull()

            assertNotNull(error)
            assertTrue(error is SourceRecoveryRequiredException)
            assertEquals(SourceRecoveryReason.HISTORY_PRUNED, error.reason)
            assertEquals("source_recovery", scalarText(db, "SELECT kind FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals("history_pruned", scalarText(db, "SELECT source_recovery_reason FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals(sourceId, scalarText(db, "SELECT source_recovery_source_id FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_recovery_source_bundle_id FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            client.close()
            val restartedClient = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            restartedClient.open().getOrThrow()
            val info = restartedClient.sourceInfo().getOrThrow()
            assertTrue(info.rebuildRequired)
            assertTrue(info.sourceRecoveryRequired)
            assertEquals(SourceRecoveryReason.HISTORY_PRUNED, info.sourceRecoveryReason)
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_commitTimeSourceSequenceChanged_setsDurableSourceRecoveryState() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                409 to """{"error":"source_sequence_changed","message":"server source sequence changed"}"""
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val error = client.pushPending().exceptionOrNull()

            assertNotNull(error)
            assertTrue(error is SourceRecoveryRequiredException)
            assertEquals(SourceRecoveryReason.SOURCE_SEQUENCE_CHANGED, error.reason)
            assertEquals("source_recovery", scalarText(db, "SELECT kind FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals(
                "source_sequence_changed",
                scalarText(db, "SELECT source_recovery_reason FROM _sync_operation_state WHERE singleton_key = 1"),
            )
            assertEquals(sourceId, scalarText(db, "SELECT source_recovery_source_id FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_recovery_source_bundle_id FROM _sync_operation_state WHERE singleton_key = 1"))
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1, pushServer.uploadedChunks.size)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_committedReplayPruned_requiresKeepSourceRebuild_andAdvancesSourceFloor() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            committedBundleChunkError = { _, _ ->
                409 to """{"error":"history_pruned","message":"committed replay is below retained floor"}"""
            }
        }
        pushServer.install(server)
        server.createContext("/sync/snapshot-sessions") { exchange ->
            respondJson(
                exchange,
                200,
                """
                {
                  "snapshot_id": "snapshot-committed-pruned",
                  "snapshot_bundle_seq": 1,
                  "row_count": 1,
                  "byte_count": 32,
                  "expires_at": "2026-03-22T00:00:00Z"
                }
                """.trimIndent(),
            )
        }
        server.createContext("/sync/snapshot-sessions/snapshot-committed-pruned") { exchange ->
            when (exchange.requestMethod) {
                "GET" -> respondJson(
                    exchange,
                    200,
                    """
                    {
                      "snapshot_id": "snapshot-committed-pruned",
                      "snapshot_bundle_seq": 1,
                      "next_row_ordinal": 1,
                      "has_more": false,
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "row_version": 1,
                          "payload": {"id":"user-1","name":"Ada"}
                        }
                      ]
                    }
                    """.trimIndent(),
                )

                "DELETE" -> {
                    exchange.sendResponseHeaders(204, -1)
                    exchange.close()
                }

                else -> {
                    exchange.sendResponseHeaders(405, -1)
                    exchange.close()
                }
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = client.pushPending().exceptionOrNull()

            assertTrue(firstError is RebuildRequiredException)
            assertEquals(1L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals("committed_remote", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT source_bundle_id FROM _sync_outbox_bundle WHERE singleton_key = 1"))

            client.rebuild().getOrThrow()

            assertEquals(sourceId, scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals(0L, scalarLong(db, "SELECT rebuild_required FROM _sync_attachment_state WHERE singleton_key = 1"))
            assertEquals("none", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'"))
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_alreadyCommittedWithDifferentCommittedRows_throwsSourceSequenceMismatch() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val committedRows = listOf(
            BundleRow(
                schema = "main",
                table = "users",
                key = mapOf("id" to "user-1"),
                op = "INSERT",
                rowVersion = 1,
                payload = buildJsonObject {
                    put("id", JsonPrimitive("user-1"))
                    put("name", JsonPrimitive("Server Ada"))
                },
            ),
        )
        val committedHash = computeCommittedBundleHash(committedRows)
        var createdSourceId = ""
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                val request = json.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    exchange.requestBody.readBytes().decodeToString(),
                )
                createdSourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "status": "already_committed",
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash"
                    }
                    """.trimIndent(),
                )
            }
            createContext("/sync/committed-bundles/1/rows") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash",
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "op": "INSERT",
                          "row_version": 1,
                          "payload": {"id":"user-1","name":"Server Ada"}
                        }
                      ],
                      "next_row_ordinal": 0,
                      "has_more": false
                    }
                    """.trimIndent(),
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Local Ada')")

            val error = client.pushPending().exceptionOrNull()

            assertNotNull(error)
            assertTrue(
                error is SourceSequenceMismatchException,
                "expected SourceSequenceMismatchException, got ${error::class.qualifiedName}: ${error.message}",
            )
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(
                1L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_restartReusesExistingOutboundSnapshot_insteadOfFreezingNewSnapshot() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var failFirstCommit = true
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            commitError = { _, _ ->
                if (failFirstCommit) {
                    failFirstCommit = false
                    500 to """{"error":"push_session_commit_failed","message":"simulated restart window"}"""
                } else {
                    null
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val firstClient = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            firstClient.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val firstError = firstClient.pushPending().exceptionOrNull()
            assertNotNull(firstError)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")

            val secondClient = newClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            secondClient.openAndConnect("user-1").getOrThrow()
            secondClient.pushPending().getOrThrow()

            assertEquals(listOf(1L, 1L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(listOf("user-1", "user-1"), pushServer.uploadedChunks.map { it.rows.single().key.getValue("id") })
            assertEquals(listOf("user-2:INSERT"), dirtyKeysAndOps(db))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_finalReplayDefersForeignKeys_forSelfReferentialRows() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createEmployeesTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("employees", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.transaction(TransactionMode.IMMEDIATE) {
                db.execSQL("PRAGMA defer_foreign_keys = ON")
                db.execSQL("INSERT INTO employees(id, manager_id, name) VALUES('employee-2', 'employee-1', 'Bob')")
                db.execSQL("INSERT INTO employees(id, manager_id, name) VALUES('employee-1', NULL, 'Alice')")
            }

            client.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM employees"))
            assertEquals("employee-1", scalarText(db, "SELECT manager_id FROM employees WHERE id = 'employee-2'"))
            assertEquals("Alice", scalarText(db, "SELECT name FROM employees WHERE id = 'employee-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_finalReplayDefersForeignKeys_forImmediateCyclicGraphs() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createImmediateAuthorsAndProfilesCycleTables(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("authors", syncKeyColumnName = "id"),
                    SyncTable("profiles", syncKeyColumnName = "id"),
                )
            )
            client.openAndConnect("user-1").getOrThrow()

            db.transaction(TransactionMode.IMMEDIATE) {
                db.execSQL("PRAGMA defer_foreign_keys = ON")
                db.execSQL("INSERT INTO authors(id, profile_id, name) VALUES('author-1', 'profile-1', 'Author')")
                db.execSQL("INSERT INTO profiles(id, author_id, bio) VALUES('profile-1', 'author-1', 'Cyclic')")
            }

            client.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("profile-1", scalarText(db, "SELECT profile_id FROM authors WHERE id = 'author-1'"))
            assertEquals("author-1", scalarText(db, "SELECT author_id FROM profiles WHERE id = 'profile-1'"))
            assertEquals("Cyclic", scalarText(db, "SELECT bio FROM profiles WHERE id = 'profile-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushThenPull_withStaleRemoteBundleAndRealColumn_doesNotRequireSecondSync() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createScoredUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        val remoteRows = listOf(
            BundleRow(
                schema = "main",
                table = "users",
                key = mapOf("id" to "remote-user"),
                op = "INSERT",
                rowVersion = 1,
                payload = buildJsonObject {
                    put("id", JsonPrimitive("remote-user"))
                    put("name", JsonPrimitive("Remote Ada"))
                    put("score", JsonPrimitive(1.25))
                },
            )
        )
        pushServer.bundles += FakeChunkedSyncServer.StoredBundle(
            bundleSeq = 1,
            sourceId = "peer-a",
            sourceBundleId = 11,
            bundleHash = computeCommittedBundleHash(remoteRows),
            rows = remoteRows,
        )
        pushServer.nextBundleSeq = 2L
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name, score) VALUES('local-user', 'Local Bob', 6.57111473696007)")

            client.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(2, pushServer.bundles.size)

            client.pullToStable().getOrThrow()

            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Remote Ada", scalarText(db, "SELECT name FROM users WHERE id = 'remote-user'"))
            assertEquals("Local Bob", scalarText(db, "SELECT name FROM users WHERE id = 'local-user'"))
            assertEquals(2, pushServer.bundles.size)
            assertEquals(2L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_successfulReplayRestoresCaptureForOrdinaryWrites() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1"))

            db.execSQL("UPDATE users SET name = 'Ada updated' WHERE id = 'user-1'")

            assertEquals(listOf("user-1:UPDATE"), dirtyKeysAndOps(db))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_failedReplayRollbackCannotLeaveLeakedApplyMode_andLaterWritesAreCaptured() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val committedRows = listOf(
            BundleRow(
                schema = "main",
                table = "users",
                key = mapOf("id" to "user-1"),
                op = "INSERT",
                rowVersion = 1,
                payload = buildJsonObject {
                    put("id", JsonPrimitive("user-1"))
                },
            )
        )
        val committedHash = computeCommittedBundleHash(committedRows)
        var createdSourceId = ""
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                val request = json.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    exchange.requestBody.readBytes().decodeToString(),
                )
                createdSourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-bad-replay",
                      "status": "staging",
                      "planned_row_count": 1,
                      "next_expected_row_ordinal": 0
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-bad-replay/chunks") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-bad-replay",
                      "next_expected_row_ordinal": 1
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-bad-replay/commit") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash"
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/committed-bundles/1/rows") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash",
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "op": "INSERT",
                          "row_version": 1,
                          "payload": {"id":"user-1"}
                        }
                      ],
                      "next_row_ordinal": 0,
                      "has_more": false
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val error = client.pushPending().exceptionOrNull()
            assertNotNull(error)
            assertTrue(
                error is SourceSequenceMismatchException ||
                    error.message?.contains("does not match the canonical prepared outbox rows") == true ||
                    error.message?.contains("payload for users must contain every table column") == true,
            )
            assertEquals(0L, scalarLong(db, "SELECT apply_mode FROM _sync_apply_state WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")

            assertEquals(listOf("user-2:INSERT"), dirtyKeysAndOps(db))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_sameSourceReusedAfterDetach_preservesSourceSequence() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")
            client.pushPending().getOrThrow()
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )

            assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            assertEquals(
                2L,
                scalarLong(
                    db,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1)",
                ),
            )

            db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")

            val report = client.pushPending().getOrThrow()

            assertEquals(PushOutcome.COMMITTED, report.outcome)
            assertEquals(listOf(1L, 2L), pushServer.createRequests.map { it.sourceBundleId })
            assertEquals(2, pushServer.bundles.size)
            assertEquals(2L, pushServer.bundles.last().sourceBundleId)
            assertEquals(emptyList<String>(), dirtyKeysAndOps(db))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncThenDetach_oneRoundSuccess_detachesAfterCleanSync() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val result = client.syncThenDetach().getOrThrow()

            assertTrue(result.isSuccess())
            assertEquals(DetachOutcome.DETACHED, result.detach)
            assertEquals(1, result.syncRounds)
            assertEquals(0L, result.remainingPendingRowCount)
            assertEquals(PushOutcome.COMMITTED, result.lastSync.pushOutcome)
            client.open().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncThenDetach_retriesWhenFreshWritesArriveDuringFirstRound_andEventuallyDetaches() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var insertedFollowup = false
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            beforePullResponse = { _, _ ->
                if (!insertedFollowup) {
                    insertedFollowup = true
                    runBlocking {
                        db.execSQL("INSERT INTO users(id, name) VALUES('user-2', 'Grace')")
                    }
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val result = client.syncThenDetach().getOrThrow()

            assertTrue(result.isSuccess())
            assertEquals(DetachOutcome.DETACHED, result.detach)
            assertEquals(2, result.syncRounds)
            assertEquals(0L, result.remainingPendingRowCount)
            assertEquals(listOf(1L, 2L), pushServer.createRequests.map { it.sourceBundleId })
            client.open().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncThenDetach_stopsAfterNoPendingProgress_andReturnsBlocked() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        var insertedRounds = 0
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson).apply {
            beforePullResponse = { _, _ ->
                if (insertedRounds < 2) {
                    insertedRounds += 1
                    val userId = "late-user-$insertedRounds"
                    runBlocking {
                        db.execSQL("INSERT INTO users(id, name) VALUES('$userId', 'Late $insertedRounds')")
                    }
                }
            }
        }
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val result = client.syncThenDetach().getOrThrow()

            assertFalse(result.isSuccess())
            assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, result.detach)
            assertEquals(2, result.syncRounds)
            assertEquals(1L, result.remainingPendingRowCount)
            assertEquals(listOf(1L, 2L), pushServer.createRequests.map { it.sourceBundleId })
            client.open().getOrThrow()
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("user-1", scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun syncThenDetach_syncFailure_doesNotImplicitlyDetach() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        var sourceId = ""
        val committedBundleHash = sha256Hex(
            canonicalizeJsonElement(
                JsonArray(
                    listOf(
                        buildJsonObject {
                            put("row_ordinal", JsonPrimitive(0))
                            put("schema", JsonPrimitive("main"))
                            put("table", JsonPrimitive("users"))
                            put("key", buildJsonObject {
                                put("id", JsonPrimitive("user-1"))
                            })
                            put("op", JsonPrimitive("INSERT"))
                            put("row_version", JsonPrimitive(1))
                            put("payload", buildJsonObject {
                                put("id", JsonPrimitive("user-1"))
                                put("name", JsonPrimitive("Ada"))
                            })
                        },
                    ),
                ),
            ).encodeToByteArray(),
        )
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                val request = json.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    exchange.requestBody.readBytes().decodeToString(),
                )
                sourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-1",
                      "status": "staging",
                      "planned_row_count": 1,
                      "next_expected_row_ordinal": 0
                    }
                    """.trimIndent(),
                )
            }
            createContext("/sync/push-sessions/push-1/chunks") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-1",
                      "next_expected_row_ordinal": 1
                    }
                    """.trimIndent(),
                )
            }
            createContext("/sync/push-sessions/push-1/commit") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$sourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedBundleHash"
                    }
                    """.trimIndent(),
                )
            }
            createContext("/sync/committed-bundles/1/rows") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$sourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedBundleHash",
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "op": "INSERT",
                          "row_version": 1,
                          "payload": {"id":"user-1","name":"Ada"}
                        }
                      ],
                      "next_row_ordinal": 0,
                      "has_more": false
                    }
                    """.trimIndent(),
                )
            }
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    500,
                    """
                    {"error":"pull_failed","message":"forced pull failure"}
                    """.trimIndent(),
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            val persistedSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val error = client.syncThenDetach().exceptionOrNull()

            assertNotNull(error)
            client.open().getOrThrow()
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals("user-1", scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_serializesBlobUuidKeysAndPayloadsForCanonicalWireFormat() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobDocsTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("blob_docs", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL(
                """
                INSERT INTO blob_docs(id, name, payload)
                VALUES (x'00112233445566778899aabbccddeeff', 'Blob Doc', x'68656c6c6f')
                """.trimIndent()
            )

            client.pushPending().getOrThrow()

            val row = pushServer.uploadedChunks.single().rows.single()
            assertEquals("00112233-4455-6677-8899-aabbccddeeff", row.key["id"])
            val payload = (row.payload ?: error("expected uploaded payload")).jsonObject
            assertEquals("00112233-4455-6677-8899-aabbccddeeff", payload["id"]?.jsonPrimitive?.content)
            assertEquals("Blob Doc", payload["name"]?.jsonPrimitive?.content)
            assertEquals("aGVsbG8=", payload["payload"]?.jsonPrimitive?.content)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_preservesNullBlobPayloadFieldsOnWire() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createTypedRowsTable(db)
        val server = newServer()
        val pushServer = FakeChunkedSyncServer(json, ::queryParam, ::respondJson)
        pushServer.install(server)
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("typed_rows", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL(
                """
                INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
                VALUES('typed-1', 'Typed Row', NULL, NULL, 0, 1.25, NULL, NULL)
                """.trimIndent()
            )

            client.pushPending().getOrThrow()

            val row = pushServer.uploadedChunks.single().rows.single()
            val payload = (row.payload ?: error("expected uploaded payload")).jsonObject
            assertEquals("typed-1", row.key["id"])
            assertEquals(JsonNull, payload["note"])
            assertEquals(JsonNull, payload["count_value"])
            assertEquals("0", payload["enabled_flag"]?.jsonPrimitive?.content)
            assertEquals("1.25", payload["rating"]?.jsonPrimitive?.content)
            assertEquals(JsonNull, payload["data"])
            assertEquals(JsonNull, payload["created_at"])
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_acceptsCommittedTimestampPayloadEquivalentByInstantAcrossOffsets() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createTypedRowsTable(db)
        val committedRows = listOf(
            BundleRow(
                schema = "main",
                table = "typed_rows",
                key = mapOf("id" to "typed-1"),
                op = "INSERT",
                rowVersion = 1,
                payload = buildJsonObject {
                    put("id", JsonPrimitive("typed-1"))
                    put("name", JsonPrimitive("Typed Row"))
                    put("note", JsonNull)
                    put("count_value", JsonNull)
                    put("enabled_flag", JsonPrimitive(0))
                    put("rating", JsonPrimitive(1.25))
                    put("data", JsonNull)
                    put("created_at", JsonPrimitive("2026-03-24T20:42:11+02:00"))
                },
            )
        )
        val committedHash = computeCommittedBundleHash(committedRows)
        var createdSourceId = ""
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                val request = json.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    exchange.requestBody.readBytes().decodeToString(),
                )
                createdSourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-typed-offset",
                      "status": "staging",
                      "planned_row_count": 1,
                      "next_expected_row_ordinal": 0
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-typed-offset/chunks") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-typed-offset",
                      "next_expected_row_ordinal": 1
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-typed-offset/commit") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash"
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/committed-bundles/1/rows") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash",
                      "rows": [
                        {
                          "schema": "main",
                          "table": "typed_rows",
                          "key": {"id":"typed-1"},
                          "op": "INSERT",
                          "row_version": 1,
                          "payload": {
                            "id":"typed-1",
                            "name":"Typed Row",
                            "note": null,
                            "count_value": null,
                            "enabled_flag": 0,
                            "rating": 1.25,
                            "data": null,
                            "created_at": "2026-03-24T20:42:11+02:00"
                          }
                        }
                      ],
                      "next_row_ordinal": 0,
                      "has_more": false
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("typed_rows", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL(
                """
                INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
                VALUES('typed-1', 'Typed Row', NULL, NULL, 0, 1.25, NULL, '2026-03-24T18:42:11Z')
                """.trimIndent()
            )

            client.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(
                OffsetDateTime.parse("2026-03-24T18:42:11Z").toInstant(),
                OffsetDateTime.parse(scalarText(db, "SELECT created_at FROM typed_rows WHERE id = 'typed-1'")).toInstant(),
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_rejectsCommittedTimestampPayloadWhenPreparedValueIsNaiveLocalText() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createTypedRowsTable(db)
        val committedRows = listOf(
            BundleRow(
                schema = "main",
                table = "typed_rows",
                key = mapOf("id" to "typed-1"),
                op = "INSERT",
                rowVersion = 1,
                payload = buildJsonObject {
                    put("id", JsonPrimitive("typed-1"))
                    put("name", JsonPrimitive("Typed Row"))
                    put("note", JsonNull)
                    put("count_value", JsonNull)
                    put("enabled_flag", JsonPrimitive(0))
                    put("rating", JsonPrimitive(1.25))
                    put("data", JsonNull)
                    put("created_at", JsonPrimitive("2026-03-24T20:42:11+02:00"))
                },
            )
        )
        val committedHash = computeCommittedBundleHash(committedRows)
        var createdSourceId = ""
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                val request = json.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    exchange.requestBody.readBytes().decodeToString(),
                )
                createdSourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "status": "already_committed",
                      "push_id": "",
                      "next_expected_row_ordinal": 0,
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash"
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/committed-bundles/1/rows") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash",
                      "rows": [
                        {
                          "schema": "main",
                          "table": "typed_rows",
                          "key": {"id":"typed-1"},
                          "op": "INSERT",
                          "row_version": 1,
                          "payload": {
                            "id":"typed-1",
                            "name":"Typed Row",
                            "note": null,
                            "count_value": null,
                            "enabled_flag": 0,
                            "rating": 1.25,
                            "data": null,
                            "created_at": "2026-03-24T20:42:11+02:00"
                          }
                        }
                      ],
                      "next_row_ordinal": 0,
                      "has_more": false
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("typed_rows", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL(
                """
                INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
                VALUES('typed-1', 'Typed Row', NULL, NULL, 0, 1.25, NULL, '2026-03-24 18:42:11')
                """.trimIndent()
            )

            val error = client.pushPending().exceptionOrNull()

            assertNotNull(error)
            assertTrue(
                error is SourceSequenceMismatchException,
                "expected SourceSequenceMismatchException, got ${error::class.qualifiedName}: ${error.message}",
            )
            assertEquals("prepared", scalarText(db, "SELECT state FROM _sync_outbox_bundle WHERE singleton_key = 1"))
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("2026-03-24 18:42:11", scalarText(db, "SELECT created_at FROM typed_rows WHERE id = 'typed-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pushPending_replaysBooleanPayloadIntoIntegerColumnWithoutFailing() = runBlocking {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createTypedRowsTable(db)
        val committedRows = listOf(
            BundleRow(
                schema = "main",
                table = "typed_rows",
                key = mapOf("id" to "typed-1"),
                op = "INSERT",
                rowVersion = 1,
                payload = buildJsonObject {
                    put("id", JsonPrimitive("typed-1"))
                    put("name", JsonPrimitive("Typed Row"))
                    put("note", JsonNull)
                    put("count_value", JsonNull)
                    put("enabled_flag", JsonPrimitive(false))
                    put("rating", JsonPrimitive(1.25))
                    put("data", JsonNull)
                    put("created_at", JsonNull)
                },
            )
        )
        val committedHash = computeCommittedBundleHash(committedRows)
        var createdSourceId = ""
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                val request = json.decodeFromString(
                    PushSessionCreateRequest.serializer(),
                    exchange.requestBody.readBytes().decodeToString(),
                )
                createdSourceId = exchange.requestHeaders.getFirst("Oversync-Source-ID").orEmpty()
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-typed-bool",
                      "status": "staging",
                      "planned_row_count": 1,
                      "next_expected_row_ordinal": 0
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-typed-bool/chunks") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "push_id": "push-typed-bool",
                      "next_expected_row_ordinal": 1
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/push-sessions/push-typed-bool/commit") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash"
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/committed-bundles/1/rows") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "bundle_seq": 1,
                      "source_id": "$createdSourceId",
                      "source_bundle_id": 1,
                      "row_count": 1,
                      "bundle_hash": "$committedHash",
                      "rows": [
                        {
                          "schema": "main",
                          "table": "typed_rows",
                          "key": {"id":"typed-1"},
                          "op": "INSERT",
                          "row_version": 1,
                          "payload": {
                            "id":"typed-1",
                            "name":"Typed Row",
                            "note": null,
                            "count_value": null,
                            "enabled_flag": false,
                            "rating": 1.25,
                            "data": null,
                            "created_at": null
                          }
                        }
                      ],
                      "next_row_ordinal": 0,
                      "has_more": false
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("typed_rows", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            db.execSQL(
                """
                INSERT INTO typed_rows(id, name, note, count_value, enabled_flag, rating, data, created_at)
                VALUES('typed-1', 'Typed Row', NULL, NULL, 0, 1.25, NULL, NULL)
                """.trimIndent()
            )

            client.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(db, "SELECT enabled_flag FROM typed_rows WHERE id = 'typed-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

}
