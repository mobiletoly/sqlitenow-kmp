package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BundlePullContractTest : BundleClientContractTestSupport() {
    @Test
    fun syncOperations_rejectOverlap_andExpectedContentionHelperStaysNarrow() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val createStarted = CountDownLatch(1)
        val releaseCreate = CountDownLatch(1)
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
                        }
                    )
                )
            ).encodeToByteArray()
        )
        val server = newServer().apply {
            createContext("/sync/push-sessions") { exchange ->
                createStarted.countDown()
                check(releaseCreate.await(5, TimeUnit.SECONDS))
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
                    """.trimIndent()
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
                    """.trimIndent()
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
                    """.trimIndent()
                )
            }
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 1,
                      "has_more": false,
                      "bundles": []
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
            sourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1")
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val inFlightPush = async(Dispatchers.Default) { client.pushPending() }
            assertTrue(createStarted.await(5, TimeUnit.SECONDS))

            val overlappingErrors = listOf(
                client.pushPending().exceptionOrNull(),
                client.pullToStable().exceptionOrNull(),
                client.sync().exceptionOrNull(),
                client.rebuild(RebuildMode.KEEP_SOURCE).exceptionOrNull(),
                client.rebuild(
                    mode = RebuildMode.ROTATE_SOURCE,
                    newSourceId = randomTestSourceId("pull-overlap-rotate"),
                ).exceptionOrNull(),
            )
            overlappingErrors.forEach { error ->
                assertTrue(error is SyncOperationInProgressException)
                assertTrue(isExpectedSyncContention(error))
            }

            assertTrue(!isExpectedSyncContention(PendingPushReplayException(1)))
            assertTrue(!isExpectedSyncContention(RebuildRequiredException()))
            assertTrue(!isExpectedSyncContention(RuntimeException("transport failed")))

            releaseCreate.countDown()
            inFlightPush.await().getOrThrow()
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_hydrate_andRecover_rejectPendingPushReplay() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL(
                """
                UPDATE _sync_outbox_bundle
                SET state = 'prepared',
                    source_id = (SELECT current_source_id FROM _sync_attachment_state WHERE singleton_key = 1),
                    source_bundle_id = 1,
                    initialization_id = '',
                    canonical_request_hash = 'pending-replay',
                    row_count = 1,
                    remote_bundle_hash = '',
                    remote_bundle_seq = 0
                WHERE singleton_key = 1
                """.trimIndent()
            )
            db.execSQL(
                """
                INSERT INTO _sync_outbox_rows(
                  source_bundle_id,
                  row_ordinal,
                  schema_name,
                  table_name,
                  key_json,
                  wire_key_json,
                  op,
                  base_row_version,
                  local_payload,
                  wire_payload
                )
                VALUES (
                  1,
                  0,
                  'main',
                  'users',
                  '{"id":"user-1"}',
                  '{"id":"user-1"}',
                  'INSERT',
                  0,
                  '{"id":"user-1","name":"Ada"}',
                  '{"id":"user-1","name":"Ada"}'
                )
                """.trimIndent()
            )

            assertTrue(client.pullToStable().exceptionOrNull() is PendingPushReplayException)
            assertTrue(client.rebuild(RebuildMode.KEEP_SOURCE).exceptionOrNull() is PendingPushReplayException)
            assertTrue(
                client.rebuild(
                    mode = RebuildMode.ROTATE_SOURCE,
                    newSourceId = randomTestSourceId("pull-pending-rotate"),
                ).exceptionOrNull() is PendingPushReplayException
            )
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_reportsAlreadyAtTargetWhenNothingNewIsAvailable() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 0,
                      "has_more": false,
                      "bundles": []
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

            val report = client.pullToStable().getOrThrow()

            assertEquals(RemoteSyncOutcome.ALREADY_AT_TARGET, report.outcome)
            assertEquals(AuthorityStatus.AUTHORITATIVE_EMPTY, report.status.authority)
            assertEquals(0L, report.status.lastBundleSeqSeen)
            assertEquals(null, report.restore)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pauseDownloads_doesNotSkipExplicitPullToStable() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 1,
                      "has_more": false,
                      "bundles": [
                        {
                          "bundle_seq": 1,
                          "source_id": "peer-a",
                          "source_bundle_id": 10,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "op": "INSERT",
                              "row_version": 1,
                              "payload": {
                                "id":"user-1",
                                "name":"Ada"
                              }
                            }
                          ]
                        }
                      ]
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
            client.pauseDownloads()

            val report = client.pullToStable().getOrThrow()

            assertEquals(RemoteSyncOutcome.APPLIED_INCREMENTAL, report.outcome)
            assertEquals(AuthorityStatus.AUTHORITATIVE_MATERIALIZED, report.status.authority)
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_appliesBlobUuidKeysAndPayloadsFromGoWireFormat() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobDocsTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 2,
                      "has_more": false,
                      "bundles": [
                        {
                          "bundle_seq": 2,
                          "source_id": "peer-a",
                          "source_bundle_id": 11,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "blob_docs",
                              "key": {"id":"00112233-4455-6677-8899-aabbccddeeff"},
                              "op": "INSERT",
                              "row_version": 5,
                              "payload": {
                                "id":"00112233-4455-6677-8899-aabbccddeeff",
                                "name":"Blob Doc",
                                "payload":"aGVsbG8="
                              }
                            }
                          ]
                        }
                      ]
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("blob_docs", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            client.pullToStable().getOrThrow()

            assertEquals(2L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("Blob Doc", scalarText(db, "SELECT name FROM blob_docs"))
            assertEquals("00112233445566778899aabbccddeeff", scalarText(db, "SELECT lower(hex(id)) FROM blob_docs"))
            assertEquals("68656c6c6f", scalarText(db, "SELECT lower(hex(payload)) FROM blob_docs"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_appliesBlobUuidReferenceColumnsFromCanonicalWireFormat() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobFilesAndReviewsTables(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 2,
                      "has_more": false,
                      "bundles": [
                        {
                          "bundle_seq": 2,
                          "source_id": "peer-a",
                          "source_bundle_id": 11,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "files",
                              "key": {"id":"00112233-4455-6677-8899-aabbccddeeff"},
                              "op": "INSERT",
                              "row_version": 5,
                              "payload": {
                                "id":"00112233-4455-6677-8899-aabbccddeeff",
                                "name":"Blob File",
                                "data":"aGVsbG8="
                              }
                            },
                            {
                              "schema": "main",
                              "table": "file_reviews",
                              "key": {"id":"11112222-3333-4444-5555-666677778888"},
                              "op": "INSERT",
                              "row_version": 6,
                              "payload": {
                                "id":"11112222-3333-4444-5555-666677778888",
                                "file_id":"00112233-4455-6677-8899-aabbccddeeff",
                                "review":"ok"
                              }
                            }
                          ]
                        }
                      ]
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(
                db,
                http,
                syncTables = listOf(
                    SyncTable("files", syncKeyColumnName = "id"),
                    SyncTable("file_reviews", syncKeyColumnName = "id"),
                ),
            )
            client.openAndConnect("user-1").getOrThrow()

            client.pullToStable().getOrThrow()

            assertEquals("00112233445566778899aabbccddeeff", scalarText(db, "SELECT lower(hex(id)) FROM files"))
            assertEquals("68656c6c6f", scalarText(db, "SELECT lower(hex(data)) FROM files"))
            assertEquals("11112222333344445555666677778888", scalarText(db, "SELECT lower(hex(id)) FROM file_reviews"))
            assertEquals("00112233445566778899aabbccddeeff", scalarText(db, "SELECT lower(hex(file_id)) FROM file_reviews"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_rejectsNonCanonicalBlobWireEncodings() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createBlobDocsTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 2,
                      "has_more": false,
                      "bundles": [
                        {
                          "bundle_seq": 2,
                          "source_id": "peer-a",
                          "source_bundle_id": 11,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "blob_docs",
                              "key": {"id":"00112233-4455-6677-8899-aabbccddeeff"},
                              "op": "INSERT",
                              "row_version": 5,
                              "payload": {
                                "id":"00112233-4455-6677-8899-aabbccddeeff",
                                "name":"Blob Doc",
                                "payload":"68656c6c6f"
                              }
                            }
                          ]
                        }
                      ]
                    }
                    """.trimIndent()
                )
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("blob_docs", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            val error = client.pullToStable().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("invalid canonical wire blob encoding") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_appliesBundlesToFrozenCeiling() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        var firstAfter = ""
        var secondAfter = ""
        var secondTarget = ""
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                val after = queryParam(exchange, "after_bundle_seq")
                val target = queryParam(exchange, "target_bundle_seq")
                when (after) {
                    "0" -> {
                        firstAfter = after
                        respondJson(
                            exchange,
                            200,
                            """
                            {
                              "stable_bundle_seq": 2,
                              "has_more": true,
                              "bundles": [
                                {
                                  "bundle_seq": 1,
                                  "source_id": "peer-a",
                                  "source_bundle_id": 11,
                                  "rows": [
                                    {
                                      "schema": "main",
                                      "table": "users",
                                      "key": {"id":"user-1"},
                                      "op": "INSERT",
                                      "row_version": 1,
                                      "payload": {"id":"user-1","name":"Ada"}
                                    }
                                  ]
                                }
                              ]
                            }
                            """.trimIndent()
                        )
                    }
                    "1" -> {
                        secondAfter = after
                        secondTarget = target
                        respondJson(
                            exchange,
                            200,
                            """
                            {
                              "stable_bundle_seq": 2,
                              "has_more": false,
                              "bundles": [
                                {
                                  "bundle_seq": 2,
                                  "source_id": "peer-b",
                                  "source_bundle_id": 12,
                                  "rows": [
                                    {
                                      "schema": "main",
                                      "table": "users",
                                      "key": {"id":"user-2"},
                                      "op": "INSERT",
                                      "row_version": 1,
                                      "payload": {"id":"user-2","name":"Grace"}
                                    }
                                  ]
                                }
                              ]
                            }
                            """.trimIndent()
                        )
                    }
                    else -> error("unexpected after_bundle_seq=$after")
                }
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            client.pullToStable().getOrThrow()

            assertEquals("0", firstAfter)
            assertEquals("1", secondAfter)
            assertEquals("2", secondTarget)
            assertEquals(2L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(2L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_rejectsDirtyRows() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer()
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada')")

            val error = client.pullToStable().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("dirty rows") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_historyPrunedFallsBackToSnapshotHydrate() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    409,
                    """{"error":"history_pruned","message":"after_bundle_seq 0 is below retained floor 5"}"""
                )
            }
            createContext("/sync/snapshot-sessions") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "snapshot_id": "snapshot-pruned",
                      "snapshot_bundle_seq": 9,
                      "row_count": 1,
                      "byte_count": 32,
                      "expires_at": "2026-03-22T00:00:00Z"
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/snapshot-sessions/snapshot-pruned") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "snapshot_id": "snapshot-pruned",
                      "snapshot_bundle_seq": 9,
                      "next_row_ordinal": 1,
                      "has_more": false,
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "row_version": 2,
                          "payload": {"id":"user-1","name":"Ada"}
                        }
                      ]
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

            client.pullToStable().getOrThrow()

            assertEquals(9L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_rejectsHiddenScopeColumnInBundlePayload() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 1,
                      "has_more": false,
                      "bundles": [
                        {
                          "bundle_seq": 1,
                          "source_id": "peer-a",
                          "source_bundle_id": 11,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "op": "INSERT",
                              "row_version": 2,
                              "payload": {
                                "id":"user-1",
                                "_sync_scope_id":"forbidden",
                                "name":"Ada"
                              }
                            }
                          ]
                        }
                      ]
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

            val error = client.pullToStable().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("_sync_scope_id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun hydrate_rejectsHiddenScopeColumnInSnapshotPayload() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/snapshot-sessions") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "snapshot_id": "snapshot-hidden-scope",
                      "snapshot_bundle_seq": 3,
                      "row_count": 1,
                      "byte_count": 32,
                      "expires_at": "2026-03-22T00:00:00Z"
                    }
                    """.trimIndent()
                )
            }
            createContext("/sync/snapshot-sessions/snapshot-hidden-scope") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "snapshot_id": "snapshot-hidden-scope",
                      "snapshot_bundle_seq": 3,
                      "next_row_ordinal": 1,
                      "has_more": false,
                      "rows": [
                        {
                          "schema": "main",
                          "table": "users",
                          "key": {"id":"user-1"},
                          "row_version": 2,
                          "payload": {
                            "id":"user-1",
                            "_sync_scope_id":"forbidden",
                            "name":"Ada"
                          }
                        }
                      ]
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

            val error = client.rebuild(RebuildMode.KEEP_SOURCE).exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("_sync_scope_id") == true)
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_rejectsMalformedPullResponse() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 1,
                      "has_more": false,
                      "bundles": [
                        {
                          "bundle_seq": 1,
                          "source_id": "peer-a",
                          "source_bundle_id": 1,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "op": "INSERT",
                              "row_version": 0,
                              "payload": {"id":"user-1","name":"Ada"}
                            }
                          ]
                        }
                      ]
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

            val error = client.pullToStable().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("bundle row row_version 0 must be positive") == true)
            assertEquals(0L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_rejectsIncompletePullBeforeFrozenCeiling() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 2,
                      "has_more": false,
                      "bundles": [
                        {
                          "bundle_seq": 1,
                          "source_id": "peer-a",
                          "source_bundle_id": 1,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "op": "INSERT",
                              "row_version": 1,
                              "payload": {"id":"user-1","name":"Ada"}
                            }
                          ]
                        }
                      ]
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

            val error = client.pullToStable().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("pull ended early at bundle seq 1 before stable bundle seq 2") == true)
            assertEquals(1L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_rejectsMidSessionFrozenCeilingChange() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                when (queryParam(exchange, "after_bundle_seq")) {
                    "0" -> respondJson(
                        exchange,
                        200,
                        """
                        {
                          "stable_bundle_seq": 2,
                          "has_more": true,
                          "bundles": [
                            {
                              "bundle_seq": 1,
                              "source_id": "peer-a",
                              "source_bundle_id": 1,
                              "rows": [
                                {
                                  "schema": "main",
                                  "table": "users",
                                  "key": {"id":"user-1"},
                                  "op": "INSERT",
                                  "row_version": 1,
                                  "payload": {"id":"user-1","name":"Ada"}
                                }
                              ]
                            }
                          ]
                        }
                        """.trimIndent()
                    )
                    "1" -> respondJson(
                        exchange,
                        200,
                        """
                        {
                          "stable_bundle_seq": 3,
                          "has_more": false,
                          "bundles": [
                            {
                              "bundle_seq": 2,
                              "source_id": "peer-b",
                              "source_bundle_id": 2,
                              "rows": [
                                {
                                  "schema": "main",
                                  "table": "users",
                                  "key": {"id":"user-2"},
                                  "op": "INSERT",
                                  "row_version": 1,
                                  "payload": {"id":"user-2","name":"Grace"}
                                }
                              ]
                            }
                          ]
                        }
                        """.trimIndent()
                    )
                    else -> error("unexpected after_bundle_seq")
                }
            }
        }
        server.start()
        val http = newHttpClient(server)
        try {
            val client = newClient(db, http, syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")))
            client.openAndConnect("user-1").getOrThrow()

            val error = client.pullToStable().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("stable bundle seq changed from 2 to 3") == true)
            assertEquals(1L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }

    @Test
    fun pullToStable_failedBundleApplyLeavesCheckpointUnchanged() = runBlocking<Unit> {
        val db = BundledSqliteConnectionProvider.openConnection(":memory:", debug = false)
        createUsersTable(db)
        val server = newServer().apply {
            createContext("/sync/pull") { exchange ->
                respondJson(
                    exchange,
                    200,
                    """
                    {
                      "stable_bundle_seq": 1,
                      "has_more": false,
                      "bundles": [
                        {
                          "bundle_seq": 1,
                          "source_id": "peer-a",
                          "source_bundle_id": 1,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "op": "INSERT",
                              "row_version": 1,
                              "payload": {"id":"user-1"}
                            }
                          ]
                        }
                      ]
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

            val error = client.pullToStable().exceptionOrNull()
            assertTrue(error != null)
            assertTrue(error.message?.contains("payload for users must contain every table column") == true)
            assertEquals(0L, client.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
        } finally {
            http.close()
            server.stop(0)
            db.close()
        }
    }
}
