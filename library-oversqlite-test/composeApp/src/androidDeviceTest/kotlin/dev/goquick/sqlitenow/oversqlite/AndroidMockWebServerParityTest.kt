package dev.goquick.sqlitenow.oversqlite

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.e2e.generated.RealServerGeneratedDatabase
import dev.goquick.sqlitenow.oversqlite.e2e.generated.UserSelectAllResult
import dev.goquick.sqlitenow.oversqlite.e2e.generated.VersionBasedDatabaseMigrations
import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import mockwebserver3.MockResponse
import mockwebserver3.MockWebServer
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotNull
import org.junit.Test
import org.junit.runner.RunWith
import java.util.concurrent.TimeUnit

@RunWith(AndroidJUnit4::class)
class AndroidMockWebServerParityTest {
    @Test
    fun generatedHelper_pullToStableReEmitsGeneratedReactiveQuery() = runBlocking {
        val server = MockWebServer()
        server.start()
        val http = newMockWebServerHttpClient(server)
        val database = RealServerGeneratedDatabase(":memory:", VersionBasedDatabaseMigrations(), debug = true)
        try {
            enqueueCapabilities(server)
            enqueueInitializeEmptyConnect(server)
            server.enqueue(
                jsonMockResponse(
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
                                  "row_version": 1,
                                  "payload": {
                                    "id":"user-1",
                                    "name":"Generated Ada",
                                    "email":"generated-ada@example.com",
                                    "created_at":"2026-03-22T00:00:00Z",
                                    "updated_at":"2026-03-22T00:00:00Z"
                                  }
                                }
                              ]
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )

            database.open()
            database.enableTableChangeNotifications()
            val client = database.newOversqliteClient(schema = "main", httpClient = http)
            client.openAndAttach("user-1").getOrThrow()

            val emissions = mutableListOf<List<UserSelectAllResult>>()
            val firstEmission = CompletableDeferred<Unit>()
            val collector = launch {
                database.user.selectAll.asFlow().take(2).collect { rows ->
                    emissions += rows
                    if (!firstEmission.isCompleted) {
                        firstEmission.complete(Unit)
                    }
                }
            }

            withTimeout(5_000) {
                firstEmission.await()
                client.pullToStable().getOrThrow()
                collector.join()
            }

            assertEquals(2, emissions.size)
            assertEquals(emptyList<UserSelectAllResult>(), emissions.first())
            assertEquals(
                listOf(
                    UserSelectAllResult(
                        id = "user-1",
                        name = "Generated Ada",
                        email = "generated-ada@example.com",
                    )
                ),
                emissions.last(),
            )

            val capabilitiesRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(capabilitiesRequest)
            assertEquals("/sync/capabilities", capabilitiesRequest?.url?.encodedPath)

            val connectRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(connectRequest)
            assertEquals("/sync/connect", connectRequest?.url?.encodedPath)

            val pullRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(pullRequest)
            assertEquals("/sync/pull", pullRequest?.url?.encodedPath)
            assertEquals("0", pullRequest?.url?.queryParameter("after_bundle_seq"))
        } finally {
            http.close()
            server.close()
            database.close()
        }
    }

    @Test
    fun pushPullHydrate_workAgainstMockWebServer3() = runBlocking {
        val db = newInMemoryDb()
        val server = MockWebServer()
        server.start()
        val http = newMockWebServerHttpClient(server)
        try {
            db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            val committedBundleHash = "334a3338e0a497a647a6eb263d78db3b5df06702597ce166111603e9395956ba"
            val client = DefaultOversqliteClient(
                db = db,
                config = OversqliteConfig(
                    schema = "main",
                    syncTables = listOf(SyncTable("users", syncKeyColumnName = "id"))
                ),
                http = http,
            )
            client.open().getOrThrow()
            val sourceId = client.sourceInfo().getOrThrow().currentSourceId
            enqueueCapabilities(server)
            enqueueInitializeEmptyConnect(server)
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "push_id": "push-1",
                          "status": "staging",
                          "planned_row_count": 1,
                          "next_expected_row_ordinal": 0,
                          "source_id": "$sourceId",
                          "source_bundle_id": 1
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "push_id": "push-1",
                          "next_expected_row_ordinal": 1
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
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
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "bundle_seq": 1,
                          "source_id": "$sourceId",
                          "source_bundle_id": 1,
                          "row_count": 1,
                          "bundle_hash": "$committedBundleHash",
                          "next_row_ordinal": 0,
                          "has_more": false,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-1"},
                              "op": "INSERT",
                              "row_version": 7,
                              "payload": {"id":"user-1","name":"Ada Local"}
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
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
                                  "table": "users",
                                  "key": {"id":"user-2"},
                                  "op": "INSERT",
                                  "row_version": 1,
                                  "payload": {"id":"user-2","name":"Grace Hopper"}
                                }
                              ]
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "snapshot_id": "snapshot-hydrate",
                          "snapshot_bundle_seq": 9,
                          "row_count": 1,
                          "byte_count": 32,
                          "expires_at": "2026-03-22T00:00:00Z"
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "snapshot_id": "snapshot-hydrate",
                          "snapshot_bundle_seq": 9,
                          "next_row_ordinal": 1,
                          "has_more": false,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "users",
                              "key": {"id":"user-3"},
                              "row_version": 4,
                              "payload": {"id":"user-3","name":"Katherine Johnson"}
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(MockResponse.Builder().code(204).build())

            client.attach("user-1").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada Local')")

            client.pushPending().getOrThrow()
            assertEquals("Ada Local", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(1L, client.syncStatus().getOrThrow().lastBundleSeqSeen)

            client.pullToStable().getOrThrow()
            assertEquals("Grace Hopper", scalarText(db, "SELECT name FROM users WHERE id = 'user-2'"))
            assertEquals(2L, client.syncStatus().getOrThrow().lastBundleSeqSeen)

            client.rebuild().getOrThrow()
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Katherine Johnson", scalarText(db, "SELECT name FROM users WHERE id = 'user-3'"))
            assertEquals(9L, client.syncStatus().getOrThrow().lastBundleSeqSeen)

            val capabilitiesRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(capabilitiesRequest)
            assertEquals("/sync/capabilities", capabilitiesRequest?.url?.encodedPath)

            val connectRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(connectRequest)
            assertEquals("/sync/connect", connectRequest?.url?.encodedPath)

            val createPushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(createPushRequest)
            assertEquals("/sync/push-sessions", createPushRequest?.url?.encodedPath)
            val createPushJson = testJson.parseToJsonElement(createPushRequest!!.body!!.utf8()).jsonObject
            assertEquals("1", createPushJson["source_bundle_id"]?.jsonPrimitive?.content)
            assertEquals("1", createPushJson["planned_row_count"]?.jsonPrimitive?.content)

            val chunkPushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(chunkPushRequest)
            assertEquals("/sync/push-sessions/push-1/chunks", chunkPushRequest?.url?.encodedPath)
            val chunkPushJson = testJson.parseToJsonElement(chunkPushRequest!!.body!!.utf8()).jsonObject
            assertEquals("0", chunkPushJson["start_row_ordinal"]?.jsonPrimitive?.content)
            assertEquals(1, chunkPushJson["rows"]?.jsonArray?.size)
            assertEquals(
                "users",
                chunkPushJson["rows"]?.jsonArray?.first()?.jsonObject?.get("table")?.jsonPrimitive?.content
            )

            val commitPushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(commitPushRequest)
            assertEquals("/sync/push-sessions/push-1/commit", commitPushRequest?.url?.encodedPath)

            val committedRowsRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(committedRowsRequest)
            assertEquals("/sync/committed-bundles/1/rows", committedRowsRequest?.url?.encodedPath)
            assertEquals("1000", committedRowsRequest?.url?.queryParameter("max_rows"))

            val pullRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(pullRequest)
            assertEquals("/sync/pull", pullRequest?.url?.encodedPath)
            assertEquals("1", pullRequest?.url?.queryParameter("after_bundle_seq"))

            val snapshotCreateRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(snapshotCreateRequest)
            assertEquals("/sync/snapshot-sessions", snapshotCreateRequest?.url?.encodedPath)

            val snapshotChunkRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(snapshotChunkRequest)
            assertEquals("/sync/snapshot-sessions/snapshot-hydrate", snapshotChunkRequest?.url?.encodedPath)
            assertEquals("0", snapshotChunkRequest?.url?.queryParameter("after_row_ordinal"))

            val snapshotDeleteRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(snapshotDeleteRequest)
            assertEquals("/sync/snapshot-sessions/snapshot-hydrate", snapshotDeleteRequest?.url?.encodedPath)
            assertEquals("DELETE", snapshotDeleteRequest?.method)
        } finally {
            http.close()
            server.close()
            db.close()
        }
    }

    @Test
    fun pull_historyPrunedFallsBackToSnapshotHydrate_overRealHttp() = runBlocking {
        val db = newInMemoryDb()
        val server = MockWebServer()
        server.start()
        val http = newMockWebServerHttpClient(server)
        try {
            db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            enqueueCapabilities(server)
            enqueueInitializeEmptyConnect(server)
            server.enqueue(
                MockResponse.Builder()
                    .code(409)
                    .setHeader("Content-Type", "application/json")
                    .body("""{"error":"history_pruned","message":"after_bundle_seq 0 is below retained floor 5"}""")
                    .build()
            )
            server.enqueue(
                jsonMockResponse(
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
            )
            server.enqueue(
                jsonMockResponse(
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
            )
            server.enqueue(MockResponse.Builder().code(204).build())

            val client = DefaultOversqliteClient(
                db = db,
                config = OversqliteConfig(
                    schema = "main",
                    syncTables = listOf(SyncTable("users", syncKeyColumnName = "id"))
                ),
                http = http,
            )

            client.openAndAttach("user-1").getOrThrow()
            client.pullToStable().getOrThrow()

            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(9L, client.syncStatus().getOrThrow().lastBundleSeqSeen)

            val capabilitiesRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(capabilitiesRequest)
            assertEquals("/sync/capabilities", capabilitiesRequest?.url?.encodedPath)

            val connectRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(connectRequest)
            assertEquals("/sync/connect", connectRequest?.url?.encodedPath)

            val pullRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(pullRequest)
            assertEquals("/sync/pull", pullRequest?.url?.encodedPath)
            assertEquals("0", pullRequest?.url?.queryParameter("after_bundle_seq"))

            val snapshotCreateRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(snapshotCreateRequest)
            assertEquals("/sync/snapshot-sessions", snapshotCreateRequest?.url?.encodedPath)

            val snapshotChunkRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(snapshotChunkRequest)
            assertEquals("/sync/snapshot-sessions/snapshot-pruned", snapshotChunkRequest?.url?.encodedPath)

            val snapshotDeleteRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(snapshotDeleteRequest)
            assertEquals("/sync/snapshot-sessions/snapshot-pruned", snapshotDeleteRequest?.url?.encodedPath)
            assertEquals("DELETE", snapshotDeleteRequest?.method)
        } finally {
            http.close()
            server.close()
            db.close()
        }
    }

    @Test
    fun pullAndHydrate_blobPayloadFromCanonicalBase64_storesRawBytes() = runBlocking {
        val pullDb = newInMemoryDb()
        val hydrateDb = newInMemoryDb()
        val server = MockWebServer()
        server.start()
        val pullHttp = newMockWebServerHttpClient(server)
        val hydrateHttp = newMockWebServerHttpClient(server)
        try {
            pullDb.execSQL(
                """
                CREATE TABLE files (
                  id TEXT PRIMARY KEY NOT NULL,
                  name TEXT NOT NULL,
                  data BLOB NOT NULL
                )
                """.trimIndent()
            )
            hydrateDb.execSQL(
                """
                CREATE TABLE files (
                  id TEXT PRIMARY KEY NOT NULL,
                  name TEXT NOT NULL,
                  data BLOB NOT NULL
                )
                """.trimIndent()
            )

            enqueueCapabilities(server)
            enqueueInitializeEmptyConnect(server)
            enqueueCapabilities(server)
            enqueueInitializeEmptyConnect(server)
            server.enqueue(
                jsonMockResponse(
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
                                  "table": "files",
                                  "key": {"id":"file-1"},
                                  "op": "INSERT",
                                  "row_version": 1,
                                  "payload": {
                                    "id":"file-1",
                                    "name":"Blob File",
                                    "data":"AAECAwQFBgcICQoLDA0ODw=="
                                  }
                                }
                              ]
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "snapshot_id": "snapshot-blob",
                          "snapshot_bundle_seq": 1,
                          "row_count": 1,
                          "byte_count": 64,
                          "expires_at": "2026-03-22T00:00:00Z"
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "snapshot_id": "snapshot-blob",
                          "snapshot_bundle_seq": 1,
                          "next_row_ordinal": 1,
                          "has_more": false,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "files",
                              "key": {"id":"file-1"},
                              "row_version": 1,
                              "payload": {
                                "id":"file-1",
                                "name":"Blob File",
                                "data":"AAECAwQFBgcICQoLDA0ODw=="
                              }
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(MockResponse.Builder().code(204).build())

            val pullClient = DefaultOversqliteClient(
                db = pullDb,
                config = OversqliteConfig(
                    schema = "main",
                    syncTables = listOf(SyncTable("files", syncKeyColumnName = "id"))
                ),
                http = pullHttp,
            )
            val hydrateClient = DefaultOversqliteClient(
                db = hydrateDb,
                config = OversqliteConfig(
                    schema = "main",
                    syncTables = listOf(SyncTable("files", syncKeyColumnName = "id"))
                ),
                http = hydrateHttp,
            )

            pullClient.openAndAttach("user-1").getOrThrow()
            hydrateClient.openAndAttach("user-1").getOrThrow()

            pullClient.pullToStable().getOrThrow()
            hydrateClient.rebuild().getOrThrow()

            assertEquals(16L, scalarLong(pullDb, "SELECT length(data) FROM files WHERE id = 'file-1'"))
            assertEquals("000102030405060708090A0B0C0D0E0F", scalarText(pullDb, "SELECT hex(data) FROM files WHERE id = 'file-1'"))
            assertEquals(16L, scalarLong(hydrateDb, "SELECT length(data) FROM files WHERE id = 'file-1'"))
            assertEquals("000102030405060708090A0B0C0D0E0F", scalarText(hydrateDb, "SELECT hex(data) FROM files WHERE id = 'file-1'"))
        } finally {
            pullHttp.close()
            hydrateHttp.close()
            server.close()
            pullDb.close()
            hydrateDb.close()
        }
    }

    @Test
    fun push_blobPayloadEncodesRawBytesForWire() = runBlocking {
        val db = newInMemoryDb()
        val server = MockWebServer()
        server.start()
        val http = newMockWebServerHttpClient(server)
        try {
            db.execSQL(
                """
                CREATE TABLE files (
                  id TEXT PRIMARY KEY NOT NULL,
                  name TEXT NOT NULL,
                  data BLOB NOT NULL
                )
                """.trimIndent()
            )

            val bundleHash = "dcde4c0ea8f55eea93a935cf27d9982ff4e130543ae48cb4c355928c998d7fbd"
            val client = DefaultOversqliteClient(
                db = db,
                config = OversqliteConfig(
                    schema = "main",
                    syncTables = listOf(SyncTable("files", syncKeyColumnName = "id"))
                ),
                http = http,
            )
            client.open().getOrThrow()
            val sourceId = client.sourceInfo().getOrThrow().currentSourceId

            enqueueCapabilities(server)
            enqueueInitializeEmptyConnect(server)
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "push_id": "push-blob-1",
                          "status": "staging",
                          "planned_row_count": 1,
                          "next_expected_row_ordinal": 0,
                          "source_id": "$sourceId",
                          "source_bundle_id": 1
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "push_id": "push-blob-1",
                          "next_expected_row_ordinal": 1
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "bundle_seq": 1,
                          "source_id": "$sourceId",
                          "source_bundle_id": 1,
                          "row_count": 1,
                          "bundle_hash": "$bundleHash"
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "bundle_seq": 1,
                          "source_id": "$sourceId",
                          "source_bundle_id": 1,
                          "row_count": 1,
                          "bundle_hash": "$bundleHash",
                          "next_row_ordinal": 0,
                          "has_more": false,
                          "rows": [
                            {
                              "schema": "main",
                              "table": "files",
                              "key": {"id":"file-1"},
                              "op": "INSERT",
                              "row_version": 1,
                              "payload": {
                                "id":"file-1",
                                "name":"Blob File",
                                "data":"AAECAwQFBgcICQoLDA0ODw=="
                              }
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )

            client.attach("user-1").getOrThrow()
            db.execSQL(
                """
                INSERT INTO files(id, name, data)
                VALUES('file-1', 'Blob File', x'000102030405060708090A0B0C0D0E0F')
                """.trimIndent()
            )
            client.pushPending().getOrThrow()

            val capabilitiesRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(capabilitiesRequest)
            assertEquals("/sync/capabilities", capabilitiesRequest?.url?.encodedPath)

            val connectRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(connectRequest)
            assertEquals("/sync/connect", connectRequest?.url?.encodedPath)

            val createPushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(createPushRequest)
            assertEquals("/sync/push-sessions", createPushRequest?.url?.encodedPath)

            val chunkPushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(chunkPushRequest)
            assertEquals("/sync/push-sessions/push-blob-1/chunks", chunkPushRequest?.url?.encodedPath)
            val chunkPushJson = testJson.parseToJsonElement(chunkPushRequest!!.body!!.utf8()).jsonObject
            val payloadJson = chunkPushJson["rows"]!!.jsonArray.first().jsonObject["payload"]!!.jsonObject
            assertEquals("AAECAwQFBgcICQoLDA0ODw==", payloadJson["data"]!!.jsonPrimitive.content)

            val commitPushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(commitPushRequest)
            assertEquals("/sync/push-sessions/push-blob-1/commit", commitPushRequest?.url?.encodedPath)

            val committedRowsRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(committedRowsRequest)
            assertEquals("/sync/committed-bundles/1/rows", committedRowsRequest?.url?.encodedPath)

            assertEquals(16L, scalarLong(db, "SELECT length(data) FROM files WHERE id = 'file-1'"))
            assertEquals("000102030405060708090A0B0C0D0E0F", scalarText(db, "SELECT hex(data) FROM files WHERE id = 'file-1'"))
        } finally {
            http.close()
            server.close()
            db.close()
        }
    }

    private fun newMockWebServerHttpClient(server: MockWebServer): HttpClient {
        return HttpClient(OkHttp) {
            install(ContentNegotiation) {
                json(testJson)
            }
            defaultRequest {
                url(server.url("/").toString())
            }
        }
    }

    private fun jsonMockResponse(body: String): MockResponse {
        return MockResponse.Builder()
            .code(200)
            .setHeader("Content-Type", "application/json")
            .body(body)
            .build()
    }

    private fun enqueueCapabilities(server: MockWebServer) {
        server.enqueue(
            jsonMockResponse(
                """
                    {
                      "protocol_version": "v1",
                      "schema_version": 1,
                      "features": {
                        "connect_lifecycle": true
                      }
                    }
                """.trimIndent()
            )
        )
    }

    private fun enqueueInitializeEmptyConnect(server: MockWebServer) {
        server.enqueue(
            jsonMockResponse(
                """
                    {
                      "resolution": "initialize_empty"
                    }
                """.trimIndent()
            )
        )
    }

    private suspend fun scalarLong(db: SafeSQLiteConnection, sql: String): Long {
        return db.prepare(sql).use { st ->
            check(st.step())
            st.getLong(0)
        }
    }

    private suspend fun scalarText(db: SafeSQLiteConnection, sql: String): String {
        return db.prepare(sql).use { st ->
            check(st.step())
            st.getText(0)
        }
    }

}
