package dev.goquick.sqlitenow.oversqlite.platform

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.*
import dev.goquick.sqlitenow.oversqlite.platformsupport.sha256Hex
import dev.goquick.sqlitenow.oversqlite.platform.generated.RealServerGeneratedDatabase
import dev.goquick.sqlitenow.oversqlite.platform.generated.UserSelectAllResult
import dev.goquick.sqlitenow.oversqlite.platform.generated.VersionBasedDatabaseMigrations
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
        requirePlatformSuite()
        val server = MockWebServer()
        server.start()
        val http = newMockWebServerHttpClient(server)
        val database = RealServerGeneratedDatabase(":memory:", VersionBasedDatabaseMigrations(), debug = true)
        var opening: OversqliteOpening<RealServerGeneratedDatabase>? = null
        try {
            enqueueDiscovery(server, *RealServerGeneratedDatabase.syncTables.map { it.tableName }.toTypedArray())
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
                              "bundle_hash": "9ca27250b0f374b86697063fa929345062dba95c5c98aac09ba43fcdb8ae5f65",
                              "rows": [[7, 0, [["i", ["11111111-1111-4111-8111-111111111111"], 1, {
                                    "id":"11111111-1111-4111-8111-111111111111",
                                    "name":"Generated Ada",
                                    "email":"generated-ada@example.com",
                                    "created_at":"2026-03-22T00:00:00Z",
                                    "updated_at":"2026-03-22T00:00:00Z"
                                  }]]]]
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )

            opening = database.openForSync(
                config = database.buildOversqliteConfig(
                    generationId = 1L,
                    databaseId = "platform-sync",
                    serviceId = "platform-mock",
                ),
                httpClient = http,
            )
            opening.advance()
            database.enableTableChangeNotifications()
            val client = database.newOversqliteClient(generationId = 1L, databaseId = "platform-sync", serviceId = "platform-mock", httpClient = http)
            client.attach("11111111-1111-4111-8111-111111111111").getOrThrow()

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
                        id = "11111111-1111-4111-8111-111111111111",
                        name = "Generated Ada",
                        email = "generated-ada@example.com",
                    )
                ),
                emissions.last(),
            )

            val discoveryRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertEquals("/sync/generations/1", discoveryRequest?.url?.encodedPath)
            assertEquals(null, discoveryRequest?.headers?.get("Oversync-Schema-Generation-ID"))
            assertEquals(null, discoveryRequest?.headers?.get("Oversync-Schema-Generation-Fingerprint"))

            val connectRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(connectRequest)
            assertEquals("/sync/connect", connectRequest?.url?.encodedPath)

            val pullRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(pullRequest)
            assertEquals("/sync/pull", pullRequest?.url?.encodedPath)
            assertEquals("0", pullRequest?.url?.queryParameter("after_bundle_seq"))
        } finally {
            opening?.close() ?: database.close()
            http.close()
            server.close()
        }
    }

    @Test
    fun pushPullHydrate_workAgainstMockWebServer3() = runBlocking {
        requirePlatformSuite()
        val db = newInMemoryDb()
        val server = MockWebServer()
        server.start()
        val http = newMockWebServerHttpClient(server)
        try {
            db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            val committedBundleHash = "29d27bd2015db825f6b2af4c08cf81883ad20d75584918caedd859d2270c9266"
            val canonicalRequestHash = sha256Hex(
                """{"generation_fingerprint":"8cd94e6ebb91e4022dd8bb64f7d60048b8bf380d5af4982313d23d2d7999b625","generation_id":"1","rows":[{"base_row_version":"0","key":{"id":"user-1"},"op":"INSERT","payload":{"id":"user-1","name":"Ada Local"},"row_ordinal":"0","schema":"main","table":"users"}],"type":"oversync.generation-pinned-push-request.v1"}""".encodeToByteArray()
            )
            val client = newCoordinatedClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            )
            val sourceId = client.sourceInfo().getOrThrow().currentSourceId
            enqueueDiscovery(server, "users")
            enqueueInitializeEmptyConnect(server)
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "bundle_seq": 1,
                          "bundle_hash": "$committedBundleHash",
                          "rows": [[0, 0, [["i", ["user-1"], 7, {"id":"user-1","name":"Ada Local"}]]]]
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
                              "bundle_hash": "08c2b0e67ed8b95ec86536881fdc92d72ca64bde1dace715a4d8ac2c20f5137d",
                              "rows": [[0, 0, [["i", ["user-2"], 1, {"id":"user-2","name":"Grace Hopper"}]]]]
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
                          "logical_byte_count": 63,
                          "expires_at": "2026-03-22T00:00:00Z"
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "next_row_ordinal": 1,
                          "has_more": false,
                          "logical_byte_count": 63,
                          "rows": [[0, 1, [["i", ["user-3"], 4, {"id":"user-3","name":"Katherine Johnson"}]]]]
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

            val discoveryRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertEquals("/sync/generations/1", discoveryRequest?.url?.encodedPath)
            assertEquals(null, discoveryRequest?.headers?.get("Oversync-Schema-Generation-ID"))
            assertEquals(null, discoveryRequest?.headers?.get("Oversync-Schema-Generation-Fingerprint"))

            val connectRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(connectRequest)
            assertEquals("/sync/connect", connectRequest?.url?.encodedPath)

            val pushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(pushRequest)
            assertEquals("/sync/push", pushRequest?.url?.encodedPath)
            val createPushJson = testJson.parseToJsonElement(pushRequest!!.body!!.utf8()).jsonObject
            assertEquals("1", createPushJson["source_bundle_id"]?.jsonPrimitive?.content)
            val compactRows = createPushJson.getValue("rows").jsonArray
            assertEquals(1, compactRows.size)
            val compactGroup = compactRows.single().jsonArray
            assertEquals("0", compactGroup[0].jsonPrimitive.content)
            assertEquals("user-1", compactGroup[2].jsonArray.single().jsonArray[1].jsonArray.single().jsonPrimitive.content)

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
            enqueueDiscovery(server, "users")
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
                          "logical_byte_count": 49,
                          "expires_at": "2026-03-22T00:00:00Z"
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "next_row_ordinal": 1,
                          "has_more": false,
                          "logical_byte_count": 49,
                          "rows": [[0, 1, [["i", ["user-1"], 2, {"id":"user-1","name":"Ada"}]]]]
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(MockResponse.Builder().code(204).build())

            val client = newCoordinatedClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            )
            client.openAndAttach("user-1").getOrThrow()
            client.pullToStable().getOrThrow()

            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(9L, client.syncStatus().getOrThrow().lastBundleSeqSeen)

            val discoveryRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertEquals("/sync/generations/1", discoveryRequest?.url?.encodedPath)
            assertEquals(null, discoveryRequest?.headers?.get("Oversync-Schema-Generation-ID"))
            assertEquals(null, discoveryRequest?.headers?.get("Oversync-Schema-Generation-Fingerprint"))

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

            enqueueDiscovery(server, "files")
            enqueueInitializeEmptyConnect(server)
            enqueueDiscovery(server, "files")
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
                              "bundle_hash": "ce3e9ca783254fcdaef5195149bddabc707e1d3cdf7eb8daa9e89a06894c3fc9",
                              "rows": [[0, 0, [["i", ["file-1"], 1, {
                                    "id":"file-1",
                                    "name":"Blob File",
                                    "data":"AAECAwQFBgcICQoLDA0ODw=="
                                  }]]]]
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
                          "logical_byte_count": 89,
                          "expires_at": "2026-03-22T00:00:00Z"
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "next_row_ordinal": 1,
                          "has_more": false,
                          "logical_byte_count": 89,
                          "rows": [[0, 1, [["i", ["file-1"], 1, {
                                "id":"file-1",
                                "name":"Blob File",
                                "data":"AAECAwQFBgcICQoLDA0ODw=="
                              }]]]]
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(MockResponse.Builder().code(204).build())

            val pullClient = newCoordinatedClient(
                pullDb,
                pullHttp,
                syncTables = listOf(SyncTable("files", syncKeyColumnName = "id")),
            )
            val hydrateClient = newCoordinatedClient(
                hydrateDb,
                hydrateHttp,
                syncTables = listOf(SyncTable("files", syncKeyColumnName = "id")),
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

            val bundleHash = "ce3e9ca783254fcdaef5195149bddabc707e1d3cdf7eb8daa9e89a06894c3fc9"
            val canonicalRequestHash = sha256Hex(
                """{"generation_fingerprint":"9046cec4c1d716e38af72cd55222f0fd3a624bcc9875948e077ccffaba64d6f0","generation_id":"1","rows":[{"base_row_version":"0","key":{"id":"file-1"},"op":"INSERT","payload":{"data":"AAECAwQFBgcICQoLDA0ODw==","id":"file-1","name":"Blob File"},"row_ordinal":"0","schema":"main","table":"files"}],"type":"oversync.generation-pinned-push-request.v1"}""".encodeToByteArray()
            )
            val client = newCoordinatedClient(
                db,
                http,
                syncTables = listOf(SyncTable("files", syncKeyColumnName = "id")),
            )
            val sourceId = client.sourceInfo().getOrThrow().currentSourceId

            enqueueDiscovery(server, "files")
            enqueueInitializeEmptyConnect(server)
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "bundle_seq": 1,
                          "bundle_hash": "$bundleHash",
                          "rows": [[0, 0, [["i", ["file-1"], 1, {
                                "id":"file-1",
                                "name":"Blob File",
                                "data":"AAECAwQFBgcICQoLDA0ODw=="
                              }]]]]
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

            val discoveryRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertEquals("/sync/generations/1", discoveryRequest?.url?.encodedPath)
            assertEquals(null, discoveryRequest?.headers?.get("Oversync-Schema-Generation-ID"))
            assertEquals(null, discoveryRequest?.headers?.get("Oversync-Schema-Generation-Fingerprint"))

            val connectRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(connectRequest)
            assertEquals("/sync/connect", connectRequest?.url?.encodedPath)

            val pushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(pushRequest)
            assertEquals("/sync/push", pushRequest?.url?.encodedPath)
            val createPushJson = testJson.parseToJsonElement(pushRequest!!.body!!.utf8()).jsonObject
            assertEquals(canonicalRequestHash, createPushJson["canonical_request_hash"]!!.jsonPrimitive.content)
            val payloadJson = createPushJson.getValue("rows").jsonArray[0].jsonArray[2].jsonArray[0].jsonArray[3].jsonObject
            assertEquals("AAECAwQFBgcICQoLDA0ODw==", payloadJson["data"]!!.jsonPrimitive.content)

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

    private fun enqueueDiscovery(server: MockWebServer, vararg tables: String) {
        server.enqueue(jsonMockResponse(testGenerationResponse(testRegisteredTableSpecs(*tables)).toString()))
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

    private suspend fun scalarLong(db: PlatformTestDatabase, sql: String): Long {
        return db.connection().prepare(sql).use { st ->
            check(st.step())
            st.getLong(0)
        }
    }

    private suspend fun scalarText(db: PlatformTestDatabase, sql: String): String {
        return db.connection().prepare(sql).use { st ->
            check(st.step())
            st.getText(0)
        }
    }

}
