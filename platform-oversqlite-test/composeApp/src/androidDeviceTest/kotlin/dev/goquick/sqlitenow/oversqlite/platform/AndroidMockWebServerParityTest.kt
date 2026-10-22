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
            // Current committed-effect row versions equal their authority bundle sequence.
            val committedBundleHash = "323899966dbf0be8488d3bf3d8552ed830ee569eb33ea67c0e7f684cea60d1da"
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
            server.enqueue(committedResponse(sourceId, "8cd94e6ebb91e4022dd8bb64f7d60048b8bf380d5af4982313d23d2d7999b625", canonicalRequestHash,
                    "users", 1, committedBundleHash, """[[0,0,[["i",["user-1"],1,{"id":"user-1","name":"Ada Local"}]]]]"""))
            server.enqueue(MockResponse.Builder().code(204).build())
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
                              "bundle_hash": "bf4811a43c8c18bbb75fcd214a7cc432fad4cc92d2097c5f70c4178f52b13f8f",
                              "rows": [[0, 0, [["i", ["user-2"], 2, {"id":"user-2","name":"Grace Hopper"}]]]]
                            }
                          ]
                        }
                    """.trimIndent()
                )
            )
            server.enqueue(snapshotResponse(sourceId, "8cd94e6ebb91e4022dd8bb64f7d60048b8bf380d5af4982313d23d2d7999b625",
                    "snapshot-hydrate", 9, 63, """["i",0,["user-3"],4,{"id":"user-3","name":"Katherine Johnson"}]""", recoveryFloor = 2))
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

            val ackRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(ackRequest)
            assertAcknowledgedOutcome(ackRequest!!, sourceId, "8cd94e6ebb91e4022dd8bb64f7d60048b8bf380d5af4982313d23d2d7999b625", canonicalRequestHash)

            val pullRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(pullRequest)
            assertEquals("/sync/pull", pullRequest?.url?.encodedPath)
            assertEquals("1", pullRequest?.url?.queryParameter("after_bundle_seq"))

            val snapshotCreateRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(snapshotCreateRequest)
            assertEquals("/sync/snapshot-sessions", snapshotCreateRequest?.url?.encodedPath)
            assertSnapshotRequest(snapshotCreateRequest!!, sourceId, "8cd94e6ebb91e4022dd8bb64f7d60048b8bf380d5af4982313d23d2d7999b625", 2)

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
            val client = newCoordinatedClient(
                db,
                http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            )
            val sourceId = client.sourceInfo().getOrThrow().currentSourceId
            enqueueDiscovery(server, "users")
            enqueueInitializeEmptyConnect(server)
            server.enqueue(
                MockResponse.Builder()
                    .code(409)
                    .setHeader("Content-Type", "application/json")
                    .body("""{"error":"history_pruned","message":"after_bundle_seq 0 is below retained floor 5"}""")
                    .build()
            )
            server.enqueue(snapshotResponse(sourceId, "8cd94e6ebb91e4022dd8bb64f7d60048b8bf380d5af4982313d23d2d7999b625",
                    "snapshot-pruned", 9, 49, """["i",0,["user-1"],2,{"id":"user-1","name":"Ada"}]""", recoveryFloor = 0))
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
            assertSnapshotRequest(snapshotCreateRequest!!, sourceId, "8cd94e6ebb91e4022dd8bb64f7d60048b8bf380d5af4982313d23d2d7999b625", 0)

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
            val sourceId = hydrateClient.sourceInfo().getOrThrow().currentSourceId
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
            server.enqueue(snapshotResponse(sourceId, "9046cec4c1d716e38af72cd55222f0fd3a624bcc9875948e077ccffaba64d6f0",
                    "snapshot-blob", 1, 89, """["i",0,["file-1"],1,{"id":"file-1","name":"Blob File","data":"AAECAwQFBgcICQoLDA0ODw=="}]""", recoveryFloor = 0))
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



            pullClient.openAndAttach("user-1").getOrThrow()
            hydrateClient.openAndAttach("user-1").getOrThrow()

            pullClient.pullToStable().getOrThrow()
            hydrateClient.rebuild().getOrThrow()

            assertEquals(16L, scalarLong(pullDb, "SELECT length(data) FROM files WHERE id = 'file-1'"))
            assertEquals("000102030405060708090A0B0C0D0E0F", scalarText(pullDb, "SELECT hex(data) FROM files WHERE id = 'file-1'"))
            assertEquals(16L, scalarLong(hydrateDb, "SELECT length(data) FROM files WHERE id = 'file-1'"))
            assertEquals("000102030405060708090A0B0C0D0E0F", scalarText(hydrateDb, "SELECT hex(data) FROM files WHERE id = 'file-1'"))
            for (path in listOf("/sync/generations/1", "/sync/connect", "/sync/generations/1", "/sync/connect", "/sync/pull")) {
                assertEquals(path, server.takeRequest(5, TimeUnit.SECONDS)?.url?.encodedPath)
            }
            val create = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(create)
            assertSnapshotRequest(create!!, sourceId, "9046cec4c1d716e38af72cd55222f0fd3a624bcc9875948e077ccffaba64d6f0", 0)
            assertEquals("/sync/snapshot-sessions/snapshot-blob", server.takeRequest(5, TimeUnit.SECONDS)?.url?.encodedPath)
            val retire = server.takeRequest(5, TimeUnit.SECONDS)
            assertEquals("DELETE", retire?.method)
            assertEquals("/sync/snapshot-sessions/snapshot-blob", retire?.url?.encodedPath)
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
            server.enqueue(committedResponse(sourceId, "9046cec4c1d716e38af72cd55222f0fd3a624bcc9875948e077ccffaba64d6f0", canonicalRequestHash,
                    "files", 1, bundleHash, """[[0,0,[["i",["file-1"],1,{"id":"file-1","name":"Blob File","data":"AAECAwQFBgcICQoLDA0ODw=="}]]]]"""))
            server.enqueue(MockResponse.Builder().code(204).build())

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
            val ackRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(ackRequest)
            assertAcknowledgedOutcome(ackRequest!!, sourceId, "9046cec4c1d716e38af72cd55222f0fd3a624bcc9875948e077ccffaba64d6f0", canonicalRequestHash)
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

    // This HTTP fixture implements the specification framing independently from
    // the SDK recovery encoder/decoder; literals still define payloads/versions.
    private fun fixtureCanonical(value: kotlinx.serialization.json.JsonElement): String = when (value) {
        is kotlinx.serialization.json.JsonObject -> value.entries.sortedBy { it.key }.joinToString(",", "{", "}") {
            JsonPrimitive(it.key).toString() + ":" + fixtureCanonical(it.value)
        }
        is kotlinx.serialization.json.JsonArray -> value.joinToString(",", "[", "]", transform = ::fixtureCanonical)
        else -> value.toString()
    }

    private fun fixtureSha(bytes: ByteArray): String = java.security.MessageDigest.getInstance("SHA-256")
        .digest(bytes).joinToString("") { "%02x".format(it.toInt() and 255) }

    private fun assertSnapshotRequest(
        request: mockwebserver3.RecordedRequest, source: String,
        fingerprint: String, floor: Long,
    ) {
        assertEquals("POST", request.method)
        assertEquals("/sync/snapshot-sessions", request.url.encodedPath)
        assertEquals("v1", request.headers["Oversync-Protocol-Version"])
        assertEquals(source, request.headers["Oversync-Source-ID"])
        assertEquals("1", request.headers["Oversync-Schema-Generation-ID"])
        assertEquals(fingerprint, request.headers["Oversync-Schema-Generation-Fingerprint"])
        val expected = buildJsonObject { put("recovery", buildJsonObject { put("minimum_bundle_seq", JsonPrimitive(floor)) }) }
        assertEquals("snapshot seal must echo this exact original source and installed observation floor",
            expected, testJson.parseToJsonElement(request.body!!.utf8()).jsonObject)
    }

    private val committedOutcomeHashes = mutableMapOf<String, String>()

    private fun assertAcknowledgedOutcome(
        request: mockwebserver3.RecordedRequest, source: String,
        fingerprint: String, requestHash: String,
    ) {
        assertEquals("POST", request.method)
        assertEquals("/sync/push-outcomes/ack", request.url.encodedPath)
        assertEquals("v1", request.headers["Oversync-Protocol-Version"])
        assertEquals(source, request.headers["Oversync-Source-ID"])
        assertEquals("1", request.headers["Oversync-Schema-Generation-ID"])
        assertEquals(fingerprint, request.headers["Oversync-Schema-Generation-Fingerprint"])
        val expected = buildJsonObject {
            put("source_bundle_id", JsonPrimitive(1))
            put("canonical_request_hash", JsonPrimitive(requestHash))
            put("outcome_hash", JsonPrimitive(committedOutcomeHashes.getValue(source)))
        }
        assertEquals("ack must bind the exact independently framed committed certificate",
            expected, testJson.parseToJsonElement(request.body!!.utf8()).jsonObject)
    }

    private fun committedResponse(
        source: String, fingerprint: String, requestHash: String,
        table: String, bundleSeq: Long, bundleHash: String, rows: String,
    ): MockResponse {
        val compact = testJson.parseToJsonElement(rows).jsonArray
        val logical = kotlinx.serialization.json.buildJsonArray {
            for (group in compact) for (row in group.jsonArray[2].jsonArray) {
                val fields = row.jsonArray
                assertEquals(bundleSeq.toString(), fields[2].jsonPrimitive.content)
                add(kotlinx.serialization.json.buildJsonArray {
                    add(JsonPrimitive("main"));add(JsonPrimitive(table))
                    add(buildJsonObject { put("id", fields[1].jsonArray.single()) })
                    add(JsonPrimitive("I"));add(JsonPrimitive(bundleSeq.toString()));add(fields[3])
                })
            }
        }
        val effect = buildJsonObject {
            put("bundle_seq", JsonPrimitive(bundleSeq));put("bundle_hash", JsonPrimitive(bundleHash))
            put("row_count", JsonPrimitive(logical.size));put("rows", compact)
        }
        val projection = kotlinx.serialization.json.buildJsonArray {
            add(JsonPrimitive("oversqlite.push-outcome.v1"));add(JsonPrimitive(source));add(JsonPrimitive("1"))
            add(JsonPrimitive(fingerprint));add(JsonPrimitive("1"));add(JsonPrimitive(requestHash))
            add(kotlinx.serialization.json.JsonNull);add(JsonPrimitive("committed"))
            add(kotlinx.serialization.json.buildJsonArray {
                add(JsonPrimitive(bundleSeq.toString()));add(JsonPrimitive(bundleHash))
                add(JsonPrimitive(logical.size.toString()));add(logical)
            })
            add(kotlinx.serialization.json.JsonNull)
        }
        val outcomeHash = fixtureSha(fixtureCanonical(projection).encodeToByteArray())
        committedOutcomeHashes[source] = outcomeHash
        return jsonMockResponse(buildJsonObject {
            put("source_id", JsonPrimitive(source));put("schema_generation_id", JsonPrimitive("1"))
            put("schema_generation_fingerprint", JsonPrimitive(fingerprint));put("source_bundle_id", JsonPrimitive(1))
            put("canonical_request_hash", JsonPrimitive(requestHash));put("initialization_id", kotlinx.serialization.json.JsonNull)
            put("status", JsonPrimitive("committed"));put("committed", effect)
            put("outcome_hash", JsonPrimitive(outcomeHash))
        }.toString())
    }

    private fun snapshotResponse(
        source: String, fingerprint: String, id: String, boundary: Long,
        logicalBytes: Long, row: String, recoveryFloor: Long? = null,
    ): MockResponse {
        val wire = testJson.parseToJsonElement(row).jsonArray
        assertEquals(logicalBytes, fixtureCanonical(wire).encodeToByteArray().size.toLong())
        val recovery = recoveryFloor?.let { buildJsonObject { put("minimum_bundle_seq", JsonPrimitive(it)) } }
        val projectedRecovery = recoveryFloor?.let { buildJsonObject { put("minimum_bundle_seq", JsonPrimitive(it.toString())) } }
        val frames = listOf(
            kotlinx.serialization.json.buildJsonArray {
                add(JsonPrimitive("oversqlite.snapshot.v1"));add(JsonPrimitive(source));add(JsonPrimitive("1"))
                add(JsonPrimitive(fingerprint));add(JsonPrimitive(id));add(JsonPrimitive(boundary.toString()))
                add(projectedRecovery ?: kotlinx.serialization.json.JsonNull)
            },
            kotlinx.serialization.json.buildJsonArray {
                add(wire[0]);add(JsonPrimitive(wire[1].jsonPrimitive.content));add(wire[2])
                add(JsonPrimitive(wire[3].jsonPrimitive.content));add(wire[4])
            },
            kotlinx.serialization.json.buildJsonArray {
                add(JsonPrimitive("end"));add(JsonPrimitive("1"));add(JsonPrimitive(logicalBytes.toString()))
            },
        )
        val framed = java.io.ByteArrayOutputStream()
        java.io.DataOutputStream(framed).use { stream ->
            for (frame in frames) {
                val bytes = fixtureCanonical(frame).encodeToByteArray()
                stream.writeLong(bytes.size.toLong());stream.write(bytes)
            }
        }
        return jsonMockResponse(buildJsonObject {
            put("snapshot_id", JsonPrimitive(id));put("snapshot_bundle_seq", JsonPrimitive(boundary))
            put("row_count", JsonPrimitive(1));put("logical_byte_count", JsonPrimitive(logicalBytes))
            put("expires_at", JsonPrimitive("2099-01-01T00:00:00Z"))
            put("snapshot_hash", JsonPrimitive(fixtureSha(framed.toByteArray())))
            if (recovery != null) put("recovery", recovery)
        }.toString())
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
