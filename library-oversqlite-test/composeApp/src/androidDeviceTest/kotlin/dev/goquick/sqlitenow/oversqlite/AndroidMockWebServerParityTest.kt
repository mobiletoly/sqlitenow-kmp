package dev.goquick.sqlitenow.oversqlite

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.plugins.defaultRequest
import io.ktor.serialization.kotlinx.json.json
import kotlinx.coroutines.runBlocking
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
    fun pushPullHydrate_workAgainstMockWebServer3() = runBlocking {
        val db = newInMemoryDb()
        val server = MockWebServer()
        server.start()
        val http = newMockWebServerHttpClient(server)
        try {
            db.execSQL("CREATE TABLE users (id TEXT PRIMARY KEY NOT NULL, name TEXT NOT NULL)")
            server.enqueue(
                jsonMockResponse(
                    """
                        {
                          "accepted": true,
                          "bundle": {
                            "bundle_seq": 1,
                            "source_id": "device-a",
                            "source_bundle_id": 1,
                            "rows": [
                              {
                                "schema": "main",
                                "table": "users",
                                "key": {"id":"user-1"},
                                "op": "INSERT",
                                "row_version": 7,
                                "payload": {"id":"user-1","name":"Ada Server"}
                              }
                            ]
                          }
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

            val client = DefaultOversqliteClient(
                db = db,
                config = OversqliteConfig(
                    schema = "main",
                    syncTables = listOf(SyncTable("users", syncKeyColumnName = "id"))
                ),
                http = http,
                tablesUpdateListener = { }
            )

            client.bootstrap("user-1", "device-a").getOrThrow()
            db.execSQL("INSERT INTO users(id, name) VALUES('user-1', 'Ada Local')")

            client.pushPending().getOrThrow()
            assertEquals("Ada Server", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(1L, client.lastBundleSeqSeen().getOrThrow())

            client.pullToStable().getOrThrow()
            assertEquals("Grace Hopper", scalarText(db, "SELECT name FROM users WHERE id = 'user-2'"))
            assertEquals(2L, client.lastBundleSeqSeen().getOrThrow())

            client.hydrate().getOrThrow()
            assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals("Katherine Johnson", scalarText(db, "SELECT name FROM users WHERE id = 'user-3'"))
            assertEquals(9L, client.lastBundleSeqSeen().getOrThrow())

            val pushRequest = server.takeRequest(5, TimeUnit.SECONDS)
            assertNotNull(pushRequest)
            assertEquals("/sync/push", pushRequest?.url?.encodedPath)
            val pushJson = testJson.parseToJsonElement(pushRequest!!.body!!.utf8()).jsonObject
            assertEquals("1", pushJson["source_bundle_id"]?.jsonPrimitive?.content)
            assertEquals(1, pushJson["rows"]?.jsonArray?.size)
            assertEquals("users", pushJson["rows"]?.jsonArray?.first()?.jsonObject?.get("table")?.jsonPrimitive?.content)

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
                tablesUpdateListener = { }
            )

            client.bootstrap("user-1", "device-a").getOrThrow()
            client.pullToStable().getOrThrow()

            assertEquals("Ada", scalarText(db, "SELECT name FROM users WHERE id = 'user-1'"))
            assertEquals(9L, client.lastBundleSeqSeen().getOrThrow())

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
