package dev.goquick.sqlitenow.oversqlite

import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

internal class GenerationOrdinaryClientTest : CrossTargetSyncTestSupport() {
    @Test
    fun attachAdmitsDiscoveryBeforeConnectAndOrdinarySyncUsesOnlyDataPlaneRequests() =
        runOwnedWebDatabaseTest {
            val db = newDb()
            val server = MockSyncServer()
            val http = server.newHttpClient()
            val client = newClient(db, http)
            try {
                createUsersAndPostsTables(db)
                client.open().getOrThrow()
                assertEquals(emptyList(), server.requestTrace)

                client.attach("user-1").getOrThrow()
                assertEquals(
                    listOf("GET /sync/generations/1", "POST /sync/connect"),
                    server.requestTrace,
                )

                server.requestTrace.clear()
                client.sync().getOrThrow()
                assertEquals(listOf("GET /sync/pull"), server.requestTrace)

                insertUser(db, "u1", "one request")
                server.requestTrace.clear()
                client.sync().getOrThrow()
                assertEquals(listOf("POST /sync/push", "GET /sync/pull"), server.requestTrace)
                assertEquals(1, server.discoverySourceIds.size)
            } finally { client.close(); http.close(); db.close() }
        }

    @Test
    fun conflictKeepsEditsMadeAfterUploadWasFrozen() = runOwnedWebDatabaseTest {
        val scenarios = mapOf(
            "newer update" to "UPDATE users SET name='newer' WHERE id='u1'",
            "newer delete" to "DELETE FROM users WHERE id='u1'",
            "identical update" to "UPDATE users SET name='frozen' WHERE id='u1'",
        )
        for (resolution in listOf("client wins", "server wins", "explicit keep local", "merge")) {
        for ((name, mutation) in scenarios) {
            val db = newDb()
            val server = MockSyncServer()
            val http = server.newHttpClient()
            val client = newClient(db, http, resolver = when (resolution) {
                "server wins" -> ServerWinsResolver
                "explicit keep local" -> DeferConflictResolver
                "merge" -> Resolver { MergeResult.KeepMerged(buildJsonObject {
                    put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("merged"))
                }) }
                else -> ClientWinsResolver
            })
            try {
                createUsersAndPostsTables(db)
                insertUser(db, "u1", "frozen")
                insertUser(db, "u2", "sibling frozen")
                client.open().getOrThrow()
                client.attach("user-1").getOrThrow()
                server.beforePushResponse = {
                    db.execSQL(mutation)
                    db.execSQL("UPDATE users SET name='sibling newer' WHERE id='u2'")
                }
                // Conflict evidence and subsequent accepted versions must describe the same server history.
                server.addRemoteBundleForTest(listOf(BundleRow("main","users",mapOf("id" to "u1"),"INSERT",1L,buildJsonObject {
                    put("id",JsonPrimitive("u1")); put("name",JsonPrimitive("server"))
                })))
                server.conflictOverride = { row, _, _ ->
                    if (row.key["id"] == "u1") {
                        server.conflictOverride = null
                        PushConflictDetails(row.schema, row.table, row.key, row.op,
                            row.baseRowVersion, 1L, false, buildJsonObject {
                                put("id", JsonPrimitive("u1"))
                                put("name", JsonPrimitive("server"))
                            })
                    } else null
                }
                if (resolution == "explicit keep local") {
                    assertTrue(client.pushPending().isFailure)
                    val pending = client.pendingConflict().getOrThrow() ?: error("missing conflict")
                    client.resolveConflict(pending, MergeResult.KeepLocal).getOrThrow()
                }
                client.pushPending().getOrThrow()
                val expected = when (name) {
                    "newer delete" -> 0L
                    else -> 1L
                }
                val expectedName = if (name == "identical update") "frozen" else "newer"
                assertEquals(expected, db.scalarLong("SELECT count(*) FROM users WHERE id='u1'"), name)
                if (expected == 1L) assertEquals(1L,
                    db.scalarLong("SELECT count(*) FROM users WHERE id='u1' AND name='$expectedName'"), name)
                assertEquals(1L, db.scalarLong("SELECT count(*) FROM users WHERE id='u2' AND name='sibling newer'"), name)
                assertEquals(
                    2L,
                    db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"),
                    "$resolution / $name must preserve edits captured after the frozen request",
                )
                assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"), name)
                server.beforePushResponse = null
                client.pushPending().getOrThrow()
                assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"), name)
                assertEquals(
                    3L,
                    db.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state"),
                    "$resolution / $name must advance only after each committed direct push",
                )
            } finally { client.close(); http.close(); db.close() }
        }
        }
    }

    @Test
    fun failedOpenRestoresForeignKeysSetting() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            db.execSQL("CREATE TABLE users(name TEXT NOT NULL)")
            db.execSQL("PRAGMA foreign_keys = OFF")

            assertTrue(client.open().isFailure)

            assertEquals(0L, db.scalarLong("PRAGMA foreign_keys"))
        } finally { client.close(); http.close(); db.close() }
    }

    @Test
    fun replayKeepsReinsertThenDeleteCapturedAfterFrozenDelete() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            insertUser(db, "u1", "original")
            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            client.pushPending().getOrThrow()
            db.execSQL("DELETE FROM users WHERE id='u1'")
            server.beforePushResponse = {
                insertUser(db, "u1", "newer")
                db.execSQL("DELETE FROM users WHERE id='u1'")
            }
            client.pushPending().getOrThrow()
            assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM users"))
            assertEquals(1L, db.scalarLong("SELECT COUNT(*) FROM _sync_dirty_rows WHERE op='DELETE' AND payload IS NULL"))
            assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally { client.close(); http.close(); db.close() }
    }

    @Test
    fun negotiatedBatchLeavesRemainingRowsDirty() = runOwnedWebDatabaseTest {
        val scenarios = mapOf(
            "row bound" to testWireLimits().copy(maxPushRows = 1),
            "wire byte bound" to testWireLimits().copy(
                maxPushDecodedBytes = 512,
                maxRowDecodedBytes = 256,
            ),
        )
        for ((scenario, limits) in scenarios) {
        val db = newDb()
        val server = MockSyncServer().apply { wireLimits = limits }
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            val payloadSize = if (scenario == "wire byte bound") 180 else 100
            insertUser(db, "u1", "a".repeat(payloadSize))
            insertUser(db, "u2", "b".repeat(payloadSize))
            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            client.pushPending().getOrThrow()
            assertEquals(1L, db.scalarLong("SELECT COUNT(*) FROM _sync_dirty_rows"), scenario)
            assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM _sync_outbox_rows"))
            client.pushPending().getOrThrow()
            assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM _sync_dirty_rows"), scenario)
            assertEquals(2, server.pushRequestCount, scenario)
        } finally { client.close(); http.close(); db.close() }
        }
    }

    @Test
    fun oversizedPostAdmissionRowDoesNotFreezeOutbox() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer().apply {
            wireLimits = testWireLimits().copy(
                maxPushDecodedBytes = 1_024,
                maxRowDecodedBytes = 512,
            )
        }
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            insertUser(db, "u1", "x".repeat(600))
            assertTrue(client.pushPending().isFailure)
            assertEquals(1L, db.scalarLong("SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(1L, db.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(0, server.pushRequestCount)
        } finally { client.close(); http.close(); db.close() }
    }

    @Test
    fun firstRowOverLocalByteTargetStillProgressesWhenItFitsServerHardLimit() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer().apply {
            wireLimits = testWireLimits().copy(
                maxPushDecodedBytes = 2_048,
                maxRowDecodedBytes = 1_024,
            )
        }
        val http = server.newHttpClient()
        val client = newClient(db, http, maxPushDecodedBytes = 256)
        try {
            createUsersAndPostsTables(db)
            client.open().getOrThrow()
            client.attach("user-1").getOrThrow()
            insertUser(db, "u1", "x".repeat(400))

            client.pushPending().getOrThrow()

            assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, db.scalarLong("SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(2L, db.scalarLong("SELECT next_source_bundle_id FROM _sync_source_state"))
            assertEquals(1, server.pushRequestCount)
        } finally { client.close(); http.close(); db.close() }
    }

    @Test
    fun offlineRowsAdmitPushPullAndReopenWithOneFreshDiscovery() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            insertUser(db, "u1", "offline")
            client.open().getOrThrow()
            assertFalse(client.sourceInfo().getOrThrow().contractConfirmed)
            assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_admitted_contracts"))
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM _sync_dirty_rows WHERE json_extract(payload,'$.name.storage')='text'"))
            client.attach("user-1").getOrThrow()
            assertTrue(client.sourceInfo().getOrThrow().contractConfirmed)
            client.pushPending().getOrThrow()
            assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_dirty_rows"))
            server.addRemoteBundleForTest(listOf(BundleRow(
                "main", "users", mapOf("id" to "u1"), "UPDATE", 2,
                buildJsonObject { put("id", JsonPrimitive("u1")); put("name", JsonPrimitive("remote")) },
            )))
            client.pullToStable().getOrThrow()
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM users WHERE id='u1' AND name='remote'"))
            val discoveries = server.discoverySourceIds.size
            val connects = server.connectRequestCount
            client.closeFromCoordinatedOwner()
            val reopenHttp = server.newHttpClient()
            val reopened = newClient(db, reopenHttp)
            try {
                reopened.open().getOrThrow()
                assertEquals(discoveries, server.discoverySourceIds.size)
                reopened.attach("user-1").getOrThrow()
                assertTrue(reopened.sourceInfo().getOrThrow().contractConfirmed)
                assertEquals(discoveries + 1, server.discoverySourceIds.size)
                assertEquals(connects + 1, server.connectRequestCount)
                assertEquals(
                    listOf("GET /sync/generations/1", "POST /sync/connect"),
                    server.requestTrace.takeLast(2),
                )
            } finally { reopened.close(); reopenHttp.close() }
        } finally { client.close(); http.close(); db.close() }
    }

    @Test
    fun invalidActualStoragePreservesOfflineIntentAndUnconfirmedState() = runOwnedWebDatabaseTest {
        val db = newDb()
        val server = MockSyncServer()
        val http = server.newHttpClient()
        val client = newClient(db, http)
        try {
            createUsersAndPostsTables(db)
            db.execSQL("INSERT INTO users VALUES('u1', X'00ff')")
            client.open().getOrThrow()
            assertTrue(client.attach("user-1").isFailure)
            assertFalse(client.sourceInfo().getOrThrow().contractConfirmed)
            assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_admitted_contracts"))
            assertEquals(0L, db.scalarLong("SELECT count(*) FROM _sync_outbox_rows"))
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM _sync_dirty_rows WHERE json_extract(payload,'$.name.storage')='blob' AND json_extract(payload,'$.name.value')='00ff'"))
            assertEquals(1L, db.scalarLong("SELECT count(*) FROM users WHERE typeof(name)='blob' AND hex(name)='00FF'"))
        } finally { client.close(); http.close(); db.close() }
    }
}
