package dev.goquick.sqlitenow.oversqlite.realserver

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.oversqlite.ClientWinsResolver
import dev.goquick.sqlitenow.oversqlite.DefaultOversqliteClient
import dev.goquick.sqlitenow.oversqlite.MergeResult
import dev.goquick.sqlitenow.oversqlite.Resolver
import dev.goquick.sqlitenow.oversqlite.ServerWinsResolver
import io.ktor.client.HttpClient
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import java.time.OffsetDateTime

@RunWith(AndroidJUnit4::class)
class RealServerConflictTest {
    private fun updatedAtResolver(): Resolver = Resolver { conflict ->
        val serverUpdatedAt = conflict.serverRow
            ?.jsonObject
            ?.get("updated_at")
            ?.jsonPrimitive
            ?.contentOrNull
        val localUpdatedAt = conflict.localPayload
            ?.jsonObject
            ?.get("updated_at")
            ?.jsonPrimitive
            ?.contentOrNull
        val serverUpdatedAtMillis = parseUpdatedAtMillis(serverUpdatedAt)
        val localUpdatedAtMillis = parseUpdatedAtMillis(localUpdatedAt)
        if (serverUpdatedAtMillis != null && localUpdatedAtMillis != null && localUpdatedAtMillis > serverUpdatedAtMillis) {
            MergeResult.KeepLocal
        } else {
            MergeResult.AcceptServer
        }
    }

    @Test
    fun conflictingEdits_keepLocalResolver_recoversAutomaticallyToLatestLocalIntent() = runBlocking {
        withConflictHarness(
            prefix = "conflict-keep-local",
            resolverB = Resolver { MergeResult.KeepLocal },
        ) {
            val rowId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$rowId', 'Grace', 'grace@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Grace Server' WHERE id = '$rowId'")
            dbB.execSQL("UPDATE users SET name = 'Grace Client' WHERE id = '$rowId'")

            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Grace Client", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"))

            observerClient.pullToStable().getOrThrow()
            assertEquals("Grace Client", scalarText(observerDb, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals(0L, scalarLong(observerDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        }
    }

    @Test
    fun conflictingEdits_serverWinsResolver_recoversAutomaticallyToServerState() = runBlocking {
        withConflictHarness(
            prefix = "conflict-server-wins",
            resolverB = ServerWinsResolver,
        ) {
            val rowId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$rowId', 'Grace', 'grace@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Grace Server' WHERE id = '$rowId'")
            dbB.execSQL("UPDATE users SET name = 'Grace Client' WHERE id = '$rowId'")

            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Grace Server", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"))

            observerClient.pullToStable().getOrThrow()
            assertEquals("Grace Server", scalarText(observerDb, "SELECT name FROM users WHERE id = '$rowId'"))
        }
    }

    @Test
    fun conflictingEdits_clientWinsResolver_recoversAutomaticallyToLatestLocalIntent() = runBlocking {
        withConflictHarness(
            prefix = "conflict-client-wins",
            resolverB = ClientWinsResolver,
        ) {
            val rowId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$rowId', 'Grace', 'grace@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Grace Server' WHERE id = '$rowId'")
            dbB.execSQL("UPDATE users SET name = 'Grace Client' WHERE id = '$rowId'")

            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Grace Client", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"))

            observerClient.pullToStable().getOrThrow()
            assertEquals("Grace Client", scalarText(observerDb, "SELECT name FROM users WHERE id = '$rowId'"))
        }
    }

    @Test
    fun conflictingEdits_updatedAtResolver_keepsNewerLocalIntent() = runBlocking {
        var seenServerUpdatedAt: String? = null
        var seenLocalUpdatedAt: String? = null
        withConflictHarness(
            prefix = "conflict-updated-at",
            resolverB = Resolver { conflict ->
                seenServerUpdatedAt = conflict.serverRow
                    ?.jsonObject
                    ?.get("updated_at")
                    ?.jsonPrimitive
                    ?.contentOrNull
                seenLocalUpdatedAt = conflict.localPayload
                    ?.jsonObject
                    ?.get("updated_at")
                    ?.jsonPrimitive
                    ?.contentOrNull
                val serverUpdatedAt = parseUpdatedAtMillis(seenServerUpdatedAt)
                val localUpdatedAt = parseUpdatedAtMillis(seenLocalUpdatedAt)
                if (serverUpdatedAt != null && localUpdatedAt != null && localUpdatedAt > serverUpdatedAt) {
                    MergeResult.KeepLocal
                } else {
                    MergeResult.AcceptServer
                }
            },
        ) {
            val rowId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email, created_at, updated_at)
                VALUES('$rowId', 'Grace', 'grace@example.com', '2026-03-24T00:00:00Z', '2026-03-24T00:00:00Z')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Grace Server Older', updated_at = '2026-03-24T00:00:10Z' WHERE id = '$rowId'")
            dbB.execSQL("UPDATE users SET name = 'Grace Client Newer', updated_at = '2026-03-24T00:00:20Z' WHERE id = '$rowId'")

            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()

            assertEquals(
                parseUpdatedAtMillis("2026-03-24T00:00:10Z"),
                parseUpdatedAtMillis(seenServerUpdatedAt)
            )
            assertEquals(
                parseUpdatedAtMillis("2026-03-24T00:00:20Z"),
                parseUpdatedAtMillis(seenLocalUpdatedAt)
            )
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Grace Client Newer", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals(
                parseUpdatedAtMillis("2026-03-24T00:00:20Z"),
                parseUpdatedAtMillis(scalarText(dbB, "SELECT updated_at FROM users WHERE id = '$rowId'"))
            )

            observerClient.pullToStable().getOrThrow()
            assertEquals("Grace Client Newer", scalarText(observerDb, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals(
                parseUpdatedAtMillis("2026-03-24T00:00:20Z"),
                parseUpdatedAtMillis(scalarText(observerDb, "SELECT updated_at FROM users WHERE id = '$rowId'"))
            )
        }
    }

    @Test
    fun conflictingEdits_withUnseenPeerBundle_clientWinsResolverStillConverges() = runBlocking {
        withConflictHarness(
            prefix = "conflict-unseen-peer-bundle",
            resolverB = ClientWinsResolver,
        ) {
            val rowId = randomRowId()
            val peerRowId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$rowId', 'Grace', 'grace@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Grace Server' WHERE id = '$rowId'")
            clientA.pushPending().getOrThrow()

            observerDb.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$peerRowId', 'Peer Row', 'peer@example.com')
                """.trimIndent()
            )
            observerClient.pushPending().getOrThrow()

            dbB.execSQL("UPDATE users SET name = 'Grace Client' WHERE id = '$rowId'")
            clientB.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            clientA.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            assertEquals(4L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(4L, observerClient.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("Grace Client", scalarText(dbA, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals("Grace Client", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals("Grace Client", scalarText(observerDb, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals("Peer Row", scalarText(dbA, "SELECT name FROM users WHERE id = '$peerRowId'"))
            assertEquals("Peer Row", scalarText(dbB, "SELECT name FROM users WHERE id = '$peerRowId'"))
            assertEquals("Peer Row", scalarText(observerDb, "SELECT name FROM users WHERE id = '$peerRowId'"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        }
    }

    @Test
    fun conflictingEdits_keepMergedResolver_retriesMergedPayloadAgainstRealServer() = runBlocking {
        withConflictHarness(
            prefix = "conflict-keep-merged",
            resolverB = Resolver { conflict ->
                val serverRow = conflict.serverRow!!.jsonObject
                val localRow = conflict.localPayload!!.jsonObject
                MergeResult.KeepMerged(
                    buildJsonObject {
                        serverRow.forEach { (key, value) -> put(key, value) }
                        put("name", localRow.getValue("name"))
                    }
                )
            },
        ) {
            val rowId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$rowId', 'Grace', 'grace@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET email = 'grace.server@example.com' WHERE id = '$rowId'")
            dbB.execSQL("UPDATE users SET name = 'Grace Client' WHERE id = '$rowId'")

            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Grace Client", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals(
                "grace.server@example.com",
                scalarText(dbB, "SELECT email FROM users WHERE id = '$rowId'")
            )

            observerClient.pullToStable().getOrThrow()
            assertEquals("Grace Client", scalarText(observerDb, "SELECT name FROM users WHERE id = '$rowId'"))
            assertEquals(
                "grace.server@example.com",
                scalarText(observerDb, "SELECT email FROM users WHERE id = '$rowId'")
            )
        }
    }

    @Test
    fun conflictingBundle_clientWinsResolver_preservesSiblingRowsInSameRejectedBundle() = runBlocking {
        withConflictHarness(
            prefix = "conflict-sibling-preservation",
            resolverB = ClientWinsResolver,
        ) {
            val conflictedRowId = randomRowId()
            val siblingRowId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$conflictedRowId', 'Grace', 'grace@example.com')
                """.trimIndent()
            )
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$siblingRowId', 'Ada', 'ada@example.com')
                """.trimIndent()
            )
            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()
            observerClient.pullToStable().getOrThrow()

            dbA.execSQL("UPDATE users SET name = 'Grace Server' WHERE id = '$conflictedRowId'")
            dbB.execSQL("UPDATE users SET name = 'Grace Client' WHERE id = '$conflictedRowId'")
            dbB.execSQL("UPDATE users SET name = 'Ada Client' WHERE id = '$siblingRowId'")

            clientA.pushPending().getOrThrow()
            clientB.pushPending().getOrThrow()

            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals("Grace Client", scalarText(dbB, "SELECT name FROM users WHERE id = '$conflictedRowId'"))
            assertEquals("Ada Client", scalarText(dbB, "SELECT name FROM users WHERE id = '$siblingRowId'"))

            observerClient.pullToStable().getOrThrow()
            assertEquals(
                "Grace Client",
                scalarText(observerDb, "SELECT name FROM users WHERE id = '$conflictedRowId'")
            )
            assertEquals("Ada Client", scalarText(observerDb, "SELECT name FROM users WHERE id = '$siblingRowId'"))
        }
    }
}

private fun parseUpdatedAtMillis(value: String?): Long? {
    if (value == null) return null
    return runCatching { OffsetDateTime.parse(value).toInstant().toEpochMilli() }
        .recoverCatching { OffsetDateTime.parse(value.replace(" ", "T")).toInstant().toEpochMilli() }
        .getOrNull()
}

private data class ConflictHarness(
    val userId: String,
    val deviceA: String,
    val deviceB: String,
    val observerDevice: String,
    val clientA: DefaultOversqliteClient,
    val clientB: DefaultOversqliteClient,
    val observerClient: DefaultOversqliteClient,
    val dbA: SafeSQLiteConnection,
    val dbB: SafeSQLiteConnection,
    val observerDb: SafeSQLiteConnection,
)

private suspend fun withConflictHarness(
    prefix: String,
    resolverB: Resolver,
    block: suspend ConflictHarness.() -> Unit,
) {
    val config = requireRealServerConfig()
    resetRealServerState(config.baseUrl)

    val userId = randomUserId(prefix)
    val deviceA = randomSourceId("$prefix-a")
    val deviceB = randomSourceId("$prefix-b")
    val observerDevice = randomSourceId("$prefix-observer")

    val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
    val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
    val tokenObserver = issueDummySigninToken(config.baseUrl, userId, observerDevice)
    val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
    val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
    val httpObserver = newAuthenticatedHttpClient(config.baseUrl, tokenObserver)
    val dbA = newFileBackedDb()
    val dbB = newFileBackedDb()
    val observerDb = newFileBackedDb()
    try {
        createBusinessSubsetTables(dbA)
        createBusinessSubsetTables(dbB)
        createBusinessSubsetTables(observerDb)

        val clientA = newRealServerClient(dbA, config, httpA)
        val clientB = newRealServerClient(
            dbB,
            config,
            httpB,
            resolver = resolverB,
        )
        val observerClient = newRealServerClient(observerDb, config, httpObserver)

        clientA.openAndAttach(userId).getOrThrow()
        clientB.openAndAttach(userId).getOrThrow()
        observerClient.openAndAttach(userId).getOrThrow()

        ConflictHarness(
            userId = userId,
            deviceA = deviceA,
            deviceB = deviceB,
            observerDevice = observerDevice,
            clientA = clientA,
            clientB = clientB,
            observerClient = observerClient,
            dbA = dbA,
            dbB = dbB,
            observerDb = observerDb,
        ).block()
    } finally {
        httpA.close()
        httpB.close()
        httpObserver.close()
        dbA.close()
        dbB.close()
        observerDb.close()
    }
}
