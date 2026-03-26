package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.OversqliteConfig
import dev.goquick.sqlitenow.oversqlite.SyncTable
import dev.goquick.sqlitenow.oversqlite.e2e.generated.RealServerGeneratedDatabase
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerGeneratedConfigTest {
    @Test
    fun generatedOversqliteConfig_pushPullConvergesAgainstRealServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val deviceA = randomDeviceId("generated-a")
        val deviceB = randomDeviceId("generated-b")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, deviceA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, deviceB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val clientA = newRealServerClient(
                db = dbA,
                http = httpA,
                oversqliteConfig = OversqliteConfig(
                    schema = config.schema,
                    syncTables = RealServerGeneratedDatabase.syncTables,
                ),
            )
            val clientB = newRealServerClient(
                db = dbB,
                http = httpB,
                oversqliteConfig = OversqliteConfig(
                    schema = config.schema,
                    syncTables = RealServerGeneratedDatabase.syncTables,
                ),
            )

            clientA.bootstrap(userId, deviceA).getOrThrow()
            clientB.bootstrap(userId, deviceB).getOrThrow()

            val authorId = randomRowId()
            val postId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$authorId', 'Generated Ada', 'generated-ada@example.com')
                """.trimIndent()
            )
            dbA.execSQL(
                """
                INSERT INTO posts(id, title, content, author_id)
                VALUES('$postId', 'Generated Round', 'from generated config', '$authorId')
                """.trimIndent()
            )

            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            assertEquals("Generated Ada", scalarText(dbB, "SELECT name FROM users WHERE id = '$authorId'"))
            assertEquals("Generated Round", scalarText(dbB, "SELECT title FROM posts WHERE id = '$postId'"))
            assertEquals(0L, scalarLong(dbA, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(dbB, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            httpA.close()
            httpB.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun unsupportedIntegerPrimaryKeyFailsBootstrapBeforeSyncMutation() = runBlocking {
        val config = requireRealServerConfig()
        val http = newAuthenticatedHttpClient(config.baseUrl, "unused-token")
        val db = newInMemoryDb()
        try {
            createBusinessIntegerKeyTables(db)

            val client = newRealServerClient(
                db = db,
                config = config,
                http = http,
                syncTables = listOf(SyncTable("users", syncKeyColumnName = "id")),
            )

            val error = client.bootstrap(randomUserId(), randomDeviceId("invalid-key")).exceptionOrNull()
            assertTrue(error?.message?.contains("TEXT PRIMARY KEY or BLOB PRIMARY KEY") == true)
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            http.close()
            db.close()
        }
    }
}
