package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.AttachOutcome
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.AuthorityStatus
import dev.goquick.sqlitenow.oversqlite.DetachOutcome
import io.ktor.client.HttpClient
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerLifecycleContractTest {
    @Test
    fun detachAndReattachSameUser_preservesSourceContinuity_workAgainstRealServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId("same-install-user")
        val db = newInMemoryDb()
        val verifyDb = newInMemoryDb()
        createBusinessSubsetTables(db)
        createBusinessSubsetTables(verifyDb)
        val sourceId = bootstrapManagedSourceId(db, config)
        val verifySourceId = bootstrapManagedSourceId(verifyDb, config)
        val firstUserId = randomRowId()
        val firstPostId = randomRowId()
        val secondUserId = randomRowId()
        val secondPostId = randomRowId()

        val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
        val verifyToken = issueDummySigninToken(config.baseUrl, userId, verifySourceId)
        val http = newAuthenticatedHttpClient(config.baseUrl, token)
        val verifyHttp = newAuthenticatedHttpClient(config.baseUrl, verifyToken)
        try {
            val client = newRealServerClient(db, config, http)
            val verifyClient = newRealServerClient(verifyDb, config, verifyHttp)
            try {
                client.openAndAttach(userId).getOrThrow()
                val currentSourceId = client.sourceInfo().getOrThrow().currentSourceId
                insertBusinessUserAndPost(db, firstUserId, firstPostId, "same-install-first")
                client.pushPending().getOrThrow()
                assertEquals(
                    2L,
                    scalarLong(
                        db,
                        "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$currentSourceId'",
                    ),
                )

                assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
                client.open().getOrThrow()

                client.attach(userId).getOrThrow()
                assertEquals(currentSourceId, client.sourceInfo().getOrThrow().currentSourceId)
                assertEquals(
                    2L,
                    scalarLong(
                        db,
                        "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$currentSourceId'",
                    ),
                )
                assertEquals(
                    "User same-install-first",
                    scalarText(db, "SELECT name FROM users WHERE id = '$firstUserId'"),
                )

                insertBusinessUserAndPost(db, secondUserId, secondPostId, "same-install-second")
                client.pushPending().getOrThrow()
                assertEquals(
                    3L,
                    scalarLong(
                        db,
                        "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$currentSourceId'",
                    ),
                )

                verifyClient.openAndAttach(userId).getOrThrow()
                assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM users"))
                assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM posts"))
                assertEquals(
                    "User same-install-first",
                    scalarText(verifyDb, "SELECT name FROM users WHERE id = '$firstUserId'"),
                )
                assertEquals(
                    "User same-install-second",
                    scalarText(verifyDb, "SELECT name FROM users WHERE id = '$secondUserId'"),
                )
            } finally {
                client.close()
                verifyClient.close()
            }
        } finally {
            http.close()
            verifyHttp.close()
            db.close()
            verifyDb.close()
        }
    }

    @Test
    fun openAttachDetachAndDifferentUserAttach_workAgainstRealServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId("lifecycle-user")
        val otherUserId = randomUserId("lifecycle-other-user")
        val sourceIdA = randomSourceId("lifecycle-a")
        val sourceIdB = randomSourceId("lifecycle-b")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, sourceIdA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, sourceIdB)
        val tokenOther = issueDummySigninToken(config.baseUrl, otherUserId, sourceIdB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val httpOther = newAuthenticatedHttpClient(config.baseUrl, tokenOther)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val clientA = newRealServerClient(dbA, config, httpA)
            val clientB = newRealServerClient(dbB, config, httpB)
            val otherClient = newRealServerClient(dbB, config, httpOther)
            try {
                clientA.open().getOrThrow()
                assertAttachedOutcome(
                    expectedOutcome = AttachOutcome.STARTED_EMPTY,
                    actual = clientA.attach(userId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
                clientB.open().getOrThrow()
                assertAttachedOutcome(
                    expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                    actual = clientB.attach(userId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
                assertFalse(clientB.syncStatus().getOrThrow().pending.hasPendingSyncData)

                val authorId = randomRowId()
                val postId = randomRowId()
                dbA.execSQL(
                    """
                    INSERT INTO users(id, name, email)
                    VALUES('$authorId', 'Android A', 'android-a@example.com')
                    """.trimIndent(),
                )
                dbA.execSQL(
                    """
                    INSERT INTO posts(id, title, content, author_id)
                    VALUES('$postId', 'Android contract', 'android payload', '$authorId')
                    """.trimIndent(),
                )

                clientA.pushPending().getOrThrow()
                clientB.pullToStable().getOrThrow()
                assertEquals("Android A", scalarText(dbB, "SELECT name FROM users WHERE id = '$authorId'"))
                assertEquals("android payload", scalarText(dbB, "SELECT content FROM posts WHERE id = '$postId'"))

                assertEquals(DetachOutcome.DETACHED, clientB.detach().getOrThrow())
                clientB.open().getOrThrow()

                otherClient.open().getOrThrow()
                assertAttachedOutcome(
                    expectedOutcome = AttachOutcome.STARTED_EMPTY,
                    actual = otherClient.attach(otherUserId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
            } finally {
                clientA.close()
                clientB.close()
                otherClient.close()
            }
        } finally {
            httpA.close()
            httpB.close()
            httpOther.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun attachRetryLater_andFreshInstallRecovery_workAgainstRealServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId("recovery-user")
        val sourceIdA = randomSourceId("recovery-a")
        val sourceIdB = randomSourceId("recovery-b")
        val rotatedSourceId = randomSourceId("recovery-rotated")

        val tokenA = issueDummySigninToken(config.baseUrl, userId, sourceIdA)
        val tokenB = issueDummySigninToken(config.baseUrl, userId, sourceIdB)
        val httpA = newAuthenticatedHttpClient(config.baseUrl, tokenA)
        val httpB = newAuthenticatedHttpClient(config.baseUrl, tokenB)
        val dbA = newInMemoryDb()
        val dbB = newInMemoryDb()
        var resetHttp: HttpClient? = null
        var resetDb = newInMemoryDb()
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val seededUserId = randomRowId()
            val seededPostId = randomRowId()
            dbA.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$seededUserId', 'Recovery Seed', 'recovery-seed@example.com')
                """.trimIndent(),
            )
            dbA.execSQL(
                """
                INSERT INTO posts(id, title, content, author_id)
                VALUES('$seededPostId', 'Recovery Post', 'recovery payload', '$seededUserId')
                """.trimIndent(),
            )

            val clientA = newRealServerClient(dbA, config, httpA)
            val clientB = newRealServerClient(dbB, config, httpB)
            try {
                clientA.open().getOrThrow()
                assertAttachedOutcome(
                    expectedOutcome = AttachOutcome.SEEDED_FROM_LOCAL,
                    actual = clientA.attach(userId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.PENDING_LOCAL_SEED,
                )
                val pending = clientA.syncStatus().getOrThrow().pending
                assertTrue(pending.hasPendingSyncData)
                assertTrue(pending.pendingRowCount > 0)
                assertTrue(pending.blocksDetach)
                assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, clientA.detach().getOrThrow())

                clientB.open().getOrThrow()
                val retry = clientB.attach(userId).getOrThrow()
                assertTrue(retry is AttachResult.RetryLater && retry.retryAfterSeconds > 0)

                clientA.pushPending().getOrThrow()
                assertAttachedOutcome(
                    expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                    actual = clientB.attach(userId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_MATERIALIZED,
                )
                assertEquals("Recovery Seed", scalarText(dbB, "SELECT name FROM users WHERE id = '$seededUserId'"))

                createBusinessSubsetTables(resetDb)
                resetHttp = newAuthenticatedHttpClient(
                    config.baseUrl,
                    issueDummySigninToken(config.baseUrl, userId, rotatedSourceId),
                )
                val resetClient = newRealServerClient(resetDb, config, checkNotNull(resetHttp))
                try {
                    resetClient.open().getOrThrow()
                    assertAttachedOutcome(
                        expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                        actual = resetClient.attach(userId).getOrThrow(),
                        expectedAuthority = AuthorityStatus.AUTHORITATIVE_MATERIALIZED,
                    )
                    assertEquals("Recovery Seed", scalarText(resetDb, "SELECT name FROM users WHERE id = '$seededUserId'"))
                } finally {
                    resetClient.close()
                }
            } finally {
                clientA.close()
                clientB.close()
            }
        } finally {
            httpA.close()
            httpB.close()
            resetHttp?.close()
            dbA.close()
            dbB.close()
            resetDb.close()
        }
    }

    private fun assertAttachedOutcome(
        expectedOutcome: AttachOutcome,
        actual: AttachResult,
        expectedAuthority: AuthorityStatus,
    ) {
        if (actual !is AttachResult.Connected) {
            fail("Expected AttachResult.Connected($expectedOutcome), got $actual")
        }
        val connected = actual as AttachResult.Connected
        assertEquals(expectedOutcome, connected.outcome)
        assertEquals(expectedAuthority, connected.status.authority)
    }
}
