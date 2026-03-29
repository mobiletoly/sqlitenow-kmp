package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.DetachOutcome
import dev.goquick.sqlitenow.oversqlite.OpenState
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerSyncThenDetachContractTest {
    @Test
    fun syncThenDetach_cleanSuccess_detachesAndClearsLocalState_againstRealServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId("sync-detach-clean-user")
        val sourceId = randomSourceId("sync-detach-clean-source")
        val verifySourceId = randomSourceId("sync-detach-clean-verify")
        val rowUserId = randomRowId()
        val rowPostId = randomRowId()

        val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
        val verifyToken = issueDummySigninToken(config.baseUrl, userId, verifySourceId)
        val http = newAuthenticatedHttpClient(config.baseUrl, token)
        val verifyHttp = newAuthenticatedHttpClient(config.baseUrl, verifyToken)
        val db = newInMemoryDb()
        val verifyDb = newInMemoryDb()
        try {
            createBusinessSubsetTables(db)
            createBusinessSubsetTables(verifyDb)

            val client = newRealServerClient(db, config, http)
            val verifyClient = newRealServerClient(verifyDb, config, verifyHttp)
            try {
                client.openAndAttach(userId, sourceId).getOrThrow()
                insertBusinessUserAndPost(db, rowUserId, rowPostId, "sync-detach-clean")

                val result = client.syncThenDetach().getOrThrow()

                assertTrue(result.isSuccess())
                assertEquals(DetachOutcome.DETACHED, result.detach)
                assertEquals(1, result.syncRounds)
                assertEquals(0L, result.remainingPendingRowCount)
                assertEquals(OpenState.ReadyAnonymous, client.open(sourceId).getOrThrow())
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM posts"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

                verifyClient.openAndAttach(userId, verifySourceId).getOrThrow()
                assertEquals("User sync-detach-clean", scalarText(verifyDb, "SELECT name FROM users WHERE id = '$rowUserId'"))
                assertEquals("Title sync-detach-clean", scalarText(verifyDb, "SELECT title FROM posts WHERE id = '$rowPostId'"))
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
    fun syncThenDetach_retriesWhenFreshWritesArriveDuringFirstRound_andEventuallyDetaches_againstRealServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId("sync-detach-retry-user")
        val sourceId = randomSourceId("sync-detach-retry-source")
        val verifySourceId = randomSourceId("sync-detach-retry-verify")
        val firstUserId = randomRowId()
        val firstPostId = randomRowId()
        val followupUserId = randomRowId()
        val followupPostId = randomRowId()
        var armFollowupWrites = false
        var insertedFollowup = false

        val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
        val verifyToken = issueDummySigninToken(config.baseUrl, userId, verifySourceId)
        val db = newInMemoryDb()
        val verifyDb = newInMemoryDb()
        val http = newAuthenticatedHttpClient(
            baseUrl = config.baseUrl,
            token = token,
            afterResponse = { path ->
                if (armFollowupWrites && path == "/sync/pull" && !insertedFollowup) {
                    insertedFollowup = true
                    insertBusinessUserAndPost(db, followupUserId, followupPostId, "sync-detach-followup")
                }
            },
        )
        val verifyHttp = newAuthenticatedHttpClient(config.baseUrl, verifyToken)
        try {
            createBusinessSubsetTables(db)
            createBusinessSubsetTables(verifyDb)

            val client = newRealServerClient(db, config, http)
            val verifyClient = newRealServerClient(verifyDb, config, verifyHttp)
            try {
                client.openAndAttach(userId, sourceId).getOrThrow()
                insertBusinessUserAndPost(db, firstUserId, firstPostId, "sync-detach-initial")
                armFollowupWrites = true

                val result = client.syncThenDetach().getOrThrow()

                assertTrue(result.isSuccess())
                assertEquals(DetachOutcome.DETACHED, result.detach)
                assertEquals(2, result.syncRounds)
                assertEquals(0L, result.remainingPendingRowCount)
                assertTrue(insertedFollowup)
                assertEquals(OpenState.ReadyAnonymous, client.open(sourceId).getOrThrow())
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

                verifyClient.openAndAttach(userId, verifySourceId).getOrThrow()
                assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM users"))
                assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM posts"))
                assertEquals("User sync-detach-initial", scalarText(verifyDb, "SELECT name FROM users WHERE id = '$firstUserId'"))
                assertEquals("User sync-detach-followup", scalarText(verifyDb, "SELECT name FROM users WHERE id = '$followupUserId'"))
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
    fun syncThenDetach_returnsBlockedWhenFreshWritesKeepArriving_againstRealServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId("sync-detach-blocked-user")
        val sourceId = randomSourceId("sync-detach-blocked-source")
        val initialUserId = randomRowId()
        val initialPostId = randomRowId()
        var armFollowupWrites = false
        var pullResponses = 0

        val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
        val db = newInMemoryDb()
        val http = newAuthenticatedHttpClient(
            baseUrl = config.baseUrl,
            token = token,
            afterResponse = { path ->
                if (armFollowupWrites && path == "/sync/pull" && pullResponses < 2) {
                    pullResponses += 1
                    insertBusinessUserAndPost(
                        db = db,
                        userId = randomRowId(),
                        postId = randomRowId(),
                        suffix = "sync-detach-late-$pullResponses",
                    )
                }
            },
        )
        try {
            createBusinessSubsetTables(db)

            val client = newRealServerClient(db, config, http)
            try {
                client.openAndAttach(userId, sourceId).getOrThrow()
                insertBusinessUserAndPost(db, initialUserId, initialPostId, "sync-detach-blocked-initial")
                armFollowupWrites = true

                val result = client.syncThenDetach().getOrThrow()

                assertFalse(result.isSuccess())
                assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, result.detach)
                assertEquals(2, result.syncRounds)
                assertEquals(2L, result.remainingPendingRowCount)
                assertTrue(client.syncStatus().getOrThrow().pending.blocksDetach)
                assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
                assertEquals(userId, scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
                assertEquals(OpenState.ReadyAttached(userId), client.open(sourceId).getOrThrow())
                client.attach(userId).getOrThrow()
                assertTrue(client.syncStatus().getOrThrow().pending.blocksDetach)
            } finally {
                client.close()
            }
        } finally {
            http.close()
            db.close()
        }
    }

    @Test
    fun syncThenDetach_syncFailure_doesNotImplicitlyDetach_againstRealServer() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId("sync-detach-failure-user")
        val sourceId = randomSourceId("sync-detach-failure-source")
        val verifySourceId = randomSourceId("sync-detach-failure-verify")
        val rowUserId = randomRowId()
        val rowPostId = randomRowId()
        var failPull = false

        val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
        val verifyToken = issueDummySigninToken(config.baseUrl, userId, verifySourceId)
        val db = newInMemoryDb()
        val verifyDb = newInMemoryDb()
        val http = newAuthenticatedHttpClient(
            baseUrl = config.baseUrl,
            token = token,
            overrideResponse = { path ->
                if (failPull && path == "/sync/pull") {
                    InterceptedHttpResponse(
                        statusCode = 500,
                        body = """{"error":"pull_failed","message":"simulated pull failure"}""",
                    )
                } else {
                    null
                }
            },
        )
        val verifyHttp = newAuthenticatedHttpClient(config.baseUrl, verifyToken)
        try {
            createBusinessSubsetTables(db)
            createBusinessSubsetTables(verifyDb)

            val client = newRealServerClient(db, config, http)
            val verifyClient = newRealServerClient(verifyDb, config, verifyHttp)
            try {
                client.openAndAttach(userId, sourceId).getOrThrow()
                insertBusinessUserAndPost(db, rowUserId, rowPostId, "sync-detach-failure")
                failPull = true

                val error = client.syncThenDetach().exceptionOrNull()

                assertNotNull(error)
                assertEquals(OpenState.ReadyAttached(userId), client.open(sourceId).getOrThrow())
                assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
                assertEquals(userId, scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
                assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM users"))
                assertEquals(1L, scalarLong(db, "SELECT COUNT(*) FROM posts"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows"))

                verifyClient.openAndAttach(userId, verifySourceId).getOrThrow()
                assertEquals("User sync-detach-failure", scalarText(verifyDb, "SELECT name FROM users WHERE id = '$rowUserId'"))
                assertEquals("Title sync-detach-failure", scalarText(verifyDb, "SELECT title FROM posts WHERE id = '$rowPostId'"))
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
}
