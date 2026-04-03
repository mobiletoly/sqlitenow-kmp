package dev.goquick.sqlitenow.oversqlite

import io.ktor.client.call.body
import kotlinx.coroutines.test.runTest
import io.ktor.client.HttpClient
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

internal class RealServerComprehensiveTest : RealServerSupport() {
    @Test
    fun openConnectPushPullAndFreshAttach_workAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("realserver-user")
        val dbA = newDb()
        val dbB = newDb()
        val dbC = newDb()
        var httpA: HttpClient? = null
        var httpB: HttpClient? = null
        var httpC: HttpClient? = null
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)
            createBusinessSubsetTables(dbC)

            val sourceIdA = bootstrapManagedSourceId(dbA, config.baseUrl)
            val sourceIdB = bootstrapManagedSourceId(dbB, config.baseUrl)
            val sourceIdC = bootstrapManagedSourceId(dbC, config.baseUrl)
            val tokenA = issueDummySigninToken(config.baseUrl, userId, sourceIdA)
            val tokenB = issueDummySigninToken(config.baseUrl, userId, sourceIdB)
            val tokenC = issueDummySigninToken(config.baseUrl, userId, sourceIdC)
            httpA = newRealServerHttpClient(config.baseUrl, tokenA)
            httpB = newRealServerHttpClient(config.baseUrl, tokenB)
            httpC = newRealServerHttpClient(config.baseUrl, tokenC)
            val clientA = newRealServerClient(dbA, httpA)
            val clientB = newRealServerClient(dbB, httpB)
            val clientC = newRealServerClient(dbC, httpC)

            clientA.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = clientA.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            clientB.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = clientB.attach(userId).getOrThrow(),
            )

            val rowUserId = "11111111-1111-1111-1111-111111111111"
            val rowPostId = "22222222-2222-2222-2222-222222222222"
            insertBusinessUserAndPost(dbA, rowUserId, rowPostId, "real-server-smoke")

            clientA.pushPending().getOrThrow()
            clientB.pullToStable().getOrThrow()

            clientC.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = clientC.attach(userId).getOrThrow(),
            )

            assertEquals(1L, clientA.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1L, clientB.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals(1L, clientC.syncStatus().getOrThrow().lastBundleSeqSeen)
            assertEquals("User real-server-smoke", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowUserId'"))
            assertEquals("Title real-server-smoke", scalarText(dbB, "SELECT title FROM posts WHERE id = '$rowPostId'"))
            assertEquals("User real-server-smoke", scalarText(dbC, "SELECT name FROM users WHERE id = '$rowUserId'"))
            assertEquals("Payload real-server-smoke", scalarText(dbC, "SELECT content FROM posts WHERE id = '$rowPostId'"))
            assertFalse(clientA.syncStatus().getOrThrow().pending.hasPendingSyncData)
            assertFalse(clientB.syncStatus().getOrThrow().pending.hasPendingSyncData)
            assertFalse(clientC.syncStatus().getOrThrow().pending.hasPendingSyncData)
        } finally {
            httpA?.close()
            httpB?.close()
            httpC?.close()
            dbA.close()
            dbB.close()
            dbC.close()
        }
    }

    @Test
    fun connectRetryLater_pendingSyncStatus_andDetachLifecycle_workAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("lease-user")
        val dbA = newDb()
        val dbB = newDb()
        var httpA: HttpClient? = null
        var httpB: HttpClient? = null
        try {
            createBusinessSubsetTables(dbA)
            createBusinessSubsetTables(dbB)

            val sourceIdA = bootstrapManagedSourceId(dbA, config.baseUrl)
            val sourceIdB = bootstrapManagedSourceId(dbB, config.baseUrl)
            val tokenA = issueDummySigninToken(config.baseUrl, userId, sourceIdA)
            val tokenB = issueDummySigninToken(config.baseUrl, userId, sourceIdB)
            httpA = newRealServerHttpClient(config.baseUrl, tokenA)
            httpB = newRealServerHttpClient(config.baseUrl, tokenB)
            val rowUserId = "33333333-3333-3333-3333-333333333333"
            val rowPostId = "44444444-4444-4444-4444-444444444444"
            insertBusinessUserAndPost(dbA, rowUserId, rowPostId, "lease-real-server")

            val clientA = newRealServerClient(dbA, httpA)
            val clientB = newRealServerClient(dbB, httpB)

            clientA.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.SEEDED_FROM_LOCAL,
                actual = clientA.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.PENDING_LOCAL_SEED,
            )

            val dirtyStatus = clientA.syncStatus().getOrThrow().pending
            assertTrue(dirtyStatus.hasPendingSyncData)
            assertTrue(dirtyStatus.pendingRowCount > 0)
            assertTrue(dirtyStatus.blocksDetach)
            assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, clientA.detach().getOrThrow())

            clientB.open().getOrThrow()
            val retry = clientB.attach(userId).getOrThrow()
            assertIs<AttachResult.RetryLater>(retry)
            assertTrue(retry.retryAfterSeconds > 0)

            clientA.pushPending().getOrThrow()
            val cleanStatus = clientA.syncStatus().getOrThrow().pending
            assertFalse(cleanStatus.hasPendingSyncData)
            assertFalse(cleanStatus.blocksDetach)

            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = clientB.attach(userId).getOrThrow(),
            )
            assertEquals("User lease-real-server", scalarText(dbB, "SELECT name FROM users WHERE id = '$rowUserId'"))
            assertEquals("Title lease-real-server", scalarText(dbB, "SELECT title FROM posts WHERE id = '$rowPostId'"))

            assertEquals(DetachOutcome.DETACHED, clientA.detach().getOrThrow())
            clientA.open().getOrThrow()
        } finally {
            httpA?.close()
            httpB?.close()
            dbA.close()
            dbB.close()
        }
    }

    @Test
    fun syncThenDetach_flushesAndDetaches_workAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("sync-detach-user")
        val rowUserId = "12121212-1212-1212-1212-121212121212"
        val rowPostId = "34343434-3434-3434-3434-343434343434"
        val db = newDb()
        val verifyDb = newDb()
        var http: HttpClient? = null
        var verifyHttp: HttpClient? = null
        try {
            createBusinessSubsetTables(db)
            createBusinessSubsetTables(verifyDb)

            val sourceId = bootstrapManagedSourceId(db, config.baseUrl)
            val verifySourceId = bootstrapManagedSourceId(verifyDb, config.baseUrl)
            val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
            val verifyToken = issueDummySigninToken(config.baseUrl, userId, verifySourceId)
            http = newRealServerHttpClient(config.baseUrl, token)
            verifyHttp = newRealServerHttpClient(config.baseUrl, verifyToken)
            val client = newRealServerClient(db, http)
            val verifyClient = newRealServerClient(verifyDb, verifyHttp)

            client.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = client.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            insertBusinessUserAndPost(db, rowUserId, rowPostId, "sync-then-detach")

            val result = client.syncThenDetach().getOrThrow()

            assertTrue(result.isSuccess())
            assertEquals(DetachOutcome.DETACHED, result.detach)
            assertEquals(1, result.syncRounds)
            assertEquals(0L, result.remainingPendingRowCount)
            client.open().getOrThrow()
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users"))
            assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM posts"))

            verifyClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = verifyClient.attach(userId).getOrThrow(),
            )
            assertEquals("User sync-then-detach", scalarText(verifyDb, "SELECT name FROM users WHERE id = '$rowUserId'"))
            assertEquals("Title sync-then-detach", scalarText(verifyDb, "SELECT title FROM posts WHERE id = '$rowPostId'"))
        } finally {
            http?.close()
            verifyHttp?.close()
            db.close()
            verifyDb.close()
        }
    }

    @Test
    fun syncThenDetach_returnsBlockedWhenFreshWritesKeepArriving_againstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("blocked-detach-user")
        val initialUserId = "56565656-5656-5656-5656-565656565656"
        val initialPostId = "78787878-7878-7878-7878-787878787878"
        val db = newDb()
        var http: HttpClient? = null
        try {
            createBusinessSubsetTables(db)

            val sourceId = bootstrapManagedSourceId(db, config.baseUrl)
            val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
            var pullResponses = 0
            http = newRealServerHttpClient(config.baseUrl, token) { path ->
                if (path == "/sync/pull" && pullResponses < 2) {
                    pullResponses += 1
                    insertBusinessUserAndPost(
                        db = db,
                        userId = randomRealServerUuid(),
                        postId = randomRealServerUuid(),
                        suffix = "late-$pullResponses",
                    )
                }
            }
            val client = newRealServerClient(db, http)

            client.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = client.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )
            insertBusinessUserAndPost(db, initialUserId, initialPostId, "blocked-detach-initial")

            val result = client.syncThenDetach().getOrThrow()

            assertFalse(result.isSuccess())
            assertEquals(DetachOutcome.BLOCKED_UNSYNCED_DATA, result.detach)
            assertEquals(2, result.syncRounds)
            assertEquals(2L, result.remainingPendingRowCount)
            assertTrue(client.syncStatus().getOrThrow().pending.blocksDetach)
            assertEquals("attached", scalarText(db, "SELECT binding_state FROM _sync_attachment_state"))
            assertEquals(userId, scalarText(db, "SELECT attached_user_id FROM _sync_attachment_state"))
            client.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                actual = client.attach(userId).getOrThrow(),
            )
            assertTrue(client.syncStatus().getOrThrow().pending.blocksDetach)
        } finally {
            http?.close()
            db.close()
        }
    }

    @Test
    fun openIsLocalOnly_andSameUserOfflineResume_workAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("resume-user")

        val offlineHttp = newRealServerHttpClient("http://127.0.0.1:1")
        val offlineDb = newDb()
        val attachedDb = newDb()
        try {
            createBusinessSubsetTables(offlineDb)
            createBusinessSubsetTables(attachedDb)
            val offlineSourceId = bootstrapManagedSourceId(offlineDb, config.baseUrl)
            val attachedSourceId = bootstrapManagedSourceId(attachedDb, config.baseUrl)

            val offlineFreshClient = newRealServerClient(
                db = offlineDb,
                http = offlineHttp,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            offlineFreshClient.open().getOrThrow()
            assertNotNull(offlineFreshClient.attach(userId).exceptionOrNull())

            val token = issueDummySigninToken(config.baseUrl, userId, attachedSourceId)
            val onlineHttp = newRealServerHttpClient(config.baseUrl, token)
            val onlineClient = newRealServerClient(attachedDb, onlineHttp)
            try {
                onlineClient.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.STARTED_EMPTY,
                    actual = onlineClient.attach(userId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
            } finally {
                onlineClient.close()
                onlineHttp.close()
            }

            val offlineResumeHttp = newRealServerHttpClient("http://127.0.0.1:1")
            val offlineResumeClient = newRealServerClient(
                db = attachedDb,
                http = offlineResumeHttp,
                transientRetryPolicy = OversqliteTransientRetryPolicy(maxAttempts = 1),
            )
            try {
                offlineResumeClient.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                    actual = offlineResumeClient.attach(userId).getOrThrow(),
                )
            } finally {
                offlineResumeClient.close()
                offlineResumeHttp.close()
            }
        } finally {
            offlineHttp.close()
            offlineDb.close()
            attachedDb.close()
        }
    }

    @Test
    fun sameInstall_canAlternateUsersAndSeeCorrectRemoteDataAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("source-user")
        val otherUserId = randomRealServerId("source-other-user")
        val firstUserRowId = "99999999-9999-9999-9999-999999999999"
        val firstPostRowId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        val secondUserRowId = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        val secondPostRowId = "cccccccc-cccc-cccc-cccc-cccccccccccc"
        val db = newDb()
        try {
            createBusinessSubsetTables(db)

            val initialSourceId = bootstrapManagedSourceId(db, config.baseUrl)

            val tokenA = issueDummySigninToken(config.baseUrl, userId, initialSourceId)
            val httpA = newRealServerHttpClient(config.baseUrl, tokenA)
            val clientA = newRealServerClient(db, httpA)
            try {
                clientA.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.STARTED_EMPTY,
                    actual = clientA.attach(userId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
                insertBusinessUserAndPost(db, firstUserRowId, firstPostRowId, "same-install-user-a")
                clientA.pushPending().getOrThrow()
                assertEquals(DetachOutcome.DETACHED, clientA.detach().getOrThrow())
            } finally {
                clientA.close()
                httpA.close()
            }

            val sourceAfterUserA = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            assertNotEquals(initialSourceId, sourceAfterUserA)

            val tokenB = issueDummySigninToken(config.baseUrl, otherUserId, sourceAfterUserA)
            val httpB = newRealServerHttpClient(config.baseUrl, tokenB)
            val clientB = newRealServerClient(db, httpB)
            try {
                clientB.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.STARTED_EMPTY,
                    actual = clientB.attach(otherUserId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = '$firstUserRowId'"))
                insertBusinessUserAndPost(db, secondUserRowId, secondPostRowId, "same-install-user-b")
                clientB.pushPending().getOrThrow()
                assertEquals(DetachOutcome.DETACHED, clientB.detach().getOrThrow())
            } finally {
                clientB.close()
                httpB.close()
            }

            val sourceAfterUserB = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            assertNotEquals(sourceAfterUserA, sourceAfterUserB)

            val verifyHttpA = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, userId, sourceAfterUserB))
            val verifyClientA = newRealServerClient(db, verifyHttpA)
            try {
                verifyClientA.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                    actual = verifyClientA.attach(userId).getOrThrow(),
                )
                assertEquals("User same-install-user-a", scalarText(db, "SELECT name FROM users WHERE id = '$firstUserRowId'"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = '$secondUserRowId'"))
                assertEquals(DetachOutcome.DETACHED, verifyClientA.detach().getOrThrow())
            } finally {
                verifyClientA.close()
                verifyHttpA.close()
            }

            val verifyHttpB = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, otherUserId, sourceAfterUserB))
            val verifyClientB = newRealServerClient(db, verifyHttpB)
            try {
                verifyClientB.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                    actual = verifyClientB.attach(otherUserId).getOrThrow(),
                )
                assertEquals("User same-install-user-b", scalarText(db, "SELECT name FROM users WHERE id = '$secondUserRowId'"))
                assertEquals(0L, scalarLong(db, "SELECT COUNT(*) FROM users WHERE id = '$firstUserRowId'"))
            } finally {
                verifyClientB.close()
                verifyHttpB.close()
            }
        } finally {
            db.close()
        }
    }

    @Test
    fun sameInstall_heavyMultiChunkAlternationAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("heavy-user-a")
        val otherUserId = randomRealServerId("heavy-user-b")
        val db = newDb()
        val verifyDbA = newDb()
        val verifyDbB = newDb()
        try {
            createBusinessSubsetTables(db)
            createBusinessSubsetTables(verifyDbA)
            createBusinessSubsetTables(verifyDbB)

            val initialSourceId = bootstrapManagedSourceId(db, config.baseUrl)
            val verifySourceIdA = bootstrapManagedSourceId(verifyDbA, config.baseUrl)
            val verifySourceIdB = bootstrapManagedSourceId(verifyDbB, config.baseUrl)
            val userARowIds = List(3) { randomRealServerUuid() }
            val userAPostIds = List(3) { randomRealServerUuid() }
            val userBRowIds = List(3) { randomRealServerUuid() }
            val userBPostIds = List(3) { randomRealServerUuid() }

            val httpA = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, userId, initialSourceId))
            val clientA = newRealServerClient(db, httpA, uploadLimit = 1, downloadLimit = 1, snapshotChunkRows = 2)
            try {
                clientA.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.STARTED_EMPTY,
                    actual = clientA.attach(userId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
                repeat(3) { index ->
                    insertBusinessUserAndPost(
                        db = db,
                        userId = userARowIds[index],
                        postId = userAPostIds[index],
                        suffix = "heavy-a-$index",
                    )
                    clientA.pushPending().getOrThrow()
                }
                assertEquals(4L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$initialSourceId'"))
                assertEquals(DetachOutcome.DETACHED, clientA.detach().getOrThrow())
            } finally {
                clientA.close()
                httpA.close()
            }

            val sourceAfterUserA = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            assertNotEquals(initialSourceId, sourceAfterUserA)

            val httpB = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, otherUserId, sourceAfterUserA))
            val clientB = newRealServerClient(db, httpB, uploadLimit = 1, downloadLimit = 1, snapshotChunkRows = 2)
            try {
                clientB.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.STARTED_EMPTY,
                    actual = clientB.attach(otherUserId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
                repeat(3) { index ->
                    insertBusinessUserAndPost(
                        db = db,
                        userId = userBRowIds[index],
                        postId = userBPostIds[index],
                        suffix = "heavy-b-$index",
                    )
                    clientB.pushPending().getOrThrow()
                }
                assertEquals(4L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceAfterUserA'"))
                assertEquals(DetachOutcome.DETACHED, clientB.detach().getOrThrow())
            } finally {
                clientB.close()
                httpB.close()
            }

            val verifyHttpA = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, userId, verifySourceIdA))
            val verifyClientA = newRealServerClient(verifyDbA, verifyHttpA, uploadLimit = 1, downloadLimit = 1, snapshotChunkRows = 2)
            try {
                verifyClientA.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                    actual = verifyClientA.attach(userId).getOrThrow(),
                )
                repeat(3) { index ->
                    assertEquals(
                        "User heavy-a-$index",
                        scalarText(verifyDbA, "SELECT name FROM users WHERE id = '${userARowIds[index]}'"),
                    )
                    assertEquals(
                        0L,
                        scalarLong(verifyDbA, "SELECT COUNT(*) FROM users WHERE id = '${userBRowIds[index]}'"),
                    )
                }
            } finally {
                verifyClientA.close()
                verifyHttpA.close()
            }

            val verifyHttpB = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, otherUserId, verifySourceIdB))
            val verifyClientB = newRealServerClient(verifyDbB, verifyHttpB, uploadLimit = 1, downloadLimit = 1, snapshotChunkRows = 2)
            try {
                verifyClientB.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                    actual = verifyClientB.attach(otherUserId).getOrThrow(),
                )
                repeat(3) { index ->
                    assertEquals(
                        "User heavy-b-$index",
                        scalarText(verifyDbB, "SELECT name FROM users WHERE id = '${userBRowIds[index]}'"),
                    )
                    assertEquals(
                        0L,
                        scalarLong(verifyDbB, "SELECT COUNT(*) FROM users WHERE id = '${userARowIds[index]}'"),
                    )
                }
            } finally {
                verifyClientB.close()
                verifyHttpB.close()
            }
        } finally {
            db.close()
            verifyDbA.close()
            verifyDbB.close()
        }
    }

    @Test
    fun sameInstall_localSeedThenRemoteAuthoritativeRestore_usesFreshSourceAfterDetachAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val seedUserId = randomRealServerId("seed-local-user")
        val restoredUserId = randomRealServerId("restore-remote-user")
        val installSeedUserRowId = "d1d1d1d1-d1d1-d1d1-d1d1-d1d1d1d1d1d1"
        val installSeedPostRowId = "e2e2e2e2-e2e2-e2e2-e2e2-e2e2e2e2e2e2"
        val remoteSeedUserRowId = "f3f3f3f3-f3f3-f3f3-f3f3-f3f3f3f3f3f3"
        val remoteSeedPostRowId = "a4a4a4a4-a4a4-a4a4-a4a4-a4a4a4a4a4a4"
        val followupUserRowId = "b5b5b5b5-b5b5-b5b5-b5b5-b5b5b5b5b5b5"
        val followupPostRowId = "c6c6c6c6-c6c6-c6c6-c6c6-c6c6c6c6c6c6"
        val installDb = newDb()
        val remoteSeedDb = newDb()
        val verifyDb = newDb()
        var installHttpSeed: HttpClient? = null
        var remoteSeedHttp: HttpClient? = null
        var installHttpRestore: HttpClient? = null
        var verifyHttp: HttpClient? = null
        try {
            createBusinessSubsetTables(installDb)
            createBusinessSubsetTables(remoteSeedDb)
            createBusinessSubsetTables(verifyDb)

            val installSeedSourceId = bootstrapManagedSourceId(installDb, config.baseUrl)
            val remoteSeedSourceId = bootstrapManagedSourceId(remoteSeedDb, config.baseUrl)
            val verifySourceId = bootstrapManagedSourceId(verifyDb, config.baseUrl)

            installHttpSeed = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, seedUserId, installSeedSourceId),
            )
            val installSeedClient = newRealServerClient(installDb, installHttpSeed)
            try {
                insertBusinessUserAndPost(installDb, installSeedUserRowId, installSeedPostRowId, "install-local-seed")
                installSeedClient.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.SEEDED_FROM_LOCAL,
                    actual = installSeedClient.attach(seedUserId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.PENDING_LOCAL_SEED,
                )
                installSeedClient.pushPending().getOrThrow()
                assertEquals(2L, scalarLong(installDb, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$installSeedSourceId'"))
                assertEquals(DetachOutcome.DETACHED, installSeedClient.detach().getOrThrow())
            } finally {
                installSeedClient.close()
                installHttpSeed?.close()
            }

            val installRestoreSourceId = scalarText(installDb, "SELECT current_source_id FROM _sync_attachment_state")
            assertNotEquals(installSeedSourceId, installRestoreSourceId)

            remoteSeedHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, restoredUserId, remoteSeedSourceId),
            )
            val remoteSeedClient = newRealServerClient(remoteSeedDb, remoteSeedHttp)
            try {
                remoteSeedClient.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.STARTED_EMPTY,
                    actual = remoteSeedClient.attach(restoredUserId).getOrThrow(),
                    expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
                )
                insertBusinessUserAndPost(remoteSeedDb, remoteSeedUserRowId, remoteSeedPostRowId, "remote-authoritative-seed")
                remoteSeedClient.pushPending().getOrThrow()
                assertEquals(DetachOutcome.DETACHED, remoteSeedClient.detach().getOrThrow())
            } finally {
                remoteSeedClient.close()
                remoteSeedHttp?.close()
            }

            installHttpRestore = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, restoredUserId, installRestoreSourceId),
            )
            val installRestoreClient = newRealServerClient(installDb, installHttpRestore)
            try {
                installRestoreClient.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                    actual = installRestoreClient.attach(restoredUserId).getOrThrow(),
                )
                assertEquals(0L, scalarLong(installDb, "SELECT COUNT(*) FROM users WHERE id = '$installSeedUserRowId'"))
                assertEquals("User remote-authoritative-seed", scalarText(installDb, "SELECT name FROM users WHERE id = '$remoteSeedUserRowId'"))
                assertEquals(1L, scalarLong(installDb, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$installRestoreSourceId'"))

                insertBusinessUserAndPost(installDb, followupUserRowId, followupPostRowId, "restored-followup")
                installRestoreClient.pushPending().getOrThrow()
                assertEquals(2L, scalarLong(installDb, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$installRestoreSourceId'"))
                assertEquals(DetachOutcome.DETACHED, installRestoreClient.detach().getOrThrow())
            } finally {
                installRestoreClient.close()
                installHttpRestore?.close()
            }

            verifyHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, restoredUserId, verifySourceId),
            )
            val verifyClient = newRealServerClient(verifyDb, verifyHttp)
            try {
                verifyClient.open().getOrThrow()
                assertConnectedOutcome(
                    expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                    actual = verifyClient.attach(restoredUserId).getOrThrow(),
                )
                assertEquals("User remote-authoritative-seed", scalarText(verifyDb, "SELECT name FROM users WHERE id = '$remoteSeedUserRowId'"))
                assertEquals("User restored-followup", scalarText(verifyDb, "SELECT name FROM users WHERE id = '$followupUserRowId'"))
                assertEquals(0L, scalarLong(verifyDb, "SELECT COUNT(*) FROM users WHERE id = '$installSeedUserRowId'"))
            } finally {
                verifyClient.close()
                verifyHttp?.close()
            }
        } finally {
            installDb.close()
            remoteSeedDb.close()
            verifyDb.close()
        }
    }

    @Test
    fun freshSourceAfterDetach_restartsLocalSequence_againstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("reuse-user")
        val firstUserId = "55555555-5555-5555-5555-555555555555"
        val firstPostId = "66666666-6666-6666-6666-666666666666"
        val secondUserId = "77777777-7777-7777-7777-777777777777"
        val secondPostId = "88888888-8888-8888-8888-888888888888"
        val db = newDb()
        val verifyDb = newDb()
        var http: HttpClient? = null
        var verifyHttp: HttpClient? = null
        try {
            createBusinessSubsetTables(db)
            createBusinessSubsetTables(verifyDb)

            val sourceId = bootstrapManagedSourceId(db, config.baseUrl)
            val verifySourceId = bootstrapManagedSourceId(verifyDb, config.baseUrl)
            val token = issueDummySigninToken(config.baseUrl, userId, sourceId)
            val verifyToken = issueDummySigninToken(config.baseUrl, userId, verifySourceId)
            http = newRealServerHttpClient(config.baseUrl, token)
            verifyHttp = newRealServerHttpClient(config.baseUrl, verifyToken)
            val client = newRealServerClient(db, http)
            val verifyClient = newRealServerClient(verifyDb, verifyHttp)

            client.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = client.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )

            insertBusinessUserAndPost(db, firstUserId, firstPostId, "real-server-first")
            client.pushPending().getOrThrow()
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceId'"))

            assertEquals(DetachOutcome.DETACHED, client.detach().getOrThrow())
            val rotatedSourceId = scalarText(db, "SELECT current_source_id FROM _sync_attachment_state")
            assertNotEquals(sourceId, rotatedSourceId)
            http?.close()
            http = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, userId, rotatedSourceId))
            client.close()
            val restartedClient = newRealServerClient(db, http)
            restartedClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = restartedClient.attach(userId).getOrThrow(),
            )
            assertEquals(1L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSourceId'"))

            insertBusinessUserAndPost(db, secondUserId, secondPostId, "real-server-second")
            restartedClient.pushPending().getOrThrow()
            assertEquals(2L, scalarLong(db, "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$rotatedSourceId'"))

            verifyClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = verifyClient.attach(userId).getOrThrow(),
            )
            assertEquals(1L, scalarLong(verifyDb, "SELECT COUNT(*) FROM users WHERE id = '$secondUserId'"))
            assertEquals("User real-server-second", scalarText(verifyDb, "SELECT name FROM users WHERE id = '$secondUserId'"))
            assertEquals(1L, scalarLong(verifyDb, "SELECT COUNT(*) FROM posts WHERE id = '$secondPostId'"))
            assertEquals("Title real-server-second", scalarText(verifyDb, "SELECT title FROM posts WHERE id = '$secondPostId'"))
            restartedClient.close()
            http = null
        } finally {
            http?.close()
            verifyHttp?.close()
            db.close()
            verifyDb.close()
        }
    }

    @Test
    fun rotatedRebuild_usesRequestedReplacementAndOldSourceReturnsStructuredSourceRetired_againstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        resetRealServerState(config.baseUrl)

        val userId = randomRealServerId("retired-user")
        val seedDb = newDb()
        val recoverDb = newDb()
        val verifyDb = newDb()
        var seedHttp: HttpClient? = null
        var recoverHttp: HttpClient? = null
        var verifyHttp: HttpClient? = null
        var oldSourceHttp: HttpClient? = null
        try {
            createBusinessSubsetTables(seedDb)
            createBusinessSubsetTables(recoverDb)
            createBusinessSubsetTables(verifyDb)

            val seedSourceId = bootstrapManagedSourceId(seedDb, config.baseUrl)
            val recoverSourceId = bootstrapManagedSourceId(recoverDb, config.baseUrl)
            val verifySourceId = bootstrapManagedSourceId(verifyDb, config.baseUrl)
            val rotatedSourceId = randomRealServerId("rotated-source")

            seedHttp = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, userId, seedSourceId))
            recoverHttp = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, userId, recoverSourceId))
            verifyHttp = newRealServerHttpClient(config.baseUrl, issueDummySigninToken(config.baseUrl, userId, verifySourceId))

            val seedClient = newRealServerClient(seedDb, seedHttp)
            val recoverClient = newRealServerClient(recoverDb, recoverHttp)
            val verifyClient = newRealServerClient(verifyDb, verifyHttp)

            seedClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.STARTED_EMPTY,
                actual = seedClient.attach(userId).getOrThrow(),
                expectedAuthority = AuthorityStatus.AUTHORITATIVE_EMPTY,
            )

            insertBusinessUserAndPost(
                seedDb,
                "91919191-9191-9191-9191-919191919191",
                "92929292-9292-9292-9292-929292929292",
                "rotated-seed",
            )
            seedClient.pushPending().getOrThrow()

            recoverClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = recoverClient.attach(userId).getOrThrow(),
            )

            markSourceRecoveryRequired(recoverDb, replacementSourceId = rotatedSourceId)
            recoverClient.rebuild().getOrThrow()

            assertEquals(rotatedSourceId, scalarText(recoverDb, "SELECT current_source_id FROM _sync_attachment_state"))
            assertEquals(
                rotatedSourceId,
                scalarText(recoverDb, "SELECT replaced_by_source_id FROM _sync_source_state WHERE source_id = '$recoverSourceId'"),
            )

            oldSourceHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, recoverSourceId),
            )
            val oldSourceResponse = oldSourceHttp.post("sync/push-sessions") {
                header("Oversync-Source-ID", recoverSourceId)
                contentType(ContentType.Application.Json)
                setBody(
                    PushSessionCreateRequest(
                        sourceBundleId = 1L,
                        plannedRowCount = 1L,
                    ),
                )
            }
            assertEquals(HttpStatusCode.Conflict, oldSourceResponse.status)
            val retired = oldSourceResponse.body<SourceRetiredResponse>()
            assertEquals("source_retired", retired.error)
            assertEquals(recoverSourceId, retired.sourceId)
            assertEquals(rotatedSourceId, retired.replacedBySourceId)

            recoverHttp?.close()
            recoverClient.close()
            recoverHttp = newRealServerHttpClient(
                config.baseUrl,
                issueDummySigninToken(config.baseUrl, userId, rotatedSourceId),
            )
            val restartedRecoverClient = newRealServerClient(recoverDb, recoverHttp)
            restartedRecoverClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.RESUMED_ATTACHED_STATE,
                actual = restartedRecoverClient.attach(userId).getOrThrow(),
            )

            insertBusinessUserAndPost(
                recoverDb,
                "93939393-9393-9393-9393-939393939393",
                "94949494-9494-9494-9494-949494949494",
                "rotated-followup",
            )
            restartedRecoverClient.pushPending().getOrThrow()

            verifyClient.open().getOrThrow()
            assertConnectedOutcome(
                expectedOutcome = AttachOutcome.USED_REMOTE_STATE,
                actual = verifyClient.attach(userId).getOrThrow(),
            )
            verifyClient.pullToStable().getOrThrow()
            assertEquals(
                "User rotated-followup",
                scalarText(verifyDb, "SELECT name FROM users WHERE id = '93939393-9393-9393-9393-939393939393'"),
            )
        } finally {
            seedHttp?.close()
            recoverHttp?.close()
            verifyHttp?.close()
            oldSourceHttp?.close()
            seedDb.close()
            recoverDb.close()
            verifyDb.close()
        }
    }
}
