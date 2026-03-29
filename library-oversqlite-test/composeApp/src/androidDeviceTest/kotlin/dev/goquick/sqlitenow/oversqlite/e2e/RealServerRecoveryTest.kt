package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import dev.goquick.sqlitenow.oversqlite.RebuildMode
import kotlinx.coroutines.runBlocking
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.Test
import org.junit.runner.RunWith

@RunWith(AndroidJUnit4::class)
class RealServerRecoveryTest {
    @Test
    fun recover_rebuildsRotatesSourceAndAllowsFollowupSync() = runBlocking {
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val userId = randomUserId()
        val seedDevice = randomSourceId("recover-seed")
        val recoverDevice = randomSourceId("recover-target")
        val verifyDevice = randomSourceId("recover-verify")

        val seedToken = issueDummySigninToken(config.baseUrl, userId, seedDevice)
        val recoverToken = issueDummySigninToken(config.baseUrl, userId, recoverDevice)
        val verifyToken = issueDummySigninToken(config.baseUrl, userId, verifyDevice)
        val seedHttp = newAuthenticatedHttpClient(config.baseUrl, seedToken)
        var recoverHttp = newAuthenticatedHttpClient(config.baseUrl, recoverToken)
        val verifyHttp = newAuthenticatedHttpClient(config.baseUrl, verifyToken)
        val seedDb = newInMemoryDb()
        val recoverDb = newInMemoryDb()
        val verifyDb = newInMemoryDb()
        try {
            createBusinessSubsetTables(seedDb)
            createBusinessSubsetTables(recoverDb)
            createBusinessSubsetTables(verifyDb)

            val seedClient = newRealServerClient(seedDb, config, seedHttp)
            val recoverClient = newRealServerClient(recoverDb, config, recoverHttp)
            val verifyClient = newRealServerClient(verifyDb, config, verifyHttp)

            seedClient.openAndAttach(userId, seedDevice).getOrThrow()
            recoverClient.openAndAttach(userId, recoverDevice).getOrThrow()
            verifyClient.openAndAttach(userId, verifyDevice).getOrThrow()

            val authorId = randomRowId()
            val postId = randomRowId()
            seedDb.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$authorId', 'Recover Seed', 'recover-seed@example.com')
                """.trimIndent()
            )
            seedDb.execSQL(
                """
                INSERT INTO posts(id, title, content, author_id)
                VALUES('$postId', 'Recover Seed Post', 'recover-seed-payload', '$authorId')
                """.trimIndent()
            )
            seedClient.pushPending().getOrThrow()

            val sourceBefore = scalarText(
                recoverDb,
                "SELECT current_source_id FROM _sync_attachment_state",
            )
            recoverClient.rebuild(
                mode = RebuildMode.ROTATE_SOURCE,
                newSourceId = randomSourceId("recover-rotated"),
            ).getOrThrow()

            val sourceAfter = scalarText(
                recoverDb,
                "SELECT current_source_id FROM _sync_attachment_state",
            )
            assertNotEquals(sourceBefore, sourceAfter)
            assertEquals(
                1L,
                scalarLong(
                    recoverDb,
                    "SELECT next_source_bundle_id FROM _sync_source_state WHERE source_id = '$sourceAfter'",
                ),
            )
            assertEquals("Recover Seed", scalarText(recoverDb, "SELECT name FROM users WHERE id = '$authorId'"))
            assertEquals(
                "Recover Seed Post",
                scalarText(recoverDb, "SELECT title FROM posts WHERE id = '$postId'"),
            )

            recoverHttp.close()
            val rotatedToken = issueDummySigninToken(config.baseUrl, userId, sourceAfter)
            recoverHttp = newAuthenticatedHttpClient(config.baseUrl, rotatedToken)
            val postRecoverClient = newRealServerClient(recoverDb, config, recoverHttp)
            postRecoverClient.openAndAttach(userId, sourceAfter).getOrThrow()

            val newAuthorId = randomRowId()
            val newPostId = randomRowId()
            recoverDb.execSQL(
                """
                INSERT INTO users(id, name, email)
                VALUES('$newAuthorId', 'Recover Writer', 'recover-writer@example.com')
                """.trimIndent()
            )
            recoverDb.execSQL(
                """
                INSERT INTO posts(id, title, content, author_id)
                VALUES('$newPostId', 'Recover Followup', 'after-recover', '$newAuthorId')
                """.trimIndent()
            )
            postRecoverClient.pushPending().getOrThrow()
            verifyClient.pullToStable().getOrThrow()

            assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM users"))
            assertEquals(2L, scalarLong(verifyDb, "SELECT COUNT(*) FROM posts"))
            assertEquals(
                "Recover Followup",
                scalarText(verifyDb, "SELECT title FROM posts WHERE id = '$newPostId'"),
            )
            assertEquals(0L, scalarLong(recoverDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(verifyDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
        } finally {
            seedHttp.close()
            recoverHttp.close()
            verifyHttp.close()
            seedDb.close()
            recoverDb.close()
            verifyDb.close()
        }
    }
}
