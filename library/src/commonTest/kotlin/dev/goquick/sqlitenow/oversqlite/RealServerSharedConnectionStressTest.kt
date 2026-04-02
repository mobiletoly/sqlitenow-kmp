package dev.goquick.sqlitenow.oversqlite

import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.SqliteStatement
import dev.goquick.sqlitenow.core.sqlite.use
import io.ktor.client.HttpClient
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals

internal class RealServerSharedConnectionStressTest : RealServerSupport() {
    private companion object {
        const val LocalRowCount = 16
        const val RemoteRowCount = 16
        const val TotalRowCount = LocalRowCount + RemoteRowCount
    }

    @Test
    fun sharedConnectionSurvivesConcurrentAliasStarQueriesWhileSyncingAgainstRealServer() = runTest {
        val config = requireRealServerConfig() ?: return@runTest
        val remoteUpdateRounds = if (realServerHeavyModeEnabled(config)) 160 else 40
        val localUpdateIterations = if (realServerHeavyModeEnabled(config)) 1_600 else 320
        val readerCoroutines = if (realServerHeavyModeEnabled(config)) 6 else 2
        val readerIterationsPerCoroutine = if (realServerHeavyModeEnabled(config)) 3_000 else 600
        val localTransactionPauseMillis = if (realServerHeavyModeEnabled(config)) 2L else 1L
        val transactionReadEvery = if (realServerHeavyModeEnabled(config)) 4 else 8
        val httpPauseMillis = if (realServerHeavyModeEnabled(config)) 4L else 1L
        val timeoutMillis = if (realServerHeavyModeEnabled(config)) 120_000L else 45_000L
        withContext(Dispatchers.Default.limitedParallelism(1)) {
            resetRealServerState(config.baseUrl)

            val userId = randomRealServerId("shared-connection")
            val seedDbPath = createTempSqliteNowTestDbPath("sqlitenow-shared-stress-seed")
            val activeDbPath = createTempSqliteNowTestDbPath("sqlitenow-shared-stress-active")
            val seedDb = newFileBackedDb(seedDbPath)
            val activeDb = newFileBackedDb(activeDbPath)
            var seedHttp: HttpClient? = null
            var activeHttp: HttpClient? = null
            try {
                createBusinessSubsetTables(seedDb)
                createBusinessSubsetTables(activeDb)
                seedUserRows(seedDb)

                val seedSourceId = bootstrapManagedSourceId(seedDb, config.baseUrl)
                val activeSourceId = bootstrapManagedSourceId(activeDb, config.baseUrl)
                val seedToken = issueDummySigninToken(config.baseUrl, userId, seedSourceId)
                val activeToken = issueDummySigninToken(config.baseUrl, userId, activeSourceId)

                seedHttp = newRealServerHttpClient(config.baseUrl, seedToken)
                activeHttp = newRealServerHttpClient(
                    baseUrl = config.baseUrl,
                    token = activeToken,
                    afterResponse = { path ->
                        if (path.startsWith("/sync/")) {
                            delay(httpPauseMillis)
                        }
                    },
                )

                val seedClient = newRealServerClient(seedDb, seedHttp, uploadLimit = 4, downloadLimit = 1)
                val activeClient = newRealServerClient(activeDb, activeHttp, uploadLimit = 4, downloadLimit = 1)

                seedClient.awaitConnected(userId)
                seedClient.pullToStableUnlessDirty()
                seedClient.pushPending().getOrThrow()
                activeClient.awaitConnected(userId)
                activeClient.pullToStable().getOrThrow()
                assertEquals(TotalRowCount.toLong(), queryUsersWithAliasStar(activeDb))

                withTimeout(timeoutMillis) {
                    coroutineScope {
                        val remoteUpdatesFinished = CompletableDeferred<Unit>()
                        val localUpdatesFinished = CompletableDeferred<Unit>()

                        val remoteWriter = launch(Dispatchers.Default) {
                            repeat(remoteUpdateRounds) { round ->
                                incrementRemoteOwnedRow(seedDb, rowIndex = round % RemoteRowCount, round = round)
                                seedClient.pushPending().getOrThrow()
                            }
                            remoteUpdatesFinished.complete(Unit)
                        }

                        val localWriter = launch(Dispatchers.Default) {
                            repeat(localUpdateIterations) { iteration ->
                                incrementLocalOwnedRow(
                                    db = activeDb,
                                    rowIndex = iteration % LocalRowCount,
                                    iteration = iteration,
                                    transactionPauseMillis = localTransactionPauseMillis,
                                    transactionReadEvery = transactionReadEvery,
                                    expectedRowCount = TotalRowCount.toLong(),
                                )
                            }
                            localUpdatesFinished.complete(Unit)
                        }

                        val syncer = launch(Dispatchers.Default) {
                            while (!remoteUpdatesFinished.isCompleted || !localUpdatesFinished.isCompleted) {
                                activeClient.pushPending().getOrThrow()
                                if (localUpdatesFinished.isCompleted && pendingLocalChangeCount(activeDb) == 0L) {
                                    activeClient.pullToStableUnlessDirty()
                                }
                            }
                            drainPendingLocalChanges(activeClient, activeDb)
                            activeClient.pullToStable().getOrThrow()
                        }

                        val readers = List(readerCoroutines) { readerIndex ->
                            launch(Dispatchers.Default) {
                                repeat(readerIterationsPerCoroutine) { iteration ->
                                    assertEquals(
                                        TotalRowCount.toLong(),
                                        queryUsersWithAliasStar(activeDb),
                                    )
                                    readUserById(
                                        db = activeDb,
                                        id = when ((readerIndex + iteration) % 2) {
                                            0 -> localRowId((readerIndex + iteration) % LocalRowCount)
                                            else -> remoteRowId((readerIndex + iteration) % RemoteRowCount)
                                        },
                                    )
                                }
                            }
                        }

                        (listOf(remoteWriter, localWriter, syncer) + readers).joinAll()
                    }
                }

                activeClient.sync().getOrThrow()
                seedClient.pullToStable().getOrThrow()

                assertEquals(TotalRowCount.toLong(), queryUsersWithAliasStar(activeDb))
                assertEquals(TotalRowCount.toLong(), queryUsersWithAliasStar(seedDb))
                assertExpectedUserStates(activeDb, localUpdateIterations, remoteUpdateRounds)
                assertExpectedUserStates(seedDb, localUpdateIterations, remoteUpdateRounds)
                assertEquals(0L, scalarLong(activeDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(seedDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
                assertEquals(0L, scalarLong(activeDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
                assertEquals(0L, scalarLong(seedDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            } finally {
                seedHttp?.close()
                activeHttp?.close()
                seedDb.close()
                activeDb.close()
                deleteTempSqliteNowTestDbArtifacts(seedDbPath)
                deleteTempSqliteNowTestDbArtifacts(activeDbPath)
            }
        }
    }

    private suspend fun seedUserRows(
        db: SafeSQLiteConnection,
    ) = db.withExclusiveAccess {
        db.prepare(
            """
            INSERT INTO users (
                id,
                name,
                email,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """.trimIndent(),
        ).use { statement ->
            repeat(LocalRowCount) { index ->
                bindSeedUser(
                    statement = statement,
                    id = localRowId(index),
                    name = "Local Seed $index",
                    email = "local-seed-$index@example.com",
                )
            }
            repeat(RemoteRowCount) { index ->
                bindSeedUser(
                    statement = statement,
                    id = remoteRowId(index),
                    name = "Remote Seed $index",
                    email = "remote-seed-$index@example.com",
                )
            }
        }
    }

    private fun bindSeedUser(
        statement: SqliteStatement,
        id: String,
        name: String,
        email: String,
    ) {
        statement.bindText(1, id)
        statement.bindText(2, name)
        statement.bindText(3, email)
        statement.step()
        statement.reset()
        statement.clearBindings()
    }

    private suspend fun incrementLocalOwnedRow(
        db: SafeSQLiteConnection,
        rowIndex: Int,
        iteration: Int,
        transactionPauseMillis: Long,
        transactionReadEvery: Int,
        expectedRowCount: Long,
    ) {
        db.transaction(TransactionMode.IMMEDIATE) {
            db.prepare(
                """
                UPDATE users
                SET name = ?,
                    email = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """.trimIndent(),
            ).use { statement ->
                statement.bindText(1, "Local $iteration")
                statement.bindText(2, "local-$iteration@example.com")
                statement.bindText(3, localRowId(rowIndex))
                statement.step()
            }
            delay(transactionPauseMillis)
            if (iteration % transactionReadEvery == 0) {
                assertEquals(expectedRowCount, queryUsersWithAliasStar(db))
            }
        }
    }

    private suspend fun incrementRemoteOwnedRow(
        db: SafeSQLiteConnection,
        rowIndex: Int,
        round: Int,
    ) {
        db.execSQL(
            """
            UPDATE users
            SET name = 'Remote $round',
                email = 'remote-$round@example.com',
                updated_at = CURRENT_TIMESTAMP
            WHERE id = '${remoteRowId(rowIndex)}'
            """.trimIndent(),
        )
    }

    private suspend fun queryUsersWithAliasStar(
        db: SafeSQLiteConnection,
    ): Long = db.withExclusiveAccess {
        db.prepare("SELECT u.* FROM users u ORDER BY u.id").use { statement ->
            var rowCount = 0L
            while (statement.step()) {
                statement.getText(0)
                statement.getText(1)
                statement.getText(2)
                if (!statement.isNull(3)) {
                    statement.getText(3)
                }
                if (!statement.isNull(4)) {
                    statement.getText(4)
                }
                rowCount++
            }
            rowCount
        }
    }

    private suspend fun readUserById(
        db: SafeSQLiteConnection,
        id: String,
    ) = db.withExclusiveAccess {
        db.prepare("SELECT * FROM users WHERE id = ?").use { statement ->
            statement.bindText(1, id)
            if (statement.step()) {
                statement.getText(0)
                statement.getText(1)
                statement.getText(2)
                if (!statement.isNull(3)) {
                    statement.getText(3)
                }
                if (!statement.isNull(4)) {
                    statement.getText(4)
                }
            }
        }
    }

    private suspend fun assertExpectedUserStates(
        db: SafeSQLiteConnection,
        localUpdateIterations: Int,
        remoteUpdateRounds: Int,
    ) {
        repeat(LocalRowCount) { index ->
            val finalIteration = localFinalIteration(index, localUpdateIterations)
            if (finalIteration == null) {
                assertEquals(
                    "Local Seed $index",
                    scalarText(db, "SELECT name FROM users WHERE id = '${localRowId(index)}'"),
                )
                assertEquals(
                    "local-seed-$index@example.com",
                    scalarText(db, "SELECT email FROM users WHERE id = '${localRowId(index)}'"),
                )
            } else {
                assertEquals(
                    "Local $finalIteration",
                    scalarText(db, "SELECT name FROM users WHERE id = '${localRowId(index)}'"),
                )
                assertEquals(
                    "local-$finalIteration@example.com",
                    scalarText(db, "SELECT email FROM users WHERE id = '${localRowId(index)}'"),
                )
            }
        }
        repeat(RemoteRowCount) { index ->
            val finalRound = remoteFinalRound(index, remoteUpdateRounds)
            if (finalRound == null) {
                assertEquals(
                    "Remote Seed $index",
                    scalarText(db, "SELECT name FROM users WHERE id = '${remoteRowId(index)}'"),
                )
                assertEquals(
                    "remote-seed-$index@example.com",
                    scalarText(db, "SELECT email FROM users WHERE id = '${remoteRowId(index)}'"),
                )
            } else {
                assertEquals(
                    "Remote $finalRound",
                    scalarText(db, "SELECT name FROM users WHERE id = '${remoteRowId(index)}'"),
                )
                assertEquals(
                    "remote-$finalRound@example.com",
                    scalarText(db, "SELECT email FROM users WHERE id = '${remoteRowId(index)}'"),
                )
            }
        }
    }

    private suspend fun drainPendingLocalChanges(
        client: OversqliteClient,
        db: SafeSQLiteConnection,
    ) {
        repeat(64) {
            client.pushPending().getOrThrow()
            if (pendingLocalChangeCount(db) == 0L) {
                return
            }
            delay(10)
        }
        error("local dirty or outbox rows did not drain after repeated pushPending()")
    }

    private suspend fun pendingLocalChangeCount(
        db: SafeSQLiteConnection,
    ): Long {
        return scalarLong(db, "SELECT COUNT(*) FROM _sync_dirty_rows") +
            scalarLong(db, "SELECT COUNT(*) FROM _sync_outbox_rows")
    }

    private suspend fun OversqliteClient.awaitConnected(userId: String) {
        open().getOrThrow()
        repeat(6) {
            when (val attach = attach(userId).getOrThrow()) {
                is AttachResult.Connected -> return
                is AttachResult.RetryLater -> delay(maxOf(attach.retryAfterSeconds, 1L) * 1_000L)
            }
        }
        error("attach(userId) never reached Connected")
    }

    private suspend fun OversqliteClient.pullToStableUnlessDirty(): Boolean {
        val failure = pullToStable().exceptionOrNull() ?: return true
        if (failure is DirtyStateRejectedException) {
            return false
        }
        throw failure
    }

    private fun localRowId(index: Int): String =
        formatDeterministicUuid(0x1000_0000_0000L + index.toLong())

    private fun remoteRowId(index: Int): String =
        formatDeterministicUuid(0x2000_0000_0000L + index.toLong())

    private fun formatDeterministicUuid(tail: Long): String =
        "00000000-0000-4000-8000-${tail.toString(16).padStart(12, '0')}"

    private fun localFinalIteration(
        index: Int,
        localUpdateIterations: Int,
    ): Int? =
        lastUpdateForIndex(
            totalUpdates = localUpdateIterations,
            rowCount = LocalRowCount,
            index = index,
        )

    private fun remoteFinalRound(
        index: Int,
        remoteUpdateRounds: Int,
    ): Int? =
        lastUpdateForIndex(
            totalUpdates = remoteUpdateRounds,
            rowCount = RemoteRowCount,
            index = index,
        )

    private fun lastUpdateForIndex(
        totalUpdates: Int,
        rowCount: Int,
        index: Int,
    ): Int? {
        if (totalUpdates <= 0) return null
        if (index !in 0 until rowCount) return null

        val lastIteration = totalUpdates - 1
        val delta = (lastIteration - index).mod(rowCount)
        val candidate = lastIteration - delta
        return candidate.takeIf { it >= 0 }
    }
}
