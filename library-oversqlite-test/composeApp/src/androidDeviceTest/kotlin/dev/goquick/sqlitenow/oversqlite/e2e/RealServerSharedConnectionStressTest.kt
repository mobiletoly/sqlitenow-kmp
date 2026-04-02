package dev.goquick.sqlitenow.oversqlite.e2e

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SafeSQLiteConnection
import dev.goquick.sqlitenow.core.TransactionMode
import dev.goquick.sqlitenow.core.sqlite.use
import dev.goquick.sqlitenow.oversqlite.AttachResult
import dev.goquick.sqlitenow.oversqlite.OversqliteClient
import dev.goquick.sqlitenow.oversqlite.SyncTable
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import java.io.File
import java.util.UUID

@RunWith(AndroidJUnit4::class)
class RealServerSharedConnectionStressTest {
    @Test
    fun fileBackedClient_survivesConcurrentAliasStarQueriesWhileSyncing() = runBlocking {
        val stress = requireStressConfig()
        val config = requireRealServerConfig()
        resetRealServerState(config.baseUrl)

        val syncTables = listOf(SyncTable("users", syncKeyColumnName = "id"))
        val userId = randomUserId("shared-connection")
        val seedSourceId = randomSourceId("shared-seed")
        val activeSourceId = randomSourceId("shared-active")
        val seedToken = issueDummySigninToken(config.baseUrl, userId, seedSourceId)
        val activeToken = issueDummySigninToken(config.baseUrl, userId, activeSourceId)

        val seedHttp = newAuthenticatedHttpClient(config.baseUrl, seedToken)
        val activeHttp = newAuthenticatedHttpClient(
            baseUrl = config.baseUrl,
            token = activeToken,
            beforeSend = { path ->
                if (path.startsWith("/sync/")) {
                    delay(4)
                }
            },
            afterResponse = { path ->
                if (path.startsWith("/sync/")) {
                    delay(4)
                }
            },
        )
        val seedDb = newInMemoryDb()
        val activeDbFile = createTempDbFile()
        val activeDb = BundledSqliteConnectionProvider.openConnection(activeDbFile.absolutePath, debug = true)

        try {
            createUsersStressSchema(seedDb)
            createUsersStressSchema(activeDb)
            seedUserRows(seedDb, stress)

            val seedClient = newRealServerClient(
                db = seedDb,
                config = config,
                http = seedHttp,
                syncTables = syncTables,
                uploadLimit = 4,
                downloadLimit = 1,
            )
            val activeClient = newRealServerClient(
                db = activeDb,
                config = config,
                http = activeHttp,
                syncTables = syncTables,
                uploadLimit = 4,
                downloadLimit = 1,
            )

            seedClient.awaitConnected(userId)
            seedClient.pushPending().getOrThrow()
            activeClient.awaitConnected(userId)
            activeClient.pullToStable().getOrThrow()
            assertEquals(stress.totalRowCount.toLong(), queryUsersWithAliasStar(activeDb))

            withTimeout(stress.timeoutMillis) {
                coroutineScope {
                    val remoteUpdatesFinished = CompletableDeferred<Unit>()
                    val localUpdatesFinished = CompletableDeferred<Unit>()

                    val remoteWriter = launch(Dispatchers.Default) {
                        repeat(stress.remoteUpdateRounds) { round ->
                            incrementRemoteOwnedRow(seedDb, rowIndex = round % stress.remoteRowCount, round = round)
                            seedClient.pushPending().getOrThrow()
                        }
                        remoteUpdatesFinished.complete(Unit)
                    }

                    val localWriter = launch(Dispatchers.Default) {
                        repeat(stress.localUpdateIterations) { iteration ->
                            incrementLocalOwnedRow(
                                db = activeDb,
                                rowIndex = iteration % stress.localRowCount,
                                iteration = iteration,
                                transactionPauseMillis = stress.localTransactionPauseMillis,
                                transactionReadEvery = stress.transactionReadEvery,
                                expectedRowCount = stress.totalRowCount.toLong(),
                            )
                        }
                        localUpdatesFinished.complete(Unit)
                    }

                    val syncer = launch(Dispatchers.Default) {
                        while (!remoteUpdatesFinished.isCompleted || !localUpdatesFinished.isCompleted) {
                            activeClient.pushPending().getOrThrow()
                            if (localUpdatesFinished.isCompleted && pendingLocalChangeCount(activeDb) == 0L) {
                                activeClient.pullToStable().getOrThrow()
                            }
                        }
                        drainPendingLocalChanges(activeClient, activeDb)
                        activeClient.pullToStable().getOrThrow()
                    }

                    val readers = List(stress.readerCoroutines) { readerIndex ->
                        launch(Dispatchers.Default) {
                            repeat(stress.readerIterationsPerCoroutine) { iteration ->
                                assertEquals(
                                    stress.totalRowCount.toLong(),
                                    queryUsersWithAliasStar(activeDb),
                                )
                                readUserById(
                                    db = activeDb,
                                    id = when ((readerIndex + iteration) % 2) {
                                        0 -> localRowId((readerIndex + iteration) % stress.localRowCount)
                                        else -> remoteRowId((readerIndex + iteration) % stress.remoteRowCount)
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

            assertEquals(stress.totalRowCount.toLong(), queryUsersWithAliasStar(activeDb))
            assertEquals(stress.totalRowCount.toLong(), queryUsersWithAliasStar(seedDb))
            assertExpectedUserStates(activeDb, stress)
            assertExpectedUserStates(seedDb, stress)
            assertEquals(0L, scalarLong(activeDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(seedDb, "SELECT COUNT(*) FROM _sync_dirty_rows"))
            assertEquals(0L, scalarLong(activeDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
            assertEquals(0L, scalarLong(seedDb, "SELECT COUNT(*) FROM _sync_outbox_rows"))
        } finally {
            seedHttp.close()
            activeHttp.close()
            seedDb.close()
            activeDb.close()
            deleteDbArtifacts(activeDbFile)
        }
    }

    private suspend fun createUsersStressSchema(db: SafeSQLiteConnection) {
        createBusinessSubsetTables(db)
    }

    private suspend fun seedUserRows(
        db: SafeSQLiteConnection,
        stress: StressConfig,
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
            repeat(stress.localRowCount) { index ->
                bindSeedUser(
                    statement = statement,
                    id = localRowId(index),
                    name = "Local Seed $index",
                    email = "local-seed-$index@example.com",
                )
            }
            repeat(stress.remoteRowCount) { index ->
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
        statement: dev.goquick.sqlitenow.core.sqlite.SqliteStatement,
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
        stress: StressConfig,
    ) {
        repeat(stress.localRowCount) { index ->
            val finalIteration = localFinalIteration(index, stress)
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
        repeat(stress.remoteRowCount) { index ->
            val finalRound = remoteFinalRound(index, stress)
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

    private fun createTempDbFile(): File {
        val context = InstrumentationRegistry.getInstrumentation().targetContext
        return File(context.cacheDir, "oversqlite-shared-${UUID.randomUUID()}.db")
    }

    private fun deleteDbArtifacts(dbFile: File) {
        dbFile.delete()
        File("${dbFile.absolutePath}-journal").delete()
        File("${dbFile.absolutePath}-wal").delete()
        File("${dbFile.absolutePath}-shm").delete()
    }

    private fun localRowId(index: Int): String =
        formatDeterministicUuid(0x1000_0000_0000L + index.toLong())

    private fun remoteRowId(index: Int): String =
        formatDeterministicUuid(0x2000_0000_0000L + index.toLong())

    private fun formatDeterministicUuid(tail: Long): String =
        "00000000-0000-4000-8000-${tail.toString(16).padStart(12, '0')}"

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

    private fun requireStressConfig(): StressConfig {
        val args = InstrumentationRegistry.getArguments()
        val localRowCount = args.getString("oversqliteSharedStressLocalRowCount")?.toIntOrNull() ?: 16
        val remoteRowCount = args.getString("oversqliteSharedStressRemoteRowCount")?.toIntOrNull() ?: 16
        return StressConfig(
            localRowCount = localRowCount,
            remoteRowCount = remoteRowCount,
            remoteUpdateRounds = args.getString("oversqliteSharedStressRemoteUpdateRounds")?.toIntOrNull() ?: 160,
            localUpdateIterations = args.getString("oversqliteSharedStressLocalUpdateIterations")?.toIntOrNull() ?: 1_600,
            readerCoroutines = args.getString("oversqliteSharedStressReaderCoroutines")?.toIntOrNull() ?: 6,
            readerIterationsPerCoroutine = args.getString("oversqliteSharedStressReaderIterationsPerCoroutine")?.toIntOrNull() ?: 3_000,
            localTransactionPauseMillis = args.getString("oversqliteSharedStressPauseMillis")?.toLongOrNull() ?: 2L,
            transactionReadEvery = args.getString("oversqliteSharedStressTransactionReadEvery")?.toIntOrNull() ?: 4,
            timeoutMillis = args.getString("oversqliteSharedStressTimeoutMillis")?.toLongOrNull() ?: 120_000L,
        )
    }

    private companion object {
        private fun localFinalIteration(index: Int, stress: StressConfig): Int? =
            lastUpdateForIndex(
                totalUpdates = stress.localUpdateIterations,
                rowCount = stress.localRowCount,
                index = index,
            )

        private fun remoteFinalRound(index: Int, stress: StressConfig): Int? =
            lastUpdateForIndex(
                totalUpdates = stress.remoteUpdateRounds,
                rowCount = stress.remoteRowCount,
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

    private data class StressConfig(
        val localRowCount: Int,
        val remoteRowCount: Int,
        val remoteUpdateRounds: Int,
        val localUpdateIterations: Int,
        val readerCoroutines: Int,
        val readerIterationsPerCoroutine: Int,
        val localTransactionPauseMillis: Long,
        val transactionReadEvery: Int,
        val timeoutMillis: Long,
    ) {
        val totalRowCount: Int
            get() = localRowCount + remoteRowCount
    }
}
