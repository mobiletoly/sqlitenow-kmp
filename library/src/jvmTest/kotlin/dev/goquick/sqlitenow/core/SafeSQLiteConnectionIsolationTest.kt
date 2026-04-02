package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import java.nio.file.Files
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class SafeSQLiteConnectionIsolationTest {
    @Test
    fun suspendedTransaction_blocksOtherCoroutinesFromUsingSameConnection() = runBlocking {
        withTimeout(10_000) {
            withTestDatabase { database ->
                database.open()
                database.connection().execSQL(
                    """
                    CREATE TABLE items (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL
                    )
                    """.trimIndent(),
                )

                val transactionStarted = CompletableDeferred<Unit>()
                val releaseTransaction = CompletableDeferred<Unit>()
                val transactionJob = launch {
                    database.transaction(TransactionMode.IMMEDIATE) {
                        database.connection().execSQL("INSERT INTO items(id, name) VALUES (1, 'first')")
                        transactionStarted.complete(Unit)
                        releaseTransaction.await()
                        database.connection().execSQL("INSERT INTO items(id, name) VALUES (2, 'second')")
                    }
                }

                transactionStarted.await()

                val concurrentRead = async {
                    database.connection().prepare("SELECT COUNT(*) FROM items").use { statement ->
                        check(statement.step())
                        statement.getLong(0)
                    }
                }

                val prematureResult = withTimeoutOrNull(250) {
                    concurrentRead.await()
                }

                releaseTransaction.complete(Unit)
                transactionJob.join()

                assertNull(
                    prematureResult,
                    "another coroutine used the same connection while a transaction was suspended; " +
                        "leaked read result=$prematureResult",
                )

                assertEquals(
                    2L,
                    withTimeout(5_000) { concurrentRead.await() },
                )
            }
        }
    }

    private suspend fun withTestDatabase(
        block: suspend (SqliteNowDatabase) -> Unit,
    ) {
        val dbPath = Files.createTempFile("sqlitenow-isolation", ".db")
        val database = SqliteNowDatabase(dbPath.toString(), NoopMigration())
        try {
            block(database)
        } finally {
            if (database.isOpen()) {
                database.close()
            }
            dbPath.deleteIfExists()
        }
    }

    private class NoopMigration : DatabaseMigrations {
        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            return if (currentVersion == -1) 0 else currentVersion
        }
    }
}
