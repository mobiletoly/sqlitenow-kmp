package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import java.nio.file.Files
import kotlin.io.path.deleteIfExists
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

class SqliteNowExternalInvalidationTest {
    @Test
    fun reportExternalTableChanges_reEmitsReactiveQueries() = runBlocking {
        withTestDatabase { database ->
            database.open()

            val (emissions, collector) = collectFlow(database.userNamesFlow())
            try {
                assertEquals(emptyList(), receiveNext(emissions))
                delay(50)

                database.connection().execSQL(
                    """
                    INSERT INTO users(id, name) VALUES('user-1', 'Ada')
                    """.trimIndent(),
                )
                database.reportExternalTableChanges(setOf("users"))

                assertEquals(listOf("Ada"), receiveNext(emissions))
            } finally {
                collector.cancel()
                collector.join()
            }
        }
    }

    @Test
    fun reportExternalTableChanges_emptySetDoesNotEmit() = runBlocking {
        withTestDatabase { database ->
            database.open()

            val (emissions, collector) = collectFlow(database.userNamesFlow())
            try {
                assertEquals(emptyList(), receiveNext(emissions))

                database.reportExternalTableChanges(emptySet())

                assertNull(withTimeoutOrNull(300) { emissions.receive() })
            } finally {
                collector.cancel()
                collector.join()
            }
        }
    }

    @Test
    fun reportExternalTableChanges_requiresOpenDatabase() = runBlocking {
        withTestDatabase { database ->
            assertFailsWith<IllegalStateException> {
                database.reportExternalTableChanges(setOf("users"))
            }

            database.open()
            database.close()

            assertFailsWith<IllegalStateException> {
                database.reportExternalTableChanges(setOf("users"))
            }
        }
    }

    private suspend fun withTestDatabase(
        block: suspend (ReactiveUsersDatabase) -> Unit,
    ) {
        val dbPath = Files.createTempFile("sqlitenow-external-invalidation", ".db")
        val database = ReactiveUsersDatabase(dbPath.toString())
        try {
            block(database)
        } finally {
            if (database.isOpen()) {
                database.close()
            }
            dbPath.deleteIfExists()
        }
    }

    private fun <T> collectFlow(flow: Flow<T>): Pair<Channel<T>, Job> {
        val emissions = Channel<T>(Channel.UNLIMITED)
        val collector = CoroutineScope(Dispatchers.Default).launch(start = CoroutineStart.UNDISPATCHED) {
            flow.collect { value ->
                emissions.send(value)
            }
        }
        return emissions to collector
    }

    private suspend fun <T> receiveNext(channel: Channel<T>): T {
        return withTimeout(5_000) { channel.receive() }
    }

    private class ReactiveUsersDatabase(
        dbName: String,
    ) : SqliteNowDatabase(dbName, UsersMigration()) {
        fun userNamesFlow(): Flow<List<String>> = createReactiveQueryFlow(setOf("users")) {
            queryUserNames()
        }

        private suspend fun queryUserNames(): List<String> {
            return connection().prepare("SELECT name FROM users ORDER BY id").use { statement ->
                buildList {
                    while (statement.step()) {
                        add(statement.getText(0))
                    }
                }
            }
        }
    }

    private class UsersMigration : DatabaseMigrations {
        override suspend fun applyMigration(conn: SafeSQLiteConnection, currentVersion: Int): Int {
            if (currentVersion == -1) {
                conn.execSQL(
                    """
                    CREATE TABLE users (
                        id TEXT PRIMARY KEY NOT NULL,
                        name TEXT NOT NULL
                    )
                    """.trimIndent(),
                )
                return 0
            }
            return currentVersion
        }
    }
}
