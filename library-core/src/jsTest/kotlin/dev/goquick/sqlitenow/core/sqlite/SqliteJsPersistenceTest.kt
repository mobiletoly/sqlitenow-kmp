package dev.goquick.sqlitenow.core.sqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqlitePersistence
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

class SqliteJsPersistenceTest {

    @Test
    fun persistsSnapshotsOnCommitAndClose() = runTest {
        val persistence = InMemoryPersistence()
        val connection = BundledSqliteConnectionProvider.openConnection(
            dbName = "js-persistence-test.db",
            debug = false,
            config = SqliteConnectionConfig(persistence = persistence),
        )

        try {
            connection.execSQL("CREATE TABLE IF NOT EXISTS person(id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            connection.transaction {
                connection.execSQL("INSERT INTO person(name) VALUES ('Alice');")
            }
        } finally {
            connection.close()
        }

        val stored = assertNotNull(persistence.stored, "Expected persisted snapshot to be stored")
        assertTrue(stored.isNotEmpty(), "Persisted snapshot should contain data")
    }

    @Test
    fun reopensConnectionWithPersistedSnapshot() = runTest {
        val persistence = InMemoryPersistence()
        val dbName = "js-persistence-roundtrip.db"

        val first = BundledSqliteConnectionProvider.openConnection(
            dbName = dbName,
            debug = false,
            config = SqliteConnectionConfig(persistence = persistence),
        )

        try {
            first.execSQL("CREATE TABLE IF NOT EXISTS person(id INTEGER PRIMARY KEY, name TEXT NOT NULL);")
            first.transaction {
                first.execSQL("INSERT INTO person(name) VALUES ('Alice');")
            }
        } finally {
            first.close()
        }

        val stored = assertNotNull(persistence.stored, "Expected snapshot to be captured after first close")
        assertTrue(stored.isNotEmpty(), "Persisted snapshot should contain data")

        val second = BundledSqliteConnectionProvider.openConnection(
            dbName = dbName,
            debug = false,
            config = SqliteConnectionConfig(persistence = persistence),
        )

        try {
            val statement = second.prepare("SELECT name FROM person ORDER BY id")
            try {
                assertTrue(statement.step(), "Expected at least one row after reopening from snapshot")
                val name = statement.getText(0)
                assertEquals("Alice", name, "Expected persisted row to contain previously inserted data")
            } finally {
                statement.close()
            }
        } finally {
            second.close()
        }
    }

    @Test
    fun manualFlushSupportsAutoFlushDisabled() = runTest {
        val persistence = InMemoryPersistence()
        val connection = BundledSqliteConnectionProvider.openConnection(
            dbName = "js-manual-flush.db",
            debug = false,
            config = SqliteConnectionConfig(
                persistence = persistence,
                autoFlushPersistence = false,
            ),
        )

        try {
            connection.execSQL("CREATE TABLE IF NOT EXISTS counter(id INTEGER PRIMARY KEY, value INTEGER);")
            connection.execSQL("INSERT INTO counter(id, value) VALUES (1, 42);")

            assertNull(persistence.stored, "Auto-flush disabled should not persist snapshot automatically")

        connection.persistSnapshotNow()

            val stored = assertNotNull(persistence.stored, "Manual flush should persist snapshot")
            assertTrue(stored.isNotEmpty(), "Persisted bytes should not be empty")
        } finally {
            connection.close()
        }
    }

    @Test
    fun failingClosePersistenceIsReportedOnceAndCloseIsNotRetried() = runTest {
        val failure = IllegalStateException("JS_CLOSE_PERSISTENCE_SENTINEL")
        val persistence = FailingPersistence(failure)
        val connection = BundledSqliteConnectionProvider.openConnection(
            dbName = "js-close-failure.db",
            debug = false,
            config = SqliteConnectionConfig(
                persistence = persistence,
                autoFlushPersistence = false,
            ),
        )
        connection.execSQL("CREATE TABLE IF NOT EXISTS probe(id INTEGER PRIMARY KEY)")

        val observed = assertFailsWith<IllegalStateException> { connection.close() }

        assertSame(failure, generateSequence<Throwable>(observed) { it.cause }.last())
        assertEquals(1, persistence.persistCalls)
        connection.close()
        assertEquals(1, persistence.persistCalls)
    }

    private class InMemoryPersistence : SqlitePersistence {
        var stored: ByteArray? = null
            private set

        override suspend fun load(dbName: String): ByteArray? = stored

        override suspend fun persist(dbName: String, bytes: ByteArray) {
            stored = bytes.copyOf()
        }

        override suspend fun clear(dbName: String) {
            stored = null
        }
    }

    private class FailingPersistence(
        private val failure: Throwable,
    ) : SqlitePersistence {
        var persistCalls: Int = 0
            private set

        override suspend fun load(dbName: String): ByteArray? = null

        override suspend fun persist(dbName: String, bytes: ByteArray) {
            persistCalls++
            throw failure
        }

        override suspend fun clear(dbName: String) = Unit
    }
}
