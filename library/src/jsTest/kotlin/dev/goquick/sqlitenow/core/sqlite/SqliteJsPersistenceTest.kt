package dev.goquick.sqlitenow.core.sqlite

import dev.goquick.sqlitenow.core.BundledSqliteConnectionProvider
import dev.goquick.sqlitenow.core.SqliteConnectionConfig
import dev.goquick.sqlitenow.core.SqlitePersistence
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
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
}
