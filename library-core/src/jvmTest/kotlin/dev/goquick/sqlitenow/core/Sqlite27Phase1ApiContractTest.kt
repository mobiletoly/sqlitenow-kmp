package dev.goquick.sqlitenow.core

import androidx.sqlite.SQLiteConnection
import androidx.sqlite.SQLiteStatement
import androidx.sqlite.async.executeSQL as asyncExecuteSQL
import androidx.sqlite.async.prepare as asyncPrepare
import androidx.sqlite.async.step as asyncStep
import dev.goquick.sqlitenow.core.sqlite.use
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue

class Sqlite27Phase1ApiContractTest {
    @Test
    fun acceptedPhase2DirectCoreContractExecutesWithAndroidxAsyncSurfaces() = runBlocking {
        val persistence = RecordingPersistence(byteArrayOf(1, 2, 3))
        val config = SqliteConnectionConfig(
            persistence = persistence,
            autoFlushPersistence = false,
            executionContextHook = null,
        )
        var openedConfig: SqliteConnectionConfig? = null
        val provider = SqliteConnectionProvider { dbName, debug, requestedConfig ->
            openedConfig = requestedConfig
            BundledSqliteConnectionProvider.openConnection(
                dbName = dbName,
                debug = debug,
                config = requestedConfig.copy(persistence = null),
            )
        }

        val connection: SafeSQLiteConnection = provider.openConnection(
            dbName = ":memory:",
            debug = false,
            config = config,
        )
        assertSame(config, openedConfig)
        try {
            connection.withExclusiveAccess {
                val rawConnection: SQLiteConnection = connection.ref
                rawConnection.asyncExecuteSQL(
                    "CREATE TABLE phase1_contract(id INTEGER PRIMARY KEY, value TEXT NOT NULL)",
                )
                val insert: SQLiteStatement = rawConnection.asyncPrepare(
                    "INSERT INTO phase1_contract(id, value) VALUES (?, ?)",
                )
                insert.use {
                    it.bindLong(1, 7)
                    it.bindText(2, "expected-before")
                    assertFalse(it.asyncStep())
                }
            }

            connection.withExclusiveAccess {
                val statement: SQLiteStatement = connection.prepare(
                    "SELECT id, value FROM phase1_contract",
                )
                statement.use {
                    assertTrue(it.asyncStep())
                    assertEquals(7, it.getLong(0))
                    assertEquals("expected-before", it.getText(1))
                    assertFalse(it.asyncStep())
                }
            }

            connection.transaction {
                connection.execSQL(
                    "UPDATE phase1_contract SET value = 'safe-facade' WHERE id = 7",
                )
            }
            assertTrue(connection.inTransaction().not())
        } finally {
            connection.close()
        }

        assertContentEquals(byteArrayOf(1, 2, 3), persistence.load("legacy.db"))
        persistence.persist("snapshot.db", byteArrayOf(4, 5))
        persistence.clear("snapshot.db")
        assertEquals(listOf("snapshot.db"), persistence.persistedNames)
        assertEquals(listOf("snapshot.db"), persistence.clearedNames)
    }

    @Test
    fun expectedBeforeGeneratedStyleHighLevelContractRemainsSourceCompatible() = runBlocking {
        val database = ContractDatabase()
        assertFalse(database.isOpen())
        database.open()
        try {
            assertTrue(database.isOpen())
            database.transaction {
                database.connection().execSQL(
                    "INSERT INTO phase1_generated_contract(id, title) VALUES (1, 'preserved')",
                )
            }
            assertEquals("preserved", database.selectTitle())
            database.persistSnapshotNow()
            database.reportExternalTableChanges(setOf("phase1_generated_contract"))
        } finally {
            database.close()
        }
        assertFalse(database.isOpen())

        val defaultConfigConnection = BundledSqliteConnectionProvider.openConnection(
            dbName = ":memory:",
            debug = false,
        )
        defaultConfigConnection.close()
    }

    private class ContractDatabase : SqliteNowDatabase(
        dbName = ":memory:",
        migration = ContractMigrations,
    ) {
        suspend fun selectTitle(): String = connection().withExclusiveAccess {
            connection().prepare(
                "SELECT title FROM phase1_generated_contract WHERE id = 1",
            ).use {
                check(it.step())
                it.getText(0)
            }
        }
    }

    private object ContractMigrations : DatabaseMigrations {
        override suspend fun applyMigration(
            conn: SafeSQLiteConnection,
            currentVersion: Int,
        ): Int {
            conn.execSQL(
                "CREATE TABLE IF NOT EXISTS phase1_generated_contract(" +
                    "id INTEGER PRIMARY KEY, title TEXT NOT NULL)",
            )
            return 1
        }
    }

    private class RecordingPersistence(
        private val loadedBytes: ByteArray?,
    ) : SqlitePersistence {
        val persistedNames = mutableListOf<String>()
        val clearedNames = mutableListOf<String>()

        override suspend fun load(dbName: String): ByteArray? = loadedBytes

        override suspend fun persist(dbName: String, bytes: ByteArray) {
            persistedNames += dbName
        }

        override suspend fun clear(dbName: String) {
            clearedNames += dbName
        }
    }
}
