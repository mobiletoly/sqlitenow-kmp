@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.JsFun
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlinx.coroutines.await
import kotlinx.coroutines.test.runTest

class SqliteWasmPersistenceTest {

    @Test
    fun exportAndReloadSnapshotViaIndexedDb() = runTest {
        val dbName = "wasmPersistTest_${Random.nextInt()}.db"
        val storageName = "SqliteNowTest"
        val storeName = "sqlite-tests"
        val persistence = IndexedDbSqlitePersistence(storageName = storageName, storeName = storeName)

        val config = SqliteConnectionConfig(
            persistence = persistence,
            autoFlushPersistence = false,
        )

        val connection = BundledSqliteConnectionProvider.openConnection(
            dbName = dbName,
            debug = true,
            config = config,
        )

        try {
            connection.execSQL("CREATE TABLE IF NOT EXISTS notes(id INTEGER PRIMARY KEY, body TEXT)")
            connection.execSQL("INSERT INTO notes(id, body) VALUES (1, 'wasm-ok')")
            connection.persistSnapshotNow()
        } finally {
            connection.close()
        }

        val reloaded = BundledSqliteConnectionProvider.openConnection(
            dbName = dbName,
            debug = true,
            config = config.copy(autoFlushPersistence = true),
        )

        try {
            val values = collectBodies(reloaded)
            assertEquals(listOf("wasm-ok"), values)
        } finally {
            reloaded.close()
            persistence.clear(dbName)
        }
    }

    @Test
    fun choosesOpfsPersistenceWhenSupported() = runTest {
        try {
            fakeOpfsSupport()
            forceWebPersistenceOverride(true)

            val persistence = chooseDefaultWebPersistence("opfsTest.db")
            assertTrue(persistence is OpfsSqlitePersistence)
        } finally {
            forceWebPersistenceOverride(null)
        }
    }

    @Test
    fun failingClosePersistenceIsReportedOnceAndCloseIsNotRetried() = runTest {
        val failure = IllegalStateException("WASM_CLOSE_PERSISTENCE_SENTINEL")
        val persistence = FailingPersistence(failure)
        val connection = BundledSqliteConnectionProvider.openConnection(
            dbName = "wasmCloseFailure_${Random.nextInt()}.db",
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

    private suspend fun collectBodies(connection: SafeSQLiteConnection): List<String> {
        val values = mutableListOf<String>()
        val stmt = connection.prepare("SELECT body FROM notes ORDER BY id")
        try {
            while (stmt.step()) {
                values += stmt.getText(0)
            }
        } finally {
            stmt.close()
        }
        return values
    }

    private fun fakeOpfsSupport() {
        installOpfsTestStub()
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

@JsFun(
    """
    () => {
      const storage = navigator.storage || (navigator.storage = {});
      if (!storage.getDirectory) {
        storage.getDirectory = () => Promise.resolve({
          getDirectoryHandle: () => Promise.resolve({
            getFileHandle: () => Promise.resolve({
              createWritable: () => Promise.resolve({
                write: () => Promise.resolve(),
                close: () => Promise.resolve(),
                abort: () => Promise.resolve()
              })
            })
          })
        });
      }
      if (typeof FileSystemFileHandle === 'undefined') {
        globalThis.FileSystemFileHandle = function() {};
        FileSystemFileHandle.prototype.createSyncAccessHandle = function() { return Promise.resolve({}); };
      }
    }
    """
)
private external fun installOpfsTestStub()
