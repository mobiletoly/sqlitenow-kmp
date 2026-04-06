@file:OptIn(kotlin.js.ExperimentalWasmJsInterop::class)

package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.JsFun
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertEquals
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
