package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.js.Promise
import kotlinx.coroutines.await

private const val DEFAULT_STORAGE_NAME = "SqliteNow"
private const val DEFAULT_STORE_NAME = "sqlite-databases"

internal actual fun legacySnapshotPersistenceForTest(
    dbName: String,
    forceOpfs: Boolean,
): SqlitePersistence {
    return if (forceOpfs) {
        LegacyOpfsSnapshotPersistence(DEFAULT_STORAGE_NAME)
    } else {
        IndexedDbSqlitePersistence(storageName = DEFAULT_STORAGE_NAME, storeName = DEFAULT_STORE_NAME)
    }
}

internal class LegacyOpfsSnapshotPersistence(
    private val storageName: String,
) : SqlitePersistence {

    private val directoryPromise: Promise<dynamic> = ensureDirectory(storageName)

    override suspend fun load(dbName: String): ByteArray? {
        val directory = directoryPromise.await()
        val handle = runCatching {
            directory.getFileHandle(fileName(dbName)).unsafeCast<Promise<dynamic>>().await()
        }.getOrNull() ?: return null
        val file = handle.getFile().unsafeCast<Promise<dynamic>>().await()
        val buffer = file.arrayBuffer().unsafeCast<Promise<dynamic>>().await()
        return arrayBufferToByteArray(buffer)
    }

    override suspend fun persist(dbName: String, bytes: ByteArray) {
        val directory = directoryPromise.await()
        val handle = directory.getFileHandle(
            fileName(dbName),
            js("{ create: true }")
        ).unsafeCast<Promise<dynamic>>().await()

        val writable = handle.createWritable(js("{ keepExistingData: false }"))
            .unsafeCast<Promise<dynamic>>().await()
        val payload = bytes.toUint8Array()
        try {
            writable.write(payload).unsafeCast<Promise<dynamic>>().await()
            writable.close().unsafeCast<Promise<dynamic>>().await()
        } catch (t: dynamic) {
            runCatching { writable.abort()?.unsafeCast<Promise<dynamic>>()?.await() }
            throw t
        }
    }

    override suspend fun clear(dbName: String) {
        val directory = directoryPromise.await()
        runCatching {
            directory.removeEntry(fileName(dbName)).unsafeCast<Promise<dynamic>>().await()
        }
    }

    private fun fileName(dbName: String): String = "$dbName.sqlite3"

    companion object {
        private fun ensureDirectory(name: String): Promise<dynamic> {
            val storage = js("navigator.storage")
            val root = storage.getDirectory()
            runCatching {
                val result = storage.persist?.call(storage)
                if (result != null) {
                    (result as? Promise<*>)?.catch { }
                }
            }
            return root.then { dir ->
                dir.getDirectoryHandle(name, js("{ create: true }"))
                    .unsafeCast<Promise<dynamic>>()
            }
        }

        private fun arrayBufferToByteArray(buffer: dynamic): ByteArray {
            val values = js("Array.from(new Uint8Array(buffer))").unsafeCast<Array<Int>>()
            val result = ByteArray(values.size)
            for (i in result.indices) {
                result[i] = values[i].toByte()
            }
            return result
        }

        private fun ByteArray.toUint8Array(): dynamic {
            val values = Array(size) { index -> (this[index].toInt() and 0xFF).toDouble() }
            return js("new Uint8Array(values)")
        }
    }
}
