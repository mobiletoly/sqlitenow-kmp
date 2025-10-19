package dev.goquick.sqlitenow.core

import dev.goquick.sqlitenow.core.persistence.IndexedDbSqlitePersistence
import kotlin.js.Promise
import kotlinx.coroutines.await

private const val DEFAULT_STORAGE_NAME = "SqliteNow"
private const val DEFAULT_STORE_NAME = "sqlite-databases"

private var opfsOverride: Boolean? = null

internal actual fun chooseDefaultWebPersistence(dbName: String): SqlitePersistence {
    opfsOverride?.let { forced ->
        return if (forced) {
            println("[SqliteNow][OPFS] Using Origin Private File System persistence for $dbName (forced)")
            OpfsSqlitePersistence(DEFAULT_STORAGE_NAME)
        } else {
            println("[SqliteNow][IndexedDB] Using IndexedDB persistence for $dbName (forced)")
            IndexedDbSqlitePersistence(storageName = DEFAULT_STORAGE_NAME, storeName = DEFAULT_STORE_NAME)
        }
    }

    return if (OpfsSqlitePersistence.isSupported()) {
        println("[SqliteNow][OPFS] Using Origin Private File System persistence for $dbName")
        OpfsSqlitePersistence(DEFAULT_STORAGE_NAME)
    } else {
        println("[SqliteNow][IndexedDB] Using IndexedDB persistence for $dbName")
        IndexedDbSqlitePersistence(storageName = DEFAULT_STORAGE_NAME, storeName = DEFAULT_STORE_NAME)
    }
}

internal actual fun forceWebPersistenceOverride(override: Boolean?) {
    opfsOverride = override
}

internal class OpfsSqlitePersistence(
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
        fun isSupported(): Boolean {
            val secure = try {
                js("typeof self !== 'undefined' && (self.isSecureContext || (typeof location !== 'undefined' && location.hostname === 'localhost'))")
            } catch (_: dynamic) {
                false
            }
            if (secure != true) return false

            val hasNavigator = js("typeof navigator !== 'undefined'") == true
            if (!hasNavigator) return false

            val storageAvailable = try {
                js("navigator.storage && typeof navigator.storage.getDirectory === 'function'")
            } catch (_: dynamic) {
                false
            }
            if (storageAvailable != true) return false

            val writableAvailable = try {
                js("typeof FileSystemFileHandle !== 'undefined' && 'createWritable' in FileSystemFileHandle.prototype")
            } catch (_: dynamic) {
                false
            }
            return writableAvailable == true
        }

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
